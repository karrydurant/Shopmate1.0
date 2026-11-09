# RAG 知识库与检索

## 一条知识 chunk 的数据模型——KnowledgeEntry

| 组 | 字段 | 作用 |
|---|---|---|
| **身份定位** | `docId` | chunk 全局唯一 ID（`source::父::子`），融合去重键、向量索引行键 |
| | `source` | 文档相对路径，如 `after_sale/return_policy.md` |
| **正文内容** | `title` | 展示用标题（`文件名 - 小节标题`） |
| | `section` | 最具体的子标题（`###` 那层），重排器算标题命中用 |
| | `content` | chunk 正文，真正注入 prompt 的内容 |
| **层级溯源**（small-to-big 核心） | `parentId` | 父块 ID（`source::父节`） |
| | `parentSection` | 父节标题（`##` 那层） |
| | `parentText` | **整个父节全文**——子块被命中后向上展开成它喂给 LLM |
| **分类与缓存** | `domain` | 知识域（`after_sale`/`sizing`/`care`…），来自 docs/ 一级目录名，用于后过滤 |
| | `contentHash` | `SHA-1(section+content)` 前 8 字节，`LocalVectorIndex` 做内容寻址复用——内容没变重启就不重调 embedding |

## Markdown 切块器——ChunkerService

### Markdown 结构解析——parse

把整篇 Markdown 解析成一组带父子层级的小节

```java
List<Section> parse(String text) {
    List<Section> sections = new ArrayList<>();
    // 当前状态：父节名（##）、本小节名（## 或 ###）、本小节正文行
    String curParent = "";
    String curTitle = "";
    List<String> curLines = new ArrayList<>();

    // 按行扫描
    for (String raw : text.split("\r?\n")) {
        String line = stripTrailing(raw);

        // # 一级标题：整篇文档的大标题，不参与切块，跳过
        if (line.startsWith("# ") && !line.startsWith("## ")) {
            continue;
        }

        // ### 三级标题：子小节。先把正在累积的上一小节收尾
        else if (line.startsWith("### ")) {
            flushInto(sections, curParent, curTitle, curLines);
            curTitle = "";
            curLines = new ArrayList<>();
            String sub = line.substring(4).trim();
            // 若前面还没出现过 ##，就让这个 ### 自己当父节，避免父节为空
            if (curParent.isEmpty()) curParent = sub;
            curTitle = sub;
        }

        // ## 二级标题：新的父节。先收尾上一节，再把父节名和小节名都设成它
        else if (line.startsWith("## ")) {
            flushInto(sections, curParent, curTitle, curLines);
            curTitle = "";
            curLines = new ArrayList<>();
            curParent = line.substring(3).trim();
            curTitle = curParent;
        }
        
        // 普通非空行：攒进当前小节的正文
        else if (!line.isBlank()) {
            curLines.add(line.trim());
        }
    }

    // 文件结尾还有最后一节没收尾，补一次
    flushInto(sections, curParent, curTitle, curLines);
    return sections;
}
```

### 策略一：按标题切分——headingChunks

```java
List<Chunk> headingChunks(List<Section> sections, String source, String stem, Map<String, String> parents) {
    List<Chunk> chunks = new ArrayList<>();
    for (Section sec : sections) {
        String content = join(sec.lines);
        // 内容不够直接跳过
        if (content.length() < 10) continue;
        String[] hf = hierarchyFields(sec, source, parents);
        Chunk ch = new Chunk(
                docId(source, sec),
                stem + " - " + sec.title,
                sec.title,
                content,
                source);
        ch.parentId = hf[0];
        ch.parentSection = hf[1];
        ch.parentText = hf[2];
        chunks.add(ch);
    }
    return chunks;
}
```

### 策略二：结构感知+尺寸受限+重叠——sentenceChunks

结构感知：

切分的最小单位是整行，针对表结构进行了特殊处理：

1.识别表结构：把连续以 `|` 开头的行标成一个表格区间

2.切分时继承表头：一张大表被窗口切成两段后，第二段单独拿出来看就是一堆没列名的数据行。所以切到表格中间时，自动把这张表的表头行补到这个窗口开头

尺寸受限：从当前位置开始逐行往里装，字符总数一超过 `chunkSize` 就停止，如果一行的字数就超过了 chunkSize 那这一行就是一个 Chunk

重叠：相邻窗口共用几行，边界信息不丢

新窗口的起点不是上一窗口的结尾，而是往回退 `overlapUnits` 行

```java
List<Chunk> sentenceChunks(List<Section> sections, String source, String stem,
                           Map<String, String> parents) {
    List<Chunk> chunks=new ArrayList<>();
    int idx=0;  // 用于构造 chunk 编号
    for (Section sec : sections) {
        List<String> units = sec.lines;  // 本节的正文行（切分的最小单位是整行）
        List<int[]> regions = tableRegions(units); // 标出所有表格区间
        List<List<String>> regionHeaders = new ArrayList<>(); // 每个表格区间的表头行列表
        int i = 0, n = units.size();

        // 第一遍扫描：识别每张表格并抽出它的表头行
        while (i<n) {
            if (!units.get(i).startsWith("|")) { i++; continue; }
            int j = i;
            // j 指向表格区末尾
            while (j < n && units.get(j).startsWith("|")) j++;
            if (j - i >= 2) {
                // 第二行是 |---|---| 分隔行 → 表头占 2 行，否则占 1 行
                int nHead = SEPARATOR_ROW.matcher(units.get(i + 1)).find() ? 2 : 1;
                List<String> h = new ArrayList<>();
                for (int k = i; k < i + nHead; k++) h.add(units.get(k));  // 抽出这张表的表头
                regionHeaders.add(h);
            }
            i = j;
        }

        // 找到这个 chunk 的父节名、父块 ID、父节全文
        // 父块指的是 `##` 那一级，用于 small-to-big，如果 `###` 上面没有 `##` 就直接用子块的 ID
        String[] hf = hierarchyFields(sec, source, parents);

        // 滑动窗口切 chunk
        int pos=0;
        while (pos < units.size()) {
            List<String> buf = new ArrayList<>();
            int end = pos;
            int total = 0;

            // 如果 pos 落在某张表格区内部，把该表的表头行补到这个窗口前面
            List<String> head = inheritedHeader(units, pos, regions, regionHeaders);

            // 尺寸受限：逐行往里装，直到累计字符数达到 chunkSize 或没有更多行
            while (end < units.size() && total < chunkSize) {
                buf.add(units.get(end));
                total += units.get(end).length();
                end++;
            }

            List<String> combined = new ArrayList<>(head);
            combined.addAll(buf);
            String content = join(combined);

            // 太短的片段丢弃
            if (content.length() < 10) {
                pos = end;
                continue;
            }

            // 构造 chunk：docId 在节基础上加 ::c序号，保证同节多个子块 ID 不撞
            Chunk ch=new Chunk(
                    docId(source, sec) + "::c" + idx,  // 机器主键，检索/融合/去重用
                    stem + " - " + sec.title,  // 展示标题，给 LLM 看
                    sec.title,   // 纯小节标题文字，重排算标题命中用
                    content,
                    source);

            // 挂层级元数据（子块检索、父块注入用）
            ch.parentId = hf[0];
            ch.parentSection = hf[1];
            ch.parentText = hf[2];
            chunks.add(ch);
            idx++;

            // 重叠：下一窗口起点往回退 overlapUnits 行；Math.max(1,...) 防止 pos 不动死循环
            if (end < units.size()) {
                pos += Math.max(1, (end - pos) - overlapUnits);
            } else {
                pos = end;                             // 已是最后一个窗口，直接结束
            }
        }
    }
    return chunks;
}
```

## 多路召回融合——Fusion

两种融合方式：RRF、weighted

已经有各路按顺序排列的召回 chunk 列表：

稀疏臂：[(docC, 12.5), (docA, 8.1), (docB, 3.0)]  

稠密臂：[(docC, 0.82), (docD, 0.71), (docA, 0.65)]

weighted 融合：先对每一路的分数归一化，再算每个 chunk 的分数：具体就是对每个 chunk 逐路检查是否召回了它，如果是的话就加上这一路该 chunk 的分数*权重，返回排序好的列表

```java
// RRF 融合
// rankLists 是各路按分数排好的 docID 列表
// k 是 RRF 的平滑常数
// weights 是每一路的权重
public static List<Map.Entry<String, Double>> rrfFuse(List<List<String>> rankLists, int k, double[] weights) {
    // 没传权重就每路等权 1.0
    if (weights == null) {
        weights = new double[rankLists.size()];
        Arrays.fill(weights, 1.0);
    }

    // 新建两个并行列表：pairs 装非空臂，wList 装对应权重
    List<List<String>> pairs = new ArrayList<>();
    List<Double> wList = new ArrayList<>();

    // 新建两个并行列表：pairs 装非空臂，wList 装对应权重
    List<List<String>> pairs = new ArrayList<>();
    List<Double> wList = new ArrayList<>();
    // 按原下标遍历，边丢空臂边取权重——不能先把空臂删了再配权重，否则中间有空臂时权重会对错路
    for (int i = 0; i < rankLists.size(); i++) {
        List<String> lst = rankLists.get(i);
        // 跳过 null 和空列表
        if (lst != null && !lst.isEmpty()) {
            pairs.add(lst);
            // 权重数组越界时兜底取 1.0
            wList.add(i < weights.length ? weights[i] : 1.0);
        }
    }
    // 全空就返回空
    if (pairs.isEmpty()) return List.of();

    // 累加器：key → RRF 总分
    Map<String, Double> acc = new HashMap<>();
    // 逐路遍历
    for (int i = 0; i < pairs.size(); i++) {
        double w = wList.get(i);
        // 权重为 0 的路跳过
        if (w == 0) continue;
        List<String> lst = pairs.get(i);
        for (int pos = 0; pos < lst.size(); pos++) {
            String key = lst.get(pos);
            // RRF 公式：key 在这路排第 pos+1 名，贡献 w/(k + 名次)
            acc.merge(key, w / (k + pos + 1), Double::sum);
        }
    }
    // 按总分降序返回
    return sortDesc(acc);
}
```

```java
// 加权归一融合：各路先 min-max 归一，再按权重相加。
public static List<Map.Entry<String, Double>> weightedFuse(List<List<Map.Entry<String, Double>>> scoredLists, double[] weights) {
    // 没传权重默认等权
    if (weights == null) {
        weights = new double[scoredLists.size()];
        Arrays.fill(weights, 1.0);
    }

    // 边丢空臂边按原下标配权重
    List<List<Map.Entry<String, Double>>> lists = new ArrayList<>();
    List<Double> wList = new ArrayList<>();
    for (int i = 0; i < scoredLists.size(); i++) {
        List<Map.Entry<String, Double>> lst = scoredLists.get(i);
        if (lst != null && !lst.isEmpty()) {
            lists.add(lst);
            wList.add(i < weights.length ? weights[i] : 1.0);
        }
    }
    // 全空返回空
    if (lists.isEmpty()) return List.of();

    // 累加器：key → 加权融合分
    Map<String, Double> acc = new HashMap<>();
    // 逐路处理
    for (int i = 0; i < lists.size(); i++) {
        double w = wList.get(i);
        if (w == 0) continue;
        List<Map.Entry<String, Double>> scored = lists.get(i);
        // 先把这一路的原始分收集出来
        List<Double> vals = new ArrayList<>();
        for (Map.Entry<String, Double> e : scored) vals.add(e.getValue());
        // 归一到 [0,1]
        double[] norm = minmax(vals);
        // 逐篇累加：同一篇被多路命中时分数在这里叠加
        for (int j = 0; j < scored.size(); j++) {
            acc.merge(scored.get(j).getKey(), w * norm[j], Double::sum);
        }
    }
    // 按融合分降序
    return sortDesc(acc);
}
```

## RAG 的核心编排——KnowledgeDocumentStore

### 双路召回——retrieveRanked

```java
// 双臂并行召回 + 融合，返回 [(entry, score)] 已按融合分降序。
// 稀疏臂与稠密臂同时跑，任一路异常只损失那一路，另一路照常出结果。
private List<ContextBuilder.Hit> retrieveRanked(String query, int topN) {

    // ── 稀疏臂：BM25 倒排召回 ──
    java.util.concurrent.CompletableFuture<List<Map.Entry<String, Double>>> sparseFuture =
            java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    List<Map.Entry<String, Double>> sparse = bm25.search(query, topN);
                    return sparse == null ? List.<Map.Entry<String, Double>>of() : sparse;
                } catch (Exception e) {
                    log.warn("[KnowledgeStore] sparse arm failed: {}", e.getMessage());
                    return List.<Map.Entry<String, Double>>of();
                }
            });

    // ── 稠密臂：embedding + 向量索引召回 ──
    java.util.concurrent.CompletableFuture<List<Map.Entry<String, Double>>> denseFuture =
            java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                if (vectorIndex == null || vectorIndex.size() == 0) return List.<Map.Entry<String, Double>>of();
                try {
                    // 查询文本转向量
                    List<Float> qv = embedding.embed(query);
                    // 向量索引按行号返回 topN 相似行：(行号, 相似度)
                    float[] q = toFloatArray(qv);
                    // 向量索引按行号返回 topN 相似行：(行号, 相似度)
                    List<Map.Entry<Integer, Float>> denseRows = vectorIndex.search(q, topN);
                    if (denseRows == null || denseRows.isEmpty()) return List.<Map.Entry<String, Double>>of();
                    List<Map.Entry<String, Double>> dense = new ArrayList<>();
                    for (Map.Entry<Integer, Float> r : denseRows) {
                        // 行号 → 整条 entry(行所在的 chunk)
                        KnowledgeEntry e = entries.get(r.getKey());
                        if (e != null) {
                            // 当前稠密臂手里是 `(entry, score)`，稀疏臂（BM25）手里是 `(docId, score)`
                            // 融合要做的事是 "同一个 chunk 被两路都命中就加分"，所以两路用同一个字符串当名字
                            dense.add(new java.util.AbstractMap.SimpleEntry<>(e.docId, (double) r.getValue()));
                        }
                    }
                    return dense;
                } catch (Exception e) {
                    log.warn("[KnowledgeStore] dense arm failed: {}", e.getMessage());
                    return List.<Map.Entry<String, Double>>of();
                }
            });

    // 等两路都完成（任一异常已在各自 future 内吞成空列表）
    List<Map.Entry<String, Double>> sparse = sparseFuture.join();
    List<Map.Entry<String, Double>> dense = denseFuture.join();

    // 收集有效臂（空臂不进 fusion）
    List<List<Map.Entry<String, Double>>> arms = new ArrayList<>();
    Set<Integer> densePositions = new HashSet<>();
    if (!sparse.isEmpty()) {
        arms.add(sparse);
    }
    if (!dense.isEmpty()) {
        densePositions.add(arms.size()); // 记录稠密臂在 arms 里的位置
        arms.add(dense);
    }

    if (arms.isEmpty()) return List.of();

    // 双臂融合
    List<Map.Entry<String, Double>> fused = Fusion.fuse(arms, fusionMethod, rrfK, fusionAlpha, densePositions);

    // 把融合结果的 docId 还原成完整 entry，封装成 Hit 返回
    List<ContextBuilder.Hit> out = new ArrayList<>();
    for (Map.Entry<String, Double> f : fused) {
        KnowledgeEntry e = byKey.get(f.getKey());
        if (e != null) out.add(new ContextBuilder.Hit(e, f.getValue()));
    }
    return out;
}
```

### 检索——search

```java
// 返回 topK 条命中。
public List<KnowledgeHit> search(String query, int topK) {
    if (entries.isEmpty() || query == null || query.isBlank()) return List.of();

    int candidateN = Math.max(topK * 4, 20);
    List<ContextBuilder.Hit> ranked = retrieveRanked(query, candidateN);
    if (ranked.isEmpty()) return List.of();

    // 精排（对齐 Python _safe_rerank：重排挂了回退到精排前顺序）
    List<OfflineReranker.Hit> before = ranked.stream()
            .map(h -> new OfflineReranker.Hit(h.entry(), h.score())).toList();
    List<OfflineReranker.Hit> reranked = safeRerank(query, before);

    int limit = Math.min(topK, reranked.size());
    List<KnowledgeHit> out = new ArrayList<>();
    for (int i = 0; i < limit; i++) {
        OfflineReranker.Hit h = reranked.get(i);
        KnowledgeEntry e = h.entry();
        out.add(new KnowledgeHit(e.docId, e.section, e.title, e.source, e.content, h.score()));
    }
    return out;
}
```

先根据 weighted 融合的分数粗排，选取 max(topK * 4, 20) 个候选，再在候选中进行精排

精排规则：

```java
fused = 0.5 * recall      // 粗排融合分（组内归一）
      + 0.35 * sem        // query 和「标题+正文」的字符 bigram 余弦
      + 0.15 * title;     // query 和「小节标题」的 bigram 余弦
```

## 问题

### 切分时的 chunkSize 是怎么取的？是怎么衡量的？

### 为什么要设计 small-to-big？怎么实现的 small-to-big？

### 为什么要双路召回？两路结果怎么融合？

### BM25 是什么？解决什么问题？

### 公式里 k1、b 各起什么作用？

### IDF 为什么用 `ln(1+(N-df+0.5)/(df+0.5))` 而不是简单的 N/df？

### 为什么不直接用 ES？你是怎么实现 BM25 的？
