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

```java

```

### 按标题切分——headingChunks

```java

```

### 
