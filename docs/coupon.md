# 优惠券

## CouponService

### 领券——acquireCoupon

Redis 预扣 + DB 乐观锁扣减 + 发放；事务回滚时回调补偿 Redis

```java
// 领取优惠券：幂等防重 + Redis 预扣 + DB 事务 + 回滚补偿。
// 一人限领一张；Redis 预扣是前置防线，DB 乐观锁与唯一索引是最终裁决。
@Transactional(rollbackFor = Exception.class)
public boolean acquireCoupon(String userId, String couponCode) {
    // 业务幂等：先查用户券表，已领过直接拒绝（一人一券）
    Long already = userCouponMapper.selectCount(
            new LambdaQueryWrapper<UserCoupon>()
                    .eq(UserCoupon::getUserId, userId)
                    .eq(UserCoupon::getCouponCode, couponCode));
    if (already != null && already > 0) {
        throw BizException.conflict("您已领取过该优惠券");
    }

    // 并发去重：短窗口 Redis 幂等键，挡住同用户同券的并发重复请求，10 秒过期
    String idemKey = "coupon:acquire:" + userId + ":" + couponCode;
    Boolean firstRequest = redisTemplate.opsForValue()
            .setIfAbsent(idemKey, "1", 10, TimeUnit.SECONDS);
    if (Boolean.FALSE.equals(firstRequest)) {
        throw BizException.conflict("操作过于频繁，请稍后再试");
    }

    // 读券：DB 是库存权威来源。校验券是否存在、余量是否为正、是否在有效期内
    Coupon coupon = couponMapper.selectOne(
            new LambdaQueryWrapper<Coupon>().eq(Coupon::getCouponCode, couponCode));
    if (coupon == null || coupon.getRemainingCount() <= 0) {
        throw BizException.notFound("优惠券不存在或已过期");
    }
    if (coupon.getEndTime() != null && coupon.getEndTime().isBefore(LocalDateTime.now())) {
        throw BizException.badParam("优惠券已过期");
    }

    // Redis 库存键不存在时，按 DB 余量用 SETNX 建它；必须在预扣之前
    seedStockFromDbIfAbsent(couponCode, coupon.getRemainingCount());

    // Redis 预扣库存：计数减 1
    String stockKey = STOCK_KEY_PREFIX + couponCode;
    Long remain = redisTemplate.opsForValue().decrement(stockKey);
    // 减成负数说明已抢光：先把刚减的加回去，再抛异常
    if (remain == null || remain < 0) {
        redisTemplate.opsForValue().increment(stockKey);
        throw BizException.conflict("优惠券已抢光");
    }

    // 注册事务回调：事务一旦回滚，把 Redis 预扣的库存加回去，并释放幂等键
    // Redis 不参与 JDBC 事务，所以必须靠这个回调与 DB 对齐
    TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
        @Override
        public void afterCompletion(int status) {
            if (status == STATUS_ROLLED_BACK) {
                redisTemplate.opsForValue().increment(stockKey);
                redisTemplate.delete(idemKey);
                log.warn("[Coupon] rollback — Redis stock restored | code={}", couponCode);
            }
        }
    });

    // DB 乐观锁扣减：带上读到的 version，版本对不上则 0 行；不重复查库
    int affected = couponMapper.decrementRemainingCount(couponCode, coupon.getVersion());
    if (affected == 0) {
        throw BizException.conflict("优惠券库存不足，请稍后再试");
    }

    // 发放到用户账户：走 BaseMapper.insert，触发雪花主键填充
    userCouponMapper.issueToUser(userId, couponCode);

    log.info("[Coupon] acquired | userId={} | code={}", userId, couponCode);
    return true;
}
```

领券的流程：

1.业务幂等查库：

按 `userId + couponCode` 查 `user_coupon`，已领过直接抛 "您已领取过该优惠券"。这是长期约束，保证同一张券一人一份。

2.短窗口并发去重：

写 Redis 幂等键 `coupon:acquire:{userId}:{couponCode}`，`SETNX` + 10 秒 TTL；写不进去说明同用户同券有并发重复请求，快速拒绝 "操作过于频繁"。

3.读券模板并校验：

查 `coupon` 表。券不存在或 `remaining_count <= 0` → 拒绝；已过 `end_time` → 拒绝。

4.回源播种：

Redis 库存键不存在时，用 `SETNX` 按 DB 余量把它建起来；已存在则不覆盖。必须在预扣之前做，避免冷启动把券误判成抢光。

5.Redis 预扣：

`decrement` 库存计数减 1；结果为负说明已抢光，先 `increment` 把刚减的加回去，再抛 "优惠券已抢光"。

6.注册回滚补偿回调：

登记事务同步：事务一旦回滚，把 Redis 预扣的库存加回去、删掉幂等键。

7.DB 乐观锁扣减：

带读到的 `version` 执行条件 UPDATE，影响 0 行（版本冲突或余量不足）→ 拒绝 "库存不足"。

8.发放到用户账户

`issueToUser` 往 `user_coupon` 插入一条未使用券记录，打成功日志，返回 `true`

### 下单用券核销——applyToOrder

```java
/**
 * 下单用券：校验 + 核销，返回本笔订单的抵扣额。
 * 校验全部 fail-closed：任一不满足即抛异常，不悄悄按原价放行。
 */
@Transactional(rollbackFor = Exception.class)
public BigDecimal applyToOrder(String userId, String couponCode,
                               BigDecimal orderTotal, String orderNo) {

    // 没传券：直接返回 0，不查库、不碰 Redis，绝大多数无券订单走这条零开销路径
    if (couponCode == null || couponCode.isBlank()) {
        return BigDecimal.ZERO;
    }

    // 查券模板，不存在抛 404
    Coupon coupon = couponMapper.selectOne(
            new LambdaQueryWrapper<Coupon>().eq(Coupon::getCouponCode, couponCode));
    if (coupon == null) {
        throw BizException.notFound("优惠券不存在: " + couponCode);
    }

    // 类型校验：折扣券（type=1）的 discount_amount 是折扣率不是金额，显式拒绝，不按立减算
    Integer type = coupon.getType();
    if (type != null && type == TYPE_PERCENT) {
        throw BizException.badParam("折扣券暂不支持: " + couponCode);
    }
    // 既不是满减也不是立减的未知类型，拒绝
    if (type != null && type != TYPE_FULL_CUT && type != TYPE_DIRECT_CUT) {
        throw BizException.badParam("不支持的优惠券类型: " + type);
    }

    // 门槛校验：订单总额未达满减门槛则拒绝
    BigDecimal threshold = coupon.getConditionAmount();
    if (threshold != null && orderTotal.compareTo(threshold) < 0) {
        throw BizException.badParam("订单金额未达到优惠券门槛: 需满 " + threshold
                + "，实际 " + orderTotal);
    }

    // 有效期校验：已过期拒绝
    if (coupon.getEndTime() != null && coupon.getEndTime().isBefore(LocalDateTime.now())) {
        throw BizException.badParam("优惠券已过期: " + couponCode);
    }

    // 核销：条件 UPDATE（status 0→1，绑定订单号），并发下唯一裁决点，判据是受影响行数
    // 排在所有校验之后：注定失败的调用不该先把券锁成已使用
    int marked = userCouponMapper.markUsed(userId, couponCode, orderNo, LocalDateTime.now());
    if (marked == 0) {
        // 未持有 / 已使用 / 已退回，三种情况统一报不可用
        throw BizException.conflict("优惠券不可用（未持有 / 已使用 / 已退回）: " + couponCode);
    }

    // 面额为空按 0 处理
    BigDecimal faceValue = coupon.getDiscountAmount() == null
            ? BigDecimal.ZERO : coupon.getDiscountAmount();
    // 实付 = 总额 - 面额，上钳到不低于 0.01，避免抵扣成负价或零价
    BigDecimal payable = orderTotal.subtract(faceValue).max(MIN_PAY_AMOUNT);
    // 实际抵扣额 = 总额 - 实付，保留两位、四舍五入
    BigDecimal applied = orderTotal.subtract(payable).setScale(2, RoundingMode.HALF_UP);

    log.info("[Coupon] applied | userId={} | code={} | orderNo={} | total={} | discount={}",
            userId, couponCode, orderNo, orderTotal, applied);
    return applied;
}
```

### 退券——returnForOrder

```java
// 退券 —— 订单取消 / 超时 / 退款时，把它用过的那张券退回用户账户
@Transactional(rollbackFor = Exception.class)
public void returnForOrder(String couponCode, String orderNo) {
    // 券码为空说明这单没用券：直接返回，不查库、不改任何状态
    if (couponCode == null || couponCode.isBlank()) {
        return;
    }

    // 按券码查券模板，用来判断这张券现在是否已过有效期
    Coupon coupon = couponMapper.selectOne(
            new LambdaQueryWrapper<Coupon>().eq(Coupon::getCouponCode, couponCode));

    // 过期判定：券存在、有结束时间、且结束时间早于当前时刻 → 视为已过期
    boolean expired = coupon != null && coupon.getEndTime() != null
            && coupon.getEndTime().isBefore(LocalDateTime.now());

    // 退券目标状态：未过期退回 0（未使用）；已过期退回 2（已过期）
    int toStatus = expired ? STATUS_EXPIRED : STATUS_UNUSED;

    // 条件 UPDATE：只退"已使用(status=1)且绑定本订单号"的那张券；成功影响 1 行，否则 0 行
    int returned = userCouponMapper.markReturned(couponCode, orderNo, toStatus);

    if (returned == 0) {
        // 0 行 = 这单没用过券，或券已经退过一次：幂等空操作，只留痕、不抛异常
        // 与取消订单释放库存拿到 0 行同类，重复调用不会退两次
        log.info("[Coupon] nothing to return | code={} | orderNo={}", couponCode, orderNo);
    } else {
        // 正常退回：记录券码、订单号、退成了哪个状态
        log.info("[Coupon] returned | code={} | orderNo={} | toStatus={}",
                couponCode, orderNo, toStatus);
    }
}
```

退券流程：

1.空券码直接返回：`couponCode` 为空说明这单没用券，不查库、不改状态，直接结束。

2.查券模板，判断是否已过期

3.未过期 → 退成 `0`（未使用）；已过期 → 退成 `2`（已过期）

4.带条件(status=1) UPDATE 退回

5.按影响行数分流：

0 行代表：这单没用过券，或券已经退过一次 → 幂等空操作，只留痕、不抛异常

1 行代表：退回成功，记录券码、订单号、目标状态

## 对账——CouponStockReconcileScheduler

```java
public class CouponStockReconcileScheduler {

    // Redis 库存键前缀，完整键 = coupon:stock:{券码}
    private static final String STOCK_KEY_PREFIX = "coupon:stock:";

    // 单轮最多扫描的券数量，防止一次把整表捞出来压垮数据库
    private static final int BATCH_SIZE = 500;

    // Lua CAS 脚本：只有当前 Redis 值仍等于本轮观测值(ARGV[1])，才写入新值(ARGV[2])，返回 1；否则返回 0
    private static final DefaultRedisScript<Long> CAS_SET = new DefaultRedisScript<>(
            "local current = redis.call('GET', KEYS[1]) " +
                    "if current == ARGV[1] then " +
                    "  redis.call('SET', KEYS[1], ARGV[2]) " +
                    "  return 1 " +
                    "end " +
                    "return 0",
            Long.class);

    private final CouponMapper couponMapper;

    private final StringRedisTemplate redisTemplate;

    // 任务租约，多实例下抢不到租约的实例本轮跳过
    private final ScheduledJobLease jobLease;

    // 记录上一轮观测到的偏差：key=券码，value="redis值->db值"；用来判断是否连续两轮同一偏差
    private final Map<String, String> pendingDrift = new ConcurrentHashMap<>();

    // 分页游标
    private long cursor = 0;

    // 定时调度：默认 10 分钟一轮
    @Scheduled(fixedDelayString = "${shopmate.coupon.stock-reconcile-ms:600000}")
    public void reconcile() {
        // 先抢租约，拿到租约的实例才执行 reconcileLeased
        jobLease.tryRun("coupon-stock-reconcile", 600_000, this::reconcileLeased);
    }

    // 对账逻辑
    private void reconcileLeased() {
        // 按 id 游标取一页有效券
        List<Coupon> batch = couponMapper.selectActivePageForReconcile(
                cursor, LocalDateTime.now(), BATCH_SIZE);

        // 本页为空：已经扫到末尾，游标归零，下一轮从头开始
        if (batch.isEmpty()) {
            cursor = 0;
            return;
        }

        // 各类处理结果计数器，用于最后打日志观测
        int seeded = 0;    // 空键播种成功
        int repaired = 0;  // 偏差修复成功
        int deferred = 0;   // 首轮偏差，暂缓
        int casLost = 0;   // 观测后值已变，本轮放弃

        // 逐张券对账
        for (Coupon coupon : batch) {
            try {
                // 根据 reconcileOne 返回的结果累加对应计数
                switch (reconcileOne(coupon)) {
                    case SEEDED -> seeded++;      // 空键已按 DB 播种
                    case REPAIRED -> repaired++;  // 偏差已修到 DB 值
                    case DEFERRED -> deferred++;  // 第一次见偏差，下轮再确认
                    case CAS_LOST -> casLost++;   // 观测后值被改动，本轮未动
                    case IN_SYNC -> { }           // 两边一致，不计数
                }
            } catch (Exception e) {
                // 单张券出异常（如 Redis 故障）只记日志，不影响同批其他券，下一轮再试
                log.warn("[Coupon] stock reconcile failed | code={}", coupon.getCouponCode(), e);
            }
        }

        // 推进游标：不足一页说明已扫完，归零；否则停在本页最后一条的 id
        cursor = batch.size() < BATCH_SIZE ? 0 : batch.get(batch.size() - 1).getId();

        // 本轮有任何非一致结果才打汇总日志，避免空跑也刷日志
        if (seeded + repaired + deferred + casLost > 0) {
            log.info("[Coupon] stock reconcile | scanned={} | seeded={} | repaired={} "
                            + "| deferred={} | casLost={}",
                    batch.size(), seeded, repaired, deferred, casLost);
        }
    }

    // 对账单张券
    private Outcome reconcileOne(Coupon coupon) {
        // 取券码和 DB 权威余量
        String code = coupon.getCouponCode();
        Integer dbCount = coupon.getRemainingCount();

        // 券码为空、余量为空或为负：数据异常，按一致处理，不动作
        if (code == null || code.isBlank() || dbCount == null || dbCount < 0) {
            return Outcome.IN_SYNC;
        }

        // 拼出这张券的 Redis 库存键
        String key = STOCK_KEY_PREFIX + code;

        // 读 Redis 当前计数（字符串形式）
        String raw = redisTemplate.opsForValue().get(key);

        if (raw == null) {
            // SETNX：键不存在才写入，已存在则不覆盖
            Boolean seeded = redisTemplate.opsForValue()
                    .setIfAbsent(key, String.valueOf(dbCount));

            // 播种成功
            if (Boolean.TRUE.equals(seeded)) {
                log.info("[Coupon] stock key re-seeded | code={} | remaining={}", code, dbCount);
                return Outcome.SEEDED;
            }

            // 播种失败：说明有人刚播种或刚领券，本轮不动，下一轮重新观测
            // 为什么...会造成播种失败
            return Outcome.CAS_LOST;
        }

        // 把 Redis 字符串解析成数字
        long redisCount;
        try {
            redisCount = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            // 值损坏（非数字）：当成确定偏差，直接走修复
            // 怎么修复？
            log.warn("[Coupon] corrupt stock key | code={} | value={}", code, raw);
            return repair(key, code, raw, dbCount);
        }

        // Redis 与 DB 一致：清掉历史偏差记录，返回一致
        if (redisCount == dbCount) {
            pendingDrift.remove(code);
            return Outcome.IN_SYNC;
        }

        // 拼本轮偏差签名："redis值->db值"
        String signature = raw + "->" + dbCount;

        // 把签名存入 map，同时取出上一轮的旧签名
        String previous = pendingDrift.put(code, signature);

        // 与上一轮签名不同：第一次看到这个偏差，可能是在途事务，先记下，下一轮再确认
        if (!signature.equals(previous)) {
            log.warn("[Coupon] stock drift observed, will repair if it persists "
                            + "| code={} | redis={} | db={}",
                    code, redisCount, dbCount);
            return Outcome.DEFERRED;
        }

        // 连续两轮同一偏差：认定为真偏差，执行修复
        Outcome outcome = repair(key, code, raw, dbCount);

        // 修复成功才清掉偏差记录；没修成就留着，下一轮继续比对
        if (outcome == Outcome.REPAIRED) {
            pendingDrift.remove(code);
        }
        return outcome;
    }

    // 用 Lua CAS 把 Redis 值修到 DB 权威值
    private Outcome repair(String key, String code, String observed, int dbCount) {
        // 执行脚本：KEYS[1]=库存键，ARGV[1]=观测值，ARGV[2]=DB 新值；返回 1 改成功，0 未改
        Long changed = redisTemplate.execute(
                CAS_SET, List.of(key), observed, String.valueOf(dbCount));

        // 返回 1：当前值仍是观测值，已成功写成 DB 值
        if (Long.valueOf(1L).equals(changed)) {
            log.info("[Coupon] stock repaired | code={} | redis:{} -> {}",
                    code, observed, dbCount);
            return Outcome.REPAIRED;
        }

        // 返回 0：观测之后又有领券或他人修过，值已变；本轮放弃，下一轮重新观测
        log.info("[Coupon] stock repair skipped, value changed since observation | code={}", code);
        return Outcome.CAS_LOST;
    }

    // 单张券对账结果枚举
    private enum Outcome {
        IN_SYNC,   // 两边一致，无需处理
        SEEDED,    // 空键已按 DB 播种
        REPAIRED,  // 偏差已修复到 DB 值
        DEFERRED,  // 首轮偏差，暂缓等待下轮确认
        CAS_LOST   // 观测后值已被改动，本轮未写入
    }
}
```

### 单张券的判断流程——reconcileOne

```java
// 对账单张券
private Outcome reconcileOne(Coupon coupon) {
    // 取券码和 DB 权威余量
    String code = coupon.getCouponCode();
    Integer dbCount = coupon.getRemainingCount();

    // 券码为空、余量为空或为负：数据异常，按一致处理，不做任何改动
    if (code == null || code.isBlank() || dbCount == null || dbCount < 0) {
        return Outcome.IN_SYNC;
    }

    // 拼出这张券的 Redis 库存键
    String key = STOCK_KEY_PREFIX + code;

    // 读 Redis 当前计数（字符串形式）
    String raw = redisTemplate.opsForValue().get(key);

    if (raw == null) {
        // SETNX：键不存在才写入，已存在则不覆盖
        Boolean seeded = redisTemplate.opsForValue()
                .setIfAbsent(key, String.valueOf(dbCount));

        // 播种成功
        if (Boolean.TRUE.equals(seeded)) {
            log.info("[Coupon] stock key re-seeded | code={} | remaining={}", code, dbCount);
            return Outcome.SEEDED;
        }

        // 播种失败：说明有人刚播种或刚领券，本轮不动，下一轮重新观测
        return Outcome.CAS_LOST;
    }

    // 把 Redis 字符串解析成数字
    long redisCount;
    try {
        redisCount = Long.parseLong(raw);
    } catch (NumberFormatException e) {
        // 值损坏（非数字）：当成确定偏差，直接走修复
        log.warn("[Coupon] corrupt stock key | code={} | value={}", code, raw);
        return repair(key, code, raw, dbCount);
    }

    // Redis 与 DB 一致：清掉历史偏差记录，返回一致
    if (redisCount == dbCount) {
        pendingDrift.remove(code);
        return Outcome.IN_SYNC;
    }

    // 拼本轮偏差签名："redis值->db值"
    String signature = raw + "->" + dbCount;

    // 把签名存入 map，同时取出上一轮的旧签名
    String previous = pendingDrift.put(code, signature);

    // 与上一轮签名不同：第一次看到这个偏差，可能是在途事务，先记下，下一轮再确认
    if (!signature.equals(previous)) {
        log.warn("[Coupon] stock drift observed, will repair if it persists "
                        + "| code={} | redis={} | db={}",
                code, redisCount, dbCount);
        return Outcome.DEFERRED;
    }

    // 连续两轮同一偏差：认定为真偏差，执行修复
    Outcome outcome = repair(key, code, raw, dbCount);

    // 修复成功才清掉偏差记录；没修成就留着，下一轮继续比对
    if (outcome == Outcome.REPAIRED) {
        pendingDrift.remove(code);
    }
    return outcome;
}
```

为什么领券会造成播种失败：
领券路径先调 `seedStockFromDbIfAbsent`，用 `SETNX` 按 DB 余量把库存键建出来，已经有了库存键导致播种失败

| 情况 | 处理 | 结果 |
|------|------|------|
| Redis 键不存在 | SETNX 按 DB 余量播种，已存在不覆盖 | SEEDED / CAS_LOST |
| Redis 值损坏（非数字） | 当成确定偏差直接修复 | repair |
| 两边相等 | 清掉历史偏差记录 | IN_SYNC |
| 第一次看到偏差 | 记入 pendingDrift，本轮不修 | DEFERRED |
| 连续两轮同一偏差 | 执行 CAS 修复 | repair |

### 修复

```java
// 用 Lua CAS 把 Redis 值修到 DB 权威值
private Outcome repair(String key, String code, String observed, int dbCount) {
    // 执行脚本：KEYS[1]=库存键，ARGV[1]=观测值，ARGV[2]=DB 新值；返回 1 改成功，0 未改
    Long changed = redisTemplate.execute(
            CAS_SET, List.of(key), observed, String.valueOf(dbCount));

    // 返回 1：当前值仍是观测值，已成功写成 DB 值
    if (Long.valueOf(1L).equals(changed)) {
        log.info("[Coupon] stock repaired | code={} | redis:{} -> {}",
                code, observed, dbCount);
        return Outcome.REPAIRED;
    }

    // 返回 0：观测之后又有领券或他人修过，值已变；本轮放弃，下一轮重新观测
    log.info("[Coupon] stock repair skipped, value changed since observation | code={}", code);
    return Outcome.CAS_LOST;
}
```

##### lua 脚本

```java
private static final DefaultRedisScript<Long> CAS_SET = new DefaultRedisScript<>(
        "local current = redis.call('GET', KEYS[1]) " +
                "if current == ARGV[1] then " +
                "  redis.call('SET', KEYS[1], ARGV[2]) " +
                "  return 1 " +
                "end " +
                "return 0",
        Long.class);
```

先判断现在的值和之前观测的值是否相同，如果不相同证明观测之后又有领券或他人修过，本轮放弃

## SQL

### 乐观锁扣减余量——decrementRemainingCount

```sql
UPDATE coupon
SET remaining_count = remaining_count - 1, version = version + 1
WHERE coupon_code = #{couponCode} AND remaining_count > 0 AND version = #{version}
```

### 对账分页查询——selectActivePageForReconcile

```sql
SELECT id, coupon_code, remaining_count
FROM coupon
WHERE id > #{cursor} AND (end_time IS NULL OR end_time > #{now})
ORDER BY id LIMIT #{limit}
```

### 核销——markUsed

```java
UPDATE user_coupon
SET status = 1, used_time = #{usedTime}, order_no = #{orderNo}
WHERE user_id = #{userId} AND coupon_code = #{couponCode} AND status = 0
```

### 退券——markReturned

```sql
UPDATE user_coupon
SET status = #{toStatus}, used_time = NULL, order_no = NULL
WHERE coupon_code = #{couponCode} AND order_no = #{orderNo} AND status = 1
```

### 过期扫描——markExpired

```sql
UPDATE user_coupon uc
JOIN coupon c ON uc.coupon_code = c.coupon_code
SET uc.status = 2
WHERE uc.status = 0
  AND c.end_time IS NOT NULL
  AND c.end_time < #{now}
```

##### 过期扫描有一定时间窗口，会造成错误成交吗？

`getUserCoupons` 只按 `status=0` 过滤，不 JOIN 券表看时间，所以过期券暂时会出现在 "我的未使用券" 列表里

但用户真拿去下单时，`applyToOrder` 会再查券模板、判断 `endTime`，已过期直接抛 "优惠券已过期"，核销不会成功

## 问题

### 优惠券有哪几种类型？一张券从领到用掉经历哪些状态？

满减：满足门槛金额后减固定金额

折扣：目前不支持

立减：没门槛

用户持有的券有三个状态：未使用(0)、已使用(1)、已过期(2)

一张券的生命周期：领券(写入 user_coupon，记录领取时间)->下单核销(`markUsed` 把状态 0→1，写使用时间、绑定订单号)->退券(未过期退成 0，已过期退成 2)
-> 过期(未使用的券超过有效期，定时任务 `markExpired` 把状态 0→2)

### 优惠券相关的数据库表是怎么样的？`coupon` 和 `user_coupon` 两张表怎么分工？关键字段和约束是什么？

##### coupon：定义券是什么、还剩多少

| 字段 | 类型 | 约束/索引 | 说明 |
|------|------|-----------|------|
| id | BIGINT | 主键 PK | 主键 |
| coupon_code | VARCHAR(64) | NOT NULL，UNIQUE uk_coupon_code | 优惠券码 |
| title | VARCHAR(128) | NOT NULL | 标题 |
| type | TINYINT | DEFAULT 0 | 0满减 / 1折扣 / 2立减 |
| condition_amount | DECIMAL(10,2) | — | 满减门槛（立减为 0） |
| discount_amount | DECIMAL(10,2) | — | 减免金额（折扣券为折扣率） |
| total_count | INT | DEFAULT 0 | 总发放量 |
| remaining_count | INT | DEFAULT 0 | 剩余可领量 |
| version | INT | DEFAULT 0 | 乐观锁版本号 |
| start_time | DATETIME | — | 有效期开始 |
| end_time | DATETIME | — | 有效期结束 |
| create_time | DATETIME | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| update_time | DATETIME | ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

##### user_coupon：用户持有表（谁领了、用没用、绑哪笔单）

| 字段 | 类型 | 约束/索引 | 说明 |
|------|------|-----------|------|
| id | BIGINT | 主键 PK | 主键 |
| user_id | VARCHAR(64) | NOT NULL | 用户 ID |
| coupon_code | VARCHAR(64) | NOT NULL，INDEX idx_coupon_code | 优惠券码 |
| status | TINYINT | DEFAULT 0 | 0未使用 / 1已使用 / 2已过期 |
| acquire_time | DATETIME | — | 领取时间 |
| used_time | DATETIME | — | 使用时间 |
| order_no | VARCHAR(64) | — | 绑定订单号 |
| 唯一索引 | — | UNIQUE uk_user_coupon (user_id, coupon_code) | 一人限领一张 |

### 高并发领券怎么防止超发？库存扣减怎么做的？

Redis 预扣做前置挡掉大部分并发，DB 乐观锁做最终裁决，两层保证不超发；Redis 计数漂移由定时对账兜底

Redis 前置

decrement 预扣券库存计数，快速挡掉抢光后的请求，减轻 DB 压力

DB 层

领券时先读取 version 字段，要求 "版本号没变 + 还有余量" 才扣成功并把版本 +1；并发下只有版本对得上的那一个能改，后来者版本对不上、拿到 0 行被拒。

库存扣减的流程：

1.业务幂等：先查 `user_coupon`，已领过直接拒绝

2.并发去重：Redis 短窗口 `SETNX` 幂等键，挡住同用户同券的并发重复请求

3.读券校验：查 `coupon`，校验存在、余量 > 0、在有效期内

4.冷启动回源：Redis 库存键（coupon:stock:{couponCode}）不存在时用 `SETNX` 按 DB 余量写入一个键，避免冷启动把券判成抢光(读不到键就默认没有该券)

5.Redis 预扣：`decrement` 计数减 1，减成负数就 `increment` 补回并报 "已抢光"

6.挂回滚回调：注册事务同步，回滚时把 Redis 计数加回去、释放幂等键

7.DB 乐观锁扣减：带版本号更新余量

```sql
UPDATE coupon
SET remaining_cpunt = remaining_count - 1, version = version + 1
WHERE coupon_code = #{couponCode}
  AND remaining_count > 0
  AND version = #{version}
```

version=#{version} 乐观锁

8.写入 user_coupon

##### 为什么要用短窗口？

短窗口：

Redis 幂等键只存活很短一段时间就自动过期(10 s)

为什么：

1.持久性的防重不靠这个键，靠 DB 唯一索引，"同一张券一人限领一张" 是长期约束，最终由 user_coupon 表的 (user_id, coupon_code) 
唯一索引保证

2.TTL 太长会产生副作用：用户领券失败(库存不足、券过期)，但是键已经占位，接下来一段时间内想重试都被阻拦

3.避免 Redis 键堆积：领券是高频动作，长期有效会留下大量历史键

### 为什么 DB 扣减用乐观锁（version），而不是分布式锁？

单库单行扣减，数据库行锁本身就会串行化并发更新

##### 那什么情况要使用分布式锁？

分布式锁的适用场景

1.跨服务 / 跨多个数据源：同一资源分散在多个微服务、多个库，单行锁只锁得住本库一行，管不到别的库和服务，需要全局锁协调

2.全局只能一个实例执行

为什么不轻易上分布式锁：

### 一张券能不能同时抵两笔订单？并发两笔下单用同一张券怎么处理？为什么不先查后改？

不能，一张券只能抵一笔订单。

并发两笔下单用同一张券时，靠核销的 "条件 UPDATE"(`WHERE status=0`) + "受影响行数裁决" 保证只有一笔订单能成功。

之所以不先查后改是因为：
如果先 `SELECT` 查到未使用、再 `UPDATE`，两步之间有间隙：两笔订单可能都查到未使用，然后都核销成功，一张券抵两单。

### 缓存和数据库一致性怎么保证？

主路径靠事务回调对齐，残留偏差由对账任务修复；

主路径对齐靠的是领券方法里注册的 `TransactionSynchronization.afterCompletion`：DB 回滚时回调把 Redis 计数加回去、删掉幂等键(领券幂等键)。
这个机制是应用层回调，不是数据库事务本身，所以只要回调没执行到、或执行失败，主路径就失效。

主路径失效的具体场景如下：

1.Redis 已 `decrement` 预扣，DB 事务还没提交时进程被杀掉

2.回调里的 Redis 操作失败：Redis 抖动、超时、网络断开。

对账修复是定时任务把 "Redis 预扣计数" 和 "DB 权威余量" 逐张比对，发现偏差后以 DB 为准修回 Redis(具体细节见上)。
