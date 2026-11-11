# 订单与库存

## OrderService

### 创建订单——createOrder

```java
// 创建订单：幂等防重+锁定库存
// 两层幂等：requestId 挡重复请求、orderNo 唯一索引(user_id, request_id)兜底防重复落单
// 兜底靠的是唯一索引的机制
@Transactional(rollbackFor = Exception.class)
public Order createOrder(Order order, List<OrderItem> items, String requestId) {
    // 校验 1：用户身份不能为空
    if (order.getUserId() == null || order.getUserId().isBlank()) {
        throw BizException.badParam("用户不能为空");
    }

    // 校验 2：订单项不能为空。服务层可能被直接调用，不能只依赖控制器的参数校验
    if (items == null || items.isEmpty()) {
        throw BizException.badParam("订单项不能为空");
    }

    // 校验 3：每个 SKU 与数量都必须合法；数量必须大于 0，
    // 否则负数会把库存锁定变成“反向加锁”（SQL 层另有 n > 0 兜底）
    // 反向加锁：库存锁定量不增反减
    for (OrderItem item : items) {
        if (item == null || item.getSkuId() == null
                || item.getQuantity() == null || item.getQuantity() <= 0) {
            throw BizException.badParam("SKU 与购买数量不能为空，且数量必须大于 0");
        }
    }

    // 第一层幂等：requestId
    String idemKey = null;
    if (requestId != null && !requestId.isBlank()) {
        idemKey = idempotencyKey(order.getUserId(), requestId);

        // 先查库：Redis 丢键（重启/过期）时，唯一索引仍能保证不会落两单
        Order existingByRequest = orderMapper.selectOne(new LambdaQueryWrapper<Order>()
                .eq(Order::getUserId, order.getUserId())
                .eq(Order::getRequestId, requestId));
        if (existingByRequest != null) {
            log.info("[Order] idempotent hit by requestId | requestId={} | orderNo={}",
                    requestId, existingByRequest.getOrderNo());
            return existingByRequest;
        }

        // Redis 占位（在 Redis 里写入一个 String 类型的键）：SET NX，成功表示是首次请求；键值先记为 PENDING（处理中）
        Boolean firstRequest;
        try {
            // 等价于 SET idemKey "PENDING" NX EX 86400
            firstRequest = redisTemplate.opsForValue()
                    .setIfAbsent(idemKey, IDEMPOTENCY_PENDING,
                            IDEMPOTENCY_TTL_HOURS, TimeUnit.HOURS);
        } catch (Exception e) {
            // Redis 不可用不让下单失败，继续走，交给 DB 唯一索引兜底
            log.error("[Order] idempotency reservation failed — fall back to DB unique index | requestId={}",
                    requestId, e);
            firstRequest = null;
        }

        // 占位失败：说明已有相同 requestId 的请求在处理或已完成，尝试取回已有订单
        // 取回订单：先去读 Redis 幂等键里存的值
        // 是 orderNo，第一次请求已提交，键里存的是订单号，按 `orderNo + userId` 去 `orders` 表查出这张订单，返回给重复请求
        // 是 PENDING，查不到已完成订单，返回 null，上层抛 "正在处理中，请稍后重试"
        if (Boolean.FALSE.equals(firstRequest)) {
            Order replay = replayFromIdempotencyKey(idemKey, order.getUserId(), requestId);
            if (replay != null) {
                return replay;
            }
            // 键还是 PENDING：另一请求正在处理中
            throw BizException.conflict("相同 requestId 的请求正在处理中，请稍后重试");
        }

        order.setRequestId(requestId);
        // 把“提交后写入最终幂等键”注册到事务回调；回滚时自动删键
        registerIdempotencyFinalizer(idemKey, order.getOrderNo());
    }

    // 第二层幂等：orderNo 查重
    Order existing = orderMapper.selectOne(
            new LambdaQueryWrapper<Order>().eq(Order::getOrderNo, order.getOrderNo()));
    if (existing != null) {
        // 单号属于别人：冲突，拒绝
        if (!Objects.equals(existing.getUserId(), order.getUserId())) {
            throw BizException.conflict("订单号已被其他用户占用");
        }
        // 自己的重复单号：直接返回已有订单
        return existing;
    }

    // 计算金额：名称与单价一律取 DB 的 SKU 快照，不接受客户端传入
    // 留存下单那一刻的价格，后续改价不影响历史订单
    BigDecimal total = BigDecimal.ZERO;
    for (OrderItem item : items) {
        ProductSku sku = skuMapper.selectById(item.getSkuId());
        if (sku == null) {
            throw BizException.notFound("SKU 不存在: " + item.getSkuId());
        }
        item.setProductName(sku.getTitle());
        item.setPrice(sku.getPrice());
        item.setTotalPrice(sku.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())));
        total = total.add(item.getTotalPrice());
    }
    order.setTotalAmount(total);

    // 用券抵扣：门槛/有效期/券型/归属校验与核销都在 applyToOrder 内完成
    BigDecimal discount = couponService.applyToOrder(
            order.getUserId(), order.getCouponCode(), total, order.getOrderNo());
    order.setDiscountAmount(discount);
    order.setPayAmount(total.subtract(discount));

    // 创建时间
    order.setCreateTime(LocalDateTime.now());

    // 锁定库存：条件 UPDATE，可售不足时受影响行数为 0，即库存不足
    for (OrderItem item : items) {
        int affected = skuMapper.lockStock(item.getSkuId(), item.getQuantity());
        if (affected == 0) {
            throw BizException.conflict("库存不足: " + item.getProductName());
        }
    }

    // 落库：订单主表 + 订单项批量插入
    orderMapper.insert(order);

    // 订单项批量插入：k 件商品从 k 次往返降到 1 次。
    // id 需在此显式分配：order_item 没有自增列，自定义批量 SQL 绕过了 MyBatis-Plus 的默认主键填充
    for (OrderItem item : items) {
        item.setOrderId(order.getId());
        item.setId(IdWorker.getId());
    }
    orderItemMapper.insertBatch(items);

    // 无事务环境（如单测直接调用）下补写幂等键；
    // 有事务时由前面注册的 afterCommit 回调写入，保证键指向的订单已真正提交，回滚即删键
    if (idemKey != null && !TransactionSynchronizationManager.isSynchronizationActive()) {
        writeIdempotencyKey(idemKey, order.getOrderNo());
    }

    log.info("[Order] created | orderNo={} | userId={} | amount={}",
            order.getOrderNo(), order.getUserId(), order.getPayAmount());
    return order;
}
```

### 状态流转——transition

```java
// 订单状态流转
public void transition(Long orderId, OrderStatus to, String operator) {
    // 先查订单，不存在直接报 404
    Order order = orderMapper.selectById(orderId);
    if (order == null) {
        throw BizException.notFound("订单不存在: " + orderId);
    }

    // 读出订单当前状态，作为流转起点 from
    OrderStatus from = OrderStatus.of(order.getStatus());

    // 状态机校验：from → to 是否为合法流转，不合法直接抛异常
    OrderStateMachine.requireTransit(from, to);

    // CAS 乐观更新：WHERE id=? AND status=from，只有当前状态仍是 from 才更新成功
    // SQL: UPDATE orders SET status=to WHERE id=? AND status=from
    int affected = orderMapper.transitionStatus(orderId, from.getCode(), to.getCode());

    // 影响行数为 0：说明并发下状态已被别的请求改掉，本次流转失败，报冲突
    // "影响行数" 就是这条 `UPDATE` 语句实际改动了几行数据，由数据库返回
    if (affected == 0) {
        throw BizException.conflict("订单状态已变更，请刷新后重试");
    }

    // 写一条状态流转审计日志，记录这次变更的完整链路
    OrderStatusLog logRow = new OrderStatusLog();
    logRow.setOrderId(orderId);            // 哪个订单
    logRow.setFromStatus(from.getCode());   // 变更前状态
    logRow.setToStatus(to.getCode());       // 变更后状态
    // 操作者由调用方传入；为空则兜底记为 system，不从请求里猜测
    logRow.setOperator(operator == null || operator.isBlank() ? OPERATOR_SYSTEM : operator);
    logRow.setCreatedAt(LocalDateTime.now());
    statusLogMapper.insert(logRow);

    // 记录本次流转日志，便于排查
    log.info("[Order] status transition | orderId={} | {} -> {} | operator={}",
            orderId, from.getDesc(), to.getDesc(), logRow.getOperator());
}
```

## 库存操作对应的 SQL 语句——ProductSkuMapper

锁定库存 lockStock

```sql
UPDATE product_sku
SET locked_stock = locked_stock + #{n}
WHERE id = #{skuId}
  AND #{n} > 0
  AND deleted = 0
  AND (stock - locked_stock) >= #{n}
```

支付扣减 deductStock

```sql
UPDATE product_sku
SET stock = 
```

## 问题

### 反向加锁是什么意思

正常的锁定 sql

```sql
UPDATE product_sku
SET locked_stock = locked_stock + #{n}
WHERE id = #{skuId}
  AND #{n} > 0  
  AND deleted = 0
  AND (stock - locked_stock) >= #{n}
```

如果 n 是负数，锁定库存就会凭空减少，可售库存莫名增多

### 怎么实现的防超卖？为什么不用分布式锁？

通过 sql 语句而不是分布式锁， 

### 如何实现下单幂等？Redis 和 DB 各自负责什么？

下单幂等是 Redis + DB 两层。
请求进来先按 `userId + requestId` 查库，命中就返回已有订单；
没命中再用 `StringRedisTemplate` 的 `SETNX` 占位，值先记为 `PENDING`。
占位失败时按 value 区分：还是 PENDING 说明在处理中，返回 "稍后重试"；
已是 orderNo 则带 userId 回查库返回订单。
Redis 故障时放行不挡下单，最终由 `(user_id, request_id)` 唯一索引裁决(通过唯一索引的机制来实现不重复下单功能)。
最终幂等键在事务提交后才写入，回滚即删，避免留下指向未提交订单的脏键。

##### 为什么最终幂等键在事务提交后才写入

Redis 键里存的 `orderNo` 是一个 "指路牌"：后面再来相同 requestId 的请求，靠这个单号回查数据库、把已有订单找出来返回。
所以键指向的订单，必须是数据库里真实存在、能查到的。

如果在方法末尾提前写键：

1. 请求 A 走到末尾，把 `orderNo=X` 写进 Redis，但订单还没提交；

2. 请求 B（相同 requestId）进来，命中这个键，拿 `X` 去查库 —— 订单还没提交，查不到；

3. B 以为这个键是脏的，把键删掉；

4. A 的事务随后提交，订单落库了；但键已经被 B 删掉；

5. 再来相同 requestId 的请求，又查不到键、也查不到订单，再建一单—— 幂等就破了。

### 介绍一下 Redis 中的唯一索引？
