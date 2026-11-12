# 订单与库存

## OrderService

### 创建订单——createOrder

```java
// 创建订单：幂等防重+锁定库存
// 两层幂等：requestId 挡重复请求、唯一索引(user_id, request_id)兜底防重复落单
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

### 支付——payOrder

```java
public Order payOrder(String orderNo) {
    Order order = getByOrderNo(orderNo);

    // 先裁决：只有「待支付 -> 已支付」能过，并发下唯一
    transition(order.getId(), OrderStatus.PAID, OPERATOR_USER);

    // 再把锁定库存转为实际扣减（失败即抛，状态变更随之回滚）
    for (OrderItem item : orderItemsOf(order.getId())) {
        int affected = skuMapper.deductStock(item.getSkuId(), item.getQuantity());
        if (affected == 0) {
            throw BizException.conflict("库存锁定已失效，无法支付: " + item.getProductName());
        }
    }

    Order paid = getByOrderNo(orderNo);
    log.info("[Order] paid | orderNo={} | amount={}", orderNo, paid.getPayAmount());
    return paid;
}
```

### 取消、超时——cancelOrder

```java
// 取消订单
@Transactional(rollbackFor = Exception.class)
public Order cancelOrder(String orderNo, String operator) {
    // 按单号查订单，不存在抛 404
    Order order = getByOrderNo(orderNo);

    // 先 CAS 状态：待支付 → 已取消。并发下只有抢到状态的那一个能继续，
    // 已支付/已取消的单在这里就被状态机拒绝，走不到后面的退券和归还
    transition(order.getId(), OrderStatus.CANCELLED, operator);

    couponService.returnForOrder(order.getCouponCode(), orderNo);

    // 逐键归还锁定库存
    for (OrderItem item : orderItemsOf(order.getId())) {
        int affected = skuMapper.releaseStock(item.getSkuId(), item.getQuantity());
        if (affected == 0) {
            // 与支付侧扣减失败相反：这里只记 warn，不抛异常。
            // 0 行说明这批锁定量已不在账上（重复归还）
            // 如果抛异常的话会让整个事务回滚，订单状态退回
            // 超时任务下一轮又会捞到它重试，于是进入一个无法自愈的死循环，所以留痕即可
            log.warn("[Order] release affected 0 rows | orderNo={} | skuId={} | qty={}",
                    orderNo, item.getSkuId(), item.getQuantity());
        }
    }

    // 重新查库返回取消后的最新订单
    Order cancelled = getByOrderNo(orderNo);
    log.info("[Order] cancelled | orderNo={} | locked stock returned", orderNo);
    return cancelled;
}
```

### 未发货退款——refundOrder

```java
// 退款——状态流转 + 库存回补 + 退券，不碰资金
// 两次状态流转和回补、退券在同一事务，任一步抛异常，整笔事务一起回滚
@Transactional(rollbackFor = Exception.class)
public Order refundOrder(String orderNo) {
    // 按单号查订单，不存在抛 404
    Order order = getByOrderNo(orderNo);

    // 先记下退款前的来源状态：下一步 CAS 后状态已变成退款中，就读不到原来的状态了。
    // 是否回补库存，要靠这个来源状态判断
    OrderStatus from = OrderStatus.of(order.getStatus());

    // 先 CAS 裁决：只有 未发货/已发货/退货入库 → 退款中 能过，并发下唯一。
    // 抢到退款中的那一个，才会走到下面的回补和退券
    transition(order.getId(), OrderStatus.REFUNDING, OPERATOR_ADMIN);

    // 按来源状态决定是否回补库存：
    //   PAID（未发货）：货没出去，退款时加回可售池，只有这一种会补；
    //   SHIPPED（仅退款不退货）：货在用户手上，不补；
    //   RETURNED（已退货入库）：入库时已补过，再补就是重复加
    if (from == OrderStatus.PAID) {
        restoreStockOf(order, "refund");
    }

    // 只有抢到退款中状态的请求会走到这里，券不会被退两次
    couponService.returnForOrder(order.getCouponCode(), orderNo);

    // 第二步 CAS：退款中 → 已退款。
    transition(order.getId(), OrderStatus.REFUNDED, OPERATOR_ADMIN);

    // 重新查库返回退款后的最新订单
    Order refunded = getByOrderNo(orderNo);
    log.info("[Order] refunded | orderNo={} | amount={} | from={} | restocked={} | no money move",
            orderNo, refunded.getPayAmount(), from.getDesc(), from == OrderStatus.PAID);
    return refunded;
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
SET stock = stock - #{n},
    locked_stock = locked_stock - #{n}
WHERE id = #{skuId}
    AND #{n} > 0
    AND deleted = 0
    AND locked_stock >= #{n}
    AND stock >= #{n}
```

释放锁定——releaseStock

```sql
UPDATE product_sku
SET locked_stock = locked_stock - #{n}
WHERE id = #{skuId}
  AND #{n} > 0
  AND deleted = 0
  AND locked_stock >= #{n}
```

退款回补——restoreStock

```sql
UPDATE product_sku
SET stock = stock + #{n}
WHERE id = #{skuId}
  AND #{n} > 0
  AND deleted = 0
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

### 怎么实现的防超卖？

防超卖靠的是 "带库存条件的原子 UPDATE + 受影响行数裁决"：把 "库存够不够" 写进 WHERE，数据库行级完成 "判断 + 扣减"，同一行的并发修改由 InnoDB 行锁串行化，没有加分布式锁。

##### 受影响行数裁决在代码中是怎么发挥作用的？

判定成败，替代额外查库。减库存路径拿它当失败信号，0 行就抛异常回滚；还补路径拿它当告警信号，0 行只记 warn 不阻断。

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

##### "最终幂等键在事务提交后才写入"是怎么实现的？简单介绍一下对应的机制？

靠的是 Spring 事务生命周期的回调钩子。

先介绍三个角色：

1.TransactionSynchronizationManager 注册表，Spring 提供的事务同步管理器，内部用 ThreadLocal 把回调列表绑定到当前线程的事务上。业务代码通过它 "登记" 自己想在事务的哪个时间点被通知。事务一开始、代理开启事务后，`TransactionSynchronizationManager` 就把回调列表绑定到当前线程（ThreadLocal），此时 `isSynchronizationActive() = true`。

2.TransactionSynchronization：钩子接口

里面定义了事务各时点的回调方法，重写需要的就行：

| 回调方法 | 触发时机 |
|---------|---------|
| `beforeCommit` | 提交前（还能改票 / 参与表决） |
| `afterCommit` | 提交成功后；回滚不会调 |
| `beforeCompletion` | 事务结束前 |
| `afterCompletion` | 事务结束后，无论提交还是回滚都调；用 `status` 判断结果 |

3.事务拦截器(真正触发回调的人)

@Transactional 走的是 Spring 代理。事务拦截器在 `beforeCommit → commit/rollback → afterCommit → beforeCompletion → afterCompletion` 每个节点分别遍历列表调用对应钩子

具体代码实现：

```java
private void registerIdempotencyFinalizer(String key, String orderNo) {
    // 当前线程存在事务上下文，才挂钩子；没有事务就不注册
    if (TransactionSynchronizationManager.isSynchronizationActive()) {
        // 注册一个同步对象（Hook 实例）
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                // 事务提交成功后：写最终幂等键
                writeIdempotencyKey(key, orderNo);
            }

            @Override
            public void afterCompletion(int status) {
                // 事务结束后：如果是回滚，删掉占位键
                if (status == STATUS_ROLLED_BACK) {
                    redisTemplate.delete(key);
                }
            }
        });
    }
}
```

业务代码在事务内调用 `registerIdempotencyFinalizer`

`registerSynchronization` 把这个匿名 Hook 放进当前线程事务的回调列表

方法返回，代理层继续走 commit /rollback

提交成功 → 事务拦截器调 `afterCommit` → 写最终键；

回滚 → 调 `afterCompletion` 且 `status=ROLLED_BACK` → 删键

### 为什么要设置状态，设置状态有什么用？分别有什么状态？

##### 设置状态到底有什么用？

1.决定库存该怎么动

2.拦截非法操作

状态把 "哪些流转合法" 集中成一张表。比如：已完成的单不能再支付、已取消的单不能再发货、终态不能再流转。没有状态，接口被乱调（重复支付、对已发货单取消）就拦不住。

3.作为并发互斥的裁决点

支付和超时取消会撞车，两者都改 `WHERE status=待支付` 的 CAS，只有一个能成功。状态就是这个互斥锚点

4.驱动流程推进与定时任务

定时任务靠 `status=待支付 + create_time 超时` 扫描回收，售后入口按状态开放（已收货才能申请退货，未发货走退款），前端据此展示待支付、待发货、待收货等环节

5.区分相似业务的不同语义

同样是退款：未发货要回补库存、已发货仅退款不补、退货入库才补。靠退款前的来源状态判断该走哪条库存分支，这也是状态提供的信息。

##### 有哪些状态？

| 枚举名 | 中文状态 | 所处阶段 | 库存含义 |
|--------|----------|----------|----------|
| PENDING_PAYMENT | 待支付 | 下单后未付款 | 已锁定库存，尚未实扣 |
| PAID | 已支付 | 已付款，待发货 | 库存已实扣 |
| SHIPPED | 已发货 | 商品已发出 | 库存已扣，货在途中 |
| DELIVERED | 已收货 | 用户已签收 | 库存已扣，等待完成/退货 |
| COMPLETED | 已完成 | 交易结束 | 终态，不再动库存 |
| CANCELLED | 已取消 | 订单撤销 | 锁定库存已归还 |
| REFUNDING | 退款中 | 退款处理中 | 待退款完成 |
| REFUNDED | 已退款 | 退款完成 | 终态（未发货退款已回补库存） |
| RETURNING | 退货中 | 退货流程中，货在用户/路上 | 尚未回补库存 |
| RETURNED | 已退货入库 | 退货商品已入库 | 库存已回补 |

### 介绍一下这块数据库存储的表的结构？

##### product_spu 商品 SPU 表

| 字段 | 类型 | 约束/索引 | 说明 |
|------|------|-----------|------|
| id | BIGINT | 主键 PK | SPU ID |
| title | VARCHAR(256) | NOT NULL | 商品标题 |
| category_id | BIGINT | INDEX idx_category | 品类 ID |
| brand_id | BIGINT | — | 品牌 ID |
| description | TEXT | — | 商品描述 |
| main_image | VARCHAR(512) | — | 主图 URL |
| status | TINYINT | INDEX idx_status | 0 下架 / 1 上架 |
| create_time | DATETIME | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| update_time | DATETIME | ON UPDATE CURRENT_TIMESTAMP | 更新时间 |
| deleted | TINYINT(1) | DEFAULT 0 | 逻辑删除 |

##### product_sku 商品 SKU 表

| 字段 | 类型 | 约束/索引 | 说明 |
|------|------|-----------|------|
| id | BIGINT | 主键 PK | SKU ID |
| spu_id | BIGINT | NOT NULL，INDEX idx_spu | 所属 SPU |
| title | VARCHAR(256) | NOT NULL | SKU 标题 |
| price | DECIMAL(10,2) | NOT NULL | 售价 |
| original_price | DECIMAL(10,2) | — | 原价 |
| stock | INT | DEFAULT 0 | 总库存 |
| locked_stock | INT | DEFAULT 0 | 锁定库存（下单未支付） |
| sale_count | INT | DEFAULT 0 | 销量 |
| image | VARCHAR(512) | — | SKU 图片 |
| attributes | JSON | — | SKU 规格属性 |
| create_time | DATETIME | DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| update_time | DATETIME | ON UPDATE CURRENT_TIMESTAMP | 更新时间 |
| deleted | TINYINT(1) | DEFAULT 0 | 逻辑删除 |
| 约束 | — | CHECK chk_sku_stock | stock >= 0 AND locked_stock >= 0 AND locked_stock <= stock |
| 索引 | — | INDEX idx_price | 按价格筛选 |

##### orders 订单主表

| 字段 | 类型 | 约束 | 索引 | 说明 |
|------|------|------------------------|----------------------|------|
| id | BIGINT | PRIMARY KEY (非空 + 全表唯一) | 主键索引 | 订单 ID |
| order_no | VARCHAR(64) | NOT NULL, UNIQUE (不许重复) | uk_order_no 唯一索引 (UNIQUE KEY uk_order_no (order_no)) | 订单号 |
| user_id | VARCHAR(64) | NOT NULL | idx_user_id 普通索引 (INDEX idx_user_id (user_id)) , 同时是 uk_user_request 的前导列(指的是联合索引的第一列) | 用户 ID |
| total_amount | DECIMAL(10,2) | NOT NULL | — | 订单总金额 |
| discount_amount | DECIMAL(10,2) | DEFAULT 0（没传就填 0） | — | 优惠金额 |
| pay_amount | DECIMAL(10,2) | NOT NULL | — | 实付金额 |
| status | TINYINT | DEFAULT 0（默认待支付） | 是 idx_status_create_time 的前导列 | 0待支付/1已支付/2已发货/3已收货/4完成/5已取消/6退款中/7已退款/8退货中/9已退货入库 |
| address_snapshot | JSON | 可空 | — | 地址快照 |
| coupon_code | VARCHAR(64) | DEFAULT NULL（可空） | — | 使用的优惠券码 |
| request_id | VARCHAR(64) | DEFAULT NULL | 是 uk_user_request 的第二列 | 客户端幂等键 |
| source | VARCHAR(32) | DEFAULT 'agent_guide' | — | 订单来源 |
| session_id | VARCHAR(64) | DEFAULT NULL（可空） | — | 导购会话 ID |
| create_time | DATETIME | DEFAULT CURRENT_TIMESTAMP（**实际代码不用，应用层 `LocalDateTime.now()` 手动写**，避时钟漂移） | 是 idx_status_create_time 的第二列 | 创建时间 |
| update_time | DATETIME | DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP（行被改时自动刷新） | — | 更新时间 |
| deleted | TINYINT(1) | DEFAULT 0 | — | 逻辑删除 |

两个索引：

联合唯一索引 (user_id, request_id)，为下单幂等兜底

联合普通索引 (status, create_time)，用于超时扫描

##### order_item 订单明细

| 字段 | 类型 | 说明 |
|---|---|---|
| id | BIGINT PK | 雪花 ID（无自增，批量插入时手动分配） |
| order_id | BIGINT，INDEX idx_order_id | 所属订单 |
| product_id | VARCHAR(64) | 商品 ID（注意：跟 product_sku.id 的 BIGINT 不一致，是历史遗留） |
| sku_id | BIGINT | SKU ID |
| product_name / sku_desc | VARCHAR | 下单时从 SKU 快照，商品改名不影响历史订单 |
| quantity | INT | 数量 |
| price / total_price | DECIMAL(10,2) | 单价 / 小计 |
