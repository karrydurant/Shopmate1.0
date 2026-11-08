# 多级缓存机制

## 读取——ProductDetailService

商品详情读取服务——四级缓存+穿透/击穿/雪崩防护

### 读主入口——getProductDetail

```java
// 读主入口：按 productId 拿商品详情，依次走 L1 → L2 → L3 → 加锁回源
public ProductDetailVO getProductDetail(Long productId) {
    // L1: 先查本地 Caffeine
    ProductDetailVO local = localCache.getIfPresent(productId);
    if (local != null) {
        // 命中负缓存哨兵 → 商品确实不存在，直接返回 null
        return local == NULL_MARKER ? null : local;
    }

    // L2：本地没命中，查 Redis
    ProductDetailVO fromRedis = readRedis(productId);
    if (fromRedis != null) {
        return fromRedis;  // 命中正缓存，readRedis 内部已顺手回填 L1
    }
    // Redis 里存的如果是负缓存哨兵，也直接返回 null（内部回填 L1）
    if (isNegativeCached(productId)) {
        return null;
    }

    // L3：布隆过滤器
    // 只有预热成功过才允许用它否决
    if (bloom.isWarmed() && !bloom.mightContain(productId)) {
        log.debug("[ProductDetail] filtered by bloom | productId={}", productId);
        cacheNegative(productId);  // 写负缓存，下次直接命中，不用再问布隆
        return null;
    }

    // 击穿防护：分布式锁 + Double-Check
    RLock lock=redissonClient.getLock(CacheConstants.LOCK_PRODUCT_DETAIL + productId);
    boolean locked = false;
    try {
        // waitTime=lockWaitMs(1s)：最多等 1 秒拿锁；leaseTime=lockLeaseMs(10s)：锁 10 秒自动释放
        locked = lock.tryLock(lockWaitMs, lockLeaseMs, TimeUnit.MILLISECONDS);
        if (!locked) {
            // 抢不到锁 = 有人正在回源，不是"商品不存在"。
            // 降级：直读 DB 一次，writeCache=false 表示不回填缓存（避免在没持锁时抢先写）
            log.warn("[ProductDetail] lock busy, degrading to direct DB read | productId={}", productId);
            return loadFromDB(productId, false);
        }

        // Double-Check：等锁期间别人可能已经把 Redis 回填了，再查一次避免重复回源
        // 回填了内存和 Redis，通过检查 Redis 判断是否已经有其它机器回填是因为不同机器内存不共享，Redis 共享
        fromRedis = readRedis(productId);
        if (fromRedis != null) {
            return fromRedis;
        }
        if (isNegativeCached(productId)) {
            return null;
        }
        // 拿到锁且 Redis 确实没有 → 真正回源 DB，这次允许写缓存
        return loadFromDB(productId, true);
    } catch (InterruptedException e) {
        // 当前线程被中断：同样不该渲染成「商品不存在」，降级直读 DB、不写缓存
        Thread.currentThread().interrupt();
        return loadFromDB(productId, false);
    } finally {
        // 用持有者判定而不是布尔量：中途一旦有别的获取路径，布尔量会漏放或误放
        if (locked && lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }
}
```

## 写入——ProductWriteService



## 商品变更消息发布——ProductCacheNotifier



## 问题

### 负缓存哨兵长什么样？

### 什么是布隆过滤器？介绍一下布隆过滤器的原理？为什么没有预热好就不用布隆过滤器？

### 分布式锁 + Double-check 防的是什么？

把 "查 DB + 回填缓存(Redis + 内存)" 这个动作变成同一时刻只有一个请求能做，防止缓存击穿（即一瞬间有大量请求同时进来查这个商品，DB 连接池瞬间被打爆）

### 介绍一下分布式锁？为什么要使用分布式锁？这块代码中是怎么使用分布式锁的？

### waitTime=lockWaitMs(1s)：最多等 1 秒拿锁；leaseTime=lockLeaseMs(10s)：锁 10 秒自动释放
