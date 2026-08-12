package com.sparklearn.core.storage;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 广播变量的 driver 端注册表。
 *
 * <p>对应真实 Spark 的 {@code BroadcastManager}（运行在 Driver 上）。
 * 用户调 {@code SparkContext.broadcast(value)} 时，值被存进这里，拿到一个全局唯一的
 * {@code broadcastId}；{@link Broadcast} 对象序列化到 executor 时只携带这个 id，
 * executor 端反序列化后凭 id 回到这里取值——这正是广播变量区别于"闭包里直接塞个大对象"
 * 的关键：大对象不会被每个 Task 重复序列化。
 *
 * <p><b>本章简化</b>：mini-spark 是单机模式（driver 与 executor 同 JVM），
 * {@code BroadcastManager} 用静态单例即可。真实 Spark 在每个 executor 进程里各有一个
 * {@code BroadcastManager}，通过 BlockManager 从 driver 拉取广播块（{@code BroadcastBlockId}），
 * 并支持 torrent 分块拉取——这部分留到 ch15 对照真实 Spark 时展开。
 *
 * <p>和 {@link BlockManager} 的关系：两者都属于 storage 层，负责"分布式数据共享"。
 * {@link BlockManager} 管 RDD 分区缓存（{@link BlockId}），{@code BroadcastManager}
 * 管广播变量。真实 Spark 把两者统一到 {@code BlockManager} 的存储抽象下；
 * 本章为聚焦广播变量的核心机制，让 {@code BroadcastManager} 独立管理。
 */
public final class BroadcastManager {

    /** 单机模式下的全局单例（driver 与 executor 同 JVM，共享同一张注册表）。 */
    private static final BroadcastManager INSTANCE = new BroadcastManager();

    /** driver 端的广播值注册表：broadcastId → 广播值。 */
    private final Map<Long, Object> registry = new ConcurrentHashMap<>();

    /** 广播 id 分配器。 */
    private final AtomicLong nextBroadcastId = new AtomicLong();

    private BroadcastManager() {
    }

    /**
     * 单机模式下的全局访问点。
     *
     * <p>executor 端反序列化 {@link Broadcast} 对象后，通过这个方法拿到注册表，
     * 再凭 {@code broadcastId} 取值。
     */
    public static BroadcastManager getInstance() {
        return INSTANCE;
    }

    /**
     * 在 driver 端注册一个广播值，返回它的唯一 id。
     *
     * <p>对应真实 Spark 的 {@code SparkContext.broadcast()} 内部流程：
     * 值序列化后存入 driver 的 BlockManager（{@code BroadcastBlockId}），
     * 返回的 {@code Broadcast} 对象只持有 id。本章值直接存内存注册表，不经过 BlockManager。
     *
     * @param value 要广播的对象，通常是一个较小的表（JOIN 时建 hash map 用）
     * @return 全局唯一的广播 id
     */
    public long register(Object value) {
        Objects.requireNonNull(value, "value");
        long id = nextBroadcastId.getAndIncrement();
        registry.put(id, value);
        return id;
    }

    /**
     * 凭广播 id 取值。executor 端调用。
     *
     * @param broadcastId {@link #register} 返回的 id
     * @return 广播值，找不到时抛异常（广播值不应被提前清理）
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(long broadcastId) {
        Object value = registry.get(broadcastId);
        if (value == null) {
            throw new IllegalStateException("广播值不存在: broadcastId=" + broadcastId);
        }
        return (T) value;
    }

    /**
     * 释放一个广播值。对应真实 Spark 的 {@code Broadcast.destroy()}。
     *
     * <p>本章暂不对外暴露 destroy API，留给后续章节扩展。
     */
    void unregister(long broadcastId) {
        registry.remove(broadcastId);
    }

    /** 仅供测试观察。 */
    int size() {
        return registry.size();
    }
}
