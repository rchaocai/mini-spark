package com.sparklearn.core.storage;

import java.io.Serializable;

/**
 * 广播变量句柄：executor 端凭它拿到 driver 广播的值。
 *
 * <p>对应真实 Spark 的 {@code Broadcast[T]}。用户在 driver 端调
 * {@code SparkContext.broadcast(value)} 拿到一个 {@code Broadcast<T>} 对象，
 * 之后在 Task 闭包里引用它，executor 端调 {@link #value()} 取到广播的值。
 *
 * <p><b>序列化设计</b>：本对象只持有 {@code broadcastId}（一个 long），
 * 不持有值本身。Task 序列化到 executor 时，只传 {@code broadcastId}；
 * executor 端反序列化后，凭 id 从 {@link BroadcastManager} 取值。
 * 这正是广播变量区别于"闭包里直接塞个大对象"的关键——
 * 无论多少个 Task 引用同一个广播变量，大对象在序列化链路上只传一次。
 *
 * <p>真实 Spark 在 executor 端通过 {@code BlockManager} 按 {@code BroadcastBlockId}
 * 取本地副本，未命中则通过 RPC 从 driver 拉取，并支持 torrent 分块拉取。
 * 本章单机同 JVM，直接从 {@link BroadcastManager#getInstance()} 取。
 *
 * @param broadcastId driver 端 {@link BroadcastManager#register} 返回的全局唯一 id
 * @param <T> 广播值类型
 */
public record Broadcast<T>(long broadcastId) implements Serializable {

    /**
     * 取到广播的值。
     *
     * <p>executor 端调用：凭 {@code broadcastId} 从 {@link BroadcastManager} 取。
     * driver 端也能调（同 JVM，注册表共享）。
     */
    public T value() {
        return BroadcastManager.getInstance().getValue(broadcastId);
    }
}
