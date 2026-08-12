package com.sparklearn.streaming.structured.state;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 状态存储管理器：按 {@link StateStoreId} 管理全局状态存储实例。
 *
 * <p>参考 Spark 源码：{@code StateStore} 伴生对象
 *（sql/core/.../execution/streaming/state/StateStore.scala 第 119-251 行）。
 *
 * <p>Spark 通过 {@code StateStore.get(storeId, ...)} 获取或创建 provider，
 * 每个 provider 负责一组 (checkpointLocation, operatorId, partitionId) 的状态。
 * mini-spark 简化为按 (checkpointLocation, operatorId) 维护全局唯一的内存存储，
 * 同一算子跨批次共享同一个 {@link InMemoryStateStore} 实例。
 *
 * <p>关键设计：同一算子的状态存储在批次间持久存在。第 N 批 put 的状态，
 * 第 N+1 批可以通过 get 恢复，从而实现跨批次的增量聚合。
 */
public final class StateStoreManager {

    /**
     * 全局状态存储缓存：key = "checkpointLocation/operatorId"。
     *
     * <p>Spark 中每个分区有独立的 StateStore 实例（由 partitionId 区分），
     * mini-spark 使用单进程模型，同一算子的所有分区共享一个存储实例。
     */
    private static final ConcurrentHashMap<String, StateStore> stores = new ConcurrentHashMap<>();

    private StateStoreManager() {
    }

    /**
     * 获取或创建状态存储。
     *
     * <p>同一算子（相同 checkpointLocation + operatorId）在所有批次中共享同一个 StateStore 实例。
     * 第一次调用时创建，后续调用直接返回已有实例。
     *
     * <p>对应 Spark 的 {@code StateStore.get(storeId, keySchema, valueSchema, version, ...)}：
     * Spark 按 version（batchId）创建新实例并加载历史状态；
     * mini-spark 的内存实例天然跨批次持久，不需要按版本重建。
     *
     * @param stateId 状态算子标识
     * @return 对应的 StateStore 实例
     */
    public static StateStore getOrCreate(StateStoreId stateId) {
        String key = stateId.checkpointLocation() + "/op" + stateId.operatorId();
        return stores.computeIfAbsent(key, k -> new InMemoryStateStore());
    }

    /**
     * 清除所有状态存储（测试用）。
     */
    public static void clearAll() {
        stores.clear();
    }
}
