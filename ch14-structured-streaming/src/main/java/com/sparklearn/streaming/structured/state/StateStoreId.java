package com.sparklearn.streaming.structured.state;

import java.io.Serializable;
import java.util.Objects;

/**
 * 状态算子的唯一标识。
 *
 * <p>参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.OperatorStateId}
 *（位于 StatefulAggregate.scala）。
 *
 * <p>一个 StateStoreId 由三部分组成：
 * <ul>
 *   <li>{@code checkpointLocation}：检查点根目录，每个流式查询独立</li>
 *   <li>{@code operatorId}：算子编号，同一查询中多个状态算子递增分配</li>
 *   <li>{@code batchId}：微批编号，用于版本控制</li>
 * </ul>
 *
 * <p>Spark 中每个分区还有独立的 {@code partitionId}，mini-spark 使用单进程模型，
 * 状态存储在内存中全局共享，不需要按分区隔离。
 */
public record StateStoreId(
        String checkpointLocation,
        int operatorId,
        long batchId) implements Serializable {

    public StateStoreId {
        Objects.requireNonNull(checkpointLocation, "checkpointLocation");
    }

    @Override
    public String toString() {
        return "StateStoreId[" + checkpointLocation + "/op" + operatorId + "@batch" + batchId + "]";
    }
}
