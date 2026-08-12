package com.sparklearn.streaming.structured;

import java.util.Optional;

/**
 * 偏移量元数据日志，持久化记录每个微批的偏移量，支持重启后恢复进度。
 * <p>
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.MetadataLog}
 *
 * @param <T> 元数据类型（如 {@link OffsetSeq}）
 */
public interface MetadataLog<T> {

    /**
     * 写入指定 batchId 的元数据。
     * <p>
     * 对应 Spark {@code MetadataLog.add}：如果 batchId 已存在则返回 false，
     * 否则原子写入（临时文件 → rename）并返回 true。
     *
     * @return 如果是新写入返回 true，如果 batchId 已存在返回 false
     */
    boolean add(long batchId, T metadata);

    /**
     * 读取指定 batchId 的元数据。
     * <p>
     * 对应 Spark {@code MetadataLog.get}。
     *
     * @return 元数据；如果 batchId 不存在返回空
     */
    Optional<T> get(long batchId);

    /**
     * 获取最新的 batchId 及其元数据。
     * <p>
     * 对应 Spark {@code MetadataLog.getLatest}：按 batchId 倒序查找，
     * 返回第一个能成功反序列化的条目。
     *
     * @return 最新条目；如果日志为空返回空
     */
    Optional<java.util.Map.Entry<Long, T>> getLatest();

    /**
     * 清理指定 batchId 之前的所有元数据（不含 batchId 本身）。
     * <p>
     * 对应 Spark {@code MetadataLog.purge}：删除所有 {@code < threshold} 的文件。
     */
    void purge(long threshold);
}
