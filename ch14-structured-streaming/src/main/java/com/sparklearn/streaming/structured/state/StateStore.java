package com.sparklearn.streaming.structured.state;

import com.sparklearn.sql.Row;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * 状态存储接口：跨微批维护聚合状态的键值存储。
 *
 * <p>参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.state.StateStore}
 *（sql/core/.../execution/streaming/state/StateStore.scala）。
 *
 * <p>Spark 的 StateStore 使用 {@code UnsafeRow} 作为键值类型，支持版本化、
 * 增量更新追踪（{@code StoreUpdate}）和 HDFS 持久化。mini-spark 简化为
 * 内存键值存储，接口保留核心语义：
 *
 * <ul>
 *   <li>{@link #get}：按 key 查询之前批次保存的聚合状态</li>
 *   <li>{@link #put}：保存当前批次的聚合状态</li>
 *   <li>{@link #commit}：提交本批次的所有更新</li>
 *   <li>{@link #iterator}：遍历全部状态条目（Complete 模式输出全量结果时使用）</li>
 * </ul>
 *
 * <p>对应 Spark StateStore 的关键方法：
 * <pre>
 *   def get(key: UnsafeRow): Option[UnsafeRow]
 *   def put(key: UnsafeRow, value: UnsafeRow): Unit
 *   def commit(): Long
 *   def iterator(): Iterator[(UnsafeRow, UnsafeRow)]
 * </pre>
 */
public interface StateStore {

    /**
     * 按 key 获取之前批次保存的聚合状态。
     *
     * <p>对应 Spark 的 {@code StateStore.get(key: UnsafeRow): Option[UnsafeRow]}。
     * 在 {@code StateStoreRestoreExec} 中调用，用于恢复上一批次的聚合状态。
     *
     * @param key 分组键对应的 Row
     * @return 之前保存的状态，不存在则返回 empty
     */
    Optional<Row> get(Row key);

    /**
     * 保存聚合状态。
     *
     * <p>对应 Spark 的 {@code StateStore.put(key, value)}。
     * 在 {@code StateStoreSaveExec} 中调用，用于保存当前批次的聚合状态。
     *
     * @param key   分组键对应的 Row
     * @param value 聚合状态对应的 Row
     */
    void put(Row key, Row value);

    /**
     * 提交本批次的所有更新。
     *
     * <p>对应 Spark 的 {@code StateStore.commit(): Long}。
     * Spark 中 commit 返回新版本号并持久化 delta 文件；
     * mini-spark 的内存实现中 commit 是空操作（put 已直接写入内存）。
     */
    void commit();

    /**
     * 遍历全部状态条目。
     *
     * <p>对应 Spark 的 {@code StateStore.iterator(): Iterator[(UnsafeRow, UnsafeRow)]}。
     * 在 Complete 输出模式中使用：commit 后遍历所有分组的状态，输出全量聚合结果。
     *
     * @return 所有 (key, value) 条目的迭代器
     */
    Iterator<Map.Entry<Row, Row>> iterator();

    /**
     * 返回当前存储的 key 数量。
     *
     * <p>对应 Spark 的 {@code StateStore.numKeys(): Long}。
     */
    long numKeys();
}
