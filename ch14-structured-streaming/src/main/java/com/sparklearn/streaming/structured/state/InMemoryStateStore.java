package com.sparklearn.streaming.structured.state;

import com.sparklearn.sql.Row;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于内存的状态存储实现。
 *
 * <p>参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider}
 *（sql/core/.../execution/streaming/state/HDFSBackedStateStoreProvider.scala）。
 *
 * <p>Spark 使用 HDFS 文件（delta + snapshot）持久化状态，支持容错恢复。
 * mini-spark 使用内存 {@link LinkedHashMap} 存储状态，不涉及持久化，
 * 适用于教学和测试场景。
 *
 * <p>键的相等性判断基于 {@link Row#values()}（返回 {@code List<Object>}），
 * 因为 {@code List} 具有标准的 {@code equals}/{@code hashCode} 实现，
 * 而 {@link Row} 本身未重写这两个方法。
 */
public class InMemoryStateStore implements StateStore {

    /**
     * 内部存储：key 的 values 列表 → (keyRow, valueRow)。
     * 使用 LinkedHashMap 保持插入顺序，方便 Complete 模式按顺序遍历输出。
     */
    private final LinkedHashMap<List<Object>, Map.Entry<Row, Row>> store = new LinkedHashMap<>();

    @Override
    public Optional<Row> get(Row key) {
        Map.Entry<Row, Row> entry = store.get(key.values());
        return entry != null ? Optional.of(entry.getValue()) : Optional.empty();
    }

    @Override
    public void put(Row key, Row value) {
        store.put(key.values(), Map.entry(key, value));
    }

    @Override
    public void commit() {
        // 内存实现中 put 已直接写入，commit 无需额外操作
    }

    @Override
    public Iterator<Map.Entry<Row, Row>> iterator() {
        return store.values().iterator();
    }

    @Override
    public long numKeys() {
        return store.size();
    }
}
