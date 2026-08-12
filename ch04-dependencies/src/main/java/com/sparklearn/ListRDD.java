package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 从内存 List 构造的源头 RDD。
 *
 * @param <T> 元素类型
 */
public final class ListRDD<T> extends RDD<T> {

    private final List<T> data;
    private final List<Partition> partitions;

    public ListRDD(List<T> data) {
        Objects.requireNonNull(data, "data");
        this.data = data;
        // 目前仅支持单分区
        this.partitions = List.of(new Partition(0));
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    /**
     * 每次调用都返回一个新的迭代器，不缓存数据。
     */
    @Override
    public Iterator<T> compute(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (partition.index() != 0) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }
        return data.iterator();
    }

    /**
     * 源头 RDD 没有父 RDD。
     */
    @Override
    public List<Dependency<?>> dependencies() {
        return List.of();
    }
}
