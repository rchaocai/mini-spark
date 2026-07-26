package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 从内存 List 构造的源头 RDD。
 *
 * @param <T> 元素类型
 */
public final class ListRDD<T> extends RDD<T> {

    private final Supplier<Iterator<T>> supplier;
    private final List<Partition> partitions;

    public ListRDD(List<T> data) {
        Objects.requireNonNull(data, "data");
        this.supplier = data::iterator;
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
        return supplier.get();
    }

    /**
     * 源头 RDD 没有父 RDD。
     */
    @Override
    public List<Dependency<?>> dependencies() {
        return List.of();
    }
}
