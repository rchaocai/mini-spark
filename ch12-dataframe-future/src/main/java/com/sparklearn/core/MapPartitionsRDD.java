package com.sparklearn.core;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 对父 RDD 的某个分区迭代器做变换。
 *
 * @param <T> 父 RDD 的元素类型
 * @param <U> 当前 RDD 的元素类型
 */
public final class MapPartitionsRDD<T, U> extends RDD<U> {

    private final RDD<T> parent;
    private final SerializableFunction<Iterator<T>, Iterator<U>> iteratorTransform;
    private final List<Partition> partitions;
    private final List<Dependency<?>> dependencies;

    public MapPartitionsRDD(
            RDD<T> parent,
            SerializableFunction<Iterator<T>, Iterator<U>> iteratorTransform) {
        super(parent.sparkContext());
        this.parent = Objects.requireNonNull(parent, "parent");
        this.iteratorTransform = Objects.requireNonNull(
                iteratorTransform,
                "iteratorTransform");
        this.partitions = parent.partitions();
        this.dependencies = List.of(new OneToOneDependency<>(parent));
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    /**
     * 先读取父 RDD 的同号分区，再返回包装后的迭代器。
     */
    @Override
    public Iterator<U> compute(Partition partition) {
        Iterator<T> parentIterator = parent.iterator(partition);
        return iteratorTransform.apply(parentIterator);
    }

    @Override
    protected List<Dependency<?>> getDependenciesInternal() {
        return dependencies;
    }

    @Override
    public List<String> preferredLocations(Partition partition) {
        return parent.preferredLocations(partition);
    }
}
