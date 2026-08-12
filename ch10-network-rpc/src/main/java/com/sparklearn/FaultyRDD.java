package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 演示用 RDD：只在指定分区外面包一层 FaultyIterator。
 *
 * <p>它仍然是一条窄依赖。故障只是读取过程中的瞬态异常，
 * 不改变数据本身，也不参与计算结果。
 *
 * @param <T> 元素类型
 */
public final class FaultyRDD<T> extends RDD<T> {

    private final RDD<T> parent;
    private final int faultyPartitionIndex;
    private final int failOnNextCall;
    private final AtomicInteger remainingFailures;
    private final List<Dependency<?>> dependencies;

    public FaultyRDD(
            RDD<T> parent,
            int faultyPartitionIndex,
            int failOnNextCall,
            AtomicInteger remainingFailures) {
        super(parent.sparkContext());
        this.parent = Objects.requireNonNull(parent, "parent");
        if (parent.partitions().stream()
                .noneMatch(partition -> partition.index() == faultyPartitionIndex)) {
            throw new IllegalArgumentException(
                    "unknown faulty partition: " + faultyPartitionIndex);
        }
        if (failOnNextCall <= 0) {
            throw new IllegalArgumentException("failOnNextCall must be positive");
        }
        this.faultyPartitionIndex = faultyPartitionIndex;
        this.failOnNextCall = failOnNextCall;
        this.remainingFailures = Objects.requireNonNull(
                remainingFailures,
                "remainingFailures");
        this.dependencies = List.of(new OneToOneDependency<>(parent));
    }

    @Override
    public List<Partition> partitions() {
        return parent.partitions();
    }

    @Override
    public Iterator<T> compute(Partition partition) {
        Iterator<T> iterator = parent.iterator(partition);
        if (partition.index() != faultyPartitionIndex) {
            return iterator;
        }
        return new FaultyIterator<>(iterator, failOnNextCall, remainingFailures);
    }

    @Override
    public List<Dependency<?>> dependencies() {
        return dependencies;
    }

    @Override
    public List<String> preferredLocations(Partition partition) {
        return parent.preferredLocations(partition);
    }
}
