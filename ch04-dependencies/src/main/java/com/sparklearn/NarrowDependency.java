package com.sparklearn;

import java.util.List;
import java.util.Objects;

/**
 * 窄依赖：子 RDD 的每个分区只依赖父 RDD 的有限几个分区。
 *
 * @param <T> 父 RDD 的元素类型
 */
public abstract non-sealed class NarrowDependency<T> implements Dependency<T> {

    private final RDD<T> rdd;

    protected NarrowDependency(RDD<T> rdd) {
        this.rdd = Objects.requireNonNull(rdd, "rdd");
    }

    @Override
    public RDD<T> rdd() {
        return rdd;
    }

    /**
     * 子 RDD 的第 outputPartition 个分区，依赖父 RDD 的哪些分区。
     */
    public abstract List<Integer> getParents(int outputPartition);
}
