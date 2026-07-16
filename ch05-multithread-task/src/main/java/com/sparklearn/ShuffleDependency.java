package com.sparklearn;

import java.util.Objects;

/**
 * Shuffle 依赖占位：子分区会读取多个父分区写出的 shuffle 结果。
 *
 * <p>完整的 shuffle 读写逻辑会在后续展开。
 *
 * @param <T> 父 RDD 的元素类型
 */
public final class ShuffleDependency<T> implements Dependency<T> {

    private final RDD<T> rdd;

    public ShuffleDependency(RDD<T> rdd) {
        this.rdd = Objects.requireNonNull(rdd, "rdd");
    }

    @Override
    public RDD<T> rdd() {
        return rdd;
    }
}
