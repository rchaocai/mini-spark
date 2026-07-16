package com.sparklearn;

import java.util.Objects;

/**
 * 宽依赖占位：子分区可能依赖父 RDD 的多个分区。
 *
 * <p>完整的 shuffle 读写逻辑会在第 6 章展开。
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
