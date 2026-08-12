package com.sparklearn;

import java.util.Objects;

/**
 * Shuffle 依赖占位：子分区会读取多个父分区写出的 shuffle 结果。
 *
 * <p>这里只标出依赖类型，不包含 shuffle 的读写逻辑。
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
