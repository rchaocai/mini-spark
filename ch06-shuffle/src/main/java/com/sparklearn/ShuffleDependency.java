package com.sparklearn;

import java.util.Objects;

/**
 * Shuffle 依赖：下游分区需要读取多个父分区写出的中间结果。
 *
 * <p>这里记录父 RDD；具体的文件读写由 ShuffledRDD 完成。
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
