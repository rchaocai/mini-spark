package com.sparklearn;

import java.util.Objects;

/**
 * Shuffle 依赖：子分区会读取多个父分区写出的 shuffle 结果。
 *
 * <p>这里不只描述血缘边，还保存一个物化 map 输出的钩子。
 * DAGScheduler 遇到这条依赖时，可以先把 shuffle map 输出写到磁盘。
 *
 * @param <T> 父 RDD 的元素类型
 */
public final class ShuffleDependency<T> implements Dependency<T> {

    private final RDD<T> rdd;
    private final Runnable materialize;

    public ShuffleDependency(RDD<T> rdd) {
        this(rdd, () -> {
        });
    }

    public ShuffleDependency(RDD<T> rdd, Runnable materialize) {
        this.rdd = Objects.requireNonNull(rdd, "rdd");
        this.materialize = Objects.requireNonNull(materialize, "materialize");
    }

    @Override
    public RDD<T> rdd() {
        return rdd;
    }

    /**
     * 写出这条 shuffle 边界上的 Map 端中间文件。
     */
    public void materialize() {
        materialize.run();
    }
}
