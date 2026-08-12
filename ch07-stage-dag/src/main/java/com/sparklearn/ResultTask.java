package com.sparklearn;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * ResultStage 里的单个分区任务。
 *
 * <p>它计算最终 RDD 的一个分区，再把分区结果交给当前 action 的函数处理。
 *
 * @param <T> RDD 元素类型
 * @param <U> 单个分区的返回值类型
 */
public final class ResultTask<T, U> implements Callable<U> {

    private final RDD<T> rdd;
    private final Partition partition;
    private final Function<Iterator<T>, U> partitionFunction;
    private final boolean verbose;

    public ResultTask(
            RDD<T> rdd,
            Partition partition,
            Function<Iterator<T>, U> partitionFunction,
            boolean verbose) {
        this.rdd = Objects.requireNonNull(rdd, "rdd");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.partitionFunction = Objects.requireNonNull(
                partitionFunction, "partitionFunction");
        this.verbose = verbose;
    }

    @Override
    public U call() {
        if (verbose) {
            System.out.println("  [" + Thread.currentThread().getName()
                    + "] 开始计算结果分区 " + partition.index());
        }

        U result = partitionFunction.apply(rdd.iterator(partition));

        if (verbose) {
            System.out.println("  [" + Thread.currentThread().getName()
                    + "] 结果分区 " + partition.index() + " 完成");
        }
        return result;
    }
}
