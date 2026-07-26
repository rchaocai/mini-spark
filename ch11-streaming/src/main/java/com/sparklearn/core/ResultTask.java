package com.sparklearn.core;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * ResultStage 里的单个分区任务。
 *
 * <p>它计算最终 RDD 的一个分区，再把分区结果交给当前 action 的函数处理。
 *
 * @param <T> RDD 元素类型
 * @param <U> 单个分区的返回值类型
 */
public final class ResultTask<T, U> extends Task<U> {

    private final RDD<T> rdd;
    private final Partition partition;
    private final SerializableFunction<Iterator<T>, U> partitionFunction;
    private final boolean verbose;

    public ResultTask(
            int stageId,
            RDD<T> rdd,
            Partition partition,
            SerializableFunction<Iterator<T>, U> partitionFunction,
            boolean verbose) {
        super(stageId, partition.index());
        this.rdd = Objects.requireNonNull(rdd, "rdd");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.partitionFunction = Objects.requireNonNull(
                partitionFunction, "partitionFunction");
        this.verbose = verbose;
    }

    @Override
    protected U runTask(TaskContext context) {
        if (verbose) {
            System.out.println("  [" + Thread.currentThread().getName()
                    + "] 开始计算结果分区 " + context.partition());
        }

        U result = partitionFunction.apply(rdd.iterator(partition));

        if (verbose) {
            System.out.println("  [" + Thread.currentThread().getName()
                    + "] 结果分区 " + context.partition() + " 完成");
        }
        return result;
    }

    @Override
    public List<String> preferredLocations() {
        return rdd.preferredLocations(partition);
    }

    @Override
    public String toString() {
        return "ResultTask(" + stageId() + ", " + partition.index() + ")";
    }
}
