package com.sparklearn;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 最小版 SparkContext：保存调度器，并作为 action 进入调度层的入口。
 */
public final class SparkContext implements AutoCloseable {

    private final TaskScheduler taskScheduler;

    public SparkContext(int numberOfThreads) {
        this(numberOfThreads, false);
    }

    public SparkContext(int numberOfThreads, boolean verbose) {
        this.taskScheduler = new TaskScheduler(numberOfThreads, verbose);
    }

    public <T> RDD<T> parallelize(List<T> data, int numberOfPartitions) {
        return new ListRDD<>(this, data, numberOfPartitions);
    }

    public <T, U> List<U> runJob(RDD<T> rdd, Function<List<T>, U> partitionFunction) {
        Objects.requireNonNull(rdd, "rdd");
        Objects.requireNonNull(partitionFunction, "partitionFunction");

        List<List<T>> partitions = taskScheduler.collectPartitions(rdd);
        List<U> result = new ArrayList<>();
        for (List<T> partition : partitions) {
            result.add(partitionFunction.apply(partition));
        }
        return result;
    }

    @Override
    public void close() {
        taskScheduler.close();
    }
}
