package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 最小版 SparkContext：保存调度器，并作为 action 进入 DAG 调度层的入口。
 */
public final class SparkContext implements AutoCloseable {

    private static final int DEFAULT_MAX_TASK_RETRIES = 3;

    private final TaskScheduler taskScheduler;
    private final DAGScheduler dagScheduler;

    public SparkContext(int numberOfThreads) {
        this(numberOfThreads, DEFAULT_MAX_TASK_RETRIES, false);
    }

    public SparkContext(int numberOfThreads, boolean verbose) {
        this(numberOfThreads, DEFAULT_MAX_TASK_RETRIES, verbose);
    }

    public SparkContext(
            int numberOfThreads,
            int maxTaskRetries,
            boolean verbose) {
        this(new LocalTaskScheduler(numberOfThreads, maxTaskRetries, verbose), verbose);
    }

    public SparkContext(TaskScheduler taskScheduler, boolean verbose) {
        this.taskScheduler = Objects.requireNonNull(taskScheduler, "taskScheduler");
        this.dagScheduler = new DAGScheduler(verbose);
    }

    public <T> RDD<T> parallelize(List<T> data, int numberOfPartitions) {
        return new ListRDD<>(this, data, numberOfPartitions);
    }

    public <T> RDD<T> parallelize(
            List<T> data,
            int numberOfPartitions,
            List<List<String>> preferredLocations) {
        return new ListRDD<>(
                this,
                data,
                numberOfPartitions,
                preferredLocations);
    }

    public <T, U> List<U> runJob(
            RDD<T> rdd,
            SerializableFunction<Iterator<T>, U> partitionFunction) {
        Objects.requireNonNull(rdd, "rdd");
        Objects.requireNonNull(partitionFunction, "partitionFunction");
        return dagScheduler.runJob(rdd, taskScheduler, partitionFunction);
    }

    @Override
    public void close() {
        taskScheduler.close();
    }
}
