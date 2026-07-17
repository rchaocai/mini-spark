package com.sparklearn;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 单机多线程版 TaskScheduler。
 *
 * <p>每个分区创建一个独立计算，提交到固定大小的线程池；分区计算返回自己的结果，
 * 调用 action 的线程统一合并。
 */
public final class TaskScheduler implements AutoCloseable {

    private final ExecutorService executor;
    private final boolean verbose;

    public TaskScheduler(int numberOfThreads) {
        this(numberOfThreads, false);
    }

    public TaskScheduler(int numberOfThreads, boolean verbose) {
        if (numberOfThreads <= 0) {
            throw new IllegalArgumentException("numberOfThreads must be positive");
        }
        this.executor = Executors.newFixedThreadPool(numberOfThreads);
        this.verbose = verbose;
    }

    /**
     * 并行运行所有分区，每个分区返回一个独立结果列表。
     */
    public <T> List<List<T>> collectPartitions(RDD<T> rdd) {
        Objects.requireNonNull(rdd, "rdd");

        List<Future<List<T>>> futures = new ArrayList<>();
        for (Partition partition : rdd.partitions()) {
            futures.add(executor.submit(new CollectTask<>(rdd, partition, verbose)));
        }

        List<List<T>> result = new ArrayList<>();
        for (Future<List<T>> future : futures) {
            result.add(await(future));
        }
        return result;
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    private static <T> T await(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("task interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("task failed", e.getCause());
        }
    }

}
