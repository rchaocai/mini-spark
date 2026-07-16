package com.sparklearn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BinaryOperator;

/**
 * 单机多线程版 TaskScheduler。
 *
 * <p>每个分区创建一个 Task，提交到固定大小的线程池；Task 返回自己的结果，
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
     * 并行 collect：每个分区一个 Task，最后按分区顺序合并结果。
     */
    public <T> List<T> collect(RDD<T> rdd) {
        Objects.requireNonNull(rdd, "rdd");

        List<Future<List<T>>> futures = new ArrayList<>();
        for (Partition partition : rdd.partitions()) {
            futures.add(executor.submit(new Task<>(rdd, partition, verbose)));
        }

        List<T> result = new ArrayList<>();
        for (Future<List<T>> future : futures) {
            result.addAll(await(future));
        }
        return result;
    }

    /**
     * 并行 count：每个分区独立计数，最后由调用 action 的线程累加。
     */
    public <T> long count(RDD<T> rdd) {
        Objects.requireNonNull(rdd, "rdd");

        List<Future<Long>> futures = new ArrayList<>();
        for (Partition partition : rdd.partitions()) {
            futures.add(executor.submit(() -> countPartition(rdd, partition)));
        }

        long total = 0;
        for (Future<Long> future : futures) {
            total += await(future);
        }
        return total;
    }

    /**
     * 并行 reduce：每个分区先归并，再把各分区结果归并成最终结果。
     *
     * <p>operator 必须满足结合律，例如整数加法；否则分区内先归并会改变结果。
     */
    public <T> T reduce(RDD<T> rdd, BinaryOperator<T> operator) {
        Objects.requireNonNull(rdd, "rdd");
        Objects.requireNonNull(operator, "operator");

        List<Future<PartitionResult<T>>> futures = new ArrayList<>();
        for (Partition partition : rdd.partitions()) {
            futures.add(executor.submit(() -> reducePartition(rdd, partition, operator)));
        }

        T result = null;
        boolean hasResult = false;
        for (Future<PartitionResult<T>> future : futures) {
            PartitionResult<T> partitionResult = await(future);
            if (!partitionResult.hasValue()) {
                continue;
            }
            if (!hasResult) {
                result = partitionResult.value();
                hasResult = true;
            } else {
                result = operator.apply(result, partitionResult.value());
            }
        }

        if (!hasResult) {
            throw new NoSuchElementException("reduce on empty RDD");
        }
        return result;
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    private static <T> long countPartition(RDD<T> rdd, Partition partition) {
        long count = 0;
        Iterator<T> iterator = rdd.iterator(partition);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    private static <T> PartitionResult<T> reducePartition(
            RDD<T> rdd,
            Partition partition,
            BinaryOperator<T> operator) {
        Iterator<T> iterator = rdd.iterator(partition);
        if (!iterator.hasNext()) {
            return PartitionResult.empty();
        }

        T result = iterator.next();
        while (iterator.hasNext()) {
            result = operator.apply(result, iterator.next());
        }
        return PartitionResult.of(result);
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

    private record PartitionResult<T>(boolean hasValue, T value) {
        static <T> PartitionResult<T> empty() {
            return new PartitionResult<>(false, null);
        }

        static <T> PartitionResult<T> of(T value) {
            return new PartitionResult<>(true, value);
        }
    }
}
