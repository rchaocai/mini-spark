package com.sparklearn;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 单机多线程版 TaskScheduler。
 *
 * <p>DAGScheduler 创建好一批 Task 后，TaskScheduler 负责把它们提交到固定大小的
 * 线程池，并按提交顺序收集结果。
 */
public final class TaskScheduler implements AutoCloseable {

    private final ExecutorService executor;
    private final int maxTaskRetries;
    private final boolean verbose;

    public TaskScheduler(int numberOfThreads) {
        this(numberOfThreads, 3, false);
    }

    public TaskScheduler(int numberOfThreads, int maxTaskRetries) {
        this(numberOfThreads, maxTaskRetries, false);
    }

    public TaskScheduler(
            int numberOfThreads,
            int maxTaskRetries,
            boolean verbose) {
        if (numberOfThreads <= 0) {
            throw new IllegalArgumentException("numberOfThreads must be positive");
        }
        if (maxTaskRetries < 0) {
            throw new IllegalArgumentException("maxTaskRetries must not be negative");
        }
        this.executor = Executors.newFixedThreadPool(numberOfThreads);
        this.maxTaskRetries = maxTaskRetries;
        this.verbose = verbose;
    }

    /**
     * 把一批任务提交到线程池，并按任务的提交顺序返回结果。
     *
     * <p>某个任务失败时，只重新提交这个任务。重新执行 call() 会再次调用
     * rdd.iterator(partition)，从而沿血缘为该分区创建一条新的迭代器链。
     */
    public <T> List<T> submitTasks(List<? extends Callable<T>> tasks) {
        Objects.requireNonNull(tasks, "tasks");

        List<Future<T>> futures = new ArrayList<>();
        for (Callable<T> task : tasks) {
            futures.add(executor.submit(task));
        }

        List<T> result = new ArrayList<>();
        for (int index = 0; index < futures.size(); index++) {
            try {
                result.add(awaitWithRetry(
                        tasks.get(index),
                        futures.get(index)));
            } catch (FetchFailedException e) {
                awaitRemainingTasks(futures, index + 1);
                throw e;
            }
        }
        return result;
    }

    @Override
    public void close() {
        executor.shutdown();
    }

    private <T> T awaitWithRetry(
            Callable<T> task,
            Future<T> initialFuture) {
        Future<T> future = initialFuture;
        int retries = 0;

        while (true) {
            try {
                return future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("task interrupted", e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof FetchFailedException fetchFailure) {
                    throw fetchFailure;
                }
                if (retries >= maxTaskRetries) {
                    throw new IllegalStateException(
                            task + " failed after "
                                    + (retries + 1) + " attempts",
                            e.getCause());
                }

                retries++;
                if (verbose) {
                    System.out.println("  [重试] " + task
                            + " 失败: " + e.getCause().getMessage()
                            + "，开始第 " + retries + " 次重试");
                }
                future = executor.submit(task);
            }
        }
    }

    /**
     * Fetch 失败要回到 DAGScheduler 重写 Map 输出。
     * 在此之前先等同批任务退出，避免它们一边读旧文件、一边被恢复任务覆盖。
     */
    private static void awaitRemainingTasks(
            List<? extends Future<?>> futures,
            int startIndex) {
        for (int index = startIndex; index < futures.size(); index++) {
            try {
                futures.get(index).get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("task interrupted", e);
            } catch (ExecutionException ignored) {
                // 当前 Stage 即将重新提交，这批任务的其他失败不再单独处理。
            }
        }
    }
}
