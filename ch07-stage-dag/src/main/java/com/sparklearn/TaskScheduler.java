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

    public TaskScheduler(int numberOfThreads) {
        if (numberOfThreads <= 0) {
            throw new IllegalArgumentException("numberOfThreads must be positive");
        }
        this.executor = Executors.newFixedThreadPool(numberOfThreads);
    }

    /**
     * 把一批任务提交到线程池，并按任务的提交顺序返回结果。
     */
    public <T> List<T> submitTasks(List<? extends Callable<T>> tasks) {
        Objects.requireNonNull(tasks, "tasks");

        List<Future<T>> futures = new ArrayList<>();
        for (Callable<T> task : tasks) {
            futures.add(executor.submit(task));
        }

        List<T> result = new ArrayList<>();
        for (Future<T> future : futures) {
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
