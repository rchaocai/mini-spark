package com.sparklearn.scheduler;

import com.sparklearn.executor.TaskExecutionEnvironment;
import com.sparklearn.storage.BlockManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 单机多线程版 TaskScheduler。
 *
 * <p>本章的核心变化：submitTasks 不再同步阻塞地等待所有 Task 完成，
 * 而是把 Task 提交到线程池后立即返回。每个 Task 完成时，通过
 * {@link TaskCompletionHandler} 回调通知 DAGScheduler——DAGScheduler
 * 再把事件投回自己的事件队列，串行处理。
 *
 * <p>这样 TaskScheduler 只管"执行"，DAGScheduler 只管"调度"与"容错"，
 * 两者通过回调解耦。Task 失败不再由 TaskScheduler 重试，而是上报给
 * DAGScheduler，由它在事件循环里决定重试同一个 Task 还是回退到父 Stage。
 */
public final class LocalTaskScheduler implements TaskScheduler {

    private final ExecutorService executor;
    private final BlockManager blockManager;

    public LocalTaskScheduler(int numberOfThreads) {
        if (numberOfThreads <= 0) {
            throw new IllegalArgumentException("numberOfThreads must be positive");
        }
        this.executor = Executors.newFixedThreadPool(numberOfThreads);
        this.blockManager = new BlockManager(createLocalDir());
    }

    private static Path createLocalDir() {
        try {
            return Files.createTempDirectory("mini-spark-local-");
        } catch (IOException e) {
            throw new IllegalStateException("无法创建本地缓存目录", e);
        }
    }

    @Override
    public void submitTasks(
            List<? extends Task<?>> tasks,
            int stageId,
            TaskCompletionHandler handler) {
        Objects.requireNonNull(tasks, "tasks");
        Objects.requireNonNull(handler, "handler");
        for (int i = 0; i < tasks.size(); i++) {
            submitTask(tasks.get(i), stageId, i, handler);
        }
    }

    @Override
    public void submitTask(
            Task<?> task,
            int stageId,
            int partitionIndex,
            TaskCompletionHandler handler) {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(handler, "handler");
        executor.submit(() -> {
            TaskExecutionEnvironment.set("local", null, blockManager);
            try {
                Object result = task.run(0);
                handler.taskSucceeded(stageId, partitionIndex, result);
            } catch (Throwable e) {
                handler.taskFailed(stageId, partitionIndex, e);
            } finally {
                TaskExecutionEnvironment.clear();
            }
        });
    }

    @Override
    public void removeRdd(int rddId) {
        blockManager.removeRdd(rddId);
    }

    @Override
    public void close() {
        executor.shutdown();
        blockManager.clear();
    }
}
