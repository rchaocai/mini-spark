package com.sparklearn;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
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
 * <p>这样 TaskScheduler 只管"执行"，DAGScheduler 只管"调度"，
 * 两者通过回调解耦，多个作业的 Task 可以在线程池里交替并行跑。
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
     * 把一批 Task 提交到线程池，立即返回（非阻塞）。
     *
     * <p>每个 Task 完成后，回调 {@link TaskCompletionHandler}：
     * <ul>
     *   <li>成功 → {@link TaskCompletionHandler#taskSucceeded(int, int, Object)}</li>
     *   <li>失败 → {@link TaskCompletionHandler#taskFailed(int, int, Throwable)}</li>
     * </ul>
     *
     * @param tasks       要执行的 Task 列表（按分区编号排列）
     * @param stageId     所属 Stage 编号
     * @param handler     完成回调
     */
    public void submitTasks(
            List<? extends Callable<?>> tasks,
            int stageId,
            TaskCompletionHandler handler) {
        Objects.requireNonNull(tasks, "tasks");
        Objects.requireNonNull(handler, "handler");

        for (int i = 0; i < tasks.size(); i++) {
            final int partitionIndex = i;
            Callable<?> task = tasks.get(i);
            executor.submit(() -> {
                try {
                    Object result = task.call();
                    handler.taskSucceeded(stageId, partitionIndex, result);
                } catch (Throwable e) {
                    handler.taskFailed(stageId, partitionIndex, e);
                }
            });
        }
    }

    @Override
    public void close() {
        executor.shutdown();
    }
}
