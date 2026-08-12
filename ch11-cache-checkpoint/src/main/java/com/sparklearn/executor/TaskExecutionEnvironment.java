package com.sparklearn.executor;

import com.sparklearn.scheduler.TaskContext;
import com.sparklearn.storage.BlockManager;

/**
 * Executor 工作线程持有的运行时信息。
 *
 * <p>它类似 SparkEnv 的一个极小切片：Task 本身仍是纯序列化对象，真正执行时再由
 * Executor 注入对外地址和本地磁盘目录。
 */
public final class TaskExecutionEnvironment {

    private static final ThreadLocal<Environment> CURRENT = new ThreadLocal<>();
    private static final ThreadLocal<TaskContext> CURRENT_TASK_CONTEXT = new ThreadLocal<>();

    private TaskExecutionEnvironment() {
    }

    public static void set(String executorAddress, String localDir, BlockManager blockManager) {
        CURRENT.set(new Environment(executorAddress, localDir, blockManager));
    }

    public static Environment current() {
        return CURRENT.get();
    }

    public static void setTaskContext(TaskContext context) {
        CURRENT_TASK_CONTEXT.set(context);
    }

    public static TaskContext taskContext() {
        return CURRENT_TASK_CONTEXT.get();
    }

    public static void clearTaskContext() {
        CURRENT_TASK_CONTEXT.remove();
    }

    public static void clear() {
        CURRENT.remove();
        CURRENT_TASK_CONTEXT.remove();
    }

    public record Environment(String executorAddress, String localDir, BlockManager blockManager) {
    }
}
