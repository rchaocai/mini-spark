package com.sparklearn;

import java.io.Serializable;
import java.util.List;

/**
 * DAGScheduler 创建的最小执行单元。
 *
 * <p>真实 Spark 的 Task 也是可序列化对象，并由 Executor 调用 run()。
 * 本章保留同样的骨架，让线程池版和网络版都提交 Task，而不是提交任意 Callable。
 *
 * @param <T> Task 返回值类型
 */
public abstract class Task<T> implements Serializable {

    private final int stageId;
    private final int partition;

    protected Task(int stageId, int partition) {
        this.stageId = stageId;
        this.partition = partition;
    }

    public final T run(int attemptId) {
        return runTask(new TaskContext(stageId, partition, attemptId));
    }

    protected abstract T runTask(TaskContext context);

    public List<String> preferredLocations() {
        return List.of();
    }

    protected final int stageId() {
        return stageId;
    }

    protected final int partition() {
        return partition;
    }
}

