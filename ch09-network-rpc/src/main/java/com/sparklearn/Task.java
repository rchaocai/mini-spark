package com.sparklearn;

import java.io.Serializable;
import java.util.List;

/**
 * DAGScheduler 创建的最小执行单元。
 *
 * <p>Task 是可序列化对象，由调度器提交，再由线程池或 Executor 调用 run()。
 * 这样线程池版和网络版都提交同一种执行单元，而不是提交任意 Callable。
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
