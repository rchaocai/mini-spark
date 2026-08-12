package com.sparklearn.core.scheduler;

import com.sparklearn.core.executor.TaskExecutionEnvironment;
import com.sparklearn.core.scheduler.TaskContext;

import java.io.Serializable;
import java.util.List;

/**
 * DAGScheduler 创建的最小执行单元。
 *
 * <p>Task 是可序列化对象，由调度器提交，再由线程池或 Executor 调用 run()。
 * 这样线程池版和网络版都提交同一种执行单元，而不是提交任意 Callable。
 *
 * <p>{@code preferredLocations} 由 DAGScheduler 在创建 Task 时计算好：它综合了
 * 缓存位置（BlockManagerMaster 中记录的 Executor 地址）和 RDD 自身的偏好位置。
 * NetworkTaskScheduler 根据它选择最优 Executor，实现缓存感知的数据本地性。
 *
 * @param <T> Task 返回值类型
 */
public abstract class Task<T> implements Serializable {

    private final int stageId;
    private final int partition;
    private final List<String> preferredLocations;

    protected Task(int stageId, int partition) {
        this(stageId, partition, List.of());
    }

    protected Task(int stageId, int partition, List<String> preferredLocations) {
        this.stageId = stageId;
        this.partition = partition;
        this.preferredLocations = preferredLocations == null
                ? List.of()
                : List.copyOf(preferredLocations);
    }

    public final T run(int attemptId) {
        TaskExecutionEnvironment.Environment environment =
                TaskExecutionEnvironment.current();
        TaskContext context = new TaskContext(
                stageId,
                partition,
                attemptId,
                environment == null ? null : environment.executorAddress(),
                environment == null ? null : environment.localDir());
        TaskExecutionEnvironment.setTaskContext(context);
        try {
            return runTask(context);
        } finally {
            TaskExecutionEnvironment.clearTaskContext();
        }
    }

    protected abstract T runTask(TaskContext context);

    public List<String> preferredLocations() {
        return preferredLocations;
    }

    protected final int stageId() {
        return stageId;
    }

    protected final int partition() {
        return partition;
    }
}
