package com.sparklearn.core.scheduler;

/**
 * Task 完成回调：TaskScheduler 在 Task 执行结束后回调此接口。
 *
 * <p>DAGScheduler 实现此接口，在回调里把事件投回自己的事件队列，
 * 让事件循环线程串行处理所有状态变更。
 */
public interface TaskCompletionHandler {

    /**
     * Task 成功完成。
     *
     * @param stageId        所属 Stage
     * @param partitionIndex 分区编号
     * @param result         Task 返回值
     */
    void taskSucceeded(int stageId, int partitionIndex, Object result);

    /**
     * Task 执行失败。
     *
     * @param stageId        所属 Stage
     * @param partitionIndex 分区编号
     * @param error          失败原因
     */
    void taskFailed(int stageId, int partitionIndex, Throwable error);
}
