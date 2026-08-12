package com.sparklearn;

import java.util.Iterator;
import java.util.function.Function;

/**
 * DAGScheduler 处理的事件类型。
 *
 * <p>采用事件队列架构：任何线程都可以往队列里投递事件（比如 Task 完成、新 Job 提交），
 * 但只有 DAGScheduler 的单一事件循环线程会读取并处理它们。这样所有调度决策都不需要加锁。
 *
 * @see DAGScheduler
 */
public sealed interface DAGSchedulerEvent {

    /**
     * 新作业提交：把最终 RDD 和处理函数交给 DAGScheduler。
     *
     * @param rdd               最终 RDD
     * @param partitionFunction 每个分区的处理函数
     * @param jobId             作业编号（同时作为优先级，越小越先）
     * @param waiter            等待结果的 JobWaiter
     */
    record JobSubmitted(
            RDD<?> rdd,
            Function<Iterator<?>, ?> partitionFunction,
            int jobId,
            JobListener waiter
    ) implements DAGSchedulerEvent {
    }

    /**
     * 一个 Task 成功完成。
     *
     * @param stageId        所属 Stage
     * @param partitionIndex 分区编号
     * @param result         Task 的返回值（ShuffleMapTask 为 null，ResultTask 为分区结果）
     */
    record TaskCompleted(
            int stageId,
            int partitionIndex,
            Object result
    ) implements DAGSchedulerEvent {
    }

    /**
     * 一个 Task 失败。
     *
     * @param stageId        所属 Stage
     * @param partitionIndex 分区编号
     * @param error          失败原因
     */
    record TaskFailed(
            int stageId,
            int partitionIndex,
            Throwable error
    ) implements DAGSchedulerEvent {
    }

    /**
     * 停止 DAGScheduler 的事件循环。
     */
    record StopDAGScheduler() implements DAGSchedulerEvent {
    }
}
