package com.sparklearn.scheduler;

import java.io.Serializable;

/**
 * 单个 Task 尝试的运行上下文。
 *
 * @param stageId   所属 Stage 编号
 * @param partition 分区编号
 * @param attemptId 当前 Task 的第几次尝试
 */
public record TaskContext(
        int stageId,
        int partition,
        int attemptId,
        String executorAddress,
        String executorLocalDir) implements Serializable {

    public TaskContext(int stageId, int partition, int attemptId) {
        this(stageId, partition, attemptId, null, null);
    }
}
