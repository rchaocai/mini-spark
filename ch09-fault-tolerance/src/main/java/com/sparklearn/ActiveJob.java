package com.sparklearn;

import java.util.Iterator;
import java.util.function.Function;

/**
 * 正在运行的作业：记录 jobId、finalStage、分区处理函数和等待结果的 listener。
 *
 * @param jobId              作业编号（同时作为优先级）
 * @param finalStage         最终的 ResultStage
 * @param partitionFunction  每个分区的处理函数（类型擦除后存入）
 * @param waiter             等待结果的 JobListener（通常是 JobWaiter）
 */
public record ActiveJob(
        int jobId,
        Stage finalStage,
        Function<Iterator<?>, ?> partitionFunction,
        JobListener waiter
) {
}
