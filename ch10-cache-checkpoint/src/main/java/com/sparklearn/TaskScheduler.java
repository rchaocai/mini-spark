package com.sparklearn;

import java.util.List;

/**
 * 底层任务调度接口。
 *
 * <p>DAGScheduler 把每个 Stage 的 Task 交给它；具体实现再决定是本地线程池执行，
 * 还是发到 Executor 执行。
 */
public interface TaskScheduler extends AutoCloseable {

    <T> List<T> submitTasks(List<? extends Task<T>> tasks);

    @Override
    void close();
}
