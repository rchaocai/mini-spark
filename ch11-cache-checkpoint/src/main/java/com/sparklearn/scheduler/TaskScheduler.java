package com.sparklearn.scheduler;

import com.sparklearn.storage.BlockManagerMaster;

import java.util.List;

/**
 * 底层任务调度接口。
 *
 * <p>DAGScheduler 把每个 Stage 的 Task 交给它；具体实现再决定是本地线程池执行，
 * 还是发到 Executor 执行。
 *
 * <p>本章的变化：submitTasks 不再同步阻塞地等待所有 Task 完成，而是把 Task 提交后
 * 立即返回。每个 Task 完成时，通过 {@link TaskCompletionHandler} 回调通知 DAGScheduler。
 * Task 失败不再由 TaskScheduler 重试，而是上报给 DAGScheduler，由它在事件循环里
 * 决定重试同一个 Task 还是回退到父 Stage。
 */
public interface TaskScheduler extends AutoCloseable {

    /**
     * 把一批 Task 提交执行，立即返回（非阻塞）。
     *
     * <p>每个 Task 完成后，回调 {@link TaskCompletionHandler}：
     * <ul>
     *   <li>成功 → {@link TaskCompletionHandler#taskSucceeded(int, int, Object)}</li>
     *   <li>失败 → {@link TaskCompletionHandler#taskFailed(int, int, Throwable)}</li>
     * </ul>
     *
     * @param tasks       要执行的 Task 列表（按分区编号排列，列表索引即 partitionIndex）
     * @param stageId     所属 Stage 编号
     * @param handler     完成回调
     */
    void submitTasks(
            List<? extends Task<?>> tasks,
            int stageId,
            TaskCompletionHandler handler);

    /**
     * 提交单个 Task，使用显式分区编号（用于 Task 重试或 Fetch 恢复后重提失败 Task）。
     *
     * <p>与 {@link #submitTasks} 不同，此方法的 partitionIndex 不依赖列表索引，
     * 而是由调用方显式指定。这样重试分区 1 的 Task 时，回调里拿到的 partitionIndex 仍是 1。
     *
     * @param task            要执行的 Task
     * @param stageId         所属 Stage 编号
     * @param partitionIndex  分区编号
     * @param handler         完成回调
     */
    void submitTask(
            Task<?> task,
            int stageId,
            int partitionIndex,
            TaskCompletionHandler handler);

    /** 删除各执行端属于某个 RDD 的缓存块。 */
    default void removeRdd(int rddId) {
        // 不维护缓存的调度器无需处理。
    }

    /**
     * 注入 Driver 端的 BlockManagerMaster，用于回传缓存块位置。
     *
     * <p>只有 NetworkTaskScheduler 需要实现：它在收到 Executor 的 Task 结果时，
     * 把结果里携带的 BlockId 列表注册到 BlockManagerMaster，让 DAGScheduler 后续
     * 能根据缓存位置选择最优 Executor。
     *
     * <p>LocalTaskScheduler 无需实现：所有 Task 共用一个进程内的 BlockManager，
     * 缓存本地性天然成立。
     */
    default void setBlockManagerMaster(BlockManagerMaster master) {
        // 默认 no-op
    }

    @Override
    void close();
}
