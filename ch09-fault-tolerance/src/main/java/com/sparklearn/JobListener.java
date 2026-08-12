package com.sparklearn;

/**
 * 作业监听器：作业的每个 Task 成功时回调，整个作业失败时也回调。
 *
 * <p>JobWaiter 实现了这个接口，在收齐所有分区结果后唤醒等待的线程。
 */
public interface JobListener {

    /**
     * 某个分区的 Task 成功完成。
     *
     * @param index  分区编号
     * @param result 分区结果
     */
    void taskSucceeded(int index, Object result);

    /**
     * 整个作业失败。
     *
     * @param error 失败原因
     */
    void jobFailed(Throwable error);
}
