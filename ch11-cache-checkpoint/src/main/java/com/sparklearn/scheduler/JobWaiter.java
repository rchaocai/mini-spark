package com.sparklearn.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 等待一个作业的全部 Task 完成，并收集每个分区的结果。
 *
 * <p>runJob 在调用线程里构造 JobWaiter，把 JobSubmitted 事件投进事件队列后，
 * 调用 {@link #awaitResult()} 阻塞自己。事件循环线程在处理 TaskCompleted 时
 * 回调 {@link #taskSucceeded}，当全部分区收齐时 notifyAll 唤醒调用线程。
 *
 * <p>这样调用线程虽然阻塞，但 DAGScheduler 的事件循环线程是自由的——
 * 它可以继续处理别的 JobSubmitted，让多个作业的 Task 在线程池里并行跑。
 *
 * @param <T> 单个分区的结果类型
 */
public final class JobWaiter<T> implements JobListener {

    private final int totalTasks;
    private final List<T> results;
    private int finishedTasks = 0;
    private boolean jobFinished = false;
    private Throwable error = null;

    public JobWaiter(int totalTasks) {
        this.totalTasks = totalTasks;
        this.results = new ArrayList<>(Collections.nCopies(totalTasks, null));
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void taskSucceeded(int index, Object result) {
        if (jobFinished) {
            throw new IllegalStateException("taskSucceeded() called on a finished JobWaiter");
        }
        results.set(index, (T) result);
        finishedTasks++;
        if (finishedTasks == totalTasks) {
            jobFinished = true;
            notifyAll();
        }
    }

    @Override
    public synchronized void jobFailed(Throwable error) {
        if (jobFinished) {
            throw new IllegalStateException("jobFailed() called on a finished JobWaiter");
        }
        jobFinished = true;
        this.error = error;
        notifyAll();
    }

    /**
     * 阻塞当前线程，直到作业完成（成功或失败）。
     *
     * @return 各分区的结果列表（按分区编号排列）
     * @throws RuntimeException 如果作业失败
     */
    public synchronized List<T> awaitResult() {
        while (!jobFinished) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Job interrupted", e);
            }
        }
        if (error != null) {
            throw new RuntimeException("Job failed", error);
        }
        return new ArrayList<>(results);
    }
}
