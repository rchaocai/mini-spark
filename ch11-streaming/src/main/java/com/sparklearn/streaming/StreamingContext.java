package com.sparklearn.streaming;

import com.sparklearn.core.RDD;
import com.sparklearn.core.SparkContext;
import com.sparklearn.streaming.dstream.QueueInputDStream;
import com.sparklearn.streaming.dstream.SocketInputDStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Spark Streaming 入口。
 *
 * <p>它不替代 {@link SparkContext}，而是站在 RDD 内核之上：
 * 按固定 batch 间隔推进逻辑时间，让每个 DStream 生成对应 RDD，
 * 再把输出操作提交成普通 Spark job。
 */
public final class StreamingContext implements AutoCloseable {

    private final SparkContext sparkContext;
    private final Duration batchDuration;
    private final DStreamGraph graph = new DStreamGraph();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private Time currentTime;
    private int batchesStarted;

    public StreamingContext(SparkContext sparkContext, Duration batchDuration) {
        this.sparkContext = Objects.requireNonNull(sparkContext, "sparkContext");
        this.batchDuration = Objects.requireNonNull(batchDuration, "batchDuration");
        this.graph.setBatchDuration(batchDuration);
        // 从 0 起算，第一个 batch 落在 batchDuration。
        this.currentTime = new Time(0L);
    }

    public SparkContext sparkContext() {
        return sparkContext;
    }

    public Duration batchDuration() {
        return batchDuration;
    }

    public Time currentTime() {
        return currentTime;
    }

    public int batchesStarted() {
        return batchesStarted;
    }

    void registerInputStream(InputDStream<?> inputStream) {
        graph.addInputStream(inputStream);
    }

    void registerOutputStream(DStream<?> outputStream) {
        graph.addOutputStream(outputStream);
    }

    /**
     * 从 RDD 队列创建输入流。每个 batch 默认只取一个 RDD。
     */
    public <T> DStream<T> queueStream(Queue<RDD<T>> queue) {
        return queueStream(queue, true, sparkContext.parallelize(Collections.<T>emptyList(), 1));
    }

    public <T> DStream<T> queueStream(Queue<RDD<T>> queue, boolean oneAtATime) {
        return queueStream(queue, oneAtATime, sparkContext.parallelize(Collections.<T>emptyList(), 1));
    }

    public <T> DStream<T> queueStream(Queue<RDD<T>> queue, boolean oneAtATime, RDD<T> defaultRDD) {
        return new QueueInputDStream<>(this, queue, oneAtATime, defaultRDD);
    }

    /**
     * 连到 host:port，把从 socket 读到的文本行接成一条输入流。
     * 每个 batch 拿到的是这个 batch 窗口内通过 socket 到达的行。
     */
    public DStream<String> socketTextStream(String host, int port) {
        return new SocketInputDStream(this, host, port);
    }

    /**
     * 启动流计算图。教学版不挂真实时钟线程，改由 {@link #advance()} 手动推进。
     */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("StreamingContext already started");
        }
        if (stopped.get()) {
            throw new IllegalStateException("StreamingContext already stopped");
        }
        graph.start(currentTime);
        System.out.println("StreamingContext started, batchDuration=" + batchDuration
                + ", zeroTime=" + currentTime);
    }

    /**
     * 推进一个 batch：时间 + batchDuration，生成并执行该时刻的输出 job。
     *
     * @return 本 batch 执行的 job 数；没有数据时可能为 0
     */
    public int advance() {
        ensureStarted();
        currentTime = currentTime.plus(batchDuration);
        batchesStarted++;
        List<StreamingJob> jobs = graph.generateJobs(currentTime);
        System.out.println("=== batch @" + currentTime + " jobs=" + jobs.size() + " ===");
        for (StreamingJob job : jobs) {
            job.run();
        }
        // 清理过旧 batch 的元数据，避免无限堆积。
        for (DStream<?> output : graph.outputStreams()) {
            output.clearOldMetadata(currentTime);
        }
        return jobs.size();
    }

    /**
     * 连续推进若干个 batch。
     */
    public void advance(int batches) {
        if (batches < 0) {
            throw new IllegalArgumentException("batches must be non-negative");
        }
        for (int i = 0; i < batches; i++) {
            advance();
        }
    }

    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        if (started.get()) {
            graph.stop();
        }
        System.out.println("StreamingContext stopped after " + batchesStarted + " batches");
    }

    @Override
    public void close() {
        stop();
        sparkContext.close();
    }

    private void ensureStarted() {
        if (!started.get()) {
            throw new IllegalStateException("StreamingContext has not been started");
        }
        if (stopped.get()) {
            throw new IllegalStateException("StreamingContext already stopped");
        }
    }
}
