package com.sparklearn.core;

import com.sparklearn.core.rdd.FileRDD;
import com.sparklearn.core.rdd.ListRDD;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.scheduler.DAGScheduler;
import com.sparklearn.core.scheduler.LocalTaskScheduler;
import com.sparklearn.core.scheduler.TaskScheduler;
import com.sparklearn.core.storage.BlockManagerMaster;
import com.sparklearn.core.storage.Broadcast;
import com.sparklearn.core.storage.BroadcastManager;
import com.sparklearn.core.util.SerializableFunction;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 最小版 SparkContext：保存调度器，并作为 action 进入 DAG 调度层的入口。
 *
 * <p>本章的变化：构造时启动 DAGScheduler 的事件循环线程，关闭时先停事件循环再关
 * TaskScheduler。Task 重试上限通过构造参数传入 DAGScheduler，由它在事件循环里
 * 统一处理。
 */
public final class SparkContext implements AutoCloseable {

    private static final int DEFAULT_MAX_TASK_RETRIES = 3;

    private final TaskScheduler taskScheduler;
    private final DAGScheduler dagScheduler;
    private final BlockManagerMaster blockManagerMaster = new BlockManagerMaster();
    private final AtomicInteger nextRddId = new AtomicInteger();
    private volatile File checkpointRoot;

    public SparkContext(int numberOfThreads) {
        this(numberOfThreads, DEFAULT_MAX_TASK_RETRIES, false);
    }

    public SparkContext(int numberOfThreads, boolean verbose) {
        this(numberOfThreads, DEFAULT_MAX_TASK_RETRIES, verbose);
    }

    public SparkContext(
            int numberOfThreads,
            int maxTaskRetries,
            boolean verbose) {
        this(new LocalTaskScheduler(numberOfThreads), maxTaskRetries, verbose);
    }

    public SparkContext(TaskScheduler taskScheduler, boolean verbose) {
        this(taskScheduler, DEFAULT_MAX_TASK_RETRIES, verbose);
    }

    public SparkContext(
            TaskScheduler taskScheduler,
            int maxTaskRetries,
            boolean verbose) {
        this.taskScheduler = Objects.requireNonNull(taskScheduler, "taskScheduler");
        taskScheduler.setBlockManagerMaster(blockManagerMaster);
        this.dagScheduler = new DAGScheduler(
                taskScheduler, maxTaskRetries, verbose, blockManagerMaster);
        this.dagScheduler.start();
    }

    public <T> RDD<T> parallelize(List<T> data, int numberOfPartitions) {
        return new ListRDD<>(this, data, numberOfPartitions);
    }

    /**
     * 从本地文本文件创建 RDD，默认切成 2 个分区。
     */
    public RDD<String> textFile(String path) {
        return textFile(path, 2);
    }

    /**
     * 从本地文本文件创建 RDD，指定分区数。
     *
     * <p>对应真实 Spark 的 {@code SparkContext.textFile()}——
     * 真实版支持 HDFS/S3/HTTP 等多种源，这里简化为本地文件。
     */
    public RDD<String> textFile(String path, int numberOfPartitions) {
        return new FileRDD(this, path, numberOfPartitions);
    }

    public <T> RDD<T> parallelize(
            List<T> data,
            int numberOfPartitions,
            List<List<String>> preferredLocations) {
        return new ListRDD<>(
                this,
                data,
                numberOfPartitions,
                preferredLocations);
    }

    public <T, U> List<U> runJob(
            RDD<T> rdd,
            SerializableFunction<Iterator<T>, U> partitionFunction) {
        Objects.requireNonNull(rdd, "rdd");
        Objects.requireNonNull(partitionFunction, "partitionFunction");
        List<U> result = dagScheduler.runJob(rdd, partitionFunction);
        rdd.doCheckpoint();
        return result;
    }

    public <T, U> List<U> runJobWithoutCheckpoint(
            RDD<T> rdd,
            SerializableFunction<Iterator<T>, U> partitionFunction) {
        return dagScheduler.runJob(rdd, partitionFunction);
    }

    public int newRddId() {
        return nextRddId.getAndIncrement();
    }

    /** 设置所有 Executor 都能访问的 checkpoint 根目录。 */
    public void setCheckpointDir(String path) {
        Objects.requireNonNull(path, "path");
        File root = new File(path).getAbsoluteFile();
        if (!root.exists() && !root.mkdirs()) {
            throw new IllegalArgumentException(
                    "无法创建 checkpoint 根目录: " + root);
        }
        checkpointRoot = root;
    }

    public File checkpointDirectoryFor(int rddId) {
        requireCheckpointDir();
        return new File(checkpointRoot, "rdd-" + rddId);
    }

    public void requireCheckpointDir() {
        if (checkpointRoot == null) {
            throw new IllegalStateException(
                    "checkpoint 之前必须先调用 SparkContext.setCheckpointDir(path)");
        }
    }

    public void unpersistRDD(int rddId) {
        taskScheduler.removeRdd(rddId);
        blockManagerMaster.removeRdd(rddId);
    }

    /**
     * 广播一个值到所有 executor。
     *
     * <p>对应真实 Spark 的 {@code SparkContext.broadcast(value)}。值被存进
     * {@link BroadcastManager} 的 driver 端注册表，拿到一个全局唯一的 {@code broadcastId}；
     * 返回的 {@link Broadcast} 对象只持有这个 id。在 Task 闭包里引用 {@code Broadcast}
     * 时，序列化链路只传 {@code broadcastId}（一个 long），不传值；
     * executor 端调 {@link Broadcast#value()} 凭 id 取值。
     *
     * <p>典型场景：JOIN 时把较小的右表广播到所有 executor 建 hash map，
     * 避免每个 Task 重复 collect 右表。
     *
     * @param value 要广播的值，通常是一个较小的对象
     * @return 只持有 broadcastId 的 {@link Broadcast} 句柄
     */
    public <T> Broadcast<T> broadcast(T value) {
        Objects.requireNonNull(value, "value");
        long id = BroadcastManager.getInstance().register(value);
        return new Broadcast<>(id);
    }

    @Override
    public void close() {
        // 先停 DAGScheduler 事件循环（发 StopDAGScheduler 事件）
        dagScheduler.stop();
        // 再关 TaskScheduler 线程池
        taskScheduler.close();
    }
}
