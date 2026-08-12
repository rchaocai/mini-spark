package com.sparklearn.core.rdd;

import com.sparklearn.core.*;
import com.sparklearn.core.executor.TaskExecutionEnvironment;
import com.sparklearn.core.scheduler.TaskContext;
import com.sparklearn.core.storage.BlockId;
import com.sparklearn.core.storage.StorageLevel;
import com.sparklearn.core.util.*;

import java.io.*;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RDD 抽象：描述分区、依赖，以及每个分区的计算方式。
 *
 * @param <T> 元素类型
 */
public abstract class RDD<T> implements Serializable {

    private final transient SparkContext sparkContext;
    private final int id;
    private StorageLevel storageLevel = StorageLevel.NONE;
    private boolean checkpointRequested;
    private volatile boolean checkpointed;
    private RDD<T> checkpointRDD;
    private transient boolean checkpointing;
    private final AtomicInteger computeCount = new AtomicInteger();

    protected RDD(SparkContext sparkContext) {
        this.sparkContext = Objects.requireNonNull(sparkContext, "sparkContext");
        this.id = sparkContext.newRddId();
    }

    /**
     * 当前 RDD 的分区列表。
     */
    public List<Partition> partitions() {
        if (checkpointed) {
            return checkpointRDD.partitions();
        }
        return getPartitionsInternal();
    }

    /** 子类返回 checkpoint 前的原始分区。 */
    protected abstract List<Partition> getPartitionsInternal();

    /**
     * 计算一个具体分区的数据。
     */
    public abstract Iterator<T> compute(Partition partition);

    /**
     * 当前 RDD 依赖的父 RDD 列表。
     */
    public final List<Dependency<?>> dependencies() {
        if (checkpointed) {
            return List.of(new OneToOneDependency<>(checkpointRDD));
        }
        return getDependenciesInternal();
    }

    /**
     * 子类只描述原本的依赖；checkpoint 后是否切断血缘，由父类统一判断。
     */
    protected abstract List<Dependency<?>> getDependenciesInternal();

    /** checkpoint 完成后释放对子血缘的引用。 */
    protected void clearDependencies() {
    }

    /**
     * 标记当前 RDD 需要缓存（默认存内存）。
     *
     * <p>等价于 {@code persist(StorageLevel.MEMORY_ONLY)}。cache 是惰性的，
     * 这里只记下意图，不会立刻计算任何分区。
     */
    public final RDD<T> cache() {
        return persist(StorageLevel.MEMORY_ONLY);
    }

    /**
     * 标记当前 RDD 按指定存储级别缓存。
     *
     * <p>persist 是惰性的。这里只记下意图，不会立刻计算任何分区。
     * 常用级别：{@link StorageLevel#MEMORY_ONLY}、{@link StorageLevel#DISK_ONLY}、
     * {@link StorageLevel#MEMORY_AND_DISK}。
     */
    public final RDD<T> persist(StorageLevel level) {
        this.storageLevel = level == null ? StorageLevel.NONE : level;
        return this;
    }

    /** 返回当前存储级别。 */
    public final StorageLevel getStorageLevel() {
        return storageLevel;
    }

    /**
     * 清掉当前 RDD 的缓存，方便示例和测试重新观察计算次数。
     */
    public final void uncache() {
        storageLevel = StorageLevel.NONE;
        if (sparkContext != null) {
            sparkContext.unpersistRDD(id);
        }
    }

    /**
     * 标记当前 RDD 需要 checkpoint。
     *
     * <p>checkpoint 和 cache 一样是惰性的。这里只记录意图，不会立刻计算分区。
     * 后续 action 成功后，Driver 会启动独立作业写 checkpoint 文件。
     *
     * <p>checkpoint 作业会重新计算分区；为避免重算，这里会自动给当前 RDD 加上
     * 内存缓存（若用户未指定其它存储级别）。这样 action 触发的计算会先把分区
     * 物化进缓存，checkpoint 作业就能直接命中缓存，而不必沿血缘重算。
     */
    public final void checkpoint() {
        sparkContext().requireCheckpointDir();
        if (storageLevel == StorageLevel.NONE) {
            cache();
        }
        checkpointRequested = true;
    }

    public final boolean isCheckpointed() {
        return checkpointed;
    }

    public final int getComputeCount() {
        return computeCount.get();
    }

    public final void resetComputeCount() {
        computeCount.set(0);
    }

    /**
     * 读取一个分区的数据。
     */
    public final Iterator<T> iterator(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (storageLevel == StorageLevel.NONE) {
            return computeOrReadCheckpoint(partition);
        }

        TaskExecutionEnvironment.Environment environment =
                TaskExecutionEnvironment.current();
        if (environment == null || environment.blockManager() == null) {
            return computeOrReadCheckpoint(partition);
        }
        return environment.blockManager().getOrCompute(
                new BlockId(id, partition.index()),
                storageLevel,
                () -> computeOrReadCheckpoint(partition));
    }

    private Iterator<T> computeOrReadCheckpoint(Partition partition) {
        if (checkpointed) {
            return checkpointRDD.iterator(partition);
        }
        return computeTracked(partition);
    }

    /**
     * Driver-side checkpoint hook, called after a successful job.
     *
     * <p>Tasks only write partition files; the original RDD object on the driver
     * owns the state transition and lineage truncation.
     */
    public final void doCheckpoint() {
        if (checkpointRequested) {
            checkpointThisRdd();
            return;
        }
        for (Dependency<?> dependency : getDependenciesInternal()) {
            dependency.rdd().doCheckpoint();
        }
    }

    private void checkpointThisRdd() {
        synchronized (this) {
            if (checkpointed || checkpointing) {
                return;
            }
            checkpointing = true;
        }
        try {
            File targetDir = sparkContext().checkpointDirectoryFor(id);
            if (!targetDir.exists() && !targetDir.mkdirs()) {
                throw new IllegalStateException(
                        "无法创建 checkpoint 目录: " + targetDir);
            }

            sparkContext().runJobWithoutCheckpoint(this, iterator -> {
                TaskContext context = TaskExecutionEnvironment.taskContext();
                if (context == null) {
                    throw new IllegalStateException(
                            "checkpoint writer requires a running TaskContext");
                }
                writeCheckpointFile(
                        targetDir,
                        new Partition(context.partition()),
                        context.attemptId(),
                        iterator);
                return null;
            });

            synchronized (this) {
                checkpointRDD = new CheckpointRDD<>(
                        sparkContext(), targetDir, partitions().size());
                checkpointed = true;
                clearDependencies();
                checkpointing = false;
            }
        } catch (RuntimeException e) {
            synchronized (this) {
                checkpointing = false;
            }
            throw e;
        }
    }

    public final int id() {
        return id;
    }

    /**
     * 当前分区更适合在哪些 Executor 上计算。默认没有偏好。
     */
    public final List<String> preferredLocations(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (checkpointed) {
            return checkpointRDD.preferredLocations(partition);
        }
        return getPreferredLocationsInternal(partition);
    }

    /** 子类返回 checkpoint 前的原始位置偏好。 */
    protected List<String> getPreferredLocationsInternal(Partition partition) {
        return List.of();
    }

    public final SparkContext sparkContext() {
        if (sparkContext == null) {
            throw new IllegalStateException(
                    "SparkContext is only available in the driver JVM");
        }
        return sparkContext;
    }

    /**
     * 一对一变换。这里只记录变换，不消费数据。
     */
    public <U> MapPartitionsRDD<T, U> map(
            SerializableFunction<T, U> elementFunction) {
        Objects.requireNonNull(elementFunction, "elementFunction");
        return new MapPartitionsRDD<>(
                this,
                iterator -> new MappingIterator<>(iterator, elementFunction));
    }

    /**
     * 测试和演示用变换：让指定分区在读取到第 N 个元素时模拟失败。
     */
    public FaultyRDD<T> failOnNext(
            int partitionIndex,
            int failOnNextCall,
            AtomicInteger remainingFailures) {
        return new FaultyRDD<>(
                this,
                partitionIndex,
                failOnNextCall,
                remainingFailures);
    }

    /**
     * 只保留满足条件的元素。这里只记录变换，不消费数据。
     */
    public MapPartitionsRDD<T, T> filter(
            SerializablePredicate<T> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        return new MapPartitionsRDD<>(
                this,
                iterator -> new FilteringIterator<>(iterator, predicate));
    }

    /**
     * 对每个分区的迭代器整体做变换。
     *
     * <p>和 {@link #map} 不同：map 是逐元素变换，mapPartitions 拿到的是整个分区的迭代器，
     * 可以在内部做任意复杂的处理（比如代码生成把多个算子融合成一个循环）。
     */
    public <U> MapPartitionsRDD<T, U> mapPartitions(
            SerializableFunction<Iterator<T>, Iterator<U>> partitionFunction) {
        Objects.requireNonNull(partitionFunction, "partitionFunction");
        return new MapPartitionsRDD<>(this, partitionFunction);
    }

    /**
     * 把每个元素展开成多个元素。这里只记录变换，不消费数据。
     */
    public <U> MapPartitionsRDD<T, U> flatMap(
            SerializableFunction<T, List<U>> elementFunction) {
        Objects.requireNonNull(elementFunction, "elementFunction");
        return new MapPartitionsRDD<>(
                this,
                iterator -> new FlatMappingIterator<>(iterator, elementFunction));
    }

    /**
     * 按 key 合并 value。只有元素类型是 KeyValuePair 的 RDD 才应该调用它。
     */
    @SuppressWarnings("unchecked")
    public <K, V> ShuffledRDD<K, V> reduceByKey(
            SerializableBinaryOperator<V> reduceFunction,
            int numberOfReducePartitions) {
        Objects.requireNonNull(reduceFunction, "reduceFunction");
        return new ShuffledRDD<>((RDD<KeyValuePair<K, V>>) this, numberOfReducePartitions, reduceFunction);
    }

    /**
     * 遍历所有分区，把结果逐个收集到内存。
     */
    public List<T> collect() {
        List<List<T>> partitionResults =
                sparkContext.runJob(this, RDD::collectPartition);
        List<T> result = new ArrayList<>();
        for (List<T> partitionResult : partitionResults) {
            result.addAll(partitionResult);
        }
        return result;
    }

    /**
     * 统计所有分区里的元素个数。
     */
    public long count() {
        List<Long> partitionCounts =
                sparkContext.runJob(this, RDD::countPartition);
        long total = 0;
        for (long partitionCount : partitionCounts) {
            total += partitionCount;
        }
        return total;
    }

    /**
     * 使用二元函数把所有元素归并成一个结果。
     */
    public T reduce(SerializableBinaryOperator<T> operator) {
        Objects.requireNonNull(operator, "operator");
        List<PartitionResult<T>> partitionResults =
                sparkContext.runJob(this, iterator -> reducePartition(iterator, operator));

        T result = null;
        boolean hasResult = false;
        for (PartitionResult<T> partitionResult : partitionResults) {
            if (!partitionResult.hasValue()) {
                continue;
            }
            if (!hasResult) {
                result = partitionResult.value();
                hasResult = true;
            } else {
                result = operator.apply(result, partitionResult.value());
            }
        }

        if (!hasResult) {
            throw new NoSuchElementException("reduce on empty RDD");
        }
        return result;
    }

    private static <T> List<T> collectPartition(Iterator<T> iterator) {
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    private static long countPartition(Iterator<?> iterator) {
        long count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    private static <T> PartitionResult<T> reducePartition(
            Iterator<T> iterator,
            SerializableBinaryOperator<T> operator) {
        if (!iterator.hasNext()) {
            return PartitionResult.empty();
        }

        T result = iterator.next();
        while (iterator.hasNext()) {
            result = operator.apply(result, iterator.next());
        }
        return PartitionResult.of(result);
    }

    private Iterator<T> computeTracked(Partition partition) {
        computeCount.incrementAndGet();
        return compute(partition);
    }

    private static <T> List<T> materialize(Iterator<T> iterator) {
        List<T> values = new ArrayList<>();
        iterator.forEachRemaining(values::add);
        return values;
    }

    private static <T> void writeCheckpointFile(
            File dir,
            Partition partition,
            int attemptId,
            Iterator<T> values) {
        File file = checkpointFile(dir, partition);
        File temporary = new File(
                dir,
                "." + file.getName() + "-attempt-" + attemptId + "-" + UUID.randomUUID());
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(temporary)))) {
            out.writeObject(materialize(values));
        } catch (IOException e) {
            throw new UncheckedIOException("写入 checkpoint 文件失败: " + file, e);
        }
        try {
            try {
                Files.move(
                        temporary.toPath(),
                        file.toPath(),
                        StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(
                        temporary.toPath(),
                        file.toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            temporary.delete();
            throw new UncheckedIOException("提交 checkpoint 文件失败: " + file, e);
        }
    }

    private static File checkpointFile(File dir, Partition partition) {
        return new File(dir, "part-" + partition.index() + ".bin");
    }

    private record PartitionResult<T>(boolean hasValue, T value) implements Serializable {
        static <T> PartitionResult<T> empty() {
            return new PartitionResult<>(false, null);
        }

        static <T> PartitionResult<T> of(T value) {
            return new PartitionResult<>(true, value);
        }
    }
}
