package com.sparklearn;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RDD 抽象：描述分区、依赖，以及每个分区的计算方式。
 *
 * @param <T> 元素类型
 */
public abstract class RDD<T> implements Serializable {

    private final transient SparkContext sparkContext;
    private boolean shouldCache;
    private boolean checkpointRequested;
    private boolean checkpointed;
    private File checkpointDir;
    private final Map<Integer, List<T>> cache = new ConcurrentHashMap<>();
    private final Set<Integer> checkpointedPartitions = ConcurrentHashMap.newKeySet();
    private final AtomicInteger computeCount = new AtomicInteger();

    protected RDD(SparkContext sparkContext) {
        this.sparkContext = Objects.requireNonNull(sparkContext, "sparkContext");
    }

    /**
     * 当前 RDD 的分区列表。
     */
    public abstract List<Partition> partitions();

    /**
     * 计算一个具体分区的数据。
     */
    public abstract Iterator<T> compute(Partition partition);

    /**
     * 当前 RDD 依赖的父 RDD 列表。
     */
    public final List<Dependency<?>> dependencies() {
        if (checkpointed) {
            return List.of();
        }
        return getDependenciesInternal();
    }

    /**
     * 子类只描述原本的依赖；checkpoint 后是否切断血缘，由父类统一判断。
     */
    protected abstract List<Dependency<?>> getDependenciesInternal();

    /**
     * 标记当前 RDD 需要缓存。
     *
     * <p>cache 是惰性的。这里只记下意图，不会立刻计算任何分区。
     */
    public final RDD<T> cache() {
        shouldCache = true;
        return this;
    }

    /**
     * 清掉当前 RDD 的内存缓存，方便示例和测试重新观察计算次数。
     */
    public final void uncache() {
        cache.clear();
        shouldCache = false;
    }

    /**
     * 标记当前 RDD 需要 checkpoint。
     *
     * <p>checkpoint 和 cache 一样是惰性的。这里只记录意图，不会立刻计算分区。
     * 真正写 checkpoint 文件的时机，是后续 action 触发 iterator(partition) 之后。
     */
    public final void checkpoint() {
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
        if (checkpointed) {
            return readCheckpointFile(partition);
        }
        if (checkpointRequested) {
            return checkpointPartition(partition);
        }
        return iteratorWithoutCheckpoint(partition);
    }

    private Iterator<T> iteratorWithoutCheckpoint(Partition partition) {
        if (!shouldCache) {
            return computeTracked(partition);
        }

        List<T> cached = cache.get(partition.index());
        if (cached != null) {
            return new ArrayList<>(cached).iterator();
        }

        List<T> computed = materialize(computeTracked(partition));
        cache.put(partition.index(), computed);
        return new ArrayList<>(computed).iterator();
    }

    private Iterator<T> checkpointPartition(Partition partition) {
        File dir = ensureCheckpointDir();
        File file = checkpointFile(dir, partition);
        if (file.exists()) {
            return readCheckpointFile(partition);
        }
        List<T> values = materialize(iteratorWithoutCheckpoint(partition));
        writeCheckpointFile(dir, partition, values);
        markCheckpointPartitionComplete(partition);
        return new ArrayList<>(values).iterator();
    }

    private synchronized File ensureCheckpointDir() {
        if (checkpointDir != null) {
            return checkpointDir;
        }
        try {
            checkpointDir = Files.createTempDirectory("spark-checkpoint-").toFile();
            return checkpointDir;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private synchronized void markCheckpointPartitionComplete(Partition partition) {
        checkpointedPartitions.add(partition.index());
        if (checkpointedPartitions.size() == partitions().size()) {
            checkpointed = true;
        }
    }

    /**
     * 当前分区更适合在哪些 Executor 上计算。默认没有偏好。
     */
    public List<String> preferredLocations(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        return List.of();
    }

    protected final SparkContext sparkContext() {
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
        return ShuffledRDD.reduceByKey(
                (RDD<KeyValuePair<K, V>>) this,
                numberOfReducePartitions,
                reduceFunction);
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

    @SuppressWarnings("unchecked")
    private Iterator<T> readCheckpointFile(Partition partition) {
        if (checkpointDir == null) {
            throw new IllegalStateException("checkpointDir is not available");
        }
        File file = checkpointFile(checkpointDir, partition);
        try (ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(file)))) {
            List<T> values = (List<T>) in.readObject();
            return values.iterator();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("读取 checkpoint 文件失败: " + file, e);
        }
    }

    private void writeCheckpointFile(
            File dir,
            Partition partition,
            List<T> values) {
        File file = checkpointFile(dir, partition);
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(file)))) {
            out.writeObject(values);
        } catch (IOException e) {
            throw new UncheckedIOException("写入 checkpoint 文件失败: " + file, e);
        }
    }

    private static File checkpointFile(File dir, Partition partition) {
        return new File(dir, "part-" + partition.index() + ".bin");
    }

    private record PartitionResult<T>(boolean hasValue, T value) {
        static <T> PartitionResult<T> empty() {
            return new PartitionResult<>(false, null);
        }

        static <T> PartitionResult<T> of(T value) {
            return new PartitionResult<>(true, value);
        }
    }
}
