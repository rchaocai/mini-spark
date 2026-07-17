package com.sparklearn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RDD 抽象：描述分区、依赖，以及每个分区的计算方式。
 *
 * @param <T> 元素类型
 */
public abstract class RDD<T> implements Serializable {

    private final transient SparkContext sparkContext;

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
    public abstract List<Dependency<?>> dependencies();

    /**
     * 读取一个分区的数据。
     */
    public final Iterator<T> iterator(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        return compute(partition);
    }

    /**
     * 当前分区更适合在哪些 Worker 上计算。默认没有偏好。
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

    private record PartitionResult<T>(boolean hasValue, T value) {
        static <T> PartitionResult<T> empty() {
            return new PartitionResult<>(false, null);
        }

        static <T> PartitionResult<T> of(T value) {
            return new PartitionResult<>(true, value);
        }
    }
}
