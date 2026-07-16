package com.sparklearn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * RDD 抽象：描述分区、依赖，以及每个分区的计算方式。
 *
 * @param <T> 元素类型
 */
public abstract class RDD<T> {

    private final SparkContext sparkContext;

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
     *
     * <p>现在直接调用 compute(partition)，后续会在这里插入缓存判断。
     */
    public final Iterator<T> iterator(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        return compute(partition);
    }

    protected final SparkContext sparkContext() {
        return sparkContext;
    }

    /**
     * 一对一变换。这里只记录变换，不消费数据。
     */
    public <U> MapPartitionsRDD<T, U> map(Function<T, U> elementFunction) {
        Objects.requireNonNull(elementFunction, "elementFunction");
        return new MapPartitionsRDD<>(
                this,
                iterator -> new MappingIterator<>(iterator, elementFunction));
    }

    /**
     * 只保留满足条件的元素。这里只记录变换，不消费数据。
     */
    public MapPartitionsRDD<T, T> filter(Predicate<T> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        return new MapPartitionsRDD<>(
                this,
                iterator -> new FilteringIterator<>(iterator, predicate));
    }

    /**
     * 把每个元素展开成多个元素。这里只记录变换，不消费数据。
     */
    public <U> MapPartitionsRDD<T, U> flatMap(
            Function<T, List<U>> elementFunction) {
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
            BinaryOperator<V> reduceFunction,
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
        List<List<T>> partitionResults = sparkContext.runJob(this, List::copyOf);
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
                sparkContext.runJob(this, partition -> (long) partition.size());
        long total = 0;
        for (long partitionCount : partitionCounts) {
            total += partitionCount;
        }
        return total;
    }

    /**
     * 使用二元函数把所有元素归并成一个结果。
     */
    public T reduce(BinaryOperator<T> operator) {
        Objects.requireNonNull(operator, "operator");
        List<PartitionResult<T>> partitionResults =
                sparkContext.runJob(this, partition -> reducePartition(partition, operator));

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

    private static <T> PartitionResult<T> reducePartition(
            List<T> partition,
            BinaryOperator<T> operator) {
        if (partition.isEmpty()) {
            return PartitionResult.empty();
        }

        T result = partition.get(0);
        for (int i = 1; i < partition.size(); i++) {
            result = operator.apply(result, partition.get(i));
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
