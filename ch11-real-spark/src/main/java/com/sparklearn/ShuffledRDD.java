package com.sparklearn;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * reduceByKey 的结果 RDD。
 *
 * <p>Map 阶段由 DAGScheduler 先触发：遍历父 RDD 的每个分区，按 key
 * 哈希写到 N 个本地文件。Reduce 阶段只读取属于当前分区的所有文件，
 * 再把相同 key 的值合并一次。也就是说，compute() 不再补做 Map 阶段。
 *
 * <p>为了减少写盘量，Map 端先在每个桶里做一次本地 combine。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public final class ShuffledRDD<K, V> extends RDD<KeyValuePair<K, V>> {

    private final List<Partition> partitions;
    private final File shuffleDir;
    private final SerializableBinaryOperator<V> reduceFunc;
    private final int numMapPartitions;
    private final ShuffleDependency<K, V> shuffleDependency;

    private ShuffledRDD(RDD<KeyValuePair<K, V>> parent, int numReducePartitions,
                        SerializableBinaryOperator<V> reduceFunc) {
        super(parent.sparkContext());
        Objects.requireNonNull(parent, "parent");
        if (numReducePartitions <= 0) {
            throw new IllegalArgumentException("numReducePartitions must be positive");
        }
        this.reduceFunc = Objects.requireNonNull(reduceFunc, "reduceFunc");
        this.numMapPartitions = parent.partitions().size();

        List<Partition> partitionList = new ArrayList<>();
        for (int index = 0; index < numReducePartitions; index++) {
            partitionList.add(new Partition(index));
        }
        this.partitions = List.copyOf(partitionList);

        try {
            this.shuffleDir = java.nio.file.Files.createTempDirectory("spark-shuffle-").toFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.shuffleDependency = new ShuffleDependency<>(
                parent,
                numReducePartitions,
                shuffleDir,
                reduceFunc);
    }

    /**
     * 构造 reduceByKey 的结果 RDD。
     *
     * <p>只构造、不落盘；Map 阶段会在 action 触发后由 DAGScheduler 执行。
     */
    public static <K, V> ShuffledRDD<K, V> reduceByKey(
            RDD<KeyValuePair<K, V>> parent,
            int numReducePartitions,
            SerializableBinaryOperator<V> reduceFunc) {
        return new ShuffledRDD<>(parent, numReducePartitions, reduceFunc);
    }

    @Override
    public Iterator<KeyValuePair<K, V>> compute(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (partition.index() < 0 || partition.index() >= partitions.size()) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }

        return toKeyValuePairs(readAndMergeReducePartition(partition.index()));
    }

    private Map<K, V> readAndMergeReducePartition(int reduceId) {
        Map<K, V> merged = new HashMap<>();
        for (int mapId = 0; mapId < numMapPartitions; mapId++) {
            Map<K, V> mapOutput = readMapOutput(mapId, reduceId);
            for (var entry : mapOutput.entrySet()) {
                merged.merge(entry.getKey(), entry.getValue(), reduceFunc);
            }
        }
        return merged;
    }

    private Iterator<KeyValuePair<K, V>> toKeyValuePairs(Map<K, V> merged) {
        List<KeyValuePair<K, V>> result = new ArrayList<>();
        for (var entry : merged.entrySet()) {
            result.add(new KeyValuePair<>(entry.getKey(), entry.getValue()));
        }
        return result.iterator();
    }

    @SuppressWarnings("unchecked")
    private Map<K, V> readMapOutput(int mapId, int reduceId) {
        File file = shuffleDependency.mapOutputFile(mapId, reduceId);
        Map<K, V> result = new HashMap<>();
        try (ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(file)))) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                K key = (K) in.readObject();
                V value = (V) in.readObject();
                result.put(key, value);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new FetchFailedException(
                    shuffleDependency,
                    mapId,
                    reduceId,
                    file,
                    e);
        }
        return result;
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    @Override
    protected List<Dependency<?>> getDependenciesInternal() {
        return List.of(shuffleDependency);
    }

    /**
     * 返回 shuffle 中间文件的目录，供示例程序和测试查看。
     */
    public File shuffleDir() {
        return shuffleDir;
    }

    int numMapPartitions() {
        return numMapPartitions;
    }

    File mapOutputFile(int mapId, int reduceId) {
        return shuffleDependency.mapOutputFile(mapId, reduceId);
    }
}
