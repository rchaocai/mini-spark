package com.sparklearn;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;

/**
 * reduceByKey 的结果 RDD。
 *
 * <p>Map 阶段在第一次 compute 时触发：遍历父 RDD 的每个分区，
 * 按 key 哈希写到 N 个本地文件。Reduce 阶段读取属于当前分区的所有文件，
 * 再把相同 key 的值合并一次。
 *
 * <p>为了减少写盘量，Map 端先在每个桶里做一次本地 combine。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public final class ShuffledRDD<K, V> extends RDD<KeyValuePair<K, V>> {

    private final RDD<KeyValuePair<K, V>> parent;
    private final int numReducePartitions;
    private final List<Partition> partitions;
    private final File shuffleDir;
    private final BinaryOperator<V> reduceFunc;
    private final int numMapPartitions;

    private volatile boolean mapPhaseDone = false;

    private ShuffledRDD(RDD<KeyValuePair<K, V>> parent, int numReducePartitions,
                        BinaryOperator<V> reduceFunc) {
        this.parent = Objects.requireNonNull(parent, "parent");
        if (numReducePartitions <= 0) {
            throw new IllegalArgumentException("numReducePartitions must be positive");
        }
        this.numReducePartitions = numReducePartitions;
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
    }

    /**
     * 构造 reduceByKey 的结果 RDD。
     *
     * <p>只构造、不落盘；Map 阶段会在结果 RDD 首次被消费时惰性执行。
     */
    public static <K, V> ShuffledRDD<K, V> reduceByKey(
            RDD<KeyValuePair<K, V>> parent,
            int numReducePartitions,
            BinaryOperator<V> reduceFunc) {
        return new ShuffledRDD<>(parent, numReducePartitions, reduceFunc);
    }

    private void runMapPhase() {
        for (Partition mapPart : parent.partitions()) {
            int mapId = mapPart.index();

            List<Map<K, V>> buckets = new ArrayList<>();
            for (int i = 0; i < numReducePartitions; i++) {
                buckets.add(new HashMap<>());
            }

            Iterator<KeyValuePair<K, V>> it = parent.iterator(mapPart);
            while (it.hasNext()) {
                KeyValuePair<K, V> kv = it.next();
                int bucketId = partition(kv.key(), numReducePartitions);
                buckets.get(bucketId).merge(kv.key(), kv.value(), reduceFunc);
            }

            for (int reduceId = 0; reduceId < numReducePartitions; reduceId++) {
                writeMapOutput(mapId, reduceId, buckets.get(reduceId));
            }
        }
    }

    /**
     * 按 key.hashCode() 把记录放进 [0, numPartitions) 的桶里。
     */
    static int partition(Object key, int numPartitions) {
        return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }

    private void writeMapOutput(int mapId, int reduceId, Map<K, V> data) {
        File file = mapOutputFile(mapId, reduceId);
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(file)))) {
            out.writeInt(data.size());
            for (var entry : data.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("写入 shuffle 文件失败: " + file, e);
        }
    }

    @Override
    public Iterator<KeyValuePair<K, V>> compute(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (partition.index() < 0 || partition.index() >= partitions.size()) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }

        ensureMapPhase();
        return toKeyValuePairs(readAndMergeReducePartition(partition.index()));
    }

    private void ensureMapPhase() {
        if (!mapPhaseDone) {
            synchronized (this) {
                if (!mapPhaseDone) {
                    runMapPhase();
                    mapPhaseDone = true;
                }
            }
        }
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
        File file = mapOutputFile(mapId, reduceId);
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
            throw new RuntimeException("读取 shuffle 文件失败: " + file, e);
        }
        return result;
    }

    private File mapOutputFile(int mapId, int reduceId) {
        return new File(shuffleDir, "map_" + mapId + "_reduce_" + reduceId);
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    @Override
    public List<Dependency<?>> dependencies() {
        return List.of(new ShuffleDependency<>(parent));
    }

    /**
     * 返回 shuffle 中间文件的目录，供 demo 和测试查看。
     */
    public File shuffleDir() {
        return shuffleDir;
    }
}
