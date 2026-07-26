package com.sparklearn;

import java.io.File;
import java.util.Objects;

/**
 * Shuffle 依赖：子分区会读取多个父分区写出的 shuffle 结果。
 *
 * <p>它描述写 shuffle 文件所需的信息。DAGScheduler 遇到这条依赖时，
 * 会为父 RDD 的每个分区创建 ShuffleMapTask。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public final class ShuffleDependency<K, V> implements Dependency<KeyValuePair<K, V>> {

    private final RDD<KeyValuePair<K, V>> rdd;
    private final int numReducePartitions;
    private final File shuffleDir;
    private final SerializableBinaryOperator<V> reduceFunc;

    public ShuffleDependency(
            RDD<KeyValuePair<K, V>> rdd,
            int numReducePartitions,
            File shuffleDir,
            SerializableBinaryOperator<V> reduceFunc) {
        this.rdd = Objects.requireNonNull(rdd, "rdd");
        if (numReducePartitions <= 0) {
            throw new IllegalArgumentException("numReducePartitions must be positive");
        }
        this.numReducePartitions = numReducePartitions;
        this.shuffleDir = Objects.requireNonNull(shuffleDir, "shuffleDir");
        this.reduceFunc = Objects.requireNonNull(reduceFunc, "reduceFunc");
    }

    @Override
    public RDD<KeyValuePair<K, V>> rdd() {
        return rdd;
    }

    /**
     * Reduce 分区数，也就是每个 Map 分区要写出的文件个数。
     */
    public int numReducePartitions() {
        return numReducePartitions;
    }

    public File shuffleDir() {
        return shuffleDir;
    }

    public SerializableBinaryOperator<V> reduceFunc() {
        return reduceFunc;
    }

    /**
     * 按 key.hashCode() 把记录放进 [0, numPartitions) 的桶里。
     */
    public int partition(Object key) {
        return (key.hashCode() & Integer.MAX_VALUE) % numReducePartitions;
    }

    public File mapOutputFile(int mapId, int reduceId) {
        return new File(shuffleDir, "map_" + mapId + "_reduce_" + reduceId);
    }
}
