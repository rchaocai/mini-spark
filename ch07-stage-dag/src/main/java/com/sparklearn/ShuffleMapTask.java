package com.sparklearn;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * ShuffleMapStage 里的单个分区任务。
 *
 * <p>它读取父 RDD 的一个 Map 分区，按 key 分桶，并写出
 * map_x_reduce_y 这样的 shuffle 中间文件。
 */
public final class ShuffleMapTask<K, V> implements Callable<Void> {

    private final RDD<KeyValuePair<K, V>> rdd;
    private final Partition partition;
    private final ShuffleDependency<K, V> dependency;

    public ShuffleMapTask(
            RDD<KeyValuePair<K, V>> rdd,
            Partition partition,
            ShuffleDependency<K, V> dependency) {
        this.rdd = rdd;
        this.partition = partition;
        this.dependency = dependency;
    }

    @Override
    public Void call() {
        List<Map<K, V>> buckets = new ArrayList<>();
        for (int i = 0; i < dependency.numReducePartitions(); i++) {
            buckets.add(new HashMap<>());
        }

        Iterator<KeyValuePair<K, V>> iterator = rdd.iterator(partition);
        while (iterator.hasNext()) {
            KeyValuePair<K, V> kv = iterator.next();
            int bucketId = dependency.partition(kv.key());
            buckets.get(bucketId).merge(kv.key(), kv.value(), dependency.reduceFunc());
        }

        for (int reduceId = 0; reduceId < dependency.numReducePartitions(); reduceId++) {
            writeMapOutput(partition.index(), reduceId, buckets.get(reduceId));
        }
        return null;
    }

    private void writeMapOutput(int mapId, int reduceId, Map<K, V> data) {
        File file = dependency.mapOutputFile(mapId, reduceId);
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
}
