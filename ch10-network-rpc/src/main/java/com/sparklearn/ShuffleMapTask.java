package com.sparklearn;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * ShuffleMapStage 里的单个分区任务。
 *
 * <p>它读取父 RDD 的一个 Map 分区，按 key 分桶，并写出
 * map_x_reduce_y 这样的 shuffle 中间文件。
 */
public final class ShuffleMapTask<K, V> extends Task<MapOutputStatus> {

    private final RDD<KeyValuePair<K, V>> rdd;
    private final Partition partition;
    private final ShuffleDependency<K, V> dependency;

    public ShuffleMapTask(
            int stageId,
            RDD<KeyValuePair<K, V>> rdd,
            Partition partition,
            ShuffleDependency<K, V> dependency) {
        super(stageId, partition.index());
        this.rdd = rdd;
        this.partition = partition;
        this.dependency = dependency;
    }

    @Override
    protected MapOutputStatus runTask(TaskContext context) {
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

        List<String> files = new ArrayList<>();
        List<Long> sizes = new ArrayList<>();
        for (int reduceId = 0; reduceId < dependency.numReducePartitions(); reduceId++) {
            File file = writeMapOutput(context, partition.index(), reduceId, buckets.get(reduceId));
            files.add(file.getAbsolutePath());
            sizes.add(file.length());
        }
        return new MapOutputStatus(
                partition.index(),
                context.executorAddress(),
                files,
                sizes);
    }

    @Override
    public List<String> preferredLocations() {
        return rdd.preferredLocations(partition);
    }

    private File writeMapOutput(
            TaskContext context,
            int mapId,
            int reduceId,
            Map<K, V> data) {
        File file = context.executorLocalDir() == null
                ? dependency.mapOutputFile(mapId, reduceId)
                : new File(context.executorLocalDir(),
                        dependency.shuffleId() + "_map_" + mapId + "_reduce_" + reduceId);
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            throw new IllegalStateException("无法创建 shuffle 目录: " + parent);
        }
        File temporary = new File(
                file.getParentFile(),
                file.getName() + ".attempt-" + context.attemptId()
                        + "-" + UUID.randomUUID());
        try (ObjectOutputStream out = new ObjectOutputStream(
                new BufferedOutputStream(new FileOutputStream(temporary)))) {
            out.writeInt(data.size());
            for (var entry : data.entrySet()) {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("写入 shuffle 文件失败: " + file, e);
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
            throw new UncheckedIOException("提交 shuffle 文件失败: " + file, e);
        }
        return file;
    }

    @Override
    public String toString() {
        return "ShuffleMapTask(" + stageId() + ", " + partition.index() + ")";
    }
}
