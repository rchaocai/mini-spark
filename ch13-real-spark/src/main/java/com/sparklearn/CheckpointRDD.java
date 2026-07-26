package com.sparklearn;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Checkpoint 恢复用的叶子 RDD：从 checkpoint 文件读取已物化的分区数据。
 *
 * <p>某个 RDD 被 checkpoint 后，它的 {@link #dependencies()} 改指向一个
 * CheckpointRDD——调度器追到这里就停，因为 CheckpointRDD 自身没有父依赖；
 * 读取时由 {@link #compute(Partition)} 直接读 checkpoint 文件，不再沿原血缘重算。
 *
 * <p>对应真实 Spark 0.7+ 的 {@code rdd/CheckpointRDD.scala}。
 *
 * @param <T> 元素类型
 */
public final class CheckpointRDD<T> extends RDD<T> {

    private final File checkpointDir;
    private final List<Partition> partitionList;

    public CheckpointRDD(SparkContext sparkContext, File checkpointDir, int numPartitions) {
        super(sparkContext);
        this.checkpointDir = Objects.requireNonNull(checkpointDir, "checkpointDir");
        if (numPartitions <= 0) {
            throw new IllegalArgumentException("numPartitions must be positive");
        }
        List<Partition> partitions = new ArrayList<>();
        for (int index = 0; index < numPartitions; index++) {
            partitions.add(new Partition(index));
        }
        this.partitionList = List.copyOf(partitions);
    }

    @Override
    public List<Partition> partitions() {
        return partitionList;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<T> compute(Partition partition) {
        File file = new File(checkpointDir, "part-" + partition.index() + ".bin");
        try (ObjectInputStream in = new ObjectInputStream(
                new BufferedInputStream(new FileInputStream(file)))) {
            List<T> values = (List<T>) in.readObject();
            return values.iterator();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("读取 checkpoint 文件失败: " + file, e);
        }
    }

    @Override
    protected List<Dependency<?>> getDependenciesInternal() {
        return List.of();
    }
}
