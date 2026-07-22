package com.sparklearn.core;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 演示用 RDD：在指定 Reduce 分区第一次计算前，删除一个 Map 输出文件。
 *
 * <p>删除只发生一次。DAGScheduler 重算对应 Map 分区并重提 ResultStage 后，
 * 新文件不会再次被删除。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public final class MissingMapOutputRDD<K, V>
        extends RDD<KeyValuePair<K, V>> {

    private final ShuffledRDD<K, V> parent;
    private final int missingMapId;
    private final int targetReduceId;
    private final AtomicBoolean deleted = new AtomicBoolean();
    private final List<Dependency<?>> dependencies;

    public MissingMapOutputRDD(
            ShuffledRDD<K, V> parent,
            int missingMapId,
            int targetReduceId) {
        super(parent.sparkContext());
        this.parent = Objects.requireNonNull(parent, "parent");
        if (missingMapId < 0
                || missingMapId >= parent.numMapPartitions()) {
            throw new IllegalArgumentException(
                    "unknown map partition: " + missingMapId);
        }
        if (targetReduceId < 0
                || targetReduceId >= parent.partitions().size()) {
            throw new IllegalArgumentException(
                    "unknown reduce partition: " + targetReduceId);
        }
        this.missingMapId = missingMapId;
        this.targetReduceId = targetReduceId;
        this.dependencies = List.of(new OneToOneDependency<>(parent));
    }

    @Override
    public List<Partition> partitions() {
        return parent.partitions();
    }

    @Override
    public Iterator<KeyValuePair<K, V>> compute(Partition partition) {
        if (partition.index() == targetReduceId
                && deleted.compareAndSet(false, true)) {
            deleteMapOutput();
        }
        return parent.iterator(partition);
    }

    @Override
    protected List<Dependency<?>> getDependenciesInternal() {
        return dependencies;
    }

    @Override
    public List<String> preferredLocations(Partition partition) {
        return parent.preferredLocations(partition);
    }

    private void deleteMapOutput() {
        File file = parent.mapOutputFile(
                missingMapId,
                targetReduceId);
        if (!file.delete()) {
            throw new IllegalStateException(
                    "无法删除待模拟丢失的 shuffle 文件: " + file);
        }
    }
}
