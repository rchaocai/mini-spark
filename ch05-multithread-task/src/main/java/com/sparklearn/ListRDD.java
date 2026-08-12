package com.sparklearn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 从内存 List 构造的源头 RDD，支持把数据切成多个分区。
 *
 * @param <T> 元素类型
 */
public final class ListRDD<T> extends RDD<T> {

    private final List<T> data;
    private final List<Partition> partitions;

    public ListRDD(List<T> data) {
        this(data, 1);
    }

    public ListRDD(List<T> data, int numberOfPartitions) {
        Objects.requireNonNull(data, "data");
        if (numberOfPartitions <= 0) {
            throw new IllegalArgumentException("numberOfPartitions must be positive");
        }

        this.data = data;
        List<Partition> partitionList = new ArrayList<>();
        for (int index = 0; index < numberOfPartitions; index++) {
            partitionList.add(new Partition(index));
        }
        this.partitions = List.copyOf(partitionList);
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    /**
     * 返回当前分区对应的 subList 迭代器，不复制原始数据。
     */
    @Override
    public Iterator<T> compute(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (partition.index() < 0 || partition.index() >= partitions.size()) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }

        int start = startOffset(partition.index());
        int end = startOffset(partition.index() + 1);
        return data.subList(start, end).iterator();
    }

    /**
     * 源头 RDD 没有父 RDD。
     */
    @Override
    public List<Dependency<?>> dependencies() {
        return List.of();
    }

    private int startOffset(int partitionIndex) {
        int baseSize = data.size() / partitions.size();
        int remainder = data.size() % partitions.size();
        return partitionIndex * baseSize + Math.min(partitionIndex, remainder);
    }
}
