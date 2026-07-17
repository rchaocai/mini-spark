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
    private final List<List<String>> preferredLocations;

    public ListRDD(SparkContext sparkContext, List<T> data) {
        this(sparkContext, data, 1);
    }

    public ListRDD(SparkContext sparkContext, List<T> data, int numberOfPartitions) {
        this(sparkContext, data, numberOfPartitions, List.of());
    }

    public ListRDD(
            SparkContext sparkContext,
            List<T> data,
            int numberOfPartitions,
            List<List<String>> preferredLocations) {
        super(sparkContext);
        Objects.requireNonNull(data, "data");
        Objects.requireNonNull(preferredLocations, "preferredLocations");
        if (numberOfPartitions <= 0) {
            throw new IllegalArgumentException("numberOfPartitions must be positive");
        }
        if (!preferredLocations.isEmpty()
                && preferredLocations.size() != numberOfPartitions) {
            throw new IllegalArgumentException(
                    "preferredLocations must match numberOfPartitions");
        }

        this.data = data;
        List<Partition> partitionList = new ArrayList<>();
        for (int index = 0; index < numberOfPartitions; index++) {
            partitionList.add(new Partition(index));
        }
        this.partitions = List.copyOf(partitionList);
        this.preferredLocations = copyLocations(
                preferredLocations,
                numberOfPartitions);
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

    @Override
    public List<String> preferredLocations(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (partition.index() < 0 || partition.index() >= partitions.size()) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }
        return preferredLocations.get(partition.index());
    }

    private int startOffset(int partitionIndex) {
        int baseSize = data.size() / partitions.size();
        int remainder = data.size() % partitions.size();
        return partitionIndex * baseSize + Math.min(partitionIndex, remainder);
    }

    private static List<List<String>> copyLocations(
            List<List<String>> locations,
            int numberOfPartitions) {
        if (locations.isEmpty()) {
            List<List<String>> empty = new ArrayList<>();
            for (int index = 0; index < numberOfPartitions; index++) {
                empty.add(List.of());
            }
            return List.copyOf(empty);
        }

        List<List<String>> result = new ArrayList<>();
        for (List<String> partitionLocations : locations) {
            result.add(List.copyOf(partitionLocations));
        }
        return List.copyOf(result);
    }
}
