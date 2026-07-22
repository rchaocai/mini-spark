package com.sparklearn.core;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * 把多个 RDD 的分区拼成一个 RDD。Streaming 的 window 会用到它。
 */
public final class UnionRDD<T> extends RDD<T> {

    private final List<RDD<T>> rdds;
    private final List<Partition> partitions;
    private final List<RDD<T>> partitionOwners;
    private final List<Partition> partitionSources;

    public UnionRDD(SparkContext sparkContext, List<RDD<T>> rdds) {
        super(sparkContext);
        this.rdds = List.copyOf(Objects.requireNonNull(rdds, "rdds"));
        if (this.rdds.isEmpty()) {
            throw new IllegalArgumentException("rdds must not be empty");
        }
        List<Partition> allPartitions = new ArrayList<>();
        List<RDD<T>> owners = new ArrayList<>();
        List<Partition> sources = new ArrayList<>();
        int index = 0;
        for (RDD<T> rdd : this.rdds) {
            for (Partition partition : rdd.partitions()) {
                allPartitions.add(new Partition(index++));
                owners.add(rdd);
                sources.add(partition);
            }
        }
        this.partitions = List.copyOf(allPartitions);
        this.partitionOwners = List.copyOf(owners);
        this.partitionSources = List.copyOf(sources);
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    @Override
    public Iterator<T> compute(Partition partition) {
        int index = partition.index();
        return partitionOwners.get(index).iterator(partitionSources.get(index));
    }

    @Override
    protected List<Dependency<?>> getDependenciesInternal() {
        List<Dependency<?>> dependencies = new ArrayList<>();
        for (RDD<T> rdd : rdds) {
            dependencies.add(new OneToOneDependency<>(rdd));
        }
        return dependencies;
    }
}
