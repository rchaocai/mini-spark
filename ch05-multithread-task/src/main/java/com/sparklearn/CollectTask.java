package com.sparklearn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * collect 使用的分区任务：计算一个 RDD 分区并返回其中的全部元素。
 *
 * <p>CollectTask 不写共享容器，只把当前分区的结果作为返回值交给调度器。
 *
 * @param <T> 元素类型
 */
public final class CollectTask<T> implements Callable<List<T>> {

    private final RDD<T> rdd;
    private final Partition partition;
    private final boolean verbose;

    public CollectTask(RDD<T> rdd, Partition partition) {
        this(rdd, partition, false);
    }

    public CollectTask(RDD<T> rdd, Partition partition, boolean verbose) {
        this.rdd = Objects.requireNonNull(rdd, "rdd");
        this.partition = Objects.requireNonNull(partition, "partition");
        this.verbose = verbose;
    }

    @Override
    public List<T> call() {
        if (verbose) {
            System.out.println("  [" + Thread.currentThread().getName()
                    + "] 开始计算分区 " + partition.index());
        }

        List<T> result = new ArrayList<>();
        Iterator<T> iterator = rdd.iterator(partition);
        while (iterator.hasNext()) {
            T element = iterator.next();
            result.add(element);
        }

        if (verbose) {
            System.out.println("  [" + Thread.currentThread().getName()
                    + "] 分区 " + partition.index()
                    + " 完成，共 " + result.size() + " 条");
        }
        return result;
    }
}
