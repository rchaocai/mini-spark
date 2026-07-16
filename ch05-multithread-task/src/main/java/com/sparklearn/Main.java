package com.sparklearn;

import java.util.Arrays;
import java.util.List;

/**
 * 第 5 章演示入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        demonstrateMultiPartitionListRDD();
        demonstrateSerialAndParallelCollect();
        demonstrateParallelPipeline();
        demonstrateParallelCountAndReduce();
    }

    private static void demonstrateMultiPartitionListRDD() {
        System.out.println("=== 1. ListRDD：一份数据切成多个分区 ===");
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        ListRDD<Integer> rdd = new ListRDD<>(data, 4);

        System.out.println("数据: " + data);
        for (Partition partition : rdd.partitions()) {
            System.out.println("分区 " + partition.index() + ": "
                    + collectPartition(rdd, partition));
        }
        System.out.println();
    }

    private static void demonstrateSerialAndParallelCollect() {
        System.out.println("=== 2. 串行 collect 与并行 collect ===");
        ListRDD<Integer> rdd = new ListRDD<>(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8),
                4);

        System.out.println("串行 collect(): " + rdd.collect());
        try (TaskScheduler scheduler = new TaskScheduler(4, true)) {
            System.out.println("并行 collect(): " + scheduler.collect(rdd));
        }
        System.out.println();
    }

    private static void demonstrateParallelPipeline() {
        System.out.println("=== 3. 流水线照旧，执行方式换成多线程 ===");
        RDD<Integer> pipeline = new ListRDD<>(
                Arrays.asList(1, 2, 3, 4, 5, 6),
                3)
                .map(number -> number * 10)
                .filter(number -> number > 30);

        try (TaskScheduler scheduler = new TaskScheduler(3, true)) {
            System.out.println("结果: " + scheduler.collect(pipeline));
        }
        System.out.println();
    }

    private static void demonstrateParallelCountAndReduce() {
        System.out.println("=== 4. 并行 count 与 reduce ===");
        RDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5), 3);

        try (TaskScheduler scheduler = new TaskScheduler(3)) {
            System.out.println("count(): " + scheduler.count(rdd));
            System.out.println("reduce(sum): " + scheduler.reduce(rdd, Integer::sum));
        }
    }

    private static <T> List<T> collectPartition(RDD<T> rdd, Partition partition) {
        List<T> result = new java.util.ArrayList<>();
        rdd.iterator(partition).forEachRemaining(result::add);
        return result;
    }
}
