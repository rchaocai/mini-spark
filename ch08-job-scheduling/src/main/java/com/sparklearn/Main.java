package com.sparklearn;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * 多作业调度演示入口。
 *
 * <p>从两个线程同时提交两个独立的 Job，观察事件循环如何交替处理它们——
 * 两个 Job 的 ShuffleMapStage 和 ResultStage 的 Task 在同一线程池里并行跑。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        demonstrateJobScheduling();
    }

    private static void demonstrateJobScheduling() throws Exception {
        System.out.println("=== 多作业调度：两个 Job 同时提交 ===\n");

        // Job 1 的数据
        List<KeyValuePair<String, Integer>> words1 = Arrays.asList(
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1));

        // Job 2 的数据
        List<KeyValuePair<String, Integer>> words2 = Arrays.asList(
                new KeyValuePair<>("java", 1),
                new KeyValuePair<>("python", 1),
                new KeyValuePair<>("java", 1),
                new KeyValuePair<>("rust", 1),
                new KeyValuePair<>("java", 1),
                new KeyValuePair<>("python", 1));

        int numReducePartitions = 2;

        try (SparkContext sc = new SparkContext(4, true)) {
            // 构造两个独立的 RDD 血缘
            ShuffledRDD<String, Integer> job1Rdd = sc.parallelize(words1, 3)
                    .map(Function.identity())
                    .reduceByKey(Integer::sum, numReducePartitions);

            ShuffledRDD<String, Integer> job2Rdd = sc.parallelize(words2, 3)
                    .map(Function.identity())
                    .reduceByKey(Integer::sum, numReducePartitions);

            System.out.println("Job 1 期望: hello→3, world→2, spark→1");
            System.out.println("Job 2 期望: java→3, python→2, rust→1");
            System.out.println("—— 两个 Job 同时提交，观察事件循环如何交替处理\n");

            // 从两个线程同时提交
            Thread t1 = new Thread(() -> {
                List<KeyValuePair<String, Integer>> result = job1Rdd.collect();
                System.out.println("\nJob 1 结果: " + result);
            }, "Job1-Thread");

            Thread t2 = new Thread(() -> {
                List<KeyValuePair<String, Integer>> result = job2Rdd.collect();
                System.out.println("\nJob 2 结果: " + result);
            }, "Job2-Thread");

            t1.start();
            t2.start();
            t1.join();
            t2.join();

            System.out.println("\n=== 两个 Job 都已完成 ===");
        }
    }
}
