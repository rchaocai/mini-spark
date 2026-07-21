package com.sparklearn;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 第 11 章 · 致敬工业级 Spark —— 演示入口。
 *
 * <p>本章核心验证点：
 * <ol>
 *   <li>打印源码对照地图——每个手写类 → 真实 Spark 文件/方法</li>
 *   <li>用前 10 章的代码跑一段真实 pipeline，看一条 job 怎么跑完</li>
 *   <li>跑一次失败与恢复，看两层恢复在运行时怎么发生</li>
 * </ol>
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        // ==================== Part A: 源码对照地图 ====================
        SparkSourceMapDemo.print();

        // ==================== Part B: 跑一段 pipeline，看 job 怎么跑完 ====================
        System.out.println();
        System.out.println("=".repeat(78));
        System.out.println("演示：用你前 10 章写的代码跑一段 pipeline");
        System.out.println("=".repeat(78));

        List<String> words = List.of("hello", "world", "hello", "spark", "world", "hello");
        System.out.println("\n输入: " + words + "\n");

        try (SparkContext sc = new SparkContext(2, true)) {
            RDD<String> rdd = sc.parallelize(words, 3);

            // map: word → (word, 1)
            RDD<KeyValuePair<String, Integer>> pairs = rdd.map(
                    w -> new KeyValuePair<>(w, 1));

            // reduceByKey 用 lambda，避免 Integer::sum 在跨 JVM 场景下的序列化坑
            ShuffledRDD<String, Integer> counts = pairs.reduceByKey(
                    (Integer left, Integer right) -> left + right,
                    2);

            List<KeyValuePair<String, Integer>> result = counts.collect();
            System.out.println("\n结果: " + result);
        }

        // ==================== Part C: 失败与恢复的运行时 ====================
        runFaultRecovery();

        System.out.println("\n[完成]");
    }

    /**
     * 第 8 章讲过两层恢复为什么这么设计；这里把它们跑起来，看运行时的样子。
     *
     * <p>第一层：Task 抛异常，TaskScheduler 只重试这一个 Task。
     * 第二层：Map 输出文件丢失，DAGScheduler 回到父 Stage，只重算丢失的那个 Map 分区。
     */
    private static void runFaultRecovery() {
        System.out.println();
        System.out.println("=".repeat(78));
        System.out.println("演示：失败时，两层恢复怎样运行");
        System.out.println("=".repeat(78));

        List<KeyValuePair<String, Integer>> pairs = List.of(
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("java", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("hello", 1));

        // --- 第一层：Task 抛异常 → TaskScheduler 重试同一个 Task ---
        System.out.println("\n【第一层】Task 抛异常 → TaskScheduler 重试同一个 Task");
        AtomicInteger transientFailures = new AtomicInteger(1);
        try (SparkContext sc = new SparkContext(2, 1, true)) {
            ShuffledRDD<String, Integer> counts = sc.parallelize(pairs, 3)
                    .failOnNext(0, 2, transientFailures)
                    .reduceByKey((Integer left, Integer right) -> left + right, 2);
            System.out.println("结果: " + counts.collect());
        }

        // --- 第二层：Map 输出文件丢失 → FetchFailed → 回父 Stage 重算 ---
        System.out.println("\n【第二层】Map 输出文件丢失 → FetchFailed → 回父 Stage 重算");
        try (SparkContext sc = new SparkContext(2, 0, true)) {
            ShuffledRDD<String, Integer> shuffled = sc.parallelize(pairs, 3)
                    .reduceByKey((Integer left, Integer right) -> left + right, 2);
            // Reduce 分区 0 第一次计算前，删掉 map 分区 1 的输出文件
            RDD<KeyValuePair<String, Integer>> missing =
                    new MissingMapOutputRDD<>(shuffled, 1, 0);
            System.out.println("结果: " + missing.collect());
        }
    }
}
