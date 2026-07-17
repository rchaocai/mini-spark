package com.sparklearn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Task 失败、重试与分区重算演示入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        List<KeyValuePair<String, Integer>> words = Arrays.asList(
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("java", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("hello", 1));

        try (SparkContext sc = new SparkContext(2, 2, true)) {
            Map<String, Integer> expected = runWithoutFailure(sc, words);
            Map<String, Integer> taskRecovered =
                    runWithTaskFailure(sc, words);
            Map<String, Integer> fetchRecovered =
                    runWithMissingMapOutput(sc, words);

            System.out.println("\n=== 4. 对比结果 ===");
            System.out.println("无故障结果: " + expected);
            System.out.println("Task 重试后: " + taskRecovered);
            System.out.println("Fetch 恢复后: " + fetchRecovered);
            System.out.println("结果一致: "
                    + (expected.equals(taskRecovered)
                    && expected.equals(fetchRecovered)));
        }
    }

    private static Map<String, Integer> runWithoutFailure(
            SparkContext sc,
            List<KeyValuePair<String, Integer>> words) {
        System.out.println("=== 1. 先执行一次无故障作业 ===");
        ShuffledRDD<String, Integer> shuffled = sc.parallelize(words, 3)
                .map(Function.identity())
                .reduceByKey(Integer::sum, 2);
        return toSortedMap(shuffled.collect());
    }

    private static Map<String, Integer> runWithTaskFailure(
            SparkContext sc,
            List<KeyValuePair<String, Integer>> words) {
        System.out.println("\n=== 2. 让 Map 分区 0 第一次读取到第 2 条时失败 ===");
        AtomicInteger remainingFailures = new AtomicInteger(1);

        ShuffledRDD<String, Integer> shuffled =
                sc.parallelize(words, 3)
                        .map(Function.identity())
                        .failOnNext(0, 2, remainingFailures)
                        .reduceByKey(Integer::sum, 2);
        return toSortedMap(shuffled.collect());
    }

    private static Map<String, Integer> runWithMissingMapOutput(
            SparkContext sc,
            List<KeyValuePair<String, Integer>> words) {
        System.out.println("\n=== 3. 删除一个已完成的 Map 输出 ===");

        ShuffledRDD<String, Integer> shuffled =
                sc.parallelize(words, 3)
                        .map(Function.identity())
                        .reduceByKey(Integer::sum, 2);
        RDD<KeyValuePair<String, Integer>> missingOutput =
                new MissingMapOutputRDD<>(
                        shuffled,
                        1,
                        0);
        return toSortedMap(missingOutput.collect());
    }

    private static Map<String, Integer> toSortedMap(
            List<KeyValuePair<String, Integer>> values) {
        Map<String, Integer> result = values.stream().collect(Collectors.toMap(
                KeyValuePair::key,
                KeyValuePair::value));
        return new TreeMap<>(result);
    }
}
