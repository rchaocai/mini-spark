package com.sparklearn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * 线程池执行与 Socket Executor 执行的对比入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length > 0 && "network".equals(args[0])) {
            runNetworkDemo(args);
            return;
        }
        runLocalDemo();
    }

    private static void runLocalDemo() {
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
            long started = System.nanoTime();
            Map<String, Integer> result = runWordCount(sc, words);
            long elapsedMillis = elapsedMillis(started);

            System.out.println("=== 本地线程池执行 ===");
            System.out.println("结果: " + result);
            System.out.println("耗时: " + elapsedMillis + " ms");
        }
    }

    private static void runNetworkDemo(String[] args) {
        String executorAddress = args.length >= 2 ? args[1] : "localhost:9091";
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

        try (SparkContext sc = new SparkContext(
                new NetworkTaskScheduler(List.of(executorAddress), 2, true),
                true)) {
            long started = System.nanoTime();
            Map<String, Integer> result = runWordCount(sc, words);
            long elapsedMillis = elapsedMillis(started);

            System.out.println("=== Socket Executor 执行 ===");
            System.out.println("Executor: " + executorAddress);
            System.out.println("结果: " + result);
            System.out.println("耗时: " + elapsedMillis + " ms");
        }
    }

    private static Map<String, Integer> runWordCount(
            SparkContext sc,
            List<KeyValuePair<String, Integer>> words) {
        ShuffledRDD<String, Integer> shuffled = sc.parallelize(words, 3)
                .map(value -> value)
                .reduceByKey((left, right) -> left + right, 2);
        return toSortedMap(shuffled.collect());
    }

    private static Map<String, Integer> toSortedMap(
            List<KeyValuePair<String, Integer>> values) {
        Map<String, Integer> result = values.stream().collect(Collectors.toMap(
                KeyValuePair::key,
                KeyValuePair::value));
        return new TreeMap<>(result);
    }

    private static long elapsedMillis(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000;
    }
}
