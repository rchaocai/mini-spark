package com.sparklearn.streaming;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.RDD;
import com.sparklearn.core.SparkContext;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * 第 11 章演示：在 RDD 内核上跑一个最小 Streaming WordCount。
 *
 * <p>输入不是 socket，而是预先准备好的 RDD 队列——每个 batch 吃一个 RDD。
 * 这样你能清楚看到：Streaming = 定时重复提交的一批批普通 Spark job。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        System.out.println("=== 第 11 章 · Spark Streaming 与 DStream ===");
        try (SparkContext sc = new SparkContext(2, false);
             StreamingContext ssc = new StreamingContext(sc, Duration.seconds(1))) {

            Queue<RDD<String>> lines = new LinkedList<>();
            lines.add(sc.parallelize(List.of("hello spark", "hello stream"), 2));
            lines.add(sc.parallelize(List.of("spark streaming", "hello spark"), 2));
            lines.add(sc.parallelize(List.of("mini spark streaming"), 1));

            DStream<String> input = ssc.queueStream(lines);
            DStream<String> words = input.flatMap(line -> List.of(line.split("\\s+")));
            DStream<KeyValuePair<String, Integer>> wordCounts = words
                    .map(word -> new KeyValuePair<>(word, 1))
                    .reduceByKey(Integer::sum, 2);

            List<String> printedBatches = new ArrayList<>();
            wordCounts.foreachRDD((rdd, time) -> {
                List<KeyValuePair<String, Integer>> counts = rdd.collect();
                System.out.println("-------------------------------------------");
                System.out.println("Time: " + time);
                System.out.println("-------------------------------------------");
                counts.stream()
                        .sorted((a, b) -> a.key().compareTo(b.key()))
                        .forEach(pair -> System.out.println(pair.key() + " -> " + pair.value()));
                System.out.println();
                printedBatches.add(time.toString());
            });

            // 窗口版：最近 2 个 batch 合并后再 count。
            DStream<KeyValuePair<String, Integer>> windowCounts = words
                    .window(Duration.seconds(2), Duration.seconds(1))
                    .map(word -> new KeyValuePair<>(word, 1))
                    .reduceByKey(Integer::sum, 2);
            windowCounts.foreachRDD((rdd, time) -> {
                System.out.println("[window 2s] @" + time + " => " + rdd.collect());
            });

            ssc.start();
            // 3 个输入 batch；第 4 个 batch 队列已空，输出 job 数为 0。
            ssc.advance(4);

            System.out.println("完成 batch 数: " + ssc.batchesStarted());
            System.out.println("有结果的 batch: " + printedBatches);
        }
    }
}
