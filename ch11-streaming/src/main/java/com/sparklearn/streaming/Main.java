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
 * <p>两段输入：先用预先准备好的 RDD 队列（每个 batch 吃一个 RDD），
 * 再换一条从 socket 读行的流。两边都让你看到：
 * Streaming = 定时重复提交的一批批普通 Spark job。
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
                if (!counts.isEmpty()) {
                    printedBatches.add(time.toString());
                }
            });

            // 窗口版：最近 2 个 batch 合并后再 count。
            DStream<KeyValuePair<String, Integer>> windowCounts = words
                    .window(Duration.seconds(2), Duration.seconds(1))
                    .map(word -> new KeyValuePair<>(word, 1))
                    .reduceByKey(Integer::sum, 2);
            windowCounts.foreachRDD((rdd, time) -> {
                List<KeyValuePair<String, Integer>> counts = rdd.collect();
                System.out.println("[window 2s] @" + time);
                counts.stream()
                        .sorted((a, b) -> a.key().compareTo(b.key()))
                        .forEach(pair -> System.out.println(pair.key() + " -> " + pair.value()));
                System.out.println();
            });

            ssc.start();
            // 推进 4 个 batch：前 3 个各吃队列里一个 RDD；第 4 个队列已空，
            // 输入流回退到空 RDD，仍会生成并执行 job（对空数据跑一遍）。
            ssc.advance(4);

            System.out.println("完成 batch 数: " + ssc.batchesStarted());
            System.out.println("有结果的 batch: " + printedBatches);
        }

        socketDemo();
    }

    /**
     * 第二个演示：输入换成一条真正的 socket 流。
     *
     * <p>LineServer 是个本地 TCP 桩，按我们的节奏往 socket 写行；SocketInputDStream
     * 后台线程把它们攒进缓冲，每个 batch 的 compute 把缓冲排空当这一批数据。
     * 故意让第二个 batch 没数据——你会看到那一批不产生 job。
     */
    private static void socketDemo() {
        System.out.println();
        System.out.println("=== socket 输入流演示 ===");
        try (LineServer server = new LineServer()) {
            server.start();

            try (SparkContext sc = new SparkContext(2, false);
                 StreamingContext ssc = new StreamingContext(sc, Duration.seconds(1))) {

                DStream<String> words = ssc.socketTextStream("localhost", server.port())
                        .flatMap(line -> List.of(line.split("\\s+")));
                DStream<KeyValuePair<String, Integer>> wordCounts = words
                        .map(word -> new KeyValuePair<>(word, 1))
                        .reduceByKey(Integer::sum, 2);
                wordCounts.foreachRDD((rdd, time) -> {
                    List<KeyValuePair<String, Integer>> counts = rdd.collect();
                    System.out.println("-------------------------------------------");
                    System.out.println("Time: " + time);
                    System.out.println("-------------------------------------------");
                    counts.stream()
                            .sorted((a, b) -> a.key().compareTo(b.key()))
                            .forEach(pair -> System.out.println(pair.key() + " -> " + pair.value()));
                    System.out.println();
                });

                ssc.start();
                sendAndAdvance(server, ssc, List.of("apple banana", "banana cherry")); // batch1：两行
                sendAndAdvance(server, ssc, List.of());                                   // batch2：没数据
                sendAndAdvance(server, ssc, List.of("cherry date"));                      // batch3：一行

                System.out.println("完成 batch 数: " + ssc.batchesStarted());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** 往 server 塞一组行，等后台读取线程把它们读进缓冲，再推进一个 batch。 */
    private static void sendAndAdvance(LineServer server, StreamingContext ssc, List<String> lines)
            throws InterruptedException {
        server.send(lines);
        Thread.sleep(100);
        ssc.advance();
    }
}
