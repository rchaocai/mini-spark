package com.sparklearn.streaming;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.RDD;
import com.sparklearn.core.SparkContext;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamingWordCountTest {

    @Test
    void microBatchWordCountAndWindow() {
        try (SparkContext sc = new SparkContext(2, false);
             StreamingContext ssc = new StreamingContext(sc, Duration.seconds(1))) {

            Queue<RDD<String>> lines = new LinkedList<>();
            lines.add(sc.parallelize(List.of("a a b"), 1));
            lines.add(sc.parallelize(List.of("a c"), 1));

            DStream<String> words = ssc.queueStream(lines)
                    .flatMap(line -> List.of(line.split("\\s+")));

            Map<Time, Map<String, Integer>> batchCounts = new LinkedHashMap<>();
            DStream<KeyValuePair<String, Integer>> counts = words
                    .map(word -> new KeyValuePair<String, Integer>(word, 1))
                    .reduceByKey(Integer::sum, 2);
            counts.foreachRDD((rdd, time) -> batchCounts.put(time, toMap(rdd.collect())));

            Map<Time, Map<String, Integer>> windowCounts = new LinkedHashMap<>();
            DStream<KeyValuePair<String, Integer>> windowed = words
                    .window(Duration.seconds(2), Duration.seconds(1))
                    .map(word -> new KeyValuePair<String, Integer>(word, 1))
                    .reduceByKey(Integer::sum, 2);
            windowed.foreachRDD((rdd, time) -> windowCounts.put(time, toMap(rdd.collect())));

            ssc.start();
            ssc.advance(2);

            assertEquals(2, batchCounts.size());
            assertEquals(Map.of("a", 2, "b", 1), batchCounts.get(new Time(1000)));
            assertEquals(Map.of("a", 1, "c", 1), batchCounts.get(new Time(2000)));

            assertEquals(Map.of("a", 2, "b", 1), windowCounts.get(new Time(1000)));
            assertEquals(Map.of("a", 3, "b", 1, "c", 1), windowCounts.get(new Time(2000)));
            assertTrue(ssc.batchesStarted() >= 2);
        }
    }

    @Test
    void queueStreamWithoutOneAtATimeKeepsCurrentQueueContents() {
        try (SparkContext sc = new SparkContext(2, false);
             StreamingContext ssc = new StreamingContext(sc, Duration.seconds(1))) {

            Queue<RDD<String>> lines = new LinkedList<>();
            lines.add(sc.parallelize(List.of("a"), 1));
            lines.add(sc.parallelize(List.of("b"), 1));

            DStream<String> words = ssc.queueStream(lines, false)
                    .flatMap(line -> List.of(line.split("\\s+")));

            Map<Time, Long> batchCounts = new LinkedHashMap<>();
            words.foreachRDD((rdd, time) -> batchCounts.put(time, rdd.count()));

            ssc.start();
            ssc.advance(2);

            assertEquals(2L, batchCounts.get(new Time(1000)));
            assertEquals(2L, batchCounts.get(new Time(2000)));
        }
    }

    @Test
    void socketTextStreamDrainsPerBatchAndSkipsEmptyBatch() throws Exception {
        try (LineServer server = new LineServer()) {
            server.start();
            try (SparkContext sc = new SparkContext(2, false);
                 StreamingContext ssc = new StreamingContext(sc, Duration.seconds(1))) {

                DStream<String> words = ssc.socketTextStream("localhost", server.port())
                        .flatMap(line -> List.of(line.split("\\s+")));
                Map<Time, Map<String, Integer>> batchCounts = new LinkedHashMap<>();
                DStream<KeyValuePair<String, Integer>> counts = words
                        .map(word -> new KeyValuePair<String, Integer>(word, 1))
                        .reduceByKey(Integer::sum, 2);
                counts.foreachRDD((rdd, time) -> batchCounts.put(time, toMap(rdd.collect())));

                ssc.start();
                sendAndAdvance(server, ssc, List.of("apple banana", "banana cherry")); // batch1
                sendAndAdvance(server, ssc, List.of());                                  // batch2 没数据
                sendAndAdvance(server, ssc, List.of("cherry date"));                     // batch3

                assertEquals(2, batchCounts.size());
                assertEquals(Map.of("apple", 1, "banana", 2, "cherry", 1),
                        batchCounts.get(new Time(1000)));
                assertEquals(Map.of("cherry", 1, "date", 1),
                        batchCounts.get(new Time(3000)));
            }
        }
    }

    private static void sendAndAdvance(LineServer server, StreamingContext ssc, List<String> lines)
            throws InterruptedException {
        server.send(lines);
        Thread.sleep(150);
        ssc.advance();
    }

    @Test
    void windowKeepsEnoughHistoryAcrossSeveralBatches() {
        try (SparkContext sc = new SparkContext(2, false);
             StreamingContext ssc = new StreamingContext(sc, Duration.seconds(1))) {

            Queue<RDD<String>> lines = new LinkedList<>();
            lines.add(sc.parallelize(List.of("a"), 1));
            lines.add(sc.parallelize(List.of("b"), 1));
            lines.add(sc.parallelize(List.of("c"), 1));
            lines.add(sc.parallelize(List.of("d"), 1));

            DStream<String> windowed = ssc.queueStream(lines)
                    .window(Duration.seconds(3), Duration.seconds(1));

            Map<Time, Long> windowCounts = new LinkedHashMap<>();
            windowed.foreachRDD((rdd, time) -> windowCounts.put(time, rdd.count()));

            ssc.start();
            ssc.advance(5);

            assertEquals(1L, windowCounts.get(new Time(1000)));
            assertEquals(2L, windowCounts.get(new Time(2000)));
            assertEquals(3L, windowCounts.get(new Time(3000)));
            assertEquals(3L, windowCounts.get(new Time(4000)));
            assertEquals(2L, windowCounts.get(new Time(5000)));
        }
    }

    private static Map<String, Integer> toMap(List<KeyValuePair<String, Integer>> pairs) {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (KeyValuePair<String, Integer> pair : pairs) {
            map.put(pair.key(), pair.value());
        }
        return map;
    }
}
