package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

final class SparkSourceMapDemoTest {

    @Test
    void printContainsKeyMappings() {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            assertDoesNotThrow(SparkSourceMapDemo::print);
        } finally {
            System.setOut(original);
        }

        String output = buffer.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("RDD 五接口抽象"));
        assertTrue(output.contains("DAGScheduler"));
        assertTrue(output.contains("ShuffleMapTask"));
        assertTrue(output.contains("core/src/main/scala/spark/RDD.scala"));
        assertTrue(output.contains("getParentStages"));
    }

    @Test
    void pipelineStillRunsThroughSparkContext() {
        List<String> words = List.of("hello", "world", "hello", "spark", "world", "hello");
        try (SparkContext sc = new SparkContext(2)) {
            RDD<KeyValuePair<String, Integer>> pairs = sc.parallelize(words, 3)
                    .map(w -> new KeyValuePair<>(w, 1));
            ShuffledRDD<String, Integer> counts = pairs.reduceByKey(
                    (Integer left, Integer right) -> left + right,
                    2);
            List<KeyValuePair<String, Integer>> result = counts.collect();

            assertEquals(3, result.size());
            assertTrue(result.contains(new KeyValuePair<>("hello", 3)));
            assertTrue(result.contains(new KeyValuePair<>("world", 2)));
            assertTrue(result.contains(new KeyValuePair<>("spark", 1)));
        }
    }
}
