package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class DAGSchedulerTest {

    @Test
    void reduceByKeyIsSplitIntoShuffleMapStageAndResultStage() {
        try (SparkContext sc = new SparkContext(2)) {
            RDD<KeyValuePair<String, Integer>> source = newSourceRdd(sc);
            MapPartitionsRDD<KeyValuePair<String, Integer>, KeyValuePair<String, Integer>> mapped =
                    source.map(value -> value);
            ShuffledRDD<String, Integer> shuffled = mapped.reduceByKey(
                    (left, right) -> left + right,
                    2);

            Stage finalStage = new DAGScheduler().createResultStage(shuffled);

            assertFalse(finalStage.shuffleMap());
            assertSame(shuffled, finalStage.rdd());
            assertEquals(1, finalStage.parents().size());

            Stage parent = finalStage.parents().get(0);
            assertTrue(parent.shuffleMap());
            assertSame(mapped, parent.rdd());
            assertEquals(0, parent.parents().size());
        }
    }

    @Test
    void collectActionMaterializesShuffleBeforeCollectingResult() {
        try (SparkContext sc = new SparkContext(2)) {
            ShuffledRDD<String, Integer> shuffled = newSourceRdd(sc)
                    .map(value -> value)
                    .reduceByKey((left, right) -> left + right, 2);

            assertEquals(0, countFiles(shuffled.shuffleDir()));

            Map<String, Integer> result = toMap(shuffled.collect());

            assertEquals(Map.of(
                    "hello", 4,
                    "world", 2,
                    "spark", 2,
                    "java", 1), result);
            assertEquals(6, countFiles(shuffled.shuffleDir()));
            cleanup(shuffled.shuffleDir());
        }
    }

    @Test
    void resultTasksApplyTheActionFunctionPerPartition() {
        try (SparkContext sc = new SparkContext(3)) {
            RDD<Integer> rdd = sc.parallelize(List.of(1, 2, 3, 4, 5), 3);

            assertEquals(5, rdd.count());
            assertEquals(15, rdd.reduce((left, right) -> left + right));
        }
    }

    private static RDD<KeyValuePair<String, Integer>> newSourceRdd(SparkContext sc) {
        List<KeyValuePair<String, Integer>> data = Arrays.asList(
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("java", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("hello", 1));
        return sc.parallelize(data, 3);
    }

    private static Map<String, Integer> toMap(List<KeyValuePair<String, Integer>> values) {
        return values.stream().collect(Collectors.toMap(
                KeyValuePair::key,
                KeyValuePair::value));
    }

    private static int countFiles(File dir) {
        File[] files = dir.listFiles();
        return files == null ? 0 : files.length;
    }

    private static void cleanup(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
        dir.delete();
    }
}
