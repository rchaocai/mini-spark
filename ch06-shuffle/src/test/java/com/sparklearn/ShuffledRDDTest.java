package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class ShuffledRDDTest {

    @Test
    void reduceByKeyIsLazyAndAggregatesCorrectly() {
        ShuffledRDD<String, Integer> shuffled = newShuffledRdd();

        assertEquals(0, countFiles(shuffled.shuffleDir()));

        try (TaskScheduler scheduler = new TaskScheduler(2)) {
            Map<String, Integer> result = toMap(scheduler.collect(shuffled));
            assertEquals(Map.of(
                    "hello", 4,
                    "world", 2,
                    "spark", 2,
                    "java", 1), result);
        }

        assertEquals(6, countFiles(shuffled.shuffleDir()));
        cleanup(shuffled.shuffleDir());
    }

    @Test
    void missingShuffleFilesFailOnSecondCollect() {
        ShuffledRDD<String, Integer> shuffled = newShuffledRdd();

        try (TaskScheduler scheduler = new TaskScheduler(2)) {
            scheduler.collect(shuffled);
        }

        cleanup(shuffled.shuffleDir());

        try (TaskScheduler scheduler = new TaskScheduler(2)) {
            assertThrows(RuntimeException.class, () -> scheduler.collect(shuffled));
        }
    }

    private static ShuffledRDD<String, Integer> newShuffledRdd() {
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
        ListRDD<KeyValuePair<String, Integer>> rdd = new ListRDD<>(data, 3);
        return rdd.reduceByKey(Integer::sum, 2);
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
