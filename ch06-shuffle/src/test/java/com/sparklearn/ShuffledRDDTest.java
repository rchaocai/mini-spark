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
        try (SparkContext sc = new SparkContext(2)) {
            ShuffledRDD<String, Integer> shuffled = newShuffledRdd(sc);

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
    void missingShuffleFilesFailOnSecondCollect() {
        try (SparkContext sc = new SparkContext(2)) {
            ShuffledRDD<String, Integer> shuffled = newShuffledRdd(sc);

            shuffled.collect();

            cleanup(shuffled.shuffleDir());

            assertThrows(RuntimeException.class, shuffled::collect);
        }
    }

    private static ShuffledRDD<String, Integer> newShuffledRdd(SparkContext sc) {
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
        RDD<KeyValuePair<String, Integer>> rdd = sc.parallelize(data, 3);
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
