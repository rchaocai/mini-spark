package com.sparklearn.core;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

final class ShuffledRDDTest {

    @Test
    void computeRequiresExistingShuffleMapOutputs() {
        try (SparkContext sc = new SparkContext(2)) {
            ShuffledRDD<String, Integer> shuffled = newShuffledRdd(sc);
            File shuffleDir = shuffled.shuffleDir();

            try {
                assertThrows(
                        RuntimeException.class,
                        () -> shuffled.iterator(new Partition(0)).hasNext());
            } finally {
                shuffleDir.delete();
            }
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
        return rdd.reduceByKey((left, right) -> left + right, 2);
    }

}
