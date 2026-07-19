package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

final class CacheCheckpointTest {

    @Test
    void cacheAvoidsRecomputingUpstreamPartitions() {
        try (SparkContext sc = new SparkContext(3)) {
            RDD<Integer> source = sc.parallelize(input(), 3);
            RDD<Integer> chain = buildLongLineage(source);
            RDD<Integer> cachePoint = traceUp(chain, 3);
            cachePoint.cache();

            assertEquals(List.of(44, 50, 56, 62, 68, 74, 80), chain.collect());
            assertEquals(3, source.getComputeCount());
            assertEquals(3, cachePoint.getComputeCount());

            source.resetComputeCount();
            cachePoint.resetComputeCount();
            chain.resetComputeCount();

            assertEquals(List.of(44, 50, 56, 62, 68, 74, 80), chain.collect());
            assertEquals(0, source.getComputeCount());
            assertEquals(0, cachePoint.getComputeCount());
            assertEquals(3, chain.getComputeCount());
        }
    }

    @Test
    void checkpointCutsDependenciesAndReadsMaterializedData() {
        try (SparkContext sc = new SparkContext(3)) {
            RDD<Integer> source = sc.parallelize(input(), 3);
            RDD<Integer> chain = buildLongLineage(source);
            RDD<Integer> checkpointPoint = traceUp(chain, 3);

            assertEquals(1, checkpointPoint.dependencies().size());
            checkpointPoint.checkpoint();

            assertTrue(checkpointPoint.isCheckpointed());
            assertEquals(0, checkpointPoint.dependencies().size());
            assertEquals(List.of(48, 54, 60, 66, 72, 78, 84),
                    checkpointPoint.collect());

            source.resetComputeCount();
            checkpointPoint.resetComputeCount();

            RDD<Integer> downstream = checkpointPoint
                    .map(value -> value * 10)
                    .filter(value -> value > 200);

            assertEquals(List.of(480, 540, 600, 660, 720, 780, 840),
                    downstream.collect());
            assertEquals(0, source.getComputeCount());
            assertEquals(0, checkpointPoint.getComputeCount());
        }
    }

    private static List<Integer> input() {
        return List.of(1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    private static RDD<Integer> buildLongLineage(RDD<Integer> source) {
        return source
                .map(value -> value * 2)
                .filter(value -> value > 5)
                .map(value -> value + 10)
                .filter(value -> value < 30)
                .map(value -> value * 3)
                .filter(value -> value > 30)
                .map(value -> value - 5)
                .map(value -> value + 1);
    }

    private static RDD<Integer> traceUp(RDD<?> rdd, int steps) {
        RDD<?> current = rdd;
        for (int index = 0; index < steps; index++) {
            current = current.dependencies().get(0).rdd();
        }
        @SuppressWarnings("unchecked")
        RDD<Integer> result = (RDD<Integer>) current;
        return result;
    }
}
