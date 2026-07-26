package com.sparklearn.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

            assertFalse(checkpointPoint.isCheckpointed());
            assertEquals(1, checkpointPoint.dependencies().size());
            assertEquals(List.of(48, 54, 60, 66, 72, 78, 84),
                    checkpointPoint.collect());
            assertTrue(checkpointPoint.isCheckpointed());
            assertEquals(1, checkpointPoint.dependencies().size());
            assertTrue(checkpointPoint.dependencies().get(0).rdd() instanceof CheckpointRDD);

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

    @Test
    void cacheReducesRepeatedTrainingPassesOverSameSource() {
        try (SparkContext sc = new SparkContext(3)) {
            List<Sample> samples = samples();

            RDD<Sample> plainSource = sc.parallelize(samples, 3);
            assertEquals(List.of(3, 3, 3),
                    runTrainingEpochCounts(plainSource, samples, false));

            RDD<Sample> cachedSource = sc.parallelize(samples, 3);
            assertEquals(List.of(3, 0, 0),
                    runTrainingEpochCounts(cachedSource, samples, true));
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

    private static List<Integer> runTrainingEpochCounts(
            RDD<Sample> source,
            List<Sample> samples,
            boolean cacheSource) {
        if (cacheSource) {
            source.cache();
        }

        double weight = 0.0;
        double learningRate = 0.1;
        List<Integer> counts = new java.util.ArrayList<>();
        for (int epoch = 0; epoch < 3; epoch++) {
            source.resetComputeCount();
            double currentWeight = weight;
            double gradient = source
                    .map(sample -> (sigmoid(currentWeight * sample.x()) - sample.y()) * sample.x())
                    .reduce(Double::sum);
            weight -= learningRate * gradient / samples.size();
            counts.add(source.getComputeCount());
        }
        return List.copyOf(counts);
    }

    private static List<Sample> samples() {
        return List.of(
                new Sample(0.5, 0.0),
                new Sample(1.0, 0.0),
                new Sample(2.0, 0.0),
                new Sample(3.0, 1.0),
                new Sample(4.0, 1.0),
                new Sample(5.0, 1.0));
    }

    private record Sample(double x, double y) {
    }

    private static double sigmoid(double z) {
        return 1.0 / (1.0 + Math.exp(-z));
    }
}
