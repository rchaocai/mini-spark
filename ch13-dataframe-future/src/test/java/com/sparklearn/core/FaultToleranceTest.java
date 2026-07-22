package com.sparklearn.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

final class FaultToleranceTest {

    @Test
    void taskSchedulerRetriesOnlyTheFailedTask() {
        AtomicInteger failedTaskAttempts = new AtomicInteger();
        AtomicInteger healthyTaskAttempts = new AtomicInteger();

        Task<Integer> failsOnce = task(context -> {
            if (failedTaskAttempts.incrementAndGet() == 1) {
                throw new IllegalStateException("transient failure");
            }
            return 10;
        });
        Task<Integer> succeedsImmediately = task(context -> {
            healthyTaskAttempts.incrementAndGet();
            return 20;
        });

        try (TaskScheduler scheduler = new LocalTaskScheduler(2, 1)) {
            assertEquals(
                    List.of(10, 20),
                    scheduler.submitTasks(List.of(
                            failsOnce,
                            succeedsImmediately)));
        }

        assertEquals(2, failedTaskAttempts.get());
        assertEquals(1, healthyTaskAttempts.get());
    }

    @Test
    void taskSchedulerStopsAfterRetryLimit() {
        AtomicInteger attempts = new AtomicInteger();
        Task<Integer> alwaysFails = task(context -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("permanent failure");
        });

        try (TaskScheduler scheduler = new LocalTaskScheduler(1, 1)) {
            assertThrows(
                    IllegalStateException.class,
                    () -> scheduler.submitTasks(List.of(alwaysFails)));
        }

        assertEquals(2, attempts.get());
    }

    @Test
    void resultTaskRecomputesTheFailedPartitionFromLineage() {
        AtomicInteger remainingFailures = new AtomicInteger(1);

        try (SparkContext sc = new SparkContext(2, 1, false)) {
            RDD<Integer> source = sc.parallelize(
                    List.of(1, 2, 3, 4, 5, 6),
                    3);
            RDD<Integer> mapped = source.map(value -> value * 10);
            RDD<Integer> faulty = mapped.failOnNext(
                    0,
                    2,
                    remainingFailures);

            assertEquals(
                    List.of(10, 20, 30, 40, 50, 60),
                    faulty.collect());
        }

        assertEquals(0, remainingFailures.get());
    }

    @Test
    void shuffleMapTaskRecomputesTheFailedMapPartition() {
        AtomicInteger remainingFailures = new AtomicInteger(1);

        try (SparkContext sc = new SparkContext(2, 1, false)) {
            RDD<KeyValuePair<String, Integer>> source =
                    sc.parallelize(words(), 3);
            ShuffledRDD<String, Integer> shuffled = source
                    .failOnNext(0, 2, remainingFailures)
                    .reduceByKey((left, right) -> left + right, 2);

            try {
                assertEquals(Map.of(
                        "hello", 4,
                        "world", 2,
                        "spark", 2,
                        "java", 1), toMap(shuffled.collect()));
                assertEquals(6, countFiles(shuffled.shuffleDir()));
            } finally {
                cleanup(shuffled.shuffleDir());
            }
        }

        assertEquals(0, remainingFailures.get());
    }

    @Test
    void fetchFailureRecomputesOnlyTheMissingMapOutput() {
        AtomicIntegerArray computeCounts = new AtomicIntegerArray(3);

        try (SparkContext sc = new SparkContext(2, 0, false)) {
            RDD<KeyValuePair<String, Integer>> source =
                    sc.parallelize(words(), 3);
            RDD<KeyValuePair<String, Integer>> counted =
                    new CountingRDD<>(source, computeCounts);
            ShuffledRDD<String, Integer> shuffled = counted.reduceByKey(
                    (left, right) -> left + right,
                    2);
            RDD<KeyValuePair<String, Integer>> missingOutput =
                    new MissingMapOutputRDD<>(
                            shuffled,
                            1,
                            0);

            try {
                assertEquals(Map.of(
                        "hello", 4,
                        "world", 2,
                        "spark", 2,
                        "java", 1), toMap(missingOutput.collect()));
                assertEquals(1, computeCounts.get(0));
                assertEquals(2, computeCounts.get(1));
                assertEquals(1, computeCounts.get(2));
                assertTrue(shuffled.mapOutputFile(1, 0).isFile());
            } finally {
                cleanup(shuffled.shuffleDir());
            }
        }
    }

    private static List<KeyValuePair<String, Integer>> words() {
        return Arrays.asList(
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("world", 1),
                new KeyValuePair<>("hello", 1),
                new KeyValuePair<>("java", 1),
                new KeyValuePair<>("spark", 1),
                new KeyValuePair<>("hello", 1));
    }

    private static Task<Integer> task(
            SerializableFunction<TaskContext, Integer> function) {
        return new Task<>(0, 0) {
            @Override
            protected Integer runTask(TaskContext context) {
                return function.apply(context);
            }
        };
    }

    private static Map<String, Integer> toMap(
            List<KeyValuePair<String, Integer>> values) {
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

    private static final class CountingRDD<T> extends RDD<T> {

        private final RDD<T> parent;
        private final AtomicIntegerArray computeCounts;
        private final List<Dependency<?>> dependencies;

        private CountingRDD(
                RDD<T> parent,
                AtomicIntegerArray computeCounts) {
            super(parent.sparkContext());
            this.parent = parent;
            this.computeCounts = computeCounts;
            this.dependencies = List.of(
                    new OneToOneDependency<>(parent));
        }

        @Override
        public List<Partition> partitions() {
            return parent.partitions();
        }

        @Override
        public Iterator<T> compute(Partition partition) {
            computeCounts.incrementAndGet(partition.index());
            return parent.iterator(partition);
        }

        @Override
        protected List<Dependency<?>> getDependenciesInternal() {
            return dependencies;
        }
    }
}
