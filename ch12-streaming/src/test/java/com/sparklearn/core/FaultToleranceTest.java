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

import com.sparklearn.core.rdd.MissingMapOutputRDD;
import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.rdd.ShuffledRDD;
import org.junit.jupiter.api.Test;

/**
 * 容错测试：Task 重试与 Fetch 失败恢复。
 *
 * <p>本章的容错逻辑全部在 DAGScheduler 的事件循环里处理：
 * <ul>
 *   <li>普通 Task 异常 → DAGScheduler 重试同一个 Task（≤ maxTaskRetries 次）</li>
 *   <li>FetchFailedException → DAGScheduler 重算来源 Map 分区，恢复后只重提失败 Task</li>
 * </ul>
 */
final class FaultToleranceTest {

    @Test
    void dagSchedulerRetriesOnlyTheFailedTask() {
        AtomicInteger failedTaskAttempts = new AtomicInteger();
        AtomicInteger healthyTaskAttempts = new AtomicInteger();

        try (SparkContext sc = new SparkContext(2, 1, false)) {
            RDD<Integer> source = sc.parallelize(List.of(1, 2, 3, 4), 2);
            RDD<Integer> faulty = new FailingOnceRDD<>(
                    source,
                    0,
                    failedTaskAttempts,
                    healthyTaskAttempts);

            assertEquals(List.of(10, 20, 30, 40), faulty.map(v -> v * 10).collect());
        }

        // 失败的分区被重试了一次（共 2 次尝试）
        assertEquals(2, failedTaskAttempts.get());
        // 健康的分区只执行了一次
        assertEquals(1, healthyTaskAttempts.get());
    }

    @Test
    void dagSchedulerStopsAfterRetryLimit() {
        AtomicInteger attempts = new AtomicInteger();

        try (SparkContext sc = new SparkContext(1, 1, false)) {
            RDD<Integer> source = sc.parallelize(List.of(1, 2), 1);
            RDD<Integer> alwaysFails = new AlwaysFailingRDD<>(source, attempts);

            assertThrows(
                    RuntimeException.class,
                    () -> alwaysFails.collect());
        }

        // 第一次执行 + 1 次重试 = 2 次尝试
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

    /**
     * 测试用 RDD：指定分区第一次执行抛异常，第二次正常返回。
     */
    private static final class FailingOnceRDD<T> extends RDD<T> {

        private final RDD<T> parent;
        private final int failingPartition;
        private final AtomicInteger failedAttempts;
        private final AtomicInteger healthyAttempts;
        private final List<Dependency<?>> dependencies;

        private FailingOnceRDD(
                RDD<T> parent,
                int failingPartition,
                AtomicInteger failedAttempts,
                AtomicInteger healthyAttempts) {
            super(parent.sparkContext());
            this.parent = parent;
            this.failingPartition = failingPartition;
            this.failedAttempts = failedAttempts;
            this.healthyAttempts = healthyAttempts;
            this.dependencies = List.of(new OneToOneDependency<>(parent));
        }

        @Override
        protected List<Partition> getPartitionsInternal() {
            return parent.partitions();
        }

        @Override
        public Iterator<T> compute(Partition partition) {
            if (partition.index() == failingPartition) {
                int attempt = failedAttempts.incrementAndGet();
                if (attempt == 1) {
                    throw new RuntimeException("transient failure");
                }
            } else {
                healthyAttempts.incrementAndGet();
            }
            return parent.iterator(partition);
        }

        @Override
        protected List<Dependency<?>> getDependenciesInternal() {
            return dependencies;
        }
    }

    /**
     * 测试用 RDD：所有分区每次执行都抛异常。
     */
    private static final class AlwaysFailingRDD<T> extends RDD<T> {

        private final RDD<T> parent;
        private final AtomicInteger attempts;
        private final List<Dependency<?>> dependencies;

        private AlwaysFailingRDD(RDD<T> parent, AtomicInteger attempts) {
            super(parent.sparkContext());
            this.parent = parent;
            this.attempts = attempts;
            this.dependencies = List.of(new OneToOneDependency<>(parent));
        }

        @Override
        protected List<Partition> getPartitionsInternal() {
            return parent.partitions();
        }

        @Override
        public Iterator<T> compute(Partition partition) {
            attempts.incrementAndGet();
            throw new RuntimeException("permanent failure");
        }

        @Override
        protected List<Dependency<?>> getDependenciesInternal() {
            return dependencies;
        }
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
        protected List<Partition> getPartitionsInternal() {
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
