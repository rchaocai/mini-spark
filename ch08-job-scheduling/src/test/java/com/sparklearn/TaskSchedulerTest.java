package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

final class TaskSchedulerTest {

    @Test
    void listRddSplitsDataAcrossPartitions() {
        try (SparkContext sc = new SparkContext(3)) {
            RDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 3);

            assertEquals(List.of(new Partition(0), new Partition(1), new Partition(2)), rdd.partitions());
            assertEquals(List.of(1, 2), collectPartition(rdd, new Partition(0)));
            assertEquals(List.of(3, 4), collectPartition(rdd, new Partition(1)));
            assertEquals(List.of(5), collectPartition(rdd, new Partition(2)));
        }
    }

    @Test
    void tasksCompleteAndCallBackHandler() throws Exception {
        try (TaskScheduler scheduler = new TaskScheduler(3)) {
            CountDownLatch done = new CountDownLatch(3);
            Map<Integer, Integer> results = new ConcurrentHashMap<>();

            List<Callable<Integer>> tasks = List.of(() -> 10, () -> 20, () -> 30);

            scheduler.submitTasks(tasks, 0, new TaskCompletionHandler() {
                @Override
                public void taskSucceeded(int stageId, int partitionIndex, Object result) {
                    results.put(partitionIndex, (Integer) result);
                    done.countDown();
                }

                @Override
                public void taskFailed(int stageId, int partitionIndex, Throwable error) {
                    done.countDown();
                }
            });

            assertTrue(done.await(5, TimeUnit.SECONDS), "tasks did not complete in time");
            assertEquals(Map.of(0, 10, 1, 20, 2, 30), results);
        }
    }

    @Test
    void submitTasksRunsTasksConcurrently() throws Exception {
        CountDownLatch allStarted = new CountDownLatch(4);
        Set<String> workerThreads = ConcurrentHashMap.newKeySet();

        try (TaskScheduler scheduler = new TaskScheduler(4)) {
            List<Callable<Integer>> tasks = List.of(
                    () -> runConcurrentTask(1, allStarted, workerThreads),
                    () -> runConcurrentTask(2, allStarted, workerThreads),
                    () -> runConcurrentTask(3, allStarted, workerThreads),
                    () -> runConcurrentTask(4, allStarted, workerThreads));

            CountDownLatch done = new CountDownLatch(4);
            scheduler.submitTasks(tasks, 0, new TaskCompletionHandler() {
                @Override
                public void taskSucceeded(int stageId, int partitionIndex, Object result) {
                    done.countDown();
                }

                @Override
                public void taskFailed(int stageId, int partitionIndex, Throwable error) {
                    done.countDown();
                }
            });

            assertTrue(done.await(5, TimeUnit.SECONDS));
        }
        assertEquals(4, workerThreads.size());
    }

    @Test
    void taskFailureIsReportedViaHandler() throws Exception {
        try (TaskScheduler scheduler = new TaskScheduler(1)) {
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Throwable> errorRef = new AtomicReference<>();

            List<Callable<Integer>> tasks = List.of(() -> {
                throw new IllegalArgumentException("boom");
            });

            scheduler.submitTasks(tasks, 0, new TaskCompletionHandler() {
                @Override
                public void taskSucceeded(int stageId, int partitionIndex, Object result) {
                    done.countDown();
                }

                @Override
                public void taskFailed(int stageId, int partitionIndex, Throwable error) {
                    errorRef.set(error);
                    done.countDown();
                }
            });

            assertTrue(done.await(5, TimeUnit.SECONDS));
            assertNotNull(errorRef.get());
            assertTrue(errorRef.get() instanceof IllegalArgumentException);
        }
    }

    @Test
    void invalidThreadCountFailsClearly() {
        assertThrows(IllegalArgumentException.class, () -> new TaskScheduler(0));
    }

    // ── helpers ─────────────────────────────────────────────────

    private static <T> List<T> collectPartition(RDD<T> rdd, Partition partition) {
        List<T> result = new ArrayList<>();
        rdd.iterator(partition).forEachRemaining(result::add);
        return result;
    }

    private static int runConcurrentTask(
            int number,
            CountDownLatch allStarted,
            Set<String> workerThreads) {
        workerThreads.add(Thread.currentThread().getName());
        allStarted.countDown();
        try {
            allStarted.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return number;
    }
}
