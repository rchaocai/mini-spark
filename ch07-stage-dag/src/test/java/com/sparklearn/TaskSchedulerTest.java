package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
    void parallelTasksKeepSubmissionOrder() {
        CountDownLatch laterPartitionsFinished = new CountDownLatch(2);
        List<Integer> computationOrder = Collections.synchronizedList(new ArrayList<>());
        try (TaskScheduler scheduler = new TaskScheduler(3)) {
            List<Callable<Integer>> tasks = List.of(
                    () -> runOrderedTask(
                            1, laterPartitionsFinished, computationOrder),
                    () -> runOrderedTask(
                            2, laterPartitionsFinished, computationOrder),
                    () -> runOrderedTask(
                            3, laterPartitionsFinished, computationOrder));

            assertEquals(List.of(10, 20, 30), scheduler.submitTasks(tasks));
        }
        assertEquals(1, computationOrder.get(2));
        assertNotEquals(1, computationOrder.get(0));
        assertNotEquals(1, computationOrder.get(1));
    }

    @Test
    void submitTasksRunsTasksConcurrently() {
        CountDownLatch allPartitionsStarted = new CountDownLatch(4);
        Set<String> workerThreads = ConcurrentHashMap.newKeySet();

        try (TaskScheduler scheduler = new TaskScheduler(4)) {
            List<Callable<Integer>> tasks = List.of(
                    () -> runConcurrentTask(1, allPartitionsStarted, workerThreads),
                    () -> runConcurrentTask(2, allPartitionsStarted, workerThreads),
                    () -> runConcurrentTask(3, allPartitionsStarted, workerThreads),
                    () -> runConcurrentTask(4, allPartitionsStarted, workerThreads));

            assertEquals(List.of(1, 2, 3, 4), scheduler.submitTasks(tasks));
        }
        assertEquals(4, workerThreads.size());
    }

    @Test
    void taskFailureIsReported() {
        try (TaskScheduler scheduler = new TaskScheduler(1)) {
            List<Callable<Integer>> tasks = List.of(
                    () -> {
                        throw new IllegalArgumentException("boom");
                    });
            assertThrows(IllegalStateException.class, () -> scheduler.submitTasks(tasks));
        }
    }

    @Test
    void invalidThreadCountFailsClearly() {
        assertThrows(IllegalArgumentException.class, () -> new TaskScheduler(0));
    }

    private static <T> List<T> collectPartition(RDD<T> rdd, Partition partition) {
        List<T> result = new ArrayList<>();
        rdd.iterator(partition).forEachRemaining(result::add);
        return result;
    }

    private static int runOrderedTask(
            int number,
            CountDownLatch laterPartitionsFinished,
            List<Integer> computationOrder) {
        if (number == 1) {
            awaitLatch(laterPartitionsFinished, "later tasks did not finish first");
        }
        computationOrder.add(number);
        if (number != 1) {
            laterPartitionsFinished.countDown();
        }
        return number * 10;
    }

    private static int runConcurrentTask(
            int number,
            CountDownLatch allPartitionsStarted,
            Set<String> workerThreads) {
        workerThreads.add(Thread.currentThread().getName());
        allPartitionsStarted.countDown();
        awaitAllPartitions(allPartitionsStarted);
        return number;
    }

    private static void awaitAllPartitions(CountDownLatch allPartitionsStarted) {
        awaitLatch(allPartitionsStarted, "partitions did not run concurrently");
    }

    private static void awaitLatch(CountDownLatch latch, String timeoutMessage) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException(timeoutMessage);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("test interrupted", e);
        }
    }
}
