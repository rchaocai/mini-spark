package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class TaskSchedulerTest {

    @Test
    void listRddSplitsDataAcrossPartitions() {
        ListRDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5), 3);

        assertEquals(List.of(new Partition(0), new Partition(1), new Partition(2)), rdd.partitions());
        assertEquals(List.of(1, 2), collectPartition(rdd, new Partition(0)));
        assertEquals(List.of(3, 4), collectPartition(rdd, new Partition(1)));
        assertEquals(List.of(5), collectPartition(rdd, new Partition(2)));
    }

    @Test
    void parallelCollectKeepsPartitionOrder() {
        CountDownLatch laterPartitionsFinished = new CountDownLatch(2);
        List<Integer> computationOrder = Collections.synchronizedList(new ArrayList<>());
        RDD<Integer> rdd = new ListRDD<>(List.of(1, 2, 3), 3)
                .map(number -> {
                    if (number == 1) {
                        awaitLatch(laterPartitionsFinished, "later partitions did not finish first");
                    }
                    computationOrder.add(number);
                    if (number != 1) {
                        laterPartitionsFinished.countDown();
                    }
                    return number * 10;
                });

        try (TaskScheduler scheduler = new TaskScheduler(3)) {
            assertEquals(List.of(10, 20, 30), scheduler.collect(rdd));
        }
        assertEquals(1, computationOrder.get(2));
        assertNotEquals(1, computationOrder.get(0));
        assertNotEquals(1, computationOrder.get(1));
    }

    @Test
    void parallelCollectRunsPartitionsConcurrently() {
        CountDownLatch allPartitionsStarted = new CountDownLatch(4);
        Set<String> workerThreads = ConcurrentHashMap.newKeySet();

        RDD<Integer> rdd = new ListRDD<>(List.of(1, 2, 3, 4), 4)
                .map(number -> {
                    workerThreads.add(Thread.currentThread().getName());
                    allPartitionsStarted.countDown();
                    awaitAllPartitions(allPartitionsStarted);
                    return number;
                });

        try (TaskScheduler scheduler = new TaskScheduler(4)) {
            assertEquals(List.of(1, 2, 3, 4), scheduler.collect(rdd));
        }
        assertEquals(4, workerThreads.size());
    }

    @Test
    void countAndReduceMergePartitionResults() {
        RDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5), 3);

        try (TaskScheduler scheduler = new TaskScheduler(3)) {
            assertEquals(5, scheduler.count(rdd));
            assertEquals(15, scheduler.reduce(rdd, Integer::sum));
        }
    }

    @Test
    void reduceOnEmptyRddFailsClearly() {
        RDD<Integer> rdd = new ListRDD<>(List.of(), 3);

        try (TaskScheduler scheduler = new TaskScheduler(3)) {
            assertThrows(NoSuchElementException.class, () -> scheduler.reduce(rdd, Integer::sum));
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
