package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sparklearn.rdd.RDD;
import com.sparklearn.scheduler.*;
import com.sparklearn.util.SerializableFunction;
import org.junit.jupiter.api.Test;

/**
 * 异步版 LocalTaskScheduler 的测试。
 *
 * <p>本章的 TaskScheduler 只负责把 Task 提交到线程池并回调，不再做重试。
 * 重试逻辑由 DAGScheduler 在事件循环里处理。
 */
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
    void submitTasksRunsTasksConcurrently() throws InterruptedException {
        CountDownLatch allPartitionsStarted = new CountDownLatch(4);
        Set<String> workerThreads = ConcurrentHashMap.newKeySet();

        try (TaskScheduler scheduler = new LocalTaskScheduler(4)) {
            RecordingHandler handler = new RecordingHandler(4);
            scheduler.submitTasks(List.of(
                    task(context -> runConcurrentTask(1, allPartitionsStarted, workerThreads)),
                    task(context -> runConcurrentTask(2, allPartitionsStarted, workerThreads)),
                    task(context -> runConcurrentTask(3, allPartitionsStarted, workerThreads)),
                    task(context -> runConcurrentTask(4, allPartitionsStarted, workerThreads))),
                    0, handler);

            assertTrue(handler.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(1, 2, 3, 4), handler.results);
            assertEquals(4, workerThreads.size());
        }
    }

    @Test
    void submitTasksCallsBackOnFailure() throws InterruptedException {
        try (TaskScheduler scheduler = new LocalTaskScheduler(1)) {
            RecordingHandler handler = new RecordingHandler(1);
            scheduler.submitTasks(List.of(
                    task(context -> {
                        throw new IllegalArgumentException("boom");
                    })),
                    0, handler);

            assertTrue(handler.await(5, TimeUnit.SECONDS));
            assertEquals(0, handler.successCount.get());
            assertEquals(1, handler.failureCount.get());
            assertTrue(handler.errors.get(0) instanceof IllegalArgumentException);
        }
    }

    @Test
    void invalidThreadCountFailsClearly() {
        assertThrows(IllegalArgumentException.class, () -> new LocalTaskScheduler(0));
    }

    private static <T> List<T> collectPartition(RDD<T> rdd, Partition partition) {
        java.util.List<T> result = new java.util.ArrayList<>();
        rdd.iterator(partition).forEachRemaining(result::add);
        return result;
    }

    private static int runConcurrentTask(
            int number,
            CountDownLatch allPartitionsStarted,
            Set<String> workerThreads) {
        workerThreads.add(Thread.currentThread().getName());
        allPartitionsStarted.countDown();
        try {
            allPartitionsStarted.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return number;
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

    /** 收集 TaskScheduler 回调结果的测试辅助类。 */
    private static final class RecordingHandler implements TaskCompletionHandler {

        final List<Object> results = new java.util.ArrayList<>(java.util.Collections.nCopies(4, null));
        final AtomicInteger successCount = new AtomicInteger();
        final AtomicInteger failureCount = new AtomicInteger();
        final List<Throwable> errors = new java.util.ArrayList<>();
        private final CountDownLatch done;

        RecordingHandler(int expected) {
            this.done = new CountDownLatch(expected);
        }

        @Override
        public synchronized void taskSucceeded(int stageId, int partitionIndex, Object result) {
            results.set(partitionIndex, result);
            successCount.incrementAndGet();
            done.countDown();
        }

        @Override
        public synchronized void taskFailed(int stageId, int partitionIndex, Throwable error) {
            errors.add(error);
            failureCount.incrementAndGet();
            done.countDown();
        }

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return done.await(timeout, unit);
        }
    }
}
