package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * NetworkTaskScheduler 的异步回调测试。
 *
 * <p>本章的 NetworkTaskScheduler 与 LocalTaskScheduler 一样改为异步回调。
 * Task 失败不再由本调度器重试，而是上报给 DAGScheduler。调度器内部按
 * "stageId:partitionIndex" 维护尝试计数，每次 submitTask 递增计数，
 * 用于在 Executor 列表里轮换——DAGScheduler 重试同一个 Task 时自然落到下一个 Executor。
 */
final class NetworkTaskSchedulerTest {

    @Test
    void resultTaskCanRunInExecutorJvm() throws Exception {
        int port = availablePort();
        ExecutorHandle executor = startExecutor(port);

        try (SparkContext sc = new SparkContext(
                new NetworkTaskScheduler(List.of("localhost:" + port)),
                3,
                false)) {
            RDD<String> rdd = sc.parallelize(
                    List.of("hello", "spark", "java"),
                    2);

            assertEquals(
                    List.of("HELLO", "SPARK"),
                    rdd.map(String::toUpperCase)
                            .filter(value -> value.startsWith("S")
                                    || value.startsWith("H"))
                            .collect());
        } finally {
            executor.close();
        }
    }

    @Test
    void fetchFailureIsReportedViaCallback() throws Exception {
        int port = availablePort();
        ExecutorHandle executor = startExecutor(port);

        try (SparkContext sc = new SparkContext(1);
             NetworkTaskScheduler scheduler = new NetworkTaskScheduler(
                     List.of("localhost:" + port))) {
            ShuffleDependency<String, Integer> dependency =
                    shuffleDependency(sc);

            RecordingHandler handler = new RecordingHandler(1);
            scheduler.submitTasks(List.of(
                    new FetchFailedTask(dependency)),
                    0, handler);

            assertTrue(handler.awaitFailure(5, TimeUnit.SECONDS));
            assertEquals(0, handler.successCount.get());
            assertEquals(1, handler.failureCount.get());
            assertTrue(handler.errors.get(0) instanceof FetchFailedException);
            FetchFailedException failure = (FetchFailedException) handler.errors.get(0);
            assertEquals(0, failure.mapId());
            assertEquals(0, failure.reduceId());
        } finally {
            executor.close();
        }
    }

    @Test
    void preferredLocationIsUsedBeforeRoundRobinExecutor() throws Exception {
        int unreachablePort = availablePort();
        int preferredPort = availablePort();
        ExecutorHandle executor = startExecutor(preferredPort);

        try (NetworkTaskScheduler scheduler = new NetworkTaskScheduler(
                List.of(
                        "localhost:" + unreachablePort,
                        "localhost:" + preferredPort))) {
            RecordingHandler handler = new RecordingHandler(1);
            scheduler.submitTasks(List.of(
                    new PreferredLocationTask("localhost:" + preferredPort)),
                    0, handler);

            assertTrue(handler.await(5, TimeUnit.SECONDS));
            assertEquals(List.of("partition-0"), handler.results);
        } finally {
            executor.close();
        }
    }

    @Test
    void retryMovesTaskToTheNextExecutor() throws Exception {
        int unreachablePort = availablePort();
        int healthyPort = availablePort();
        ExecutorHandle executor = startExecutor(healthyPort);

        try (NetworkTaskScheduler scheduler = new NetworkTaskScheduler(
                List.of(
                        "localhost:" + unreachablePort,
                        "localhost:" + healthyPort))) {
            RecordingHandler handler = new RecordingHandler(1);
            // 第一次提交：preferred location 是不可达端口，失败
            // 第二次提交（模拟 DAGScheduler 重试）：attemptId=1，轮换到健康端口
            scheduler.submitTask(
                    new PreferredLocationTask("localhost:" + unreachablePort),
                    0, 0, handler);
            // 等待失败后再次提交（DAGScheduler 在事件循环里也会这样做）
            handler.awaitFailure(5, TimeUnit.SECONDS);
            scheduler.submitTask(
                    new PreferredLocationTask("localhost:" + unreachablePort),
                    0, 0, handler);

            assertTrue(handler.awaitSuccess(5, TimeUnit.SECONDS));
            assertEquals(List.of("partition-0"), handler.results);
        } finally {
            executor.close();
        }
    }

    @Test
    void tasksAreSentAndExecutedConcurrently() throws Exception {
        int port = availablePort();
        ExecutorHandle executor = startExecutor(port, 2);

        try (NetworkTaskScheduler scheduler = new NetworkTaskScheduler(
                List.of("localhost:" + port))) {
            ConcurrentTask.reset();
            RecordingHandler handler = new RecordingHandler(2);
            scheduler.submitTasks(List.of(
                    new ConcurrentTask(),
                    new ConcurrentTask()),
                    0, handler);

            assertTrue(handler.await(5, TimeUnit.SECONDS));
            assertEquals(List.of(true, true), handler.results);
        } finally {
            executor.close();
        }
    }

    @Test
    void shuffleBlocksAreFetchedAcrossExecutorLocalDisks() throws Exception {
        int firstPort = availablePort();
        int secondPort = availablePort();
        ExecutorHandle first = startExecutor(firstPort, 2);
        ExecutorHandle second = startExecutor(secondPort, 2);

        try (SparkContext sc = new SparkContext(
                new NetworkTaskScheduler(List.of(
                        "localhost:" + firstPort,
                        "localhost:" + secondPort)),
                1,
                false)) {
            ShuffledRDD<String, Integer> shuffled = sc.parallelize(
                            List.of(
                                    new KeyValuePair<>("spark", 1),
                                    new KeyValuePair<>("java", 1),
                                    new KeyValuePair<>("spark", 1),
                                    new KeyValuePair<>("rpc", 1),
                                    new KeyValuePair<>("spark", 1),
                                    new KeyValuePair<>("java", 1)),
                            4)
                    .reduceByKey(Integer::sum, 2);

            Map<String, Integer> result = shuffled.collect().stream()
                    .collect(Collectors.toMap(
                            KeyValuePair::key,
                            KeyValuePair::value));

            assertEquals(Map.of(
                    "spark", 3,
                    "java", 2,
                    "rpc", 1), result);
            assertEquals(0, countFiles(shuffled.shuffleDir()));
        } finally {
            first.close();
            second.close();
        }
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static ExecutorHandle startExecutor(int port) {
        return startExecutor(port, 2);
    }

    private static ExecutorHandle startExecutor(int port, int executorCores) {
        Executor executor = new Executor(
                port,
                "localhost:" + port,
                executorCores);
        Thread executorThread = new Thread(() -> {
            try {
                executor.start();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }, "test-executor");
        executorThread.start();
        return new ExecutorHandle(executor, executorThread);
    }

    private static int countFiles(File dir) {
        File[] files = dir.listFiles();
        return files == null ? 0 : files.length;
    }

    @SuppressWarnings("unchecked")
    private static ShuffleDependency<String, Integer> shuffleDependency(
            SparkContext sc) {
        ShuffledRDD<String, Integer> shuffled = sc.parallelize(
                        List.of(new KeyValuePair<>("hello", 1)),
                        1)
                .reduceByKey((left, right) -> left + right, 1);
        return (ShuffleDependency<String, Integer>)
                shuffled.dependencies().get(0);
    }

    private record ExecutorHandle(Executor executor, Thread thread)
            implements AutoCloseable {

        @Override
        public void close() throws Exception {
            executor.close();
            thread.join(1_000);
        }
    }

    private static final class FetchFailedTask extends Task<Void> {

        private final ShuffleDependency<String, Integer> dependency;

        private FetchFailedTask(
                ShuffleDependency<String, Integer> dependency) {
            super(0, 0);
            this.dependency = dependency;
        }

        @Override
        protected Void runTask(TaskContext context) {
            throw new FetchFailedException(
                    dependency,
                    0,
                    0,
                    new File("missing-shuffle-file"),
                    new IOException("missing"));
        }
    }

    private static final class PreferredLocationTask extends Task<String> {

        private final String location;

        private PreferredLocationTask(String location) {
            super(0, 0);
            this.location = location;
        }

        @Override
        protected String runTask(TaskContext context) {
            return "partition-" + context.partition();
        }

        @Override
        public List<String> preferredLocations() {
            return List.of(location);
        }
    }

    private static final class ConcurrentTask extends Task<Boolean> {

        private static CountDownLatch bothRunning;

        private ConcurrentTask() {
            super(0, 0);
        }

        private static void reset() {
            bothRunning = new CountDownLatch(2);
        }

        @Override
        protected Boolean runTask(TaskContext context) {
            bothRunning.countDown();
            try {
                return bothRunning.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    /** 收集 TaskScheduler 回调结果的测试辅助类。 */
    private static final class RecordingHandler implements TaskCompletionHandler {

        final List<Object> results;
        final AtomicInteger successCount = new AtomicInteger();
        final AtomicInteger failureCount = new AtomicInteger();
        final List<Throwable> errors = new java.util.ArrayList<>();
        private final CountDownLatch successLatch;
        private final CountDownLatch failureLatch;

        RecordingHandler(int expected) {
            this.results = new java.util.ArrayList<>(java.util.Collections.nCopies(expected, null));
            this.successLatch = new CountDownLatch(expected);
            this.failureLatch = new CountDownLatch(expected);
        }

        @Override
        public synchronized void taskSucceeded(int stageId, int partitionIndex, Object result) {
            if (partitionIndex < results.size()) {
                results.set(partitionIndex, result);
            }
            successCount.incrementAndGet();
            successLatch.countDown();
        }

        @Override
        public synchronized void taskFailed(int stageId, int partitionIndex, Throwable error) {
            errors.add(error);
            failureCount.incrementAndGet();
            failureLatch.countDown();
        }

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return successLatch.await(timeout, unit);
        }

        boolean awaitSuccess(long timeout, TimeUnit unit) throws InterruptedException {
            return successLatch.await(timeout, unit);
        }

        boolean awaitFailure(long timeout, TimeUnit unit) throws InterruptedException {
            return failureLatch.await(timeout, unit);
        }
    }
}
