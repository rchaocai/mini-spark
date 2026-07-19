package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import org.junit.jupiter.api.Test;

final class NetworkTaskSchedulerTest {

    @Test
    void resultTaskCanRunInExecutorJvm() throws Exception {
        int port = availablePort();
        ExecutorHandle executor = startExecutor(port);

        try (SparkContext sc = new SparkContext(
                new NetworkTaskScheduler(
                        List.of("localhost:" + port),
                        10,
                        false),
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
    void fetchFailureIsReportedToDriverWithoutTaskRetry() throws Exception {
        int port = availablePort();
        ExecutorHandle executor = startExecutor(port);

        try (SparkContext sc = new SparkContext(1);
             NetworkTaskScheduler scheduler = new NetworkTaskScheduler(
                     List.of("localhost:" + port),
                     3,
                     false)) {
            ShuffleDependency<String, Integer> dependency =
                    shuffleDependency(sc);

            FetchFailedException failure = assertThrows(
                    FetchFailedException.class,
                    () -> scheduler.submitTasks(List.of(
                            new FetchFailedTask(dependency))));

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
                        "localhost:" + preferredPort),
                0,
                false)) {
            List<String> result = scheduler.submitTasks(List.of(
                    new PreferredLocationTask("localhost:" + preferredPort)));

            assertEquals(List.of("partition-0"), result);
        } finally {
            executor.close();
        }
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static ExecutorHandle startExecutor(int port) {
        Executor executor = new Executor(port);
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
}
