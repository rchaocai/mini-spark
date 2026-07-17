package com.sparklearn;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import org.junit.jupiter.api.Test;

final class NetworkTaskSchedulerTest {

    @Test
    void resultTaskCanRunInWorkerJvm() throws Exception {
        int port = availablePort();
        Worker worker = new Worker(port);
        Thread workerThread = new Thread(() -> {
            try {
                worker.start();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }, "test-worker");
        workerThread.start();

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
            worker.close();
            workerThread.join(1_000);
        }
    }

    private static int availablePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}

