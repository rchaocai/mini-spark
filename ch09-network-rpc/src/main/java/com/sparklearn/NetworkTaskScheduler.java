package com.sparklearn;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Socket 版 TaskScheduler。
 *
 * <p>它和 TaskScheduler 一样接收 DAGScheduler 创建好的 Task；区别只是任务不再进入
 * 本地线程池，而是被序列化后写进 Socket，由另一个 JVM 里的 Worker 执行。
 */
public final class NetworkTaskScheduler implements TaskScheduler {

    private final List<String> workerAddresses;
    private final int maxTaskRetries;
    private final boolean verbose;

    public NetworkTaskScheduler(List<String> workerAddresses) {
        this(workerAddresses, 3, false);
    }

    public NetworkTaskScheduler(
            List<String> workerAddresses,
            int maxTaskRetries,
            boolean verbose) {
        Objects.requireNonNull(workerAddresses, "workerAddresses");
        if (workerAddresses.isEmpty()) {
            throw new IllegalArgumentException("workerAddresses must not be empty");
        }
        if (maxTaskRetries < 0) {
            throw new IllegalArgumentException("maxTaskRetries must not be negative");
        }
        this.workerAddresses = List.copyOf(workerAddresses);
        this.maxTaskRetries = maxTaskRetries;
        this.verbose = verbose;
    }

    @Override
    public <T> List<T> submitTasks(List<? extends Task<T>> tasks) {
        Objects.requireNonNull(tasks, "tasks");

        List<T> result = new ArrayList<>();
        for (int index = 0; index < tasks.size(); index++) {
            Task<T> task = tasks.get(index);
            result.add(sendWithRetry(task, index));
        }
        return result;
    }

    @Override
    public void close() {
        // 每个 Task 独立建连，没有需要关闭的长连接。
    }

    private <T> T sendWithRetry(Task<T> task, int taskIndex) {
        int retries = 0;
        while (true) {
            String workerAddress = workerFor(task, taskIndex);
            try {
                return sendTask(workerAddress, task, retries);
            } catch (RuntimeException e) {
                if (retries >= maxTaskRetries) {
                    throw new IllegalStateException(
                            task + " failed after "
                                    + (retries + 1) + " attempts",
                            e);
                }
                retries++;
                if (verbose) {
                    System.out.println("  [网络重试] " + task
                            + " 失败: " + e.getMessage()
                            + "，开始第 " + retries + " 次重试");
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T sendTask(String workerAddress, Task<T> task, int attemptId) {
        HostPort hostPort = HostPort.parse(workerAddress);
        if (verbose) {
            System.out.println("  [网络] 发送 " + task + " -> " + workerAddress);
        }

        try (Socket socket = new Socket(hostPort.host(), hostPort.port());
             ObjectOutputStream out = new ObjectOutputStream(
                     new BufferedOutputStream(socket.getOutputStream()))) {
            out.flush();
            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            out.writeObject(new RemoteTaskRequest<>(task, attemptId));
            out.flush();

            RemoteTaskResult<T> response =
                    (RemoteTaskResult<T>) in.readObject();
            if (!response.success()) {
                if (response.error() instanceof FetchFailedException fetchFailure) {
                    throw fetchFailure;
                }
                throw new IllegalStateException(
                        response.error().getClass().getName()
                                + ": " + response.error().getMessage(),
                        response.error());
            }
            if (verbose) {
                System.out.println("  [网络] 收到 " + task + " <- " + workerAddress);
            }
            return response.value();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("网络通信失败: " + workerAddress, e);
        }
    }

    private String workerFor(Task<?> task, int taskIndex) {
        for (String location : task.preferredLocations()) {
            if (workerAddresses.contains(location)) {
                return location;
            }
        }
        return workerAddresses.get(taskIndex % workerAddresses.size());
    }

    private record HostPort(String host, int port) {

        static HostPort parse(String address) {
            int colon = address.lastIndexOf(':');
            if (colon <= 0 || colon == address.length() - 1) {
                throw new IllegalArgumentException(
                        "worker address must be host:port: " + address);
            }
            return new HostPort(
                    address.substring(0, colon),
                    Integer.parseInt(address.substring(colon + 1)));
        }
    }
}
