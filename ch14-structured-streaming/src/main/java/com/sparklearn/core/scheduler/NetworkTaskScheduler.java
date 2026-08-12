package com.sparklearn.core.scheduler;

import com.sparklearn.core.rpc.RemoteTaskRequest;
import com.sparklearn.core.rpc.RemoteTaskResult;
import com.sparklearn.core.shuffle.FetchFailedException;
import com.sparklearn.core.storage.BlockManagerMaster;
import com.sparklearn.core.storage.RemoveRddRequest;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Socket 版 TaskScheduler。
 *
 * <p>它和 LocalTaskScheduler 一样接收 DAGScheduler 创建好的 Task；区别只是任务不再进入
 * 本地线程池，而是被序列化后写进 Socket，由另一个 JVM 里的 Executor 执行。
 *
 * <p>本章的变化：与 LocalTaskScheduler 一样改为异步回调。submitTasks 把 Task 发到
 * Executor 后立即返回，Task 完成时通过 {@link TaskCompletionHandler} 回调通知
 * DAGScheduler。Task 失败不再由本调度器重试，而是上报给 DAGScheduler。
 *
 * <p>调度器内部按 "stageId:partitionIndex" 维护尝试计数。每次 submitTask 都会递增计数，
 * 用于在 Executor 列表里轮换——这样 DAGScheduler 重试同一个 Task 时，自然会落到
 * 下一个 Executor 上，避免反复撞同一个不可达地址。
 */
public final class NetworkTaskScheduler implements TaskScheduler {

    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final int READ_TIMEOUT_MILLIS = 60_000;

    private final List<String> executorAddresses;
    private final ExecutorService clientPool;
    private final Map<String, AtomicInteger> attemptCounts = new ConcurrentHashMap<>();
    private volatile BlockManagerMaster blockManagerMaster;

    public NetworkTaskScheduler(List<String> executorAddresses) {
        if (executorAddresses == null || executorAddresses.isEmpty()) {
            throw new IllegalArgumentException("executorAddresses must not be empty");
        }
        this.executorAddresses = List.copyOf(executorAddresses);
        this.clientPool = Executors.newFixedThreadPool(
                Math.max(1, executorAddresses.size() * 4));
    }

    @Override
    public void submitTasks(
            List<? extends Task<?>> tasks,
            int stageId,
            TaskCompletionHandler handler) {
        Objects.requireNonNull(tasks, "tasks");
        Objects.requireNonNull(handler, "handler");
        for (int i = 0; i < tasks.size(); i++) {
            submitTask(tasks.get(i), stageId, i, handler);
        }
    }

    @Override
    public void submitTask(
            Task<?> task,
            int stageId,
            int partitionIndex,
            TaskCompletionHandler handler) {
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(handler, "handler");
        String key = stageId + ":" + partitionIndex;
        int attemptId = attemptCounts
                .computeIfAbsent(key, k -> new AtomicInteger())
                .getAndIncrement();
        clientPool.submit(() -> {
            try {
                String executorAddress = executorFor(task, partitionIndex, attemptId);
                RemoteTaskResult<?> response = sendTask(executorAddress, task, attemptId);
                if (response.success()) {
                    if (blockManagerMaster != null
                            && !response.cachedBlocks().isEmpty()) {
                        blockManagerMaster.updateBlocks(
                                executorAddress, response.cachedBlocks());
                    }
                    handler.taskSucceeded(stageId, partitionIndex, response.value());
                } else {
                    Throwable error = response.error();
                    if (error instanceof FetchFailedException fetchFailure) {
                        handler.taskFailed(stageId, partitionIndex, fetchFailure);
                    } else {
                        handler.taskFailed(stageId, partitionIndex,
                                new IllegalStateException(
                                        error.getClass().getName()
                                                + ": " + error.getMessage(),
                                        error));
                    }
                }
            } catch (Throwable e) {
                handler.taskFailed(stageId, partitionIndex, e);
            }
        });
    }

    @Override
    public void removeRdd(int rddId) {
        for (String executorAddress : executorAddresses) {
            sendRemoveRdd(executorAddress, rddId);
        }
    }

    @Override
    public void setBlockManagerMaster(BlockManagerMaster master) {
        this.blockManagerMaster = master;
    }

    @Override
    public void close() {
        clientPool.shutdownNow();
    }

    private void sendRemoveRdd(String executorAddress, int rddId) {
        HostPort hostPort = HostPort.parse(executorAddress);
        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(hostPort.host(), hostPort.port()),
                    CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(READ_TIMEOUT_MILLIS);
            try (ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()))) {
                out.flush();
                ObjectInputStream in = new ObjectInputStream(
                        new BufferedInputStream(socket.getInputStream()));
                out.writeObject(new RemoveRddRequest(rddId));
                out.flush();
                RemoteTaskResult<?> response =
                        (RemoteTaskResult<?>) in.readObject();
                if (!response.success()) {
                    throw new IllegalStateException(
                            "Executor 清理缓存失败: " + executorAddress,
                            response.error());
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(
                    "无法通知 Executor 清理缓存: " + executorAddress, e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> RemoteTaskResult<T> sendTask(
            String executorAddress, Task<T> task, int attemptId) {
        HostPort hostPort = HostPort.parse(executorAddress);

        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(hostPort.host(), hostPort.port()),
                    CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(READ_TIMEOUT_MILLIS);
            try (ObjectOutputStream out = new ObjectOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()))) {
                out.flush();
                ObjectInputStream in = new ObjectInputStream(
                        new BufferedInputStream(socket.getInputStream()));
                out.writeObject(new RemoteTaskRequest<>(task, attemptId));
                out.flush();

                return (RemoteTaskResult<T>) in.readObject();
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("网络通信失败: " + executorAddress, e);
        }
    }

    private String executorFor(Task<?> task, int taskIndex, int attemptId) {
        int startIndex = taskIndex % executorAddresses.size();
        for (String location : task.preferredLocations()) {
            if (executorAddresses.contains(location)) {
                startIndex = executorAddresses.indexOf(location);
                break;
            }
        }
        return executorAddresses.get(
                (startIndex + attemptId) % executorAddresses.size());
    }

    private record HostPort(String host, int port) {

        static HostPort parse(String address) {
            int colon = address.lastIndexOf(':');
            if (colon <= 0 || colon == address.length() - 1) {
                throw new IllegalArgumentException(
                        "executor address must be host:port: " + address);
            }
            return new HostPort(
                    address.substring(0, colon),
                    Integer.parseInt(address.substring(colon + 1)));
        }
    }
}
