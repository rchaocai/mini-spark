package com.sparklearn;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * 独立 JVM 里的 Executor。
 *
 * <p>Driver 发来的 Task 在这里反序列化，然后调用同一个 run() 方法执行。
 */
public final class Executor implements AutoCloseable {

    private final int port;
    private final String advertisedAddress;
    private final Path localDir;
    private final ExecutorService taskPool;
    private final Semaphore taskSlots;
    private volatile boolean running;
    private ServerSocket serverSocket;

    public Executor(int port) {
        this(port, "localhost:" + port,
                Math.max(2, Runtime.getRuntime().availableProcessors()));
    }

    public Executor(int port, String advertisedAddress, int executorCores) {
        if (executorCores <= 0) {
            throw new IllegalArgumentException("executorCores must be positive");
        }
        this.port = port;
        this.advertisedAddress = advertisedAddress;
        try {
            this.localDir = Files.createTempDirectory(
                    "mini-spark-executor-" + port + "-");
        } catch (IOException e) {
            throw new IllegalStateException("无法创建 Executor 本地目录", e);
        }
        this.taskPool = Executors.newCachedThreadPool();
        this.taskSlots = new Semaphore(executorCores);
    }

    public void start() throws IOException {
        running = true;
        try (ServerSocket socket = new ServerSocket(port)) {
            serverSocket = socket;
            System.out.println("[Executor] 监听端口 " + port);
            while (running) {
                try {
                    Socket client = socket.accept();
                    taskPool.execute(() -> handleSafely(client));
                } catch (IOException e) {
                    if (running) {
                        throw e;
                    }
                }
            }
        } finally {
            serverSocket = null;
        }
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (serverSocket != null) {
            serverSocket.close();
        }
        taskPool.shutdownNow();
        if (Files.exists(localDir)) {
            try (var paths = Files.walk(localDir)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.deleteIfExists(path);
                            } catch (IOException ignored) {
                                // 关闭阶段尽力清理，不覆盖真正的关闭结果。
                            }
                        });
            }
        }
    }

    private void handleSafely(Socket client) {
        try {
            handle(client);
        } catch (IOException e) {
            if (running) {
                System.err.println("[Executor] 请求处理失败: " + e.getMessage());
            }
        }
    }

    private void handle(Socket client) throws IOException {
        try (Socket socket = client;
             ObjectOutputStream out = new ObjectOutputStream(
                     new BufferedOutputStream(socket.getOutputStream()))) {
            out.flush();
            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            Object request = in.readObject();
            if (request instanceof RemoteTaskRequest<?> taskRequest) {
                handleTask(taskRequest, out);
            } else if (request instanceof ShuffleBlockRequest blockRequest) {
                handleShuffleBlock(blockRequest, out);
            } else {
                out.writeObject(RemoteTaskResult.failure(
                        new IllegalArgumentException("unknown request: " + request)));
            }
            out.flush();
        } catch (ClassNotFoundException e) {
            throw new IOException("Task 反序列化失败", e);
        }
    }

    private void handleTask(
            RemoteTaskRequest<?> request,
            ObjectOutputStream out) throws IOException {
        TaskExecutionEnvironment.set(
                advertisedAddress,
                localDir.toAbsolutePath().toString());
        boolean acquired = false;
        try {
            taskSlots.acquire();
            acquired = true;
            Object value = request.task().run(request.attemptId());
            out.writeObject(RemoteTaskResult.success(value));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            out.writeObject(RemoteTaskResult.failure(e));
        } catch (Throwable e) {
            out.writeObject(RemoteTaskResult.failure(e));
        } finally {
            if (acquired) {
                taskSlots.release();
            }
            TaskExecutionEnvironment.clear();
        }
    }

    private void handleShuffleBlock(
            ShuffleBlockRequest request,
            ObjectOutputStream out) throws IOException {
        try {
            Path block = localDir.resolve(request.file()).normalize();
            if (!block.getParent().equals(localDir) || !Files.isRegularFile(block)) {
                throw new IOException("shuffle block 不存在: " + request.file());
            }
            out.writeObject(ShuffleBlockResult.success(Files.readAllBytes(block)));
        } catch (Throwable e) {
            out.writeObject(ShuffleBlockResult.failure(e));
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1 || args.length > 3) {
            System.out.println(
                    "用法: java com.sparklearn.Executor <port> [advertised-host] [cores]");
            return;
        }
        int port = Integer.parseInt(args[0]);
        String host = args.length >= 2 ? args[1] : "localhost";
        int cores = args.length >= 3
                ? Integer.parseInt(args[2])
                : Math.max(2, Runtime.getRuntime().availableProcessors());
        new Executor(port, host + ":" + port, cores).start();
    }
}
