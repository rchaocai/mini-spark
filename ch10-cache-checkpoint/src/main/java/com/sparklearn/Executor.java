package com.sparklearn;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * 独立 JVM 里的 Executor。
 *
 * <p>Driver 发来的 Task 在这里反序列化，然后调用同一个 run() 方法执行。
 */
public final class Executor implements AutoCloseable {

    private final int port;
    private volatile boolean running;
    private ServerSocket serverSocket;

    public Executor(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        running = true;
        try (ServerSocket socket = new ServerSocket(port)) {
            serverSocket = socket;
            System.out.println("[Executor] 监听端口 " + port);
            while (running) {
                try {
                    handle(socket.accept());
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
    }

    @SuppressWarnings("unchecked")
    private void handle(Socket client) throws IOException {
        try (Socket socket = client;
             ObjectOutputStream out = new ObjectOutputStream(
                     new BufferedOutputStream(socket.getOutputStream()))) {
            out.flush();
            ObjectInputStream in = new ObjectInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            RemoteTaskRequest<?> request = (RemoteTaskRequest<?>) in.readObject();
            try {
                Object value = request.task().run(request.attemptId());
                out.writeObject(RemoteTaskResult.success(value));
            } catch (Throwable e) {
                out.writeObject(RemoteTaskResult.failure(e));
            }
            out.flush();
        } catch (ClassNotFoundException e) {
            throw new IOException("Task 反序列化失败", e);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("用法: java com.sparklearn.Executor <port>");
            return;
        }
        new Executor(Integer.parseInt(args[0])).start();
    }
}
