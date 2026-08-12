package com.sparklearn.streaming.structured;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 演示用的 TCP 文本服务器：监听一个端口，接受一个连接，
 * 主线程通过 {@link #send} 把一组组文本行推进来，连接线程把它们写给客户端。
 *
 * <p>它不是 Structured Streaming 的一部分，只是给 {@link SocketSource} 喂数据的演示桩——
 * 模拟"另一端持续把行写到 socket"。
 */
final class LineServer implements AutoCloseable {

    private final ServerSocket serverSocket;
    private final LinkedBlockingQueue<Group> pending = new LinkedBlockingQueue<>();
    private Thread acceptThread;
    private volatile Socket client;
    private volatile Writer writer;

    /** 服务器要写的一组行；stop 信号用一个 lines 为 null 的 Group 表示，区别于"这批不写"。 */
    private static final class Group {
        final List<String> lines;
        Group(List<String> lines) {
            this.lines = lines;
        }
    }

    LineServer() throws Exception {
        this.serverSocket = new ServerSocket(0);
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    void start() {
        acceptThread = new Thread(this::acceptLoop, "line-server-" + port());
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void acceptLoop() {
        try {
            client = serverSocket.accept();
            writer = new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8);
            while (true) {
                Group group = pending.take();
                if (group.lines == null) {
                    break;
                }
                for (String line : group.lines) {
                    writer.write(line);
                    writer.write("\n");
                }
                writer.flush();
            }
        } catch (Exception ignored) {
            // 关闭即退出
        }
    }

    /** 塞一组行让服务器写给客户端；客户端的读线程会把它们读进缓冲。 */
    void send(List<String> lines) {
        pending.add(new Group(lines));
    }

    void stop() {
        pending.add(new Group(null));
        if (client != null) {
            try {
                client.close();
            } catch (Exception ignored) {
            }
        }
        try {
            serverSocket.close();
        } catch (Exception ignored) {
        }
    }

    @Override
    public void close() {
        stop();
    }
}
