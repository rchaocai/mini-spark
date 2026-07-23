package com.sparklearn.streaming.dstream;

import com.sparklearn.streaming.Receiver;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * 从 TCP socket 读对象的接收器：连上 host:port，用 deserializer 把字节流解释成一连串对象，
 * 后台线程每读到一个就 push 进缓冲。
 */
public final class SocketReceiver<T> extends Receiver<T> {

    private final String host;
    private final int port;
    private final Deserializer<T> deserializer;
    private volatile Socket socket;
    private volatile Thread reader;

    public SocketReceiver(String host, int port, Deserializer<T> deserializer) {
        this.host = host;
        this.port = port;
        this.deserializer = deserializer;
    }

    @Override
    protected void onStart() throws Exception {
        socket = new Socket(host, port);
        reader = new Thread(this::readLoop, "socket-receiver-" + port);
        reader.setDaemon(true);
        reader.start();
    }

    private void readLoop() {
        try {
            deserializer.read(socket.getInputStream(), this::push);
        } catch (Exception ignored) {
            // 连接关闭或出错，读取线程安静退出
        }
    }

    @Override
    protected void onStop() {
        if (reader != null) {
            reader.interrupt();
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (Exception ignored) {
            }
        }
    }

    /** 默认的解释方式：UTF-8 下按行（'\n' 分隔）切成一串字符串，读一行、发一行。 */
    public static void bytesToLines(InputStream in, Consumer<String> emit) throws Exception {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                emit.accept(line);
            }
        }
    }
}
