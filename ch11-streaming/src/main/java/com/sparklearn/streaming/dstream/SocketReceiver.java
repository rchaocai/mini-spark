package com.sparklearn.streaming.dstream;

import com.sparklearn.streaming.Receiver;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.function.Function;

/**
 * 从 TCP socket 读对象的接收器：连上 host:port，用 bytesToObjects 把字节流解释成一连串对象，
 * 后台线程每读到一个就 push 进缓冲。
 */
public final class SocketReceiver<T> extends Receiver<T> {

    private final String host;
    private final int port;
    private final Function<InputStream, Iterator<T>> bytesToObjects;
    private volatile Socket socket;
    private volatile Thread reader;

    public SocketReceiver(String host, int port, Function<InputStream, Iterator<T>> bytesToObjects) {
        this.host = host;
        this.port = port;
        this.bytesToObjects = bytesToObjects;
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
            Iterator<T> objects = bytesToObjects.apply(socket.getInputStream());
            while (objects.hasNext()) {
                push(objects.next());
            }
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

    /** 默认的解释方式：UTF-8 下按行（'\n' 分隔）切成一串字符串。 */
    public static Iterator<String> bytesToLines(InputStream in) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        return new Iterator<String>() {
            private String next = readNext();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public String next() {
                String current = next;
                next = readNext();
                return current;
            }

            private String readNext() {
                try {
                    return reader.readLine();
                } catch (Exception e) {
                    return null;
                }
            }
        };
    }
}
