package com.sparklearn.streaming.dstream;

import com.sparklearn.core.RDD;
import com.sparklearn.streaming.InputDStream;
import com.sparklearn.streaming.StreamingContext;
import com.sparklearn.streaming.Time;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 从 TCP socket 读文本行的输入流。
 *
 * <p>start 时连上指定 host:port，起一个后台线程持续把读到的行攒进缓冲；
 * 每个 batch 的 compute 把缓冲里目前攒到的行排空，包成一个 RDD。
 * 没攒到行就返回 empty，这个 batch 不产生 job。
 */
public final class SocketInputDStream extends InputDStream<String> {

    private final String host;
    private final int port;
    private final LinkedBlockingQueue<String> buffer = new LinkedBlockingQueue<>();
    private volatile Socket socket;
    private volatile Thread reader;

    public SocketInputDStream(StreamingContext ssc, String host, int port) {
        super(ssc);
        this.host = host;
        this.port = port;
    }

    @Override
    public void start() {
        try {
            socket = new Socket(host, port);
        } catch (Exception e) {
            throw new RuntimeException("socket 连接失败: " + host + ":" + port, e);
        }
        reader = new Thread(this::readLoop, "socket-input-" + port);
        reader.setDaemon(true);
        reader.start();
    }

    private void readLoop() {
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                buffer.add(line);
            }
        } catch (Exception ignored) {
            // 连接关闭或出错，读取线程安静退出
        }
    }

    @Override
    public Optional<RDD<String>> compute(Time validTime) {
        List<String> drained = new ArrayList<>();
        buffer.drainTo(drained);
        if (drained.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(context().sparkContext().parallelize(drained, 1));
    }

    @Override
    public void stop() {
        if (reader != null) {
            reader.interrupt();
        }
        if (socket != null) {
            try {
                socket.close();
            } catch (Exception ignored) {
            }
        }
        buffer.clear();
    }
}
