package com.sparklearn.streaming.dstream;

import com.sparklearn.core.RDD;
import com.sparklearn.streaming.InputDStream;
import com.sparklearn.streaming.Receiver;
import com.sparklearn.streaming.StreamingContext;
import com.sparklearn.streaming.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 从 TCP socket 读对象的输入流。它自己不碰 socket，把读取交给一个 SocketReceiver；
 * 每个 batch 的 compute 把 receiver 攒到的对象排空，包成一个 RDD。没攒到就返回 empty。
 *
 * @param <T> 一条数据的类型；socketTextStream 用 bytesToLines 把它定为 String。
 */
public final class SocketInputDStream<T> extends InputDStream<T> {

    private final Receiver<T> receiver;

    public SocketInputDStream(
            StreamingContext ssc,
            String host,
            int port,
            Deserializer<T> deserializer) {
        super(ssc);
        this.receiver = new SocketReceiver<>(host, port, deserializer);
    }

    @Override
    public void start() {
        try {
            receiver.start();
        } catch (Exception e) {
            throw new RuntimeException("socket 接收器启动失败: " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<RDD<T>> compute(Time validTime) {
        List<T> drained = new ArrayList<>();
        receiver.drainTo(drained);
        if (drained.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(context().sparkContext().parallelize(drained, 1));
    }

    @Override
    public void stop() {
        receiver.stop();
    }
}
