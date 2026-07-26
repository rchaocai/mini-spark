package com.sparklearn.streaming;

import java.util.List;

/**
 * 输入流：没有父 DStream，自己负责在每个 batch 产生数据。
 */
public abstract class InputDStream<T> extends DStream<T> {

    protected InputDStream(StreamingContext ssc) {
        super(ssc);
        ssc.registerInputStream(this);
    }

    @Override
    public final List<DStream<?>> dependencies() {
        return List.of();
    }

    @Override
    public Duration slideDuration() {
        return context().batchDuration();
    }

    /** Streaming 启动时回调，可打开 receiver。教学版默认可空实现。 */
    public void start() {
    }

    /** Streaming 停止时回调。 */
    public void stop() {
    }
}
