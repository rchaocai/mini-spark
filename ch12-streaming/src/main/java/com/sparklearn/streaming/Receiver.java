package com.sparklearn.streaming;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 接收器：从一个外部数据源持续读数据、攒进缓冲。它是输入流背后那个真正去拿数据的角色——
 * 后台线程按数据源自己的节奏往里攒，输入流按 batch 节拍往外清，缓冲是两边碰头的地方。
 *
 * <p>子类实现 onStart / onStop，在读循环里每拿到一项调 push 塞进缓冲。
 */
public abstract class Receiver<T> {

    private final LinkedBlockingQueue<T> buffer = new LinkedBlockingQueue<>();

    /** 启动接收：连数据源、起后台读循环，每读到一项调 push 塞进缓冲。 */
    protected abstract void onStart() throws Exception;

    /** 停止接收：关连接、收线程。 */
    protected abstract void onStop();

    /** 后台读循环每拿到一项，调这个塞进缓冲。 */
    protected final void push(T item) {
        buffer.add(item);
    }

    /** 输入流每个 batch 调：把目前攒到的项一次性排空到 sink。 */
    public final void drainTo(List<T> sink) {
        buffer.drainTo(sink);
    }

    public final void start() throws Exception {
        onStart();
    }

    public final void stop() {
        onStop();
        buffer.clear();
    }
}
