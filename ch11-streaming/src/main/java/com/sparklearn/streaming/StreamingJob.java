package com.sparklearn.streaming;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 一个 batch 对应的一次输出动作。
 */
public final class StreamingJob {

    private static final AtomicLong NEXT_ID = new AtomicLong();

    private final long id = NEXT_ID.getAndIncrement();
    private final Time time;
    private final Runnable body;

    public StreamingJob(Time time, Runnable body) {
        this.time = Objects.requireNonNull(time, "time");
        this.body = Objects.requireNonNull(body, "body");
    }

    public Time time() {
        return time;
    }

    public void run() {
        body.run();
    }

    @Override
    public String toString() {
        return "StreamingJob#" + id + "@" + time;
    }
}
