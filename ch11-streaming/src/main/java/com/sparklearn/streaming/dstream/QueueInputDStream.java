package com.sparklearn.streaming.dstream;

import com.sparklearn.core.RDD;
import com.sparklearn.core.UnionRDD;
import com.sparklearn.streaming.InputDStream;
import com.sparklearn.streaming.StreamingContext;
import com.sparklearn.streaming.Time;

import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;

/**
 * 从队列里取 RDD 的输入流。每个 batch 默认只取一个 RDD，便于测试和演示。
 */
public final class QueueInputDStream<T> extends InputDStream<T> {

    private final Queue<RDD<T>> queue;
    private final boolean oneAtATime;
    private final RDD<T> defaultRDD;

    public QueueInputDStream(
            StreamingContext ssc,
            Queue<RDD<T>> queue,
            boolean oneAtATime,
            RDD<T> defaultRDD) {
        super(ssc);
        this.queue = Objects.requireNonNull(queue, "queue");
        this.oneAtATime = oneAtATime;
        this.defaultRDD = defaultRDD;
    }

    @Override
    public Optional<RDD<T>> compute(Time validTime) {
        if (queue.isEmpty()) {
            return Optional.ofNullable(defaultRDD);
        }
        if (oneAtATime) {
            return Optional.ofNullable(queue.poll());
        }
        ArrayList<RDD<T>> queued = new ArrayList<>(queue);
        if (queued.isEmpty()) {
            return Optional.ofNullable(defaultRDD);
        }
        return Optional.of(new UnionRDD<>(context().sparkContext(), queued));
    }
}
