package com.sparklearn.streaming.dstream;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.RDD;
import com.sparklearn.core.SerializableBinaryOperator;
import com.sparklearn.core.SerializableFunction;
import com.sparklearn.core.SerializablePredicate;
import com.sparklearn.core.UnionRDD;
import com.sparklearn.streaming.DStream;
import com.sparklearn.streaming.Duration;
import com.sparklearn.streaming.InputDStream;
import com.sparklearn.streaming.StreamingContext;
import com.sparklearn.streaming.StreamingJob;
import com.sparklearn.streaming.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 把最近若干个 batch 的 RDD 并起来，形成滑动窗口。
 *
 * <p>真实 Spark 用 {@code UnionRDD}；这里用简单的 {@link UnionRDD} 教学实现。
 */
public final class WindowedDStream<T> extends DStream<T> {

    private final DStream<T> parent;
    private final Duration windowDuration;
    private final Duration slideDuration;

    public WindowedDStream(
            DStream<T> parent,
            Duration windowDuration,
            Duration slideDuration) {
        super(parent.context());
        this.parent = Objects.requireNonNull(parent, "parent");
        this.windowDuration = Objects.requireNonNull(windowDuration, "windowDuration");
        this.slideDuration = Objects.requireNonNull(slideDuration, "slideDuration");
        if (!windowDuration.isMultipleOf(parent.slideDuration())) {
            throw new IllegalArgumentException(
                    "windowDuration must be a multiple of parent slideDuration");
        }
        if (!slideDuration.isMultipleOf(parent.slideDuration())) {
            throw new IllegalArgumentException(
                    "slideDuration must be a multiple of parent slideDuration");
        }
        parent.cache();
    }

    @Override
    public Duration slideDuration() {
        return slideDuration;
    }

    @Override
    public List<DStream<?>> dependencies() {
        return List.of(parent);
    }

    @Override
    protected Duration parentRememberDuration() {
        return rememberDuration.plus(windowDuration);
    }

    @Override
    public Optional<RDD<T>> compute(Time validTime) {
        // 窗口右闭：包含 validTime，回溯 windowDuration 宽度。
        Time from = validTime
                .minus(windowDuration)
                .plus(parent.slideDuration());
        List<RDD<T>> rdds = parent.slice(from, validTime);
        if (rdds.isEmpty()) {
            return Optional.empty();
        }
        if (rdds.size() == 1) {
            return Optional.of(rdds.get(0));
        }
        return Optional.of(new UnionRDD<>(context().sparkContext(), rdds));
    }
}
