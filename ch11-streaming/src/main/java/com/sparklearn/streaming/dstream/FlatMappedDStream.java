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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class FlatMappedDStream<T, U> extends DStream<U> {

    private final DStream<T> parent;
    private final SerializableFunction<T, List<U>> flatMapFunc;

    public FlatMappedDStream(
            DStream<T> parent,
            SerializableFunction<T, List<U>> flatMapFunc) {
        super(parent.context());
        this.parent = Objects.requireNonNull(parent, "parent");
        this.flatMapFunc = Objects.requireNonNull(flatMapFunc, "flatMapFunc");
    }

    @Override
    public Duration slideDuration() {
        return parent.slideDuration();
    }

    @Override
    public List<DStream<?>> dependencies() {
        return List.of(parent);
    }

    @Override
    public Optional<RDD<U>> compute(Time validTime) {
        return parent.getOrCompute(validTime).map(rdd -> rdd.flatMap(flatMapFunc));
    }
}
