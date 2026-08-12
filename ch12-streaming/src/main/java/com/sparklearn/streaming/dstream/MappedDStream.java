package com.sparklearn.streaming.dstream;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.util.SerializableFunction;
import com.sparklearn.streaming.DStream;
import com.sparklearn.streaming.Duration;
import com.sparklearn.streaming.Time;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class MappedDStream<T, U> extends DStream<U> {

    private final DStream<T> parent;
    private final SerializableFunction<T, U> mapFunc;

    public MappedDStream(DStream<T> parent, SerializableFunction<T, U> mapFunc) {
        super(parent.context());
        this.parent = Objects.requireNonNull(parent, "parent");
        this.mapFunc = Objects.requireNonNull(mapFunc, "mapFunc");
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
        return parent.getOrCompute(validTime).map(rdd -> rdd.map(mapFunc));
    }
}
