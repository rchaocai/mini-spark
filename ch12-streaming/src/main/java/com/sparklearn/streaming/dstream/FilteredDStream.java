package com.sparklearn.streaming.dstream;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.rdd.RDD;

import com.sparklearn.core.util.SerializablePredicate;
import com.sparklearn.streaming.DStream;
import com.sparklearn.streaming.Duration;
import com.sparklearn.streaming.Time;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class FilteredDStream<T> extends DStream<T> {

    private final DStream<T> parent;
    private final SerializablePredicate<T> filterFunc;

    public FilteredDStream(DStream<T> parent, SerializablePredicate<T> filterFunc) {
        super(parent.context());
        this.parent = Objects.requireNonNull(parent, "parent");
        this.filterFunc = Objects.requireNonNull(filterFunc, "filterFunc");
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
    public Optional<RDD<T>> compute(Time validTime) {
        return parent.getOrCompute(validTime).map(rdd -> rdd.filter(filterFunc));
    }
}
