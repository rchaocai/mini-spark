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
import java.util.function.BiConsumer;

/**
 * 输出流：不产生新 RDD，只在每个 batch 上对父流的 RDD 执行副作用。
 */
public final class ForEachDStream<T> extends DStream<Void> {

    private final DStream<T> parent;
    private final BiConsumer<RDD<T>, Time> foreachFunc;

    public ForEachDStream(DStream<T> parent, BiConsumer<RDD<T>, Time> foreachFunc) {
        super(parent.context());
        this.parent = Objects.requireNonNull(parent, "parent");
        this.foreachFunc = Objects.requireNonNull(foreachFunc, "foreachFunc");
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
    public Optional<RDD<Void>> compute(Time validTime) {
        return Optional.empty();
    }

    @Override
    public Optional<StreamingJob> generateJob(Time time) {
        return parent.getOrCompute(time).map(rdd ->
                new StreamingJob(time, () -> foreachFunc.accept(rdd, time)));
    }
}
