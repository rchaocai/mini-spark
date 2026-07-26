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

/**
 * 每个 batch 内做 reduceByKey。跨 batch 状态不在这里维护。
 */
public final class ReducedDStream<K, V> extends DStream<KeyValuePair<K, V>> {

    private final DStream<KeyValuePair<K, V>> parent;
    private final SerializableBinaryOperator<V> reduceFunc;
    private final int numberOfReducePartitions;

    public ReducedDStream(
            DStream<KeyValuePair<K, V>> parent,
            SerializableBinaryOperator<V> reduceFunc,
            int numberOfReducePartitions) {
        super(parent.context());
        this.parent = Objects.requireNonNull(parent, "parent");
        this.reduceFunc = Objects.requireNonNull(reduceFunc, "reduceFunc");
        if (numberOfReducePartitions <= 0) {
            throw new IllegalArgumentException(
                    "numberOfReducePartitions must be positive");
        }
        this.numberOfReducePartitions = numberOfReducePartitions;
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
    public Optional<RDD<KeyValuePair<K, V>>> compute(Time validTime) {
        return parent.getOrCompute(validTime)
                .map(rdd -> rdd.reduceByKey(reduceFunc, numberOfReducePartitions));
    }
}
