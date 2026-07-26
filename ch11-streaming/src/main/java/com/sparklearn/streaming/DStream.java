package com.sparklearn.streaming;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.RDD;
import com.sparklearn.core.SerializableBinaryOperator;
import com.sparklearn.core.SerializableFunction;
import com.sparklearn.core.SerializablePredicate;
import com.sparklearn.streaming.dstream.FilteredDStream;
import com.sparklearn.streaming.dstream.FlatMappedDStream;
import com.sparklearn.streaming.dstream.ForEachDStream;
import com.sparklearn.streaming.dstream.MappedDStream;
import com.sparklearn.streaming.dstream.ReducedDStream;
import com.sparklearn.streaming.dstream.WindowedDStream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Discretized Stream：连续时间上的一串 RDD。
 *
 * <p>每个 batch 边界，DStream 会为当前 {@link Time} 生成一个 RDD。
 * 变换（map / filter / reduceByKey / window）只描述“如何从父流得到这个 RDD”，
 * 输出操作（foreachRDD / print）才会真正触发计算。
 */
public abstract class DStream<T> implements Serializable {

    private final transient StreamingContext ssc;
    private final Map<Time, RDD<T>> generatedRdds = new LinkedHashMap<>();
    private Time zeroTime;
    protected Duration rememberDuration;
    private DStreamGraph graph;
    private boolean shouldCache;

    protected DStream(StreamingContext ssc) {
        this.ssc = Objects.requireNonNull(ssc, "ssc");
    }

    /** 当前流生成 RDD 的时间间隔。 */
    public abstract Duration slideDuration();

    /** 父 DStream 列表。 */
    public abstract List<DStream<?>> dependencies();

    /** 为指定时间生成 RDD；没有数据时返回 empty。 */
    public abstract Optional<RDD<T>> compute(Time validTime);

    public final StreamingContext context() {
        return ssc;
    }

    public final DStream<T> cache() {
        shouldCache = true;
        return this;
    }

    protected Duration parentRememberDuration() {
        return rememberDuration;
    }

    public final void remember(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        if (rememberDuration == null || rememberDuration.compareTo(duration) < 0) {
            rememberDuration = duration;
        }
        for (DStream<?> parent : dependencies()) {
            parent.remember(parentRememberDuration());
        }
    }

    public final void setGraph(DStreamGraph graph) {
        this.graph = graph;
        for (DStream<?> parent : dependencies()) {
            parent.setGraph(graph);
        }
    }

    public final void initialize(Time zeroTime) {
        if (this.zeroTime != null && !this.zeroTime.equals(zeroTime)) {
            throw new IllegalStateException(
                    "zeroTime already initialized to " + this.zeroTime);
        }
        this.zeroTime = zeroTime;
        if (rememberDuration == null || rememberDuration.compareTo(slideDuration()) < 0) {
            rememberDuration = slideDuration();
        }
        for (DStream<?> parent : dependencies()) {
            parent.initialize(zeroTime);
        }
    }

    final Time zeroTime() {
        return zeroTime;
    }

    final Map<Time, RDD<T>> generatedRdds() {
        return Collections.unmodifiableMap(generatedRdds);
    }

    protected boolean isTimeValid(Time time) {
        if (zeroTime == null) {
            throw new IllegalStateException(this + " has not been initialized");
        }
        if (time.compareTo(zeroTime) <= 0) {
            return false;
        }
        long delta = time.milliseconds() - zeroTime.milliseconds();
        return delta % slideDuration().milliseconds() == 0;
    }

    /**
     * 取回已生成的 RDD，或在合法时间点上现算。
     */
    public final Optional<RDD<T>> getOrCompute(Time time) {
        Objects.requireNonNull(time, "time");
        RDD<T> cached = generatedRdds.get(time);
        if (cached != null) {
            return Optional.of(cached);
        }
        if (!isTimeValid(time)) {
            return Optional.empty();
        }
        Optional<RDD<T>> computed = compute(time);
        if (computed.isEmpty()) {
            return Optional.empty();
        }
        RDD<T> rdd = computed.get();
        if (shouldCache) {
            rdd.cache();
        }
        generatedRdds.put(time, rdd);
        return Optional.of(rdd);
    }

    /**
     * 默认 job：物化当前 batch 的 RDD。
     * 输出流会覆盖它，改成真正的副作用动作。
     */
    public Optional<StreamingJob> generateJob(Time time) {
        return getOrCompute(time).map(rdd -> new StreamingJob(time, rdd::count));
    }

    public void clearOldMetadata(Time time) {
        if (rememberDuration == null) {
            return;
        }
        Time threshold = time.minus(rememberDuration);
        List<Time> oldTimes = new ArrayList<>();
        for (Time generated : generatedRdds.keySet()) {
            if (generated.compareTo(threshold) <= 0) {
                oldTimes.add(generated);
            }
        }
        for (Time oldTime : oldTimes) {
            generatedRdds.remove(oldTime);
        }
        for (DStream<?> parent : dependencies()) {
            parent.clearOldMetadata(time);
        }
    }

    /** 取窗口 [from, to] 内已经生成过的 RDD。 */
    public final List<RDD<T>> slice(Time from, Time to) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        List<RDD<T>> rdds = new ArrayList<>();
        Time current = from;
        while (current.compareTo(to) <= 0) {
            getOrCompute(current).ifPresent(rdds::add);
            current = current.plus(slideDuration());
        }
        return rdds;
    }

    public final <U> DStream<U> map(SerializableFunction<T, U> mapFunc) {
        Objects.requireNonNull(mapFunc, "mapFunc");
        return new MappedDStream<>(this, mapFunc);
    }

    public final DStream<T> filter(SerializablePredicate<T> filterFunc) {
        Objects.requireNonNull(filterFunc, "filterFunc");
        return new FilteredDStream<>(this, filterFunc);
    }

    public final <U> DStream<U> flatMap(SerializableFunction<T, List<U>> flatMapFunc) {
        Objects.requireNonNull(flatMapFunc, "flatMapFunc");
        return new FlatMappedDStream<>(this, flatMapFunc);
    }

    @SuppressWarnings("unchecked")
    public final <K, V> DStream<KeyValuePair<K, V>> reduceByKey(
            SerializableBinaryOperator<V> reduceFunc,
            int numberOfReducePartitions) {
        Objects.requireNonNull(reduceFunc, "reduceFunc");
        return new ReducedDStream<>(
                (DStream<KeyValuePair<K, V>>) this,
                reduceFunc,
                numberOfReducePartitions);
    }

    public final DStream<T> window(Duration windowDuration) {
        return window(windowDuration, slideDuration());
    }

    public final DStream<T> window(Duration windowDuration, Duration slideDuration) {
        Objects.requireNonNull(windowDuration, "windowDuration");
        Objects.requireNonNull(slideDuration, "slideDuration");
        return new WindowedDStream<>(this, windowDuration, slideDuration);
    }

    public final void foreachRDD(BiConsumer<RDD<T>, Time> foreachFunc) {
        Objects.requireNonNull(foreachFunc, "foreachFunc");
        ForEachDStream<T> output = new ForEachDStream<>(this, foreachFunc);
        ssc.registerOutputStream(output);
    }

    public final void print() {
        foreachRDD((rdd, time) -> {
            List<T> values = rdd.collect();
            System.out.println("-------------------------------------------");
            System.out.println("Time: " + time);
            System.out.println("-------------------------------------------");
            int limit = Math.min(10, values.size());
            for (int i = 0; i < limit; i++) {
                System.out.println(values.get(i));
            }
            if (values.size() > limit) {
                System.out.println("... " + (values.size() - limit) + " more");
            }
            System.out.println();
        });
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(this));
    }
}
