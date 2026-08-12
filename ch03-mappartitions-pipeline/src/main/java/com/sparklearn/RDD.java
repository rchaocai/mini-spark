package com.sparklearn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * RDD 抽象：每个 transformation 返回一个新 RDD，action 才消费数据。
 *
 * @param <T> 元素类型
 */
public abstract class RDD<T> {

    /**
     * 返回描述当前 RDD 数据流的迭代器。
     */
    public abstract Iterator<T> compute();

    /**
     * 一对一变换。这里只记录变换，不消费数据。
     */
    public <U> MapPartitionsRDD<T, U> map(Function<T, U> elementFunction) {
        Objects.requireNonNull(elementFunction, "elementFunction");
        return new MapPartitionsRDD<>(
                this,
                iterator -> new MappingIterator<>(iterator, elementFunction));
    }

    /**
     * 只保留满足条件的元素。这里只记录变换，不消费数据。
     */
    public MapPartitionsRDD<T, T> filter(Predicate<T> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        return new MapPartitionsRDD<>(
                this,
                iterator -> new FilteringIterator<>(iterator, predicate));
    }

    /**
     * 把每个元素展开成多个元素。这里只记录变换，不消费数据。
     */
    public <U> MapPartitionsRDD<T, U> flatMap(
            Function<T, List<U>> elementFunction) {
        Objects.requireNonNull(elementFunction, "elementFunction");
        return new MapPartitionsRDD<>(
                this,
                iterator -> new FlatMappingIterator<>(iterator, elementFunction));
    }

    /**
     * 第一个 action：显式调用最外层迭代器的 hasNext() 和 next()，
     * 把结果逐个收集到内存。
     */
    public List<T> collect() {
        List<T> result = new ArrayList<>();
        Iterator<T> iterator = compute();
        while (iterator.hasNext()) {
            T element = iterator.next();
            result.add(element);
        }
        return result;
    }
}
