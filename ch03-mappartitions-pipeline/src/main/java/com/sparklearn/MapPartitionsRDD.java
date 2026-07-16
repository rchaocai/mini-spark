package com.sparklearn;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * 对父 RDD 的整条迭代器数据流做变换。
 *
 * @param <T> 父 RDD 的元素类型
 * @param <U> 当前 RDD 的元素类型
 */
public final class MapPartitionsRDD<T, U> extends RDD<U> {

    private final RDD<T> parent;
    private final Function<Iterator<T>, Iterator<U>> iteratorTransform;

    public MapPartitionsRDD(
            RDD<T> parent,
            Function<Iterator<T>, Iterator<U>> iteratorTransform) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.iteratorTransform = Objects.requireNonNull(
                iteratorTransform,
                "iteratorTransform");
    }

    /**
     * 创建并返回包装后的迭代器，不在这里遍历数据。
     *
     * <p>{@code iteratorTransform.apply(...)} 对 map 来说只相当于
     * {@code new MappingIterator<>(parentIterator, elementFunction)}。
     * 数据由 collect() 对返回迭代器调用 hasNext()/next() 时逐步拉取；
     * 具体在哪个方法中读取父元素，由迭代器实现决定。
     */
    @Override
    public Iterator<U> compute() {
        Iterator<T> parentIterator = parent.compute();
        return iteratorTransform.apply(parentIterator);
    }
}
