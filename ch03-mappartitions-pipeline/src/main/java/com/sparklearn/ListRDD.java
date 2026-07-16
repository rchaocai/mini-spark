package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 第 2 章的 ListRDD：保存生成迭代器的方法，不复制原始 List。
 *
 * @param <T> 元素类型
 */
public final class ListRDD<T> extends RDD<T> {

    private final Supplier<Iterator<T>> supplier;

    public ListRDD(List<T> data) {
        Objects.requireNonNull(data, "data");
        this.supplier = data::iterator;
    }

    @Override
    public Iterator<T> compute() {
        return supplier.get();
    }
}
