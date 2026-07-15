package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * 基于 {@link List} 的 RDD 实现。
 *
 * <p>构造时不复制列表，也不提前遍历；只有调用 {@link #compute()} 时才创建迭代器。
 *
 * @param <T> 元素类型
 */
public class ListRDD<T> extends RDD<T> {

    private final Supplier<Iterator<T>> supplier;

    /**
     * @param data 原始数据列表。ListRDD <strong>不复制</strong>这份数据，
     *             只记住「怎么访问」——也就是通过 {@code data.iterator()}。
     */
    public ListRDD(List<T> data) {
        // 不写 new ArrayList<>(data)：这里只保存获取迭代器的方法。
        this.supplier = () -> data.iterator();
    }

    /**
     * 每次调用都返回一个<strong>全新的</strong>迭代器。
     */
    @Override
    public Iterator<T> compute() {
        return supplier.get();
    }
}
