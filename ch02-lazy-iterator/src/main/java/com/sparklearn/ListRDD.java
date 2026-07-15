package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * 基于 {@link List} 的 RDD 实现。
 *
 * <p>构造时不复制列表，也不提前遍历；这里只保存生成迭代器的方法。
 *
 * @param <T> 元素类型
 */
public class ListRDD<T> extends RDD<T> {

    private final Supplier<Iterator<T>> supplier;

    /**
     * @param data 原始数据列表。ListRDD <strong>不复制</strong>这份数据，
     *             只把 {@code data.iterator()} 包装成一个可延后执行的方法。
     */
    public ListRDD(List<T> data) {
        // 对 List 来说，compute() 里直接 return data.iterator() 也可以。
        // 这里使用 Supplier，是为了把“生成 Iterator 的方法”保存成统一形状。
        this.supplier = () -> data.iterator();
    }

    /**
     * 每次调用都返回一个<strong>全新的</strong>迭代器。
     */
    @Override
    public Iterator<T> compute() {
        // 到这里才真正创建 Iterator；每次调用都会创建一个新的。
        return supplier.get();
    }
}
