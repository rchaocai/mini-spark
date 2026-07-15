package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * 第 2 章核心：持有「访问方式」而非「数据副本」的 RDD 实现。
 *
 * <p>ListRDD 是全书第一个具体 RDD——它把「不存数据」这件反直觉的事
 * 直接摆在你面前：构造函数只记下「怎么拿到迭代器」，不复制任何数据。
 *
 * <p>这决定了 RDD 的数据流属性：构造时不触发计算，消费迭代器时才真正读数据。
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
        // 关键设计：不写 new ArrayList<>(data)，而是用 Supplier 包裹 iterator()
        // → 不复制数据，只存「访问方式」
        this.supplier = () -> data.iterator();
    }

    /**
     * 每次调用都返回一个<strong>全新的</strong>迭代器。
     *
     * <p>这意味着同一个 ListRDD 可以反复消费——这一点对第 8 章
     * 的容错重算至关重要：丢了分区，重新调一次 compute() 就行。
     */
    @Override
    public Iterator<T> compute() {
        return supplier.get();
    }
}
