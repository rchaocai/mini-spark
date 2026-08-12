package com.sparklearn;

import java.util.Iterator;
import java.util.List;

/**
 * 基于 {@link List} 的源头 RDD。
 *
 * <p>构造时不复制列表，也不提前遍历；只保存对原始列表的引用。
 * 真正的迭代器在 {@code compute()} 被调用时才创建。
 *
 * @param <T> 元素类型
 */
public class ListRDD<T> extends RDD<T> {

    private final List<T> data;

    public ListRDD(List<T> data) {
        this.data = data;
    }

    /**
     * 每次调用都返回一个<strong>全新的</strong>迭代器。
     */
    @Override
    public Iterator<T> compute() {
        return data.iterator();
    }
}
