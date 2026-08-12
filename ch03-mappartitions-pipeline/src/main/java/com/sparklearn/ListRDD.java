package com.sparklearn;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 从内存 List 构造的源头 RDD：持有原始 List 的引用，不复制；
 * {@code compute()} 被调用时才产出迭代器。
 *
 * @param <T> 元素类型
 */
public final class ListRDD<T> extends RDD<T> {

    private final List<T> data;

    public ListRDD(List<T> data) {
        this.data = Objects.requireNonNull(data, "data");
    }

    @Override
    public Iterator<T> compute() {
        return data.iterator();
    }
}
