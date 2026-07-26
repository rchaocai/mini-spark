package com.sparklearn.core;

import java.util.Iterator;
import java.util.Objects;

/**
 * 一对一的迭代器变换：从父迭代器取出元素后，应用函数再返回。
 *
 * @param <T> 输入元素类型
 * @param <U> 输出元素类型
 */
public final class MappingIterator<T, U> implements Iterator<U> {

    private final Iterator<T> parent;
    private final SerializableFunction<T, U> elementFunction;

    public MappingIterator(
            Iterator<T> parent,
            SerializableFunction<T, U> elementFunction) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.elementFunction = Objects.requireNonNull(elementFunction, "elementFunction");
    }

    @Override
    public boolean hasNext() {
        return parent.hasNext();
    }

    @Override
    public U next() {
        return elementFunction.apply(parent.next());
    }
}
