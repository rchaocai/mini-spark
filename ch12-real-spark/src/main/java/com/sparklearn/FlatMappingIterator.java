package com.sparklearn;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * 一对多迭代器：把每个父元素展开成一个 List，再逐个返回。
 *
 * @param <T> 输入元素类型
 * @param <U> 输出元素类型
 */
public final class FlatMappingIterator<T, U> implements Iterator<U> {

    private final Iterator<T> parent;
    private final SerializableFunction<T, List<U>> elementFunction;
    private Iterator<U> current = Collections.emptyIterator();

    public FlatMappingIterator(
            Iterator<T> parent,
            SerializableFunction<T, List<U>> elementFunction) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.elementFunction = Objects.requireNonNull(elementFunction, "elementFunction");
    }

    @Override
    public boolean hasNext() {
        while (!current.hasNext() && parent.hasNext()) {
            List<U> expanded = Objects.requireNonNull(
                    elementFunction.apply(parent.next()),
                    "flatMap function returned null");
            current = expanded.iterator();
        }
        return current.hasNext();
    }

    @Override
    public U next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return current.next();
    }
}
