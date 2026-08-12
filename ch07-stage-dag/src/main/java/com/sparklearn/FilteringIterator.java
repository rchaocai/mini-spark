package com.sparklearn;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * 过滤迭代器：跳过不满足条件的父元素，只返回匹配项。
 *
 * @param <T> 元素类型
 */
public final class FilteringIterator<T> implements Iterator<T> {

    private final Iterator<T> parent;
    private final Predicate<T> predicate;
    private T nextElement;
    private boolean hasBufferedElement;

    public FilteringIterator(Iterator<T> parent, Predicate<T> predicate) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.predicate = Objects.requireNonNull(predicate, "predicate");
    }

    @Override
    public boolean hasNext() {
        if (hasBufferedElement) {
            return true;
        }

        while (parent.hasNext()) {
            T candidate = parent.next();
            if (predicate.test(candidate)) {
                nextElement = candidate;
                hasBufferedElement = true;
                return true;
            }
        }
        return false;
    }

    @Override
    public T next() {
        if (!hasBufferedElement && !hasNext()) {
            throw new NoSuchElementException();
        }

        T result = nextElement;
        nextElement = null;
        hasBufferedElement = false;
        return result;
    }
}
