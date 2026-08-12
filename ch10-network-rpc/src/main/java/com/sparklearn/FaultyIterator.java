package com.sparklearn;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 在指定的 next() 调用上抛出异常，用来模拟一次可控的瞬态故障。
 *
 * <p>失败计数器由多次重算创建出的 FaultyIterator 共享。
 * 第一次读到故障点时抛异常；调度器重试同一个分区后，会重新创建迭代器链，
 * 此时计数器已经减少，数据可以继续流过。
 *
 * @param <T> 元素类型
 */
public final class FaultyIterator<T> implements Iterator<T> {

    private final Iterator<T> parent;
    private final int failOnNextCall;
    private final AtomicInteger remainingFailures;
    private int nextCalls;

    public FaultyIterator(
            Iterator<T> parent,
            int failOnNextCall,
            AtomicInteger remainingFailures) {
        this.parent = Objects.requireNonNull(parent, "parent");
        if (failOnNextCall <= 0) {
            throw new IllegalArgumentException("failOnNextCall must be positive");
        }
        this.failOnNextCall = failOnNextCall;
        this.remainingFailures = Objects.requireNonNull(
                remainingFailures,
                "remainingFailures");
    }

    @Override
    public boolean hasNext() {
        return parent.hasNext();
    }

    @Override
    public T next() {
        if (!parent.hasNext()) {
            throw new NoSuchElementException();
        }

        nextCalls++;
        if (nextCalls == failOnNextCall && consumeOneFailure()) {
            throw new RuntimeException(
                    "FaultyIterator 在第 " + failOnNextCall + " 次 next() 时模拟失败");
        }
        return parent.next();
    }

    private boolean consumeOneFailure() {
        while (true) {
            int current = remainingFailures.get();
            if (current <= 0) {
                return false;
            }
            if (remainingFailures.compareAndSet(current, current - 1)) {
                return true;
            }
        }
    }
}
