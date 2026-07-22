package com.sparklearn;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * 可以跨 JVM 发送的过滤条件。
 */
@FunctionalInterface
public interface SerializablePredicate<T>
        extends Predicate<T>, Serializable {
}

