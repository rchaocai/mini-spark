package com.sparklearn;

import java.io.Serializable;
import java.util.function.Function;

/**
 * 可以跨 JVM 发送的函数。
 */
@FunctionalInterface
public interface SerializableFunction<T, R>
        extends Function<T, R>, Serializable {
}

