package com.sparklearn;

import java.io.Serializable;
import java.util.function.BinaryOperator;

/**
 * 可以跨 JVM 发送的二元合并函数。
 */
@FunctionalInterface
public interface SerializableBinaryOperator<T>
        extends BinaryOperator<T>, Serializable {
}

