package com.sparklearn.core;

import java.io.Serializable;

/**
 * 键值对——reduceByKey 操作的基本数据单元。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public record KeyValuePair<K, V>(K key, V value) implements Serializable {
}
