package com.sparklearn;

import java.io.Serializable;

/**
 * RDD 的一个分区。
 *
 * @param index 分区编号，从 0 开始
 */
public record Partition(int index) implements Serializable {
}
