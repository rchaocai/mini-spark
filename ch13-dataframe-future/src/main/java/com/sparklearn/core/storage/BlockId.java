package com.sparklearn.core.storage;

import java.io.Serializable;

/**
 * 缓存块的唯一标识：RDD ID + 分区编号。
 *
 * <p>它对应真实 Spark 的 {@code RDDBlockId(rddId, splitIndex)}。BlockManager 用它做 Map 的 key，
 * BlockManagerMaster 用它追踪"哪个 Executor 上有这个块"，Task 结果用它回传"本次缓存了哪些块"。
 *
 * @param rddId           所属 RDD 的编号
 * @param partitionIndex  分区编号
 */
public record BlockId(int rddId, int partitionIndex) implements Serializable {
}
