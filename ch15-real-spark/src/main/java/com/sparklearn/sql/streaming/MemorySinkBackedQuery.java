package com.sparklearn.sql.streaming;

import com.sparklearn.streaming.structured.MemorySink;

/**
 * 扩展 {@link StreamingQuery}，当使用 {@code format("memory")} 作为 Sink 时，
 * 提供对底层 {@link MemorySink} 的访问以读取批次结果。
 * <p>
 * 这是 mini-spark 教学版额外增加的便捷接口（正式 Spark 中通过 table(queryName) 查询）。
 */
public interface MemorySinkBackedQuery extends StreamingQuery {

    /** 返回底层的 MemorySink，用于调用 {@link MemorySink#latestBatchData()} 等方法检查结果。 */
    MemorySink memorySink();
}
