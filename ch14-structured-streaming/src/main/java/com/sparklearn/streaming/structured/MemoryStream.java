package com.sparklearn.streaming.structured;

import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 基于内存队列的流式数据源，用于教学和测试。
 * 用户通过 {@link #addData(List)} 向流中添加数据，引擎按序消费。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.MemoryStream}
 */
public class MemoryStream implements Source {

    private final SQLContext sqlContext;
    private final Schema sourceSchema;
    private final List<Batch> batches = new ArrayList<>();
    private LongOffset currentOffset = new LongOffset(-1);

    /** 最后已提交的偏移量，初始 -1；commit 后推进。 */
    private LongOffset lastOffsetCommitted = new LongOffset(-1);

    public MemoryStream(SQLContext sqlContext, Schema sourceSchema) {
        this.sqlContext = sqlContext;
        this.sourceSchema = sourceSchema;
    }

    @Override
    public Schema schema() {
        return sourceSchema;
    }

    /**
     * 向流中添加一批数据。
     *
     * @return 这批数据对应的偏移量
     */
    public synchronized Offset addData(List<Row> rows) {
        currentOffset = currentOffset.increment();
        DataFrame df = sqlContext.createDataFrame("memory", rows, sourceSchema, 1);
        batches.add(new Batch(currentOffset, df));
        return currentOffset;
    }

    /**
     * 获取此流的 DataFrame 视图，用于构建流式查询。
     */
    public DataFrame toDF() {
        return new DataFrame(sqlContext, new StreamingRelation(this));
    }

    @Override
    public synchronized Optional<Batch> getNextBatch(Optional<Offset> start) {
        long startOrdinal = start
                .map(offset -> ((LongOffset) offset).offset() + 1)
                .orElse(-1L);
        long endOrdinal = currentOffset.offset();

        if (endOrdinal < 0 || startOrdinal > endOrdinal) {
            return Optional.empty();
        }

        // 基于 lastOffsetCommitted 计算列表索引（对应 Spark MemoryStream.getBatch 的切片逻辑）
        int sliceStart = (int) (startOrdinal - lastOffsetCommitted.offset() - 1);
        int sliceEnd = (int) (endOrdinal - lastOffsetCommitted.offset() - 1);

        if (sliceStart < 0) {
            sliceStart = 0;
        }
        if (sliceEnd >= batches.size()) {
            sliceEnd = batches.size() - 1;
        }
        if (sliceStart > sliceEnd) {
            return Optional.empty();
        }

        // 合并 [sliceStart, sliceEnd] 的所有批次
        List<Row> allRows = new ArrayList<>();
        for (int i = sliceStart; i <= sliceEnd; i++) {
            allRows.addAll(batches.get(i).data().collect());
        }

        DataFrame combined = sqlContext.createDataFrame("memory", allRows, sourceSchema, 1);
        return Optional.of(new Batch(currentOffset, combined));
    }

    /**
     * 回收已处理批次：计算 {@code end - lastOffsetCommitted} 得到待删除数量，
     * 从列表头部删除（trimStart），然后推进 {@code lastOffsetCommitted}。
     * <p>
     * 对应 Spark {@code MemoryStream.commit}：
     * <pre>{@code
     * val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt
     * batches.trimStart(offsetDiff)
     * lastOffsetCommitted = newOffset
     * }</pre>
     */
    @Override
    public synchronized void commit(Offset end) {
        if (!(end instanceof LongOffset newOffset)) {
            throw new IllegalArgumentException(
                    "MemoryStream.commit() received an offset that did not originate with an instance of this class: " + end);
        }
        int offsetDiff = (int) (newOffset.offset() - lastOffsetCommitted.offset());
        if (offsetDiff < 0) {
            throw new IllegalStateException(
                    "Offsets committed out of order: " + lastOffsetCommitted + " followed by " + end);
        }
        int toRemove = Math.min(offsetDiff, batches.size());
        batches.subList(0, toRemove).clear();
        lastOffsetCommitted = newOffset;
    }

    @Override
    public synchronized Offset getCurrentOffset() {
        return currentOffset;
    }

    @Override
    public String toString() {
        return "MemoryStream[offset=" + currentOffset + ", batches=" + batches.size() + "]";
    }
}
