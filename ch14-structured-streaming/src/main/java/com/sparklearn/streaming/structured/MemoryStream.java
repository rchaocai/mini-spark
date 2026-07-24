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
        long startIndex = start
                .map(offset -> ((LongOffset) offset).offset() + 1)
                .orElse(0L);

        if (startIndex >= batches.size()) {
            return Optional.empty();
        }

        // 合并从 startIndex 到 currentOffset 的所有批次
        List<Row> allRows = new ArrayList<>();
        for (long i = startIndex; i < batches.size(); i++) {
            allRows.addAll(batches.get((int) i).data().collect());
        }

        DataFrame combined = sqlContext.createDataFrame("memory", allRows, sourceSchema, 1);

        return Optional.of(new Batch(currentOffset, combined));
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
