package com.sparklearn.streaming.structured;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.DataType;
import com.sparklearn.streaming.structured.state.StateStoreManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.sparklearn.sql.catalyst.expressions.Expressions.col;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Structured Streaming 核心组件测试。
 */
class StreamingTest {

    private SQLContext sqlContext;
    private Schema wordSchema;

    @BeforeEach
    void setUp() {
        SparkContext sc = new SparkContext(2);
        sqlContext = new SQLContext(sc);
        wordSchema = Schema.of(new Field("word", DataType.OBJECT));
        // 清除状态存储，避免测试间状态泄漏
        StateStoreManager.clearAll();
    }

    // ---------- LongOffset ----------

    @Test
    void testLongOffsetIncrement() {
        LongOffset offset = new LongOffset(-1);
        assertEquals(-1, offset.offset());
        assertEquals(0, offset.increment().offset());
        assertEquals(1, offset.increment().increment().offset());
    }

    @Test
    void testLongOffsetComparison() {
        LongOffset a = new LongOffset(0);
        LongOffset b = new LongOffset(5);
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);
        assertEquals(0, a.compareTo(new LongOffset(0)));
    }

    @Test
    void testLongOffsetToString() {
        assertEquals("0", new LongOffset(0).toString());
        assertEquals("-1", new LongOffset(-1).toString());
    }

    // ---------- MemoryStream ----------

    @Test
    void testMemoryStreamAddData() {
        MemoryStream stream = new MemoryStream(sqlContext, wordSchema);
        assertEquals(-1, ((LongOffset) stream.getCurrentOffset()).offset());

        stream.addData(List.of(Row.of("word", "hello"), Row.of("word", "world")));
        assertEquals(0, ((LongOffset) stream.getCurrentOffset()).offset());
    }

    @Test
    void testMemoryStreamGetNextBatch() {
        MemoryStream stream = new MemoryStream(sqlContext, wordSchema);
        stream.addData(List.of(Row.of("word", "a"), Row.of("word", "b"), Row.of("word", "c")));

        var batch = stream.getNextBatch(Optional.empty());
        assertTrue(batch.isPresent());
        assertEquals(0, ((LongOffset) batch.get().end()).offset());

        List<Row> rows = batch.get().data().collect();
        assertEquals(3, rows.size());
        assertEquals("a", rows.get(0).get("word"));
        assertEquals("b", rows.get(1).get("word"));
        assertEquals("c", rows.get(2).get("word"));
    }

    @Test
    void testMemoryStreamAppliesSourceSchemaToPositionalRows() {
        MemoryStream stream = new MemoryStream(sqlContext, wordSchema);
        stream.addData(List.of(Row.apply("hello")));

        var batch = stream.getNextBatch(Optional.empty());
        assertTrue(batch.isPresent());
        List<Row> rows = batch.get().data().collect();
        assertEquals("hello", rows.get(0).get("word"));
        assertEquals(List.of("word"), rows.get(0).fieldNames());
    }

    @Test
    void testMemoryStreamNoNewData() {
        MemoryStream stream = new MemoryStream(sqlContext, wordSchema);
        var batch = stream.getNextBatch(Optional.empty());
        assertFalse(batch.isPresent());
    }

    @Test
    void testMemoryStreamIncrementalBatches() {
        MemoryStream stream = new MemoryStream(sqlContext, wordSchema);

        // 第 1 批
        stream.addData(List.of(Row.of("word", "x")));
        var batch1 = stream.getNextBatch(Optional.empty());
        assertTrue(batch1.isPresent());
        assertEquals(0, ((LongOffset) batch1.get().end()).offset());

        // 第 2 批（从 offset 0 之后开始）
        stream.addData(List.of(Row.of("word", "y")));
        var batch2 = stream.getNextBatch(Optional.of(new LongOffset(0)));
        assertTrue(batch2.isPresent());
        assertEquals(1, ((LongOffset) batch2.get().end()).offset());
        assertEquals(1, batch2.get().data().collect().size());
    }

    @Test
    void testMemoryStreamSchema() {
        MemoryStream stream = new MemoryStream(sqlContext, wordSchema);
        var schema = stream.schema();
        assertEquals(1, schema.fields().size());
        assertEquals("word", schema.fields().get(0).name());
    }

    @Test
    void testMemoryStreamToDF() {
        MemoryStream stream = new MemoryStream(sqlContext, wordSchema);
        var df = stream.toDF();
        assertNotNull(df);
        assertTrue(df.logicalPlan() instanceof StreamingRelation);
    }

    // ---------- MemorySink ----------

    @Test
    void testMemorySinkAddBatch() {
        MemorySink sink = new MemorySink();
        var df = sqlContext.createDataFrame("test", List.of(Row.of("val", "a"), Row.of("val", "b")), Schema.of(new Field("val", DataType.STRING)), 1);
        var batch = new Batch(new LongOffset(0), df);

        sink.addBatch(batch);
        assertEquals(0, ((LongOffset) sink.currentOffset().orElseThrow()).offset());
        assertEquals(1, sink.batchCount());
    }

    @Test
    void testMemorySinkAccumulates() {
        MemorySink sink = new MemorySink();
        var df1 = sqlContext.createDataFrame("t1", List.of(Row.of("v", 1)), Schema.of(new Field("v", DataType.INTEGER)), 1);
        var df2 = sqlContext.createDataFrame("t2", List.of(Row.of("v", 2)), Schema.of(new Field("v", DataType.INTEGER)), 1);

        sink.addBatch(new Batch(new LongOffset(0), df1));
        sink.addBatch(new Batch(new LongOffset(1), df2));

        assertEquals(1, ((LongOffset) sink.currentOffset().orElseThrow()).offset());
        assertEquals(2, sink.batchCount());
    }

    @Test
    void testMemorySinkAllData() {
        MemorySink sink = new MemorySink();
        var df = sqlContext.createDataFrame("t", List.of(Row.of("v", 1), Row.of("v", 2)), Schema.of(new Field("v", DataType.INTEGER)), 1);
        sink.addBatch(new Batch(new LongOffset(0), df));

        List<Row> all = sink.allData();
        assertEquals(2, all.size());
        assertEquals(1, all.get(0).get("v"));
        assertEquals(2, all.get(1).get("v"));
    }

    @Test
    void testMemorySinkCompleteModeClearsPreviousBatches() {
        MemorySink sink = new MemorySink(OutputMode.Complete);
        var df1 = sqlContext.createDataFrame("t1", List.of(Row.of("v", 1)), Schema.of(new Field("v", DataType.INTEGER)), 1);
        var df2 = sqlContext.createDataFrame("t2", List.of(Row.of("v", 2)), Schema.of(new Field("v", DataType.INTEGER)), 1);

        sink.addBatch(new Batch(new LongOffset(0), df1));
        assertEquals(1, sink.batchCount());

        sink.addBatch(new Batch(new LongOffset(1), df2));
        assertEquals(1, sink.batchCount()); // Complete 模式清空旧批次
        assertEquals(1, ((LongOffset) sink.currentOffset().orElseThrow()).offset());
    }

    // ---------- StreamExecution (Append 模式) ----------

    @Test
    void testStreamExecutionWordCount() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink();

        var streamDF = source.toDF();
        var resultDF = streamDF.groupBy("word").count();

        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink);

        source.addData(List.of(
                Row.of("word", "hello"),
                Row.of("word", "world"),
                Row.of("word", "hello")
        ));

        boolean progressed = execution.advance();
        assertTrue(progressed);
        assertEquals(1, execution.batchesExecuted());
        assertEquals(1, sink.batchCount());

        List<Row> results = sink.allData();
        assertEquals(2, results.size()); // hello 和 world 两组
        Map<String, Long> counts = results.stream()
                .collect(Collectors.toMap(
                        row -> (String) row.get("word"),
                        row -> (Long) row.get("count")));
        assertEquals(Map.of("hello", 2L, "world", 1L), counts);
    }

    @Test
    void testStreamExecutionWritesEmptyResultWithoutFakeRow() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink();

        var resultDF = source.toDF()
                .where(col("word").eqTo("missing"))
                .select(col("word"));
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink);

        source.addData(List.of(Row.of("word", "hello")));
        boolean progressed = execution.advance();

        assertTrue(progressed);
        assertEquals(1, sink.batchCount());
        assertEquals(0, ((LongOffset) sink.currentOffset().orElseThrow()).offset());
        assertEquals(List.of(), sink.allData());
    }

    @Test
    void testSqlStreamingWordCountUsesSamePlanShape() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        var wordsDF = source.toDF();
        wordsDF.createOrReplaceTempView("words");
        MemorySink sink = new MemorySink();

        var resultDF = sqlContext.sql("SELECT word, count(*) FROM words GROUP BY word");
        assertTrue(resultDF.logicalPlan().treeString().contains("StreamingRelation"));
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink);

        source.addData(List.of(
                Row.of("word", "hello"),
                Row.of("word", "world"),
                Row.of("word", "hello")
        ));
        execution.advance();

        Map<String, Long> counts = sink.allData().stream()
                .collect(Collectors.toMap(
                        row -> (String) row.get("word"),
                        row -> (Long) row.get("count")));
        assertEquals(Map.of("hello", 2L, "world", 1L), counts);
    }

    @Test
    void testStreamExecutionNoNewData() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink();

        var streamDF = source.toDF();
        var resultDF = streamDF.groupBy("word").count();
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink);

        boolean progressed = execution.advance();
        assertFalse(progressed);
        assertEquals(0, execution.batchesExecuted());
        assertEquals(0, sink.batchCount());
    }

    @Test
    void testStreamExecutionMultipleBatches() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink();

        var streamDF = source.toDF();
        var resultDF = streamDF.groupBy("word").count();
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink);

        // 第 1 批
        source.addData(List.of(Row.of("word", "a"), Row.of("word", "b")));
        execution.advance();
        assertEquals(1, execution.batchesExecuted());

        // 第 2 批
        source.addData(List.of(Row.of("word", "a"), Row.of("word", "c")));
        execution.advance();
        assertEquals(2, execution.batchesExecuted());

        // 第 3 批
        source.addData(List.of(Row.of("word", "d")));
        execution.advance();
        assertEquals(3, execution.batchesExecuted());
        assertEquals(3, sink.batchCount());
    }

    @Test
    void testStreamExecutionStop() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink();

        var streamDF = source.toDF();
        var resultDF = streamDF.groupBy("word").count();
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink);

        execution.stop();
        assertTrue(execution.isStopped());

        source.addData(List.of(Row.of("word", "hello")));
        boolean progressed = execution.advance();
        assertFalse(progressed);
    }

    // ---------- StructuredStreaming 入口 ----------

    @Test
    void testStructuredStreamingEntry() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink();

        var streamDF = source.toDF();
        var resultDF = streamDF.groupBy("word").count();

        var streaming = new StructuredStreaming(sqlContext);
        var execution = streaming.startQuery(resultDF, sink);
        assertNotNull(execution);

        source.addData(List.of(Row.of("word", "test")));
        execution.processAllAvailable();

        assertEquals(1, sink.batchCount());
    }

    // ---------- Batch ----------

    @Test
    void testBatchCreation() {
        var df = sqlContext.createDataFrame("test", List.of(Row.of("v", 1)), Schema.of(new Field("v", DataType.INTEGER)), 1);
        var batch = new Batch(new LongOffset(0), df);
        assertNotNull(batch.data());
        assertEquals(0, ((LongOffset) batch.end()).offset());
    }

    // ---------- Complete 输出模式 ----------

    @Test
    void testCompleteModeOutputsAllGroups() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink(OutputMode.Complete);

        var resultDF = source.toDF().groupBy("word").count();
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink, OutputMode.Complete);

        // 第 1 批：hello hello world → 全量输出 {hello:2, world:1}
        source.addData(List.of(
                Row.of("word", "hello"),
                Row.of("word", "hello"),
                Row.of("word", "world")));
        execution.advance();

        List<Row> batch1 = sink.latestBatchData();
        Map<String, Long> counts1 = toCountMap(batch1);
        assertEquals(Map.of("hello", 2L, "world", 1L), counts1);

        // 第 2 批：hello spark → 全量输出 {hello:3, world:1, spark:1}
        source.addData(List.of(
                Row.of("word", "hello"),
                Row.of("word", "spark")));
        execution.advance();

        List<Row> batch2 = sink.latestBatchData();
        Map<String, Long> counts2 = toCountMap(batch2);
        assertEquals(3, counts2.size());
        assertEquals(3L, counts2.get("hello"));
        assertEquals(1L, counts2.get("world"));
        assertEquals(1L, counts2.get("spark"));

        // Complete 模式只保留最新批次
        assertEquals(1, sink.batchCount());
    }

    @Test
    void testCompleteModeAccumulatesAcrossBatches() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink(OutputMode.Complete);

        var resultDF = source.toDF().groupBy("word").count();
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink, OutputMode.Complete);

        // 第 1 批
        source.addData(List.of(Row.of("word", "a"), Row.of("word", "a"), Row.of("word", "b")));
        execution.advance();
        assertEquals(Map.of("a", 2L, "b", 1L), toCountMap(sink.latestBatchData()));

        // 第 2 批：a 和 c 有新数据，b 无新数据但 Complete 模式仍输出
        source.addData(List.of(Row.of("word", "a"), Row.of("word", "c")));
        execution.advance();
        Map<String, Long> counts = toCountMap(sink.latestBatchData());
        assertEquals(3L, counts.get("a"));   // 2 + 1 = 3
        assertEquals(1L, counts.get("b"));   // 未更新但仍输出
        assertEquals(1L, counts.get("c"));   // 新增
    }

    // ---------- Update 输出模式 ----------

    @Test
    void testUpdateModeOutputsOnlyUpdatedGroups() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink(OutputMode.Update);

        var resultDF = source.toDF().groupBy("word").count();
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink, OutputMode.Update);

        // 第 1 批：hello hello world → 输出 {hello:2, world:1}
        source.addData(List.of(
                Row.of("word", "hello"),
                Row.of("word", "hello"),
                Row.of("word", "world")));
        execution.advance();

        List<Row> batch1 = sink.latestBatchData();
        Map<String, Long> counts1 = toCountMap(batch1);
        assertEquals(Map.of("hello", 2L, "world", 1L), counts1);

        // 第 2 批：hello spark → 只输出 {hello:3, spark:1}，world 不输出
        source.addData(List.of(
                Row.of("word", "hello"),
                Row.of("word", "spark")));
        execution.advance();

        List<Row> batch2 = sink.latestBatchData();
        Map<String, Long> counts2 = toCountMap(batch2);
        assertEquals(2, counts2.size());
        assertEquals(3L, counts2.get("hello"));
        assertEquals(1L, counts2.get("spark"));
        assertNull(counts2.get("world")); // world 未更新，不输出
    }

    @Test
    void testUpdateModeAccumulatesInSink() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink(OutputMode.Update);

        var resultDF = source.toDF().groupBy("word").count();
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink, OutputMode.Update);

        // 第 1 批
        source.addData(List.of(Row.of("word", "a"), Row.of("word", "b")));
        execution.advance();

        // 第 2 批：只有 c 有新数据
        source.addData(List.of(Row.of("word", "c")));
        execution.advance();

        // Update 模式保留所有批次（不像 Complete 那样清空）
        assertEquals(2, sink.batchCount());

        // allData 包含两批的输出
        List<Row> all = sink.allData();
        assertEquals(3, all.size()); // batch1: {a:1, b:1}, batch2: {c:1}
    }

    @Test
    void testUpdateModeStatePersistsAcrossBatches() {
        MemoryStream source = new MemoryStream(sqlContext, wordSchema);
        MemorySink sink = new MemorySink(OutputMode.Update);

        var resultDF = source.toDF().groupBy("word").count();
        var execution = new StreamExecution(sqlContext, resultDF.logicalPlan(), sink, OutputMode.Update);

        // 第 1 批：a a a → {a:3}
        source.addData(List.of(Row.of("word", "a"), Row.of("word", "a"), Row.of("word", "a")));
        execution.advance();
        assertEquals(Map.of("a", 3L), toCountMap(sink.latestBatchData()));

        // 第 2 批：a → {a:4}（状态从 3 累积到 4）
        source.addData(List.of(Row.of("word", "a")));
        execution.advance();
        assertEquals(Map.of("a", 4L), toCountMap(sink.latestBatchData()));
    }

    // ---------- 辅助方法 ----------

    private Map<String, Long> toCountMap(List<Row> rows) {
        return rows.stream()
                .collect(Collectors.toMap(
                        row -> (String) row.get("word"),
                        row -> (Long) row.get("count")));
    }
}
