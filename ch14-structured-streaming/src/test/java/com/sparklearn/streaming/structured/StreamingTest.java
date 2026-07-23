package com.sparklearn.streaming.structured;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

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
        var df = sqlContext.createDataFrame("test", List.of(Row.of("val", "a"), Row.of("val", "b")), 1);
        var batch = new Batch(new LongOffset(0), df);

        sink.addBatch(batch);
        assertEquals(0, ((LongOffset) sink.currentOffset().orElseThrow()).offset());
        assertEquals(1, sink.batchCount());
    }

    @Test
    void testMemorySinkAccumulates() {
        MemorySink sink = new MemorySink();
        var df1 = sqlContext.createDataFrame("t1", List.of(Row.of("v", 1)), 1);
        var df2 = sqlContext.createDataFrame("t2", List.of(Row.of("v", 2)), 1);

        sink.addBatch(new Batch(new LongOffset(0), df1));
        sink.addBatch(new Batch(new LongOffset(1), df2));

        assertEquals(1, ((LongOffset) sink.currentOffset().orElseThrow()).offset());
        assertEquals(2, sink.batchCount());
    }

    @Test
    void testMemorySinkAllData() {
        MemorySink sink = new MemorySink();
        var df = sqlContext.createDataFrame("t", List.of(Row.of("v", 1), Row.of("v", 2)), 1);
        sink.addBatch(new Batch(new LongOffset(0), df));

        List<Row> all = sink.allData();
        assertEquals(2, all.size());
        assertEquals(1, all.get(0).get("v"));
        assertEquals(2, all.get(1).get("v"));
    }

    // ---------- StreamExecution ----------

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
        execution.advance();

        assertEquals(1, sink.batchCount());
    }

    // ---------- Batch ----------

    @Test
    void testBatchCreation() {
        var df = sqlContext.createDataFrame("test", List.of(Row.of("v", 1)), 1);
        var batch = new Batch(new LongOffset(0), df);
        assertNotNull(batch.data());
        assertEquals(0, ((LongOffset) batch.end()).offset());
    }
}