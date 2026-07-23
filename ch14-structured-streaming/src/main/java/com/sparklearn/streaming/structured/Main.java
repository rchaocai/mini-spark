package com.sparklearn.streaming.structured;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.DataType;
import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.Schema;

import java.util.List;

import static com.sparklearn.sql.catalyst.expressions.Expressions.col;

/**
 * 第 14 章 · Structured Streaming 演示入口。
 * <p>
 * 演示：用 Structured Streaming 实现实时 WordCount。
 * 与第 11 章 DStream 版本对比，展示 Structured Streaming 基于 DataFrame/SQL 引擎的编程模型。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        System.out.println("=".repeat(72));
        System.out.println("第 14 章 · Structured Streaming — 流式 WordCount");
        System.out.println("=".repeat(72));

        try (SparkContext spark = new SparkContext(2, true)) {
            SQLContext sql = new SQLContext(spark);

            // 1. 创建流式数据源（schema: word STRING）
            Schema wordSchema = Schema.of(new Field("word", DataType.STRING));
            MemoryStream source = new MemoryStream(sql, wordSchema);

            // 2. 构建流式 DataFrame 并定义查询
            DataFrame streamDF = source.toDF();
            DataFrame resultDF = streamDF
                    .groupBy("word")
                    .count();

            System.out.println("\n流式查询逻辑计划：");
            System.out.println(resultDF.logicalPlan().treeString());

            // 3. 创建输出 Sink
            MemorySink sink = new MemorySink();

            // 4. 创建流式执行引擎
            StructuredStreaming ss = new StructuredStreaming(sql);
            StreamExecution execution = ss.startQuery(resultDF, sink);

            // 5. 模拟流式数据到达
            System.out.println("\n" + "=".repeat(72));
            System.out.println("开始模拟流式数据...");
            System.out.println("=".repeat(72));

            // 第 1 批数据
            System.out.println("\n--- 第 1 批数据 ---");
            source.addData(List.of(
                    Row.of("word", "hello"),
                    Row.of("word", "world"),
                    Row.of("word", "hello")));
            execution.advance();
            System.out.println("Sink 结果：");
            for (Row row : sink.allData()) {
                System.out.println("  " + row);
            }

            // 第 2 批数据
            System.out.println("\n--- 第 2 批数据 ---");
            source.addData(List.of(
                    Row.of("word", "spark"),
                    Row.of("word", "hello"),
                    Row.of("word", "world")));
            execution.advance();
            System.out.println("Sink 结果（累积）：");
            for (Row row : sink.allData()) {
                System.out.println("  " + row);
            }

            // 第 3 批数据
            System.out.println("\n--- 第 3 批数据 ---");
            source.addData(List.of(
                    Row.of("word", "structured"),
                    Row.of("word", "streaming"),
                    Row.of("word", "spark"),
                    Row.of("word", "hello")));
            execution.advance();
            System.out.println("Sink 结果（累积）：");
            for (Row row : sink.allData()) {
                System.out.println("  " + row);
            }

            System.out.println("\n" + "=".repeat(72));
            System.out.println("总共执行了 " + execution.batchesExecuted() + " 个微批");
            System.out.println("Sink 共接收 " + sink.batchCount() + " 个批次");
            System.out.println("=".repeat(72));

            execution.stop();
        }
    }
}