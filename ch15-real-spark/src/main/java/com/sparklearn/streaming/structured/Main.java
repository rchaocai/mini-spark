package com.sparklearn.streaming.structured;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.DataType;
import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.streaming.StreamingQuery;

import java.util.List;

/**
 * 第 15 章 · Structured Streaming 演示入口（简化链式 API 风格）。
 * <p>
 * 演示：用 Structured Streaming 实现实时 WordCount。
 * 使用 console sink，后台微批线程每处理完一批数据就自动打印到控制台，
 * 用户无需手动调用 {@code show()} 查看结果。
 *
 * <h3>典型用法</h3>
 * <pre>{@code
 * MemoryStream source = new MemoryStream(sql, wordSchema);
 * DataFrame resultDF = source.toDF().groupBy("word").count();
 *
 * // start() 内部启动后台线程，自动循环处理微批（无需手动触发）
 * StreamingQuery query = resultDF.writeStream()
 *         .outputMode("append")
 *         .format("console")
 *         .start();
 *
 * // 添加数据后等待后台线程处理完成，结果由 ConsoleSink 自动打印
 * source.addData(List.of(...));
 * query.processAllAvailable();
 * }</pre>
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        demoDataFrame();
        demoSQL();
    }

    /**
     * DataFrame API 演示：{@code streamDF.groupBy("word").count()}
     * 使用 console sink，每个微批的结果自动打印到控制台。
     */
    private static void demoDataFrame() {
        System.out.println("=".repeat(72));
        System.out.println("第 15 章 · Structured Streaming — 流式 WordCount（DataFrame API，链式风格）");
        System.out.println("=".repeat(72));

        try (SparkContext spark = new SparkContext(2, true)) {
            SQLContext sql = new SQLContext(spark);

            Schema wordSchema = Schema.of(new Field("word", DataType.STRING));
            MemoryStream source = new MemoryStream(sql, wordSchema);
            DataFrame streamDF = source.toDF();

            DataFrame resultDF = streamDF
                    .groupBy("word")
                    .count();

            System.out.println("\n流式查询逻辑计划（DataFrame API）：");
            System.out.println(resultDF.logicalPlan().treeString());

            StreamingQuery query = resultDF.writeStream()
                    .outputMode("append")
                    .format("console")
                    .start();

            System.out.println("\n" + "=".repeat(72));
            System.out.println("开始模拟流式数据...");
            System.out.println("=".repeat(72));

            System.out.println("\n--- 第 1 批数据 ---");
            source.addData(List.of(
                    Row.of("word", "hello"),
                    Row.of("word", "world"),
                    Row.of("word", "hello")));
            query.processAllAvailable();

            System.out.println("\n--- 第 2 批数据 ---");
            source.addData(List.of(
                    Row.of("word", "spark"),
                    Row.of("word", "hello"),
                    Row.of("word", "world")));
            query.processAllAvailable();

            System.out.println("\n--- 第 3 批数据 ---");
            source.addData(List.of(
                    Row.of("word", "structured"),
                    Row.of("word", "streaming"),
                    Row.of("word", "spark"),
                    Row.of("word", "hello")));
            query.processAllAvailable();

            System.out.println("\n" + "=".repeat(72));
            System.out.println("总共执行了 " + query.batchesExecuted() + " 个微批");
            System.out.println("=".repeat(72));

            query.stop();
        }
    }

    /**
     * SQL 演示：{@code sql.sql("SELECT word, count(*) FROM words GROUP BY word")}
     * 使用 console sink，每个微批的结果自动打印到控制台。
     */
    private static void demoSQL() {
        System.out.println("\n\n" + "=".repeat(72));
        System.out.println("第 15 章 · Structured Streaming — 流式 WordCount（SQL，链式风格）");
        System.out.println("=".repeat(72));

        try (SparkContext spark = new SparkContext(2, true)) {
            SQLContext sql = new SQLContext(spark);

            Schema wordSchema = Schema.of(new Field("word", DataType.STRING));
            MemoryStream source = new MemoryStream(sql, wordSchema);
            DataFrame wordsDF = source.toDF();
            wordsDF.createOrReplaceTempView("words");

            String sqlText = "SELECT word, count(*) FROM words GROUP BY word";
            System.out.println("\nSQL 查询：" + sqlText);
            DataFrame resultDF = sql.sql(sqlText);
            System.out.println("\nSQL 解析后的逻辑计划：");
            System.out.println(resultDF.logicalPlan().treeString());

            StreamingQuery query = resultDF.writeStream()
                    .outputMode("append")
                    .format("console")
                    .start();

            System.out.println("\n" + "=".repeat(72));
            System.out.println("开始模拟流式数据...");
            System.out.println("=".repeat(72));

            System.out.println("\n--- 第 1 批数据 ---");
            source.addData(List.of(
                    Row.of("word", "hello"),
                    Row.of("word", "world"),
                    Row.of("word", "hello")));
            query.processAllAvailable();

            System.out.println("\n--- 第 2 批数据 ---");
            source.addData(List.of(
                    Row.of("word", "spark"),
                    Row.of("word", "hello"),
                    Row.of("word", "world")));
            query.processAllAvailable();

            System.out.println("\n" + "=".repeat(72));
            System.out.println("SQL 方式总共执行了 " + query.batchesExecuted() + " 个微批");
            System.out.println("=".repeat(72));

            query.stop();
        }
    }
}
