package com.sparklearn.streaming.structured;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.streaming.StreamingQuery;

import java.util.List;

/**
 * 第 14 章 · Structured Streaming 演示入口。
 * <p>
 * 用一条 socket 流演示 Structured Streaming：后台起一个本地 TCP 服务器，
 * 按行把数据推给 {@link SocketSource}；Structured Streaming 的后台微批线程自动从
 * socket 取行、执行 {@code groupBy("word").count()}、把结果写进 console sink——
 * 每个微批的结果自动打印到控制台，用户无需手动调用 {@code show()}。
 *
 * <h3>典型用法</h3>
 * <pre>{@code
 * try (LineServer server = new LineServer()) {
 *     server.start();
 *     try (SparkContext spark = new SparkContext(2, false)) {
 *         SQLContext sql = new SQLContext(spark);
 *         try (SocketSource source = new SocketSource(sql, "localhost", server.port())) {
 *             DataFrame resultDF = source.toDF().groupBy("word").count();
 *
 *             // console sink：后台微批线程自动打印每个批次的结果
 *             StreamingQuery query = resultDF.writeStream()
 *                     .outputMode(OutputMode.Complete)
 *                     .format("console")
 *                     .start();
 *
 *             server.send(List.of("hello", "hello", "world"));
 *             waitForBatches(query, 1);  // 等后台线程处理完第 1 批
 *
 *             server.send(List.of("hello", "spark"));
 *             waitForBatches(query, 2);
 *
 *             query.stop();
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p><b>注意</b>：socket 是长连接，数据持续到来，没有"所有数据都处理完"的时刻，
 * 所以这里<b>不能</b>调 {@code query.processAllAvailable()}——它会一直等下去。
 * 节奏靠"推一批 → 等后台微批线程处理完 → 推下一批"来控制。
 */
public final class Main {

    /** 等待微批线程处理的轮询间隔（毫秒）。 */
    private static final long POLL_INTERVAL_MS = 20;
    /** 等待微批线程处理的总超时（毫秒）。 */
    private static final long POLL_TIMEOUT_MS = 3000;

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        demoSocketWordCount();
    }

    /**
     * socket 流式 WordCount 演示：后台微批线程自动从 socket 取行，
     * Complete 模式跨批累积词频，console sink 自动打印每个批次的结果。
     */
    private static void demoSocketWordCount() throws Exception {
        System.out.println("=".repeat(72));
        System.out.println("第 14 章 · Structured Streaming — socket 流式 WordCount（Complete）");
        System.out.println("=".repeat(72));

        try (LineServer server = new LineServer()) {
            server.start();

            try (SparkContext spark = new SparkContext(2, false)) {
                SQLContext sql = new SQLContext(spark);
                try (SocketSource source = new SocketSource(sql, "localhost", server.port())) {
                    runSocketDemo(server, source, sql);
                }
            }
        }
    }

    private static void runSocketDemo(LineServer server, SocketSource source, SQLContext sql)
            throws Exception {
        DataFrame resultDF = source.toDF().groupBy("word").count();

        StreamingQuery query = resultDF.writeStream()
                .outputMode(OutputMode.Complete)
                .format("console")
                .start();

        System.out.println("\n--- 第 1 批：hello hello world ---");
        server.send(List.of("hello", "hello", "world"));
        waitForBatches(query, 1);

        System.out.println("\n--- 第 2 批：hello spark ---");
        server.send(List.of("hello", "spark"));
        waitForBatches(query, 2);

        System.out.println("\n--- 第 3 批：world spark ---");
        server.send(List.of("world", "spark"));
        waitForBatches(query, 3);

        query.stop();
    }

    /**
     * 轮询等待后台微批线程处理到指定的批次数。
     *
     * @param expectedBatches 期望的已处理批次数
     */
    private static void waitForBatches(StreamingQuery query, int expectedBatches)
            throws Exception {
        long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MS;
        while (query.batchesExecuted() < expectedBatches) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("等待微批处理超时：期望批次=" + expectedBatches);
            }
            Thread.sleep(POLL_INTERVAL_MS);
        }
    }
}
