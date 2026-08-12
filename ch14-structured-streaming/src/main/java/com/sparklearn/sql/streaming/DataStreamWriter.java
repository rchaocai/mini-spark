package com.sparklearn.sql.streaming;

import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.Schema;
import com.sparklearn.streaming.structured.ConsoleSink;
import com.sparklearn.streaming.structured.MemorySink;
import com.sparklearn.streaming.structured.OutputMode;
import com.sparklearn.streaming.structured.Sink;
import com.sparklearn.streaming.structured.StreamExecution;
import com.sparklearn.streaming.structured.StructuredStreaming;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 流式写出入口，使用链式构建器风格配置输出模式、Sink 类型并启动查询。
 * <p>
 * 通过 {@link DataFrame#writeStream()} 获取实例。
 * <p>
 * 参考 Spark 源码：{@code org.apache.spark.sql.streaming.DataStreamWriter}
 *
 * <h3>典型用法</h3>
 * <pre>{@code
 * // start() 内部启动后台线程，自动循环处理微批
 * StreamingQuery query = resultDF.writeStream()
 *         .outputMode(OutputMode.Complete)
 *         .format("console")
 *         .start();
 *
 * // 添加数据后等待后台线程处理完成，结果由 ConsoleSink 自动打印
 * source.addData(...);
 * query.processAllAvailable();
 * }</pre>
 */
public final class DataStreamWriter {

    private final DataFrame df;
    private final SQLContext sqlContext;
    private String source = "memory";
    private OutputMode outputMode = OutputMode.Append;
    private final Map<String, String> extraOptions = new HashMap<>();

    /** 启动后持有的 Sink 实例（memory 格式为 MemorySink）。 */
    private Sink sink;

    public DataStreamWriter(DataFrame df) {
        this.df = Objects.requireNonNull(df, "df");
        this.sqlContext = df.sqlContext();
    }

    /**
     * 指定输出模式，决定 Sink 每次输出的内容范围：
     * <ul>
     *   <li>{@link OutputMode#Append}：仅输出本微批新增行（无状态聚合）</li>
     *   <li>{@link OutputMode#Complete}：每次输出全量聚合结果（需 StateStore）</li>
     *   <li>{@link OutputMode#Update}：仅输出本批次被更新的分组（需 StateStore）</li>
     * </ul>
     */
    public DataStreamWriter outputMode(OutputMode outputMode) {
        this.outputMode = Objects.requireNonNull(outputMode, "outputMode");
        return this;
    }

    /**
     * 通过字符串指定输出模式，大小写不敏感。支持 {@code "append"} / {@code "complete"} / {@code "update"}。
     */
    public DataStreamWriter outputMode(String outputMode) {
        Objects.requireNonNull(outputMode, "outputMode");
        switch (outputMode.toLowerCase()) {
            case "append":
                return outputMode(OutputMode.Append);
            case "complete":
                return outputMode(OutputMode.Complete);
            case "update":
                return outputMode(OutputMode.Update);
            default:
                throw new IllegalArgumentException(
                        "Unknown output mode '" + outputMode + "'. " +
                                "Accepted: 'append', 'complete', 'update'");
        }
    }

    /**
     * 指定输出 Sink 格式。mini-spark 教学版目前内置支持：
     * <ul>
     *   <li>{@code "memory"}（默认）：基于内存的 {@link MemorySink}，
     *       启动后可通过返回句柄获取 Sink 读取结果</li>
     *   <li>{@code "console"}：基于控制台的 {@link ConsoleSink}，
     *       每个微批的结果自动打印到标准输出</li>
     * </ul>
     */
    public DataStreamWriter format(String source) {
        this.source = Objects.requireNonNull(source, "source");
        return this;
    }

    /**
     * 查询名称，对于 memory sink 对应注册到 {@code SessionCatalog} 的临时视图名，
     * 可在 SQL 中通过该名称查询输出结果。
     */
    public DataStreamWriter queryName(String queryName) {
        extraOptions.put("queryName", queryName);
        return this;
    }

    /** 添加输出选项（mini-spark 教学版暂未使用，保留以对齐 API）。 */
    public DataStreamWriter option(String key, String value) {
        extraOptions.put(key, value);
        return this;
    }

    /** @see #option(String, String) */
    public DataStreamWriter option(String key, long value) {
        return option(key, Long.toString(value));
    }

    /** @see #option(String, String) */
    public DataStreamWriter option(String key, boolean value) {
        return option(key, Boolean.toString(value));
    }

    /** @see #option(String, String) */
    public DataStreamWriter option(String key, double value) {
        return option(key, Double.toString(value));
    }

    /** 批量添加输出选项。 */
    public DataStreamWriter options(Map<String, String> options) {
        extraOptions.putAll(options);
        return this;
    }

    /**
     * 启动流式查询，返回可控制执行的 {@link StreamingQuery} 句柄。
     * <p>
     * 如果指定了 {@code queryName}，每次微批推进后 Sink 的当前数据会自动注册为同名临时视图，
     * 可通过 {@code sql.table(queryName)} 读取结果——与 Spark 的 memory sink 行为一致。
     *
     * @throws IllegalArgumentException format 不受支持
     */
    public StreamingQuery start() {
        switch (source) {
            case "memory":
                sink = new MemorySink(outputMode);
                break;
            case "console":
                sink = new ConsoleSink();
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported streaming sink format: '" + source + "'. " +
                                "Supported: 'memory', 'console'");
        }

        String queryName = extraOptions.get("queryName");
        StructuredStreaming ss = new StructuredStreaming(sqlContext);
        StreamExecution execution = ss.startQuery(df, sink, outputMode, queryName);

        return new MemorySinkBackedQueryImpl(execution, sink);
    }
}
