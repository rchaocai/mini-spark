package com.sparklearn.sql.streaming;

import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.Schema;
import com.sparklearn.streaming.structured.MemoryStream;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 流式读取入口，使用链式构建器风格配置数据源并加载流式 {@link DataFrame}。
 * <p>
 * 通过 {@link SQLContext#readStream()} 获取实例。
 * <p>
 * 参考 Spark 源码：{@code org.apache.spark.sql.streaming.DataStreamReader}
 *
 * <h3>典型用法</h3>
 * <pre>{@code
 * DataStreamReader reader = sql.readStream()
 *         .format("memory")
 *         .schema(wordSchema);
 * DataFrame streamDF = reader.load();
 * MemoryStream source = reader.memoryStream();   // 获取底层 MemoryStream 供 addData
 * }</pre>
 */
public final class DataStreamReader {

    private final SQLContext sqlContext;
    private String source = "memory";
    private Schema userSpecifiedSchema;
    private final Map<String, String> extraOptions = new HashMap<>();

    /** 底层 MemoryStream 实例，format="memory" 时由 load() 创建。 */
    private MemoryStream memoryStream;

    public DataStreamReader(SQLContext sqlContext) {
        this.sqlContext = Objects.requireNonNull(sqlContext, "sqlContext");
    }

    /**
     * 指定输入数据源格式。mini-spark 教学版目前内置支持：
     * <ul>
     *   <li>{@code "memory"}（默认）：基于内存队列的 {@link MemoryStream}，
     *       适合教学与单元测试，加载后可通过 {@link #memoryStream()} 手动推送数据</li>
     * </ul>
     */
    public DataStreamReader format(String source) {
        this.source = Objects.requireNonNull(source, "source");
        return this;
    }

    /**
     * 指定输入 schema。对于 memory 数据源，此参数必须显式提供，
     * 因为没有任何外部数据可用于自动推断。
     */
    public DataStreamReader schema(Schema schema) {
        this.userSpecifiedSchema = Objects.requireNonNull(schema, "schema");
        return this;
    }

    /** 添加输入选项（mini-spark 教学版暂未使用，保留以对齐 API）。 */
    public DataStreamReader option(String key, String value) {
        extraOptions.put(key, value);
        return this;
    }

    /** @see #option(String, String) */
    public DataStreamReader option(String key, long value) {
        return option(key, Long.toString(value));
    }

    /** @see #option(String, String) */
    public DataStreamReader option(String key, boolean value) {
        return option(key, Boolean.toString(value));
    }

    /** @see #option(String, String) */
    public DataStreamReader option(String key, double value) {
        return option(key, Double.toString(value));
    }

    /** 批量添加输入选项。 */
    public DataStreamReader options(Map<String, String> options) {
        extraOptions.putAll(options);
        return this;
    }

    /**
     * 加载流式数据源，返回一个包含 {@code StreamingRelation} 根节点的 {@link DataFrame}。
     * 该 DataFrame 可直接参与后续的 {@code where / select / groupBy} 等查询构建。
     *
     * @throws IllegalArgumentException 未指定 schema 或 format 不受支持
     */
    public DataFrame load() {
        if (userSpecifiedSchema == null) {
            throw new IllegalArgumentException(
                    "Schema must be specified for streaming source; use DataStreamReader.schema()");
        }
        switch (source) {
            case "memory":
                memoryStream = new MemoryStream(sqlContext, userSpecifiedSchema);
                return memoryStream.toDF();
            default:
                throw new IllegalArgumentException(
                        "Unsupported streaming source format: '" + source + "'. " +
                                "Supported: 'memory'");
        }
    }

    /**
     * 加载带路径的流式数据源（仅对文件类源有意义，mini-spark 教学版暂未实现）。
     */
    public DataFrame load(String path) {
        return option("path", path).load();
    }

    /**
     * 当且仅当 {@code format("memory")} 时，返回底层的 {@link MemoryStream} 实例，
     * 用于在教学/单元测试中通过 {@link MemoryStream#addData} 手动推送数据。
     *
     * @throws IllegalStateException load() 未被调用，或 format 不是 memory
     */
    public MemoryStream memoryStream() {
        if (memoryStream == null) {
            throw new IllegalStateException(
                    "MemoryStream is only available after load() with format(\"memory\")");
        }
        return memoryStream;
    }
}
