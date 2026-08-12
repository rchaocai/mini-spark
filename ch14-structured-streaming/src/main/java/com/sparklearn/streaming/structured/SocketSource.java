package com.sparklearn.streaming.structured;

import com.sparklearn.sql.DataFrame;
import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Field;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.SQLContext;
import com.sparklearn.sql.Schema;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 从 TCP socket 按行读取的流式数据源：构造时连上 {@code host:port} 并启动一个守护线程，
 * 后台持续 {@code readLine}，每读到一行就追加进内部列表、偏移量 {@code +1}。
 *
 * <p>它和 {@link MemoryStream} 实现的是同一个 {@link Source} 接口，{@code getNextBatch}
 * 的语义也完全一致——按偏移量区间 {@code (prevEnd, currentOffset]} 切出一批行。
 * 区别只在数据来源：{@link MemoryStream} 要用户主动 {@code addData} 才有数据，
 * 而 {@code SocketSource} 由后台线程自己从 socket 持续读取，{@link StreamExecution} 的
 * 微批线程按自己的节拍来取，用户完全不碰触发。
 *
 * <p>参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.TextSocketSource}
 *（sql/core/.../execution/streaming/socket.scala）。Spark 的 {@code TextSocketSource}
 * 在 {@code commit} 时 {@code trimStart} 回收已提交的行；这里同样实现 {@link #commit}，
 * 将已处理行从缓冲中删除，避免内存随运行时间无限增长。
 */
public class SocketSource implements Source, AutoCloseable {

    private final SQLContext sqlContext;
    private final Schema sourceSchema;

    /** 保存下来的行，按到达顺序追加；commit 后从头部删除已处理行（trimStart）。 */
    private final List<Row> batches = new ArrayList<>();

    /** 当前偏移量，初始 -1 表示还没读到任何行；每读到一行 +1。 */
    private volatile LongOffset currentOffset = new LongOffset(-1);

    /** 最后已提交的偏移量，初始 -1；commit 后推进。 */
    private LongOffset lastOffsetCommitted = new LongOffset(-1);

    private volatile Socket socket;
    private volatile Thread readThread;

    /**
     * 连上 {@code host:port} 并启动后台读线程。
     *
     * @param wordColumnName 输出 DataFrame 的列名，需与查询（如 {@code groupBy("word")}）对齐
     */
    public SocketSource(SQLContext sqlContext, String host, int port, String wordColumnName) {
        this.sqlContext = sqlContext;
        this.sourceSchema = Schema.of(new Field(wordColumnName, DataType.STRING));
        try {
            this.socket = new Socket(host, port);
        } catch (Exception e) {
            throw new RuntimeException("连接 " + host + ":" + port + " 失败: " + e.getMessage(), e);
        }
        this.readThread = new Thread(this::readLoop, "socket-source-" + host + ":" + port);
        this.readThread.setDaemon(true);
        this.readThread.start();
    }

    /** 默认列名 {@code word}，与 {@code groupBy("word").count()} 对齐。 */
    public SocketSource(SQLContext sqlContext, String host, int port) {
        this(sqlContext, host, port, "word");
    }

    /**
     * 后台读循环：阻塞读一行、追加一行、偏移量 {@code +1}。
     *
     * <p>{@code readLine} 不响应 {@code interrupt}，所以关闭时靠 {@link #close()} 关 socket
     * 让它返回 {@code null}（连接结束），线程随之退出。
     */
    private void readLoop() {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                synchronized (this) {
                    batches.add(Row.of(sourceSchema.fields().get(0).name(), line));
                    currentOffset = currentOffset.increment();
                }
            }
        } catch (Exception ignored) {
            // socket 关闭或出错，读取线程安静退出
        }
    }

    @Override
    public Schema schema() {
        return sourceSchema;
    }

    /**
     * 返回从 {@code start} 之后到当前偏移量的所有行。
     *
     * <p>语义与 {@link MemoryStream#getNextBatch} 一致：{@code start} 是上一微批存进
     * {@code streamProgress} 的 {@code end}，所以 {@code startIndex = prevEnd + 1}；
     * 首次调用 {@code start} 为空，从第 0 行开始。{@code Batch.end} 永远是当前最新偏移量。
     * <p>
     * 偏移量到列表索引的映射基于 {@code lastOffsetCommitted}：
     * {@code index = offset - lastOffsetCommitted - 1}，
     * commit 删除头部行后 {@code lastOffsetCommitted} 向后推进，映射仍然正确。
     */
    @Override
    public synchronized Optional<Batch> getNextBatch(Optional<Offset> start) {
        long startOrdinal = start
                .map(offset -> ((LongOffset) offset).offset() + 1)
                .orElse(-1L);
        long endOrdinal = currentOffset.offset();

        if (endOrdinal < 0 || startOrdinal > endOrdinal) {
            return Optional.empty();
        }

        // 基于 lastOffsetCommitted 计算列表索引（对应 Spark TextSocketSource.getBatch 的切片逻辑）
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

        List<Row> slice = new ArrayList<>();
        for (int i = sliceStart; i <= sliceEnd; i++) {
            slice.add(batches.get(i));
        }

        DataFrame df = sqlContext.createDataFrame("socket", slice, sourceSchema, 1);
        return Optional.of(new Batch(currentOffset, df));
    }

    /**
     * 回收已处理行：计算 {@code end - lastOffsetCommitted} 得到待删除数量，
     * 从列表头部删除（trimStart），然后推进 {@code lastOffsetCommitted}。
     * <p>
     * 对应 Spark {@code TextSocketSource.commit}：
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
                    "SocketSource.commit() received an offset that did not originate with an instance of this class: " + end);
        }
        int offsetDiff = (int) (newOffset.offset() - lastOffsetCommitted.offset());
        if (offsetDiff < 0) {
            throw new IllegalStateException(
                    "Offsets committed out of order: " + lastOffsetCommitted + " followed by " + end);
        }
        // trimStart：等价于 Scala ListBuffer.trimStart(offsetDiff)
        int toRemove = Math.min(offsetDiff, batches.size());
        batches.subList(0, toRemove).clear();
        lastOffsetCommitted = newOffset;
    }

    @Override
    public Offset getCurrentOffset() {
        return currentOffset;
    }

    /**
     * 获取此源的 DataFrame 视图，用于构建流式查询。
     */
    public DataFrame toDF() {
        return new DataFrame(sqlContext, new StreamingRelation(this));
    }

    /** 当前已读到的偏移量（用于演示中轮询等待数据到达）。 */
    public LongOffset offset() {
        return currentOffset;
    }

    @Override
    public void close() {
        if (socket != null) {
            try {
                socket.close();
            } catch (Exception ignored) {
            }
        }
    }
}
