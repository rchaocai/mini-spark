package com.sparklearn.sql.streaming;

import com.sparklearn.streaming.structured.MemorySink;
import com.sparklearn.streaming.structured.Sink;
import com.sparklearn.streaming.structured.StreamExecution;

import java.util.Objects;

/**
 * {@link StreamingQuery} 的默认实现，包装 {@link StreamExecution}，
 * 并当 Sink 为 {@link MemorySink} 时通过 {@link MemorySinkBackedQuery} 暴露访问。
 */
final class MemorySinkBackedQueryImpl implements MemorySinkBackedQuery {

    private final StreamExecution execution;
    private final Sink sink;

    MemorySinkBackedQueryImpl(StreamExecution execution, Sink sink) {
        this.execution = Objects.requireNonNull(execution, "execution");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    @Override
    public void processAllAvailable() {
        execution.processAllAvailable();
    }

    @Override
    public void stop() {
        execution.stop();
    }

    @Override
    public boolean isStopped() {
        return execution.isStopped();
    }

    @Override
    public int batchesExecuted() {
        return execution.batchesExecuted();
    }

    @Override
    public MemorySink memorySink() {
        if (sink instanceof MemorySink ms) {
            return ms;
        }
        throw new IllegalStateException(
                "memorySink() is only available when format(\"memory\") is used for writing; " +
                        "actual sink: " + sink.getClass().getSimpleName());
    }
}
