package com.sparklearn.streaming.structured;

import java.util.Objects;

/**
 * 用 long 实现的 Offset，每个 batch 递增 1。
 * 参考 Spark 源码：{@code org.apache.spark.sql.execution.streaming.LongOffset}
 */
public record LongOffset(long offset) implements Offset {

    public LongOffset {
        if (offset < -1) {
            throw new IllegalArgumentException("offset must be >= -1: " + offset);
        }
    }

    public LongOffset increment() {
        return new LongOffset(offset + 1);
    }

    @Override
    public int compareTo(Offset other) {
        if (!(other instanceof LongOffset that)) {
            throw new IllegalArgumentException("cannot compare LongOffset with " + other.getClass().getSimpleName());
        }
        return Long.compare(offset, that.offset);
    }

    @Override
    public String toString() {
        return String.valueOf(offset);
    }
}