package com.sparklearn.streaming;

import java.io.Serializable;
import java.util.Objects;

/**
 * 时间长度。Spark Streaming 用它描述 batch 间隔、窗口宽度和滑动步长。
 */
public final class Duration implements Serializable, Comparable<Duration> {

    private final long milliseconds;

    public Duration(long milliseconds) {
        if (milliseconds <= 0) {
            throw new IllegalArgumentException("duration must be positive: " + milliseconds);
        }
        this.milliseconds = milliseconds;
    }

    public static Duration milliseconds(long milliseconds) {
        return new Duration(milliseconds);
    }

    public static Duration seconds(long seconds) {
        return new Duration(seconds * 1000L);
    }

    public long milliseconds() {
        return milliseconds;
    }

    public Duration plus(Duration other) {
        Objects.requireNonNull(other, "other");
        return new Duration(milliseconds + other.milliseconds);
    }

    public Duration times(int factor) {
        if (factor <= 0) {
            throw new IllegalArgumentException("factor must be positive: " + factor);
        }
        return new Duration(milliseconds * factor);
    }

    public boolean isMultipleOf(Duration other) {
        Objects.requireNonNull(other, "other");
        return milliseconds % other.milliseconds == 0;
    }

    @Override
    public int compareTo(Duration other) {
        return Long.compare(milliseconds, other.milliseconds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Duration duration)) {
            return false;
        }
        return milliseconds == duration.milliseconds;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(milliseconds);
    }

    @Override
    public String toString() {
        if (milliseconds % 1000 == 0) {
            return (milliseconds / 1000) + "s";
        }
        return milliseconds + "ms";
    }
}
