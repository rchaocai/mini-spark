package com.sparklearn.streaming;

import java.io.Serializable;
import java.util.Objects;

/**
 * 逻辑时间点。Streaming 不直接依赖墙钟，而是按 batch 边界生成一串 Time。
 */
public final class Time implements Serializable, Comparable<Time> {

    private final long milliseconds;

    public Time(long milliseconds) {
        this.milliseconds = milliseconds;
    }

    public long milliseconds() {
        return milliseconds;
    }

    public Time plus(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        return new Time(milliseconds + duration.milliseconds());
    }

    public Time minus(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        return new Time(milliseconds - duration.milliseconds());
    }

    public boolean isMultipleOf(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        return milliseconds % duration.milliseconds() == 0;
    }

    @Override
    public int compareTo(Time other) {
        return Long.compare(milliseconds, other.milliseconds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Time time)) {
            return false;
        }
        return milliseconds == time.milliseconds;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(milliseconds);
    }

    @Override
    public String toString() {
        return "Time(" + milliseconds + ")";
    }
}
