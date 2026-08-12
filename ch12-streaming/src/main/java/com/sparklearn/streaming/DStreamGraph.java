package com.sparklearn.streaming;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 记录输入流、输出流，并在每个 batch 时间点生成 job。
 */
public final class DStreamGraph {

    private final List<InputDStream<?>> inputStreams = new ArrayList<>();
    private final List<DStream<?>> outputStreams = new ArrayList<>();
    private Duration batchDuration;
    private Time zeroTime;
    private boolean started;

    public void setBatchDuration(Duration batchDuration) {
        if (this.batchDuration != null) {
            throw new IllegalStateException("batch duration already set");
        }
        this.batchDuration = Objects.requireNonNull(batchDuration, "batchDuration");
    }

    public Duration batchDuration() {
        return batchDuration;
    }

    public Time zeroTime() {
        return zeroTime;
    }

    public boolean started() {
        return started;
    }

    public void addInputStream(InputDStream<?> inputStream) {
        Objects.requireNonNull(inputStream, "inputStream");
        inputStream.setGraph(this);
        inputStreams.add(inputStream);
    }

    public void addOutputStream(DStream<?> outputStream) {
        Objects.requireNonNull(outputStream, "outputStream");
        outputStream.setGraph(this);
        outputStreams.add(outputStream);
    }

    public List<InputDStream<?>> inputStreams() {
        return List.copyOf(inputStreams);
    }

    public List<DStream<?>> outputStreams() {
        return List.copyOf(outputStreams);
    }

    public void start(Time zeroTime) {
        if (started) {
            throw new IllegalStateException("DStream graph already started");
        }
        if (batchDuration == null) {
            throw new IllegalStateException("batch duration not set");
        }
        if (outputStreams.isEmpty()) {
            throw new IllegalStateException("no output stream registered");
        }
        this.zeroTime = Objects.requireNonNull(zeroTime, "zeroTime");
        for (DStream<?> output : outputStreams) {
            output.initialize(zeroTime);
            output.remember(output.slideDuration());
        }
        for (InputDStream<?> input : inputStreams) {
            input.start();
        }
        started = true;
    }

    public void stop() {
        for (InputDStream<?> input : inputStreams) {
            input.stop();
        }
    }

    public List<StreamingJob> generateJobs(Time time) {
        List<StreamingJob> jobs = new ArrayList<>();
        for (DStream<?> output : outputStreams) {
            output.generateJob(time).ifPresent(jobs::add);
        }
        return jobs;
    }
}
