package com.sparklearn.rdd;

import com.sparklearn.Dependency;
import com.sparklearn.Partition;
import com.sparklearn.SparkContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * 从本地文本文件构造的源头 RDD，数据按分区在 compute 时读取。
 *
 * <p>构造时只数一遍行数，用来把行均分到各分区；真正读数据延后到
 * {@code compute(partition)}——每次 compute 被调用，才打开文件、读本分区该负责的那段行。
 * 这和生产级实现里按文件分片读取的 {@code HadoopRDD} 思路一致：数据按分区、在真正要用时才读。
 *
 * <p>设计要点：
 * <ul>
 *   <li>构造时不持有文件内容，只记录总行数来规划分区</li>
 *   <li>compute(Partition) 打开文件、跳过本分区之前的行、读本分区的行</li>
 *   <li>和 ListRDD 的区别在于数据来源是文件而非内存 List</li>
 * </ul>
 */
public final class FileRDD extends RDD<String> {

    private final String filePath;
    private final int totalLines;
    private final List<Partition> partitions;

    public FileRDD(SparkContext sparkContext, String filePath, int numberOfPartitions) {
        super(sparkContext);
        Objects.requireNonNull(filePath, "filePath");
        if (numberOfPartitions <= 0) {
            throw new IllegalArgumentException("numberOfPartitions must be positive");
        }

        this.filePath = filePath;
        this.totalLines = countLines(filePath);

        List<Partition> partitionList = new ArrayList<>();
        for (int index = 0; index < numberOfPartitions; index++) {
            partitionList.add(new Partition(index));
        }
        this.partitions = List.copyOf(partitionList);
    }

    @Override
    protected List<Partition> getPartitionsInternal() {
        return partitions;
    }

    @Override
    public Iterator<String> compute(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (partition.index() < 0 || partition.index() >= partitions.size()) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }
        int start = startOffset(partition.index());
        int end = startOffset(partition.index() + 1);
        return readLineRange(filePath, start, end).iterator();
    }

    @Override
    protected List<Dependency<?>> getDependenciesInternal() {
        return List.of();
    }

    @Override
    protected List<String> getPreferredLocationsInternal(Partition partition) {
        return List.of();
    }

    /**
     * 文件总行数。
     */
    public int totalLines() {
        return totalLines;
    }

    private int startOffset(int partitionIndex) {
        int baseSize = totalLines / partitions.size();
        int remainder = totalLines % partitions.size();
        return partitionIndex * baseSize + Math.min(partitionIndex, remainder);
    }

    private static int countLines(String filePath) {
        int count = 0;
        try (BufferedReader reader = Files.newBufferedReader(Path.of(filePath), StandardCharsets.UTF_8)) {
            while (reader.readLine() != null) {
                count++;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取文件失败: " + filePath, e);
        }
        return count;
    }

    private static List<String> readLineRange(String filePath, int startLine, int endLine) {
        int capacity = Math.max(0, endLine - startLine);
        List<String> lines = new ArrayList<>(capacity);
        try (BufferedReader reader = Files.newBufferedReader(Path.of(filePath), StandardCharsets.UTF_8)) {
            int skipped = 0;
            while (skipped < startLine) {
                if (reader.readLine() == null) {
                    return lines;
                }
                skipped++;
            }
            for (int line = startLine; line < endLine; line++) {
                String text = reader.readLine();
                if (text == null) {
                    break;
                }
                lines.add(text);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取文件失败: " + filePath, e);
        }
        return lines;
    }
}
