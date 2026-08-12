package com.sparklearn.rdd;

import com.sparklearn.Dependency;
import com.sparklearn.KeyValuePair;
import com.sparklearn.Partition;
import com.sparklearn.ShuffleDependency;
import com.sparklearn.executor.TaskExecutionEnvironment;
import com.sparklearn.scheduler.MapOutputStatus;
import com.sparklearn.shuffle.FetchFailedException;
import com.sparklearn.shuffle.ShuffleBlockRequest;
import com.sparklearn.shuffle.ShuffleBlockResult;
import com.sparklearn.util.SerializableBinaryOperator;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * reduceByKey 的结果 RDD。
 *
 * <p>Map 阶段由 DAGScheduler 先触发：遍历父 RDD 的每个分区，按 key
 * 哈希写到 N 个本地文件。Reduce 阶段只读取属于当前分区的所有文件，
 * 再把相同 key 的值合并一次。也就是说，compute() 不再补做 Map 阶段。
 *
 * <p>为了减少写盘量，Map 端先在每个桶里做一次本地 combine。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public final class ShuffledRDD<K, V> extends RDD<KeyValuePair<K, V>> {

    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final int READ_TIMEOUT_MILLIS = 60_000;

    private final List<Partition> partitions;
    private final File shuffleDir;
    private final SerializableBinaryOperator<V> reduceFunc;
    private final int numMapPartitions;
    private ShuffleDependency<K, V> shuffleDependency;
    private List<Dependency<?>> dependencies;

    public ShuffledRDD(RDD<KeyValuePair<K, V>> parent, int numReducePartitions,
                       SerializableBinaryOperator<V> reduceFunc) {
        super(parent.sparkContext());
        Objects.requireNonNull(parent, "parent");
        if (numReducePartitions <= 0) {
            throw new IllegalArgumentException("numReducePartitions must be positive");
        }
        this.reduceFunc = Objects.requireNonNull(reduceFunc, "reduceFunc");
        this.numMapPartitions = parent.partitions().size();

        List<Partition> partitionList = new ArrayList<>();
        for (int index = 0; index < numReducePartitions; index++) {
            partitionList.add(new Partition(index));
        }
        this.partitions = List.copyOf(partitionList);

        try {
            this.shuffleDir = java.nio.file.Files.createTempDirectory("spark-shuffle-").toFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.shuffleDependency = new ShuffleDependency<>(
                parent,
                numReducePartitions,
                shuffleDir,
                reduceFunc);
        this.dependencies = List.of(shuffleDependency);
    }

    @Override
    public Iterator<KeyValuePair<K, V>> compute(Partition partition) {
        Objects.requireNonNull(partition, "partition");
        if (partition.index() < 0 || partition.index() >= partitions.size()) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }

        return toKeyValuePairs(readAndMergeReducePartition(partition.index()));
    }

    private Map<K, V> readAndMergeReducePartition(int reduceId) {
        Map<K, V> merged = new HashMap<>();
        for (int mapId = 0; mapId < numMapPartitions; mapId++) {
            Map<K, V> mapOutput = readMapOutput(mapId, reduceId);
            for (var entry : mapOutput.entrySet()) {
                merged.merge(entry.getKey(), entry.getValue(), reduceFunc);
            }
        }
        return merged;
    }

    private Iterator<KeyValuePair<K, V>> toKeyValuePairs(Map<K, V> merged) {
        List<KeyValuePair<K, V>> result = new ArrayList<>();
        for (var entry : merged.entrySet()) {
            result.add(new KeyValuePair<>(entry.getKey(), entry.getValue()));
        }
        return result.iterator();
    }

    @SuppressWarnings("unchecked")
    private Map<K, V> readMapOutput(int mapId, int reduceId) {
        MapOutputStatus status = shuffleDependency.mapOutputStatus(mapId);
        File file = status == null
                ? shuffleDependency.mapOutputFile(mapId, reduceId)
                : new File(status.blockFile(reduceId));
        Map<K, V> result = new HashMap<>();
        try (ObjectInputStream in = openMapOutput(status, file, reduceId)) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                K key = (K) in.readObject();
                V value = (V) in.readObject();
                result.put(key, value);
            }
        } catch (IOException | ClassNotFoundException e) {
            throw new FetchFailedException(
                    shuffleDependency,
                    mapId,
                    reduceId,
                    file,
                    e);
        }
        return result;
    }

    private ObjectInputStream openMapOutput(
            MapOutputStatus status,
            File file,
            int reduceId) throws IOException, ClassNotFoundException {
        TaskExecutionEnvironment.Environment current =
                TaskExecutionEnvironment.current();
        if (status == null
                || !status.remote()
                || (current != null
                && status.executorAddress().equals(current.executorAddress()))) {
            return new ObjectInputStream(
                    new BufferedInputStream(new FileInputStream(file)));
        }

        byte[] bytes = fetchRemoteBlock(status, file, reduceId);
        return new ObjectInputStream(
                new BufferedInputStream(new ByteArrayInputStream(bytes)));
    }

    private byte[] fetchRemoteBlock(
            MapOutputStatus status,
            File file,
            int reduceId) throws IOException, ClassNotFoundException {
        HostPort hostPort = HostPort.parse(status.executorAddress());
        try (Socket socket = new Socket()) {
            socket.connect(
                    new InetSocketAddress(hostPort.host(), hostPort.port()),
                    CONNECT_TIMEOUT_MILLIS);
            socket.setSoTimeout(READ_TIMEOUT_MILLIS);
            try (ObjectOutputStream out = new ObjectOutputStream(
                    new java.io.BufferedOutputStream(socket.getOutputStream()))) {
                out.flush();
                ObjectInputStream in = new ObjectInputStream(
                        new BufferedInputStream(socket.getInputStream()));
                out.writeObject(new ShuffleBlockRequest(
                        Path.of(status.blockFile(reduceId))
                                .getFileName().toString()));
                out.flush();
                ShuffleBlockResult response = (ShuffleBlockResult) in.readObject();
                if (!response.success()) {
                    throw new IOException(
                            "远程 shuffle block 拉取失败: " + file,
                            response.error());
                }
                return response.bytes();
            }
        }
    }

    private record HostPort(String host, int port) {

        static HostPort parse(String address) {
            int colon = address.lastIndexOf(':');
            if (colon <= 0 || colon == address.length() - 1) {
                throw new IllegalArgumentException(
                        "executor address must be host:port: " + address);
            }
            return new HostPort(
                    address.substring(0, colon),
                    Integer.parseInt(address.substring(colon + 1)));
        }
    }

    @Override
    protected List<Partition> getPartitionsInternal() {
        return partitions;
    }

    @Override
    protected List<Dependency<?>> getDependenciesInternal() {
        return dependencies;
    }

    @Override
    protected void clearDependencies() {
        shuffleDependency = null;
        dependencies = List.of();
    }

    /**
     * 返回 shuffle 中间文件的目录，供示例程序和测试查看。
     */
    public File shuffleDir() {
        return shuffleDir;
    }

    int numMapPartitions() {
        return numMapPartitions;
    }

    public File mapOutputFile(int mapId, int reduceId) {
        return shuffleDependency.mapOutputFile(mapId, reduceId);
    }
}
