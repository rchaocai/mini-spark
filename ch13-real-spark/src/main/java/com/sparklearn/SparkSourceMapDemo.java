package com.sparklearn;

import java.util.List;

/**
 * 第 13 章核心 Demo——把 RDD 执行底座里的关键类，与 Apache Spark 源码
 * 文件/方法逐行对照。读者能指着 Spark 某文件某方法，说出它对应前面
 * 第几章实现的哪个类/方法。
 *
 * <p>本章不引入新功能——这是全书的「认知收口」。
 */
public final class SparkSourceMapDemo {

    private SparkSourceMapDemo() {
    }

    /** 对照表里的一条记录。 */
    private record Entry(
            String concept,
            int chapter,
            String ourClass,
            String ourMethod,
            String sparkFile,
            String sparkSignature
    ) {
    }

    private static final List<Entry> MAP = List.of(
            new Entry("RDD 五接口抽象", 4,
                    "RDD",
                    "partitions() / compute(p) / dependencies() / iterator(p) / preferredLocations(p)",
                    "core/src/main/scala/spark/RDD.scala",
                    "def splits: Array[Split]\ndef compute(split: Split): Iterator[T]\n@transient val dependencies: List[Dependency[_]]\nfinal def iterator(split: Split): Iterator[T]\ndef preferredLocations(split: Split): Seq[String]"),

            new Entry("分区（Partition）", 2,
                    "Partition",
                    "record Partition(int index)",
                    "core/src/main/scala/spark/Split.scala",
                    "trait Split { val index: Int }"),

            new Entry("延迟迭代 + 惰性求值", 2,
                    "ListRDD",
                    "compute(p) → 分区视图",
                    "core/src/main/scala/spark/ParallelCollection.scala",
                    "ParallelCollection (≈ 我们的 ListRDD)"),

            new Entry("MapPartitionsRDD · 嵌套迭代器流水线", 3,
                    "MapPartitionsRDD",
                    "compute(p) → iteratorTransform.apply(parent.iterator(p))",
                    "core/src/main/scala/spark/RDD.scala",
                    "class MapPartitionsRDD ...\noverride def compute(split: Split) = f(prev.iterator(split))"),

            new Entry("窄依赖（NarrowDependency）", 4,
                    "NarrowDependency / OneToOneDependency",
                    "sealed interface Dependency / getParents(outputPartition)",
                    "core/src/main/scala/spark/Dependency.scala",
                    "abstract class NarrowDependency[T](rdd: RDD[T])\n  extends Dependency(rdd, false)\n  def getParents(outputPartition: Int): Seq[Int]"),

            new Entry("宽依赖（ShuffleDependency）", 7,
                    "ShuffleDependency",
                    "class ShuffleDependency(rdd, numReducePartitions, shuffleDir, reduceFunc)",
                    "core/src/main/scala/spark/Dependency.scala",
                    "class ShuffleDependency[K, V, C](\n  shuffleId: Int, rdd: RDD[(K, V)],\n  aggregator: Aggregator[K, V, C],\n  partitioner: Partitioner)"),

            new Entry("Shuffle Map 端写文件", 6,
                    "ShuffleMapTask",
                    "runTask() → 按 key 分桶 → map_x_reduce_y 落盘",
                    "core/src/main/scala/spark/ShuffleMapTask.scala",
                    "class ShuffleMapTask extends DAGTask\n  override def run: buckets + 写文件"),

            new Entry("Shuffle Reduce 端读文件合并", 6,
                    "ShuffledRDD",
                    "compute(p) → 读所有 Map 输出 → HashMap merge",
                    "core/src/main/scala/spark/ShuffledRDD.scala",
                    "override def compute(split: Split):\n  fetcher.fetch + mergeCombiners"),

            new Entry("Stage 划分（沿宽依赖切）", 7,
                    "Stage",
                    "record Stage(id, rdd, shuffleMap, parents, shuffleDependency)",
                    "core/src/main/scala/spark/Stage.scala",
                    "class Stage(id, rdd, shuffleDep: Option[...], parents)\nval isShuffleMap = shuffleDep != None"),

            new Entry("DAGScheduler · 递归切 Stage", 7,
                    "DAGScheduler",
                    "getParentStages(rdd) → DFS 遇宽依赖切",
                    "core/src/main/scala/spark/DAGScheduler.scala",
                    "trait DAGScheduler extends Scheduler\n  newStage / getParentStages / submitTasks"),

            new Entry("Task 封装分区计算", 5,
                    "Task / ResultTask / ShuffleMapTask",
                    "Task.run(attemptId) → runTask(TaskContext)",
                    "core/src/main/scala/spark/Task.scala / ResultTask.scala / ShuffleMapTask.scala",
                    "abstract class Task[T]\nclass ResultTask / class ShuffleMapTask"),

            new Entry("容错重试", 8,
                    "LocalTaskScheduler",
                    "submitTasks() → failCount + maxTaskRetries",
                    "core/src/main/scala/spark/LocalScheduler.scala",
                    "failCount / maxFailures 重试逻辑"),

            new Entry("Cache（惰性缓存）", 10,
                    "RDD",
                    "cache() → shouldCache = true; iterator() 先查缓存",
                    "core/src/main/scala/spark/RDD.scala / CacheTracker.scala",
                    "def cache(): RDD[T] = { shouldCache = true; this }\nfinal def iterator(split): 先查 CacheTracker"),

            new Entry("Checkpoint（切断血缘）", 10,
                    "RDD",
                    "checkpoint() → 物化后 dependencies() 指向 CheckpointRDD",
                    "（0.5 无；v0.7.0 起）rdd/CheckpointRDD.scala + RDD.scala",
                    "def checkpoint()；final def dependencies 改指向 CheckpointRDD"),

            new Entry("网络 RPC", 9,
                    "NetworkTaskScheduler / Executor",
                    "Socket + ObjectStream 发 Task / 收结果",
                    "core/src/main/scala/spark/Executor.scala",
                    "MesosExecutor → 接收序列化 Task → 执行 → 回传结果"),

            new Entry("数据本地性", 9,
                    "RDD / Task",
                    "preferredLocations(p) → 返回偏好节点列表",
                    "core/src/main/scala/spark/RDD.scala",
                    "def preferredLocations(split: Split): Seq[String]")
    );

    public static void print() {
        System.out.println("=".repeat(78));
        System.out.println("第 13 章 · 致敬工业级 Spark —— 源码对照地图");
        System.out.println("=".repeat(78));
        System.out.println();
        System.out.println("下表列出 RDD 执行底座里的关键类，与 Apache Spark 源码文件/方法的");
        System.out.println("对应关系。左边是教学版代码（Java），右边是 Apache Spark（Scala）。");
        System.out.println("读法：打开右边的文件，找到对应方法，并排阅读，看关键路径如何对上。");
        System.out.println();

        String fmt = "│ %-26s │ %-3s │ %-28s │ %-36s │%n";
        String sep = "+" + "-".repeat(28) + "+" + "-".repeat(5)
                + "+" + "-".repeat(30) + "+" + "-".repeat(38) + "+";

        System.out.println(sep);
        System.out.printf(fmt, "概念", "章", "教学版类", "Spark 文件");
        System.out.println(sep);

        for (Entry e : MAP) {
            System.out.printf(fmt, e.concept(), e.chapter(), e.ourClass(), shortFile(e.sparkFile()));
        }
        System.out.println(sep);
        System.out.println();

        System.out.println("=".repeat(78));
        System.out.println("核心对照：逐条展开（教学版代码 ↔ Apache Spark）");
        System.out.println("=".repeat(78));

        for (int i = 0; i < MAP.size(); i++) {
            Entry e = MAP.get(i);
            System.out.println();
            System.out.println("─".repeat(78));
            System.out.printf("[%d/%d] %s（第 %d 章）%n", i + 1, MAP.size(), e.concept(), e.chapter());
            System.out.println("─".repeat(78));

            System.out.println("\n  你的代码（com.sparklearn." + e.ourClass() + "）：");
            System.out.println("  " + e.ourMethod().replace("\n", "\n  "));

            System.out.println("\n  Apache Spark（" + e.sparkFile() + "）：");
            System.out.println("  " + e.sparkSignature().replace("\n", "\n  "));
        }

        System.out.println();
        System.out.println("=".repeat(78));
        System.out.println("读图提示：把零件重新串成一条路");
        System.out.println("=".repeat(78));
        System.out.println();
        System.out.println("下面这段 pipeline——ListRDD → map → reduceByKey → collect——");
        System.out.println("在教学版代码和 Apache Spark 里，走的是同一条关键路径：");
        System.out.println();
        System.out.println("  1. RDD.map() 创建 MapPartitionsRDD（包装父迭代器）");
        System.out.println("  2. reduceByKey → ShuffledRDD");
        System.out.println("     → ShuffleMapTask 按 key 哈希分桶 → combine → 写本地文件");
        System.out.println("     → ShuffledRDD.compute 读文件 → merge → 返回结果");
        System.out.println("  3. DAGScheduler.createResultStage()");
        System.out.println("     → getParentStages() DFS 遇 ShuffleDependency 切 Stage");
        System.out.println("     → 先跑 ShuffleMapStage，再跑 ResultStage");
        System.out.println("  4. collect() → 遍历分区 → iterator(p) → compute(p) → 嵌套迭代器展开");
        System.out.println();
        System.out.println("前面写过的每一块核心逻辑，在 Apache Spark 源码里都有对应位置。");
        System.out.println("差别在哪？在 Scala 语法、在 Mesos/YARN 调度、在 Hadoop I/O 格式——");
        System.out.println("在工程化的外衣。核心架构——RDD、Dependency、Stage、DAGScheduler、");
        System.out.println("ShuffleMapTask、Iterator 嵌套流水线——你亲手写过了。");
    }

    private static String shortFile(String path) {
        int slash = path.lastIndexOf('/');
        return slash >= 0 ? path.substring(slash + 1) : path;
    }
}
