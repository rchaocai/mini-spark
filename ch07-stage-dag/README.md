# 第 7 章 · 划分 Stage 与 DAG

本章是逐章递进的 **mini-Spark 快照**模块，可独立编译运行。

## 运行

在仓库根目录执行：

```bash
mvn -q -pl ch07-stage-dag package
java -Dfile.encoding=UTF-8 \
  -cp ch07-stage-dag/target/classes \
  com.sparklearn.Main
```

## 对应正文

- `Stage.java`：表示 DAGScheduler 沿 Shuffle 边界切出的执行阶段。
- `DAGScheduler.java`：沿 RDD 血缘回溯，遇宽依赖切 Stage，父 Stage 先执行。
- `ShuffleMapTask.java`：运行父 RDD 的一个 Map 分区，并写出 shuffle 中间文件。
- `ShuffledRDD.java`：表示 reduceByKey 返回的下游 RDD，Reduce 阶段只读取已经写好的 shuffle 文件。
- `Main.java`：演示 `ListRDD -> MapPartitionsRDD -> ShuffledRDD` 被切成两个 Stage。
