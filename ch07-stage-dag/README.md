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
- `ShuffledRDD.java`：沿用第 6 章的 reduceByKey，并把 Map 端落盘交给 ShuffleMapStage 触发。
- `Main.java`：演示 `ListRDD -> MapPartitionsRDD -> ShuffledRDD` 被切成两个 Stage。
