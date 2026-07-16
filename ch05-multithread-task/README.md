# 第 5 章 · 从分区到 Task

本章是逐章递进的 **mini-Spark 快照**模块，可独立编译运行。

## 运行

在仓库根目录执行：

```bash
mvn -q -pl ch05-multithread-task package
java -Dfile.encoding=UTF-8 \
  -cp ch05-multithread-task/target/classes \
  com.sparklearn.Main
```

`package` 会同时运行多分区、Task 和并行 action 测试。

## 对应正文

- `ListRDD.java`：支持把内存 List 均匀切成多个分区。
- `Task.java`：封装“计算一个 RDD 的一个分区”。
- `TaskScheduler.java`：用固定线程池并行提交所有分区 Task，并合并结果。
- `RDD.java`：保留串行 `collect`、`count`、`reduce`，用于和并行版对照。
- `Main.java`：演示多分区、并行 collect/count/reduce，以及 Task 无共享状态。
