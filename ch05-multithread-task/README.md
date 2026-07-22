# 第 5 章 · 从分区到 Task

本章是逐章递进的代码快照模块，可独立编译运行。

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
- `CollectTask.java`：封装 `collect()` 对一个 RDD 分区的计算。
- `TaskScheduler.java`：用固定线程池并行提交所有分区任务，并合并结果。
- `RDD.java`：保留串行 `collect`、`count`、`reduce`，用于和并行版对照。
- `Main.java`：演示多分区、并行 collect/count/reduce，以及分区任务无共享状态。
