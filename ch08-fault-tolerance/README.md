# 第 8 章 · 容错：从 Task 重试到 Stage 恢复

本章是逐章递进的代码快照模块，可独立编译运行。

## 运行

在仓库根目录执行：

```bash
mvn -q -pl ch08-fault-tolerance package
java -Dfile.encoding=UTF-8 \
  -cp ch08-fault-tolerance/target/classes \
  com.sparklearn.Main
```

## 对应正文

- `FaultyIterator.java`：在指定的 `next()` 调用上抛出一次异常，模拟瞬态故障。
- `RDD.failOnNext(...)` / `FaultyRDD.java`：把 `FaultyIterator` 注入到指定分区，便于稳定演示重算。
- `TaskScheduler.java`：Task 失败后有限重试，重交同一个分区任务。
- `FetchFailedException.java`：把丢失的 Map/Reduce 文件坐标交给 DAGScheduler。
- `MissingMapOutputRDD.java`：删除一个已完成的 Map 输出，稳定演示 Fetch 失败。
- `DAGScheduler.java`：重算丢失输出对应的 Map 分区，再重新提交当前 Stage。
- `ShuffleMapTask.java` / `ResultTask.java`：保持无状态，重试时重新沿血缘创建迭代器链。
- `Main.java`：依次演示普通 Task 重试与 Shuffle Fetch 失败恢复。
