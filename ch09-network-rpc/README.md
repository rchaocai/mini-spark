# 第 9 章 · 从单机到分布式执行

本章从第 8 章的本地线程池调度出发，新增 Socket Executor。

核心变化：

- `TaskScheduler` 抽象出任务提交接口。
- `LocalTaskScheduler` 继续使用本地线程池。
- `NetworkTaskScheduler` 把 Task 序列化后发送到 Executor JVM。
- `Task` / `ResultTask` / `ShuffleMapTask` 对齐 Spark 源码里的任务层级。
- `Task`、`RDD` 血缘、依赖和用户闭包都升级为可序列化对象。
- `preferredLocations` 开始参与 Executor 选择。

运行：

```bash
mvn -pl ch09-network-rpc package
java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Executor 9091
```

另开一个终端：

```bash
java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Main network localhost:9091
```

本地线程池对比：

```bash
java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Main
```
