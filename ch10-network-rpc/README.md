# 第 10 章 · 从单机到分布式执行

本章从第 9 章的本地线程池调度出发，新增可并发执行、可跨节点拉取
shuffle block 的 Socket Executor。

核心变化：

- `TaskScheduler` 抽象出任务提交接口。
- `LocalTaskScheduler` 继续使用本地线程池。
- `NetworkTaskScheduler` 并发把一批 Task 序列化后发送到多个 Executor JVM。
- Executor 用 core slots 限制 Task 并发度，同时独立处理 shuffle fetch 请求。
- `MapOutputStatus` 在 Driver 记录每个 Map 输出所在的 Executor 和 block 大小。
- Reduce Task 从远端 Executor 拉取 shuffle block，不依赖共享文件系统。
- `Task` / `ResultTask` / `ShuffleMapTask` 对齐 Spark 源码里的任务层级。
- `Task`、`RDD` 血缘、依赖和用户闭包都升级为可序列化对象。
- `preferredLocations` 开始参与 Executor 选择。

运行：

```bash
mvn -pl ch09-network-rpc package
java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Executor 9091 localhost 2
java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Executor 9092 localhost 2
```

另开一个终端：

```bash
java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Main network localhost:9091 localhost:9092
```

本地线程池对比：

```bash
java -Dfile.encoding=UTF-8 -cp ch09-network-rpc/target/classes com.sparklearn.Main
```
