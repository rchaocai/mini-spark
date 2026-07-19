# 第 10 章 · Cache 与 Checkpoint

本章从第 9 章的网络执行代码继续往前走，在 `RDD.iterator(partition)`
这个统一入口上加入 cache 和 checkpoint。

核心变化：

- `cache()` 只设置标记，不立即计算分区。
- `iterator(partition)` 先读 checkpoint，再查 cache，最后才调用 `compute`。
- cache 命中时直接返回内存里的分区结果，上游血缘不会继续被遍历。
- `checkpoint()` 把所有分区写到本地临时目录，并让 `dependencies()` 返回空列表。
- `getComputeCount()` 只服务于本章 demo 和测试，用来观察重算成本。

运行：

```bash
mvn -pl ch10-cache-checkpoint package
java -Dfile.encoding=UTF-8 -cp ch10-cache-checkpoint/target/classes com.sparklearn.Main
```

运行测试：

```bash
mvn -pl ch10-cache-checkpoint test
```
