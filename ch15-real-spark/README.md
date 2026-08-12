# 第 15 章 · 致敬工业级 Spark

本章放在 Structured Streaming 之后收束全书：回到托住 Streaming 和 DataFrame 的 RDD 执行底座，
把前 14 章搭出的运行轨迹和 Apache Spark 源码并排对照，不再新增计算功能。

核心内容：

- 保留完整 RDD 执行内核（cache / checkpoint / Stage / 网络调度）。
- `Main` 跑一段 `map → reduceByKey → collect` pipeline，再演示两层失败恢复。
- 书稿侧强调运行行为：Stage 划分、shuffle 交接、两层恢复和工业实现对得上。

运行：

```bash
mvn -pl ch15-real-spark package
java -Dfile.encoding=UTF-8 -cp ch15-real-spark/target/classes com.sparklearn.Main
```

运行测试：

```bash
mvn -pl ch15-real-spark test
```

更完整的对照表见仓库根目录 `reference-notes/spark-source-map.md`。
