# 第 13 章 · 致敬工业级 Spark

本章放在 DataFrame 之后收束全书：回到托住 Streaming 和 DataFrame 的 RDD 执行底座，
把教学版运行轨迹和 Apache Spark 源码并排对照，不再新增计算功能。

核心内容：

- 保留完整 RDD 执行内核（cache / checkpoint / Stage / 网络调度）。
- 新增 `SparkSourceMapDemo`，打印 16 项「教学版类 ↔ Apache Spark 文件」。
- `Main` 先打印对照地图，再跑一段 `map → reduceByKey → collect` pipeline。
- 书稿侧强调运行行为：Stage 划分、shuffle 交接、两层恢复和工业实现对得上。

运行：

```bash
mvn -pl ch13-real-spark package
java -Dfile.encoding=UTF-8 -cp ch13-real-spark/target/classes com.sparklearn.Main
```

运行测试：

```bash
mvn -pl ch13-real-spark test
```

更完整的对照表见仓库根目录 `reference-notes/spark-source-map.md`。
