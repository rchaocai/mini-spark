# 第 12 章 · 致敬工业级 Spark

本章从第 11 章 Streaming 之后回看完整 mini-Spark 快照，并对照真实 Spark 源码，不再新增计算功能，
而是把前 10 章的关键类和真实 Apache Spark 源码逐行对照。

核心内容：

- 保留第 10 章全部可运行代码（cache / checkpoint / Stage / 网络调度）。
- 新增 `SparkSourceMapDemo`，打印 16 项「我们的类 ↔ 真实 Spark 文件」。
- `Main` 先打印对照地图，再跑一段 `map → reduceByKey → collect` pipeline。
- 书稿侧强调祛魅：核心架构一致，多出来的是工程化外衣。

运行：

```bash
mvn -pl ch12-real-spark package
java -Dfile.encoding=UTF-8 -cp ch12-real-spark/target/classes com.sparklearn.Main
```

运行测试：

```bash
mvn -pl ch12-real-spark test
```

更完整的对照表见仓库根目录 `reference-notes/spark-source-map.md`。
