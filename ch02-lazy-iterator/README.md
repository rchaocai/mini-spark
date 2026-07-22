# 第 2 章 · 数据流与延迟迭代

本章是逐章递进的代码快照模块，可独立编译运行。

## 运行

在仓库根目录执行：

```bash
mvn -q -f ch02-lazy-iterator/pom.xml package
java -Dfile.encoding=UTF-8 -cp ch02-lazy-iterator/target/classes com.sparklearn.IteratorExamples
java -Dfile.encoding=UTF-8 -cp ch02-lazy-iterator/target/classes com.sparklearn.Main
```

## 对应正文

- `IteratorExamples.java`：对应正文 2.1 节的 `ArrayList`、`Iterator`、无限数字流示例。
- `Deferred.java` 与 `Main.java`：对应正文 2.2 节的延迟计算示例。
- `RDD.java`、`ListRDD.java` 与 `Main.java`：对应正文 2.3、2.4 节的 RDD 骨架和 `compute()` 示例。
