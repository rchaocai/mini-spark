# 第 3 章 · MapPartitionsRDD 与惰性流水线

本章是逐章递进的 **mini-Spark 快照**模块，可独立编译运行。

## 运行

在仓库根目录执行：

```bash
mvn -q -pl ch03-mappartitions-pipeline package
java -Dfile.encoding=UTF-8 \
  -cp ch03-mappartitions-pipeline/target/classes \
  com.sparklearn.Main
```

`package` 会同时运行 Iterator 契约和惰性流水线测试。

## 对应正文

- `MappingIterator.java`：在 `next()` 中应用函数，实现边读取边变换。
- `MapPartitionsRDD.java`：把父 RDD 的迭代器包装成新的迭代器。
- `FilteringIterator.java` 与 `FlatMappingIterator.java`：实现基础的过滤和一对多展开。
- `RDD.java`：提供 `map`、`filter`、`flatMap` 和第一个 action：`collect`。
- `Main.java`：验证 transformation 的惰性，以及元素逐个穿透多层算子的执行顺序。
