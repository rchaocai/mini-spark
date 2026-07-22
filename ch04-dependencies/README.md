# 第 4 章 · 血缘与依赖

本章是逐章递进的代码快照模块，可独立编译运行。

## 运行

在仓库根目录执行：

```bash
mvn -q -pl ch04-dependencies package
java -Dfile.encoding=UTF-8 \
  -cp ch04-dependencies/target/classes \
  com.sparklearn.Main
```

`package` 会同时运行血缘、窄依赖和 action 测试。

## 对应正文

- `Partition.java`：把“第几块数据”落成正式类型。
- `Dependency.java`、`NarrowDependency.java`、`OneToOneDependency.java`、`ShuffleDependency.java`：描述 RDD 之间的依赖关系。
- `RDD.java`：提供 `partitions`、`compute(Partition)`、`dependencies` 和带缓存钩子位置的 `iterator(Partition)`。
- `ListRDD.java`：作为源头 RDD，只有一个分区且没有依赖。
- `MapPartitionsRDD.java`：继承父 RDD 的分区结构，并声明一条 `OneToOneDependency`。
- `Main.java`：打印血缘链，验证窄依赖，并演示 `collect`、`count`、`reduce`。
