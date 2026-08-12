# 第 13 章 · 从 RDD 到 DataFrame

本章在前 12 章的 RDD 执行底座之上，加一层最小 DataFrame / SQL 查询链路：

```text
SQL / DataFrame API
  -> LogicalPlan
  -> Catalyst 风格规则优化
  -> PhysicalPlan
  -> RDD transformation
  -> Stage / Task / Shuffle
```

核心内容：

- `Row` / `Schema`：让一批普通 `RDD<Row>` 具备列名和类型信息。
- `Expression`：把 `salary > 50000`、`salary * 1.25` 这类行内计算表示成可分析的树。
- `LogicalPlan`：用 `Scan`、`Filter`、`Project`、`Aggregate` 描述查询意图。
- `Optimizer`：通过 `CombineFilters`、`PushFilterIntoScan`、`PruneScanColumns` 改写逻辑计划。
- `PhysicalPlan`：用 `ScanExec`、`ProjectExec`、`HashAggregateExec` 把查询翻译回 RDD。
- `SqlParser`：把教学子集 SQL 解析成同一套逻辑计划，让 SQL 和 DataFrame API 汇到同一条链路。

运行：

```bash
mvn -pl ch12-dataframe-future package
java -Dfile.encoding=UTF-8 -cp ch12-dataframe-future/target/classes com.sparklearn.sql.Main
```

运行测试：

```bash
mvn -pl ch12-dataframe-future test
```

`Main` 会展示三条线：

```text
过滤 + 投影       -> 谓词下推、列裁剪，最后落到 RDD.filter/map
groupBy().count() -> Aggregate / HashAggregateExec，最后落到 RDD.reduceByKey 和 Shuffle
SQL 字符串        -> SqlParser 解析成 LogicalPlan，之后复用同一套优化和执行链路
```

看输出时重点盯住 `explainString()` 打印的三棵树：

```text
Logical Plan            原始查询意图
Optimized Logical Plan  规则改写后的查询意图
Physical Plan           准备在 mini-spark 上执行的节点
```

第 13 章会把前 12 章搭出的执行底座与 Apache Spark 源码并排对照，说明这套教学实现和工业级 Spark 在运行行为上的对应关系。
