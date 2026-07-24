# 第 12 章 · 从 RDD 到 DataFrame

本章在第 11 章拆开的 RDD 内核之上，加一层 Spark SQL / DataFrame。

## 为什么需要 DataFrame？

回顾前 11 章，我们用 RDD 实现了很多功能，但直接用 RDD 写业务代码有几个痛点：

```java
// 纯 RDD 方式：重复代码多、类型不安全、需要手动优化
rdd.filter(row -> ((Number) row.get("salary")).doubleValue() > 50000)
   .map(row -> new Pair(row.get("name"), row.get("salary")))
   .reduceByKey(...);

// DataFrame 方式：声明式、自动优化、类型安全
df.where(col("salary").gt(50000))
  .select(col("name"), col("salary"));
```

DataFrame 带来的核心价值：
- **声明式 API**：描述"要什么"，而不是"怎么做"
- **自动优化**：Catalyst 优化器自动做谓词下推、列裁剪等
- **类型安全**：编译时检查，避免运行时类型转换错误
- **统一接口**：DataFrame API 和 SQL 字符串生成相同的执行计划

## 包结构

```text
com.sparklearn.core.*                   # RDD / Stage / Shuffle / Task 执行内核
com.sparklearn.sql.*                    # SQLContext / DataFrame / Row / Schema
com.sparklearn.sql.catalyst.*           # 表达式、逻辑计划、规则优化器
com.sparklearn.sql.execution.*          # 物理计划，把逻辑计划落回 RDD
```

## 核心内容

- `DataFrame` = 带 schema 的惰性逻辑计划
- Catalyst = 不可变树 + 规则批次 + fixed point
- 谓词下推：`Filter` 进入数据源扫描（减少数据读取量）
- 列裁剪：只读后续真正用到的列（减少内存占用）
- `groupBy().count()` 最终落成 `RDD.reduceByKey()`，仍然由 DAGScheduler 切 Stage

## 推荐阅读顺序

建议按以下顺序阅读代码，层层递进理解 DataFrame 的实现：

1. **Row.java + Schema.java** - 理解数据模型：Row 是纯值序列，Schema 是外部描述
2. **Main.java** - 看 DataFrame API 怎么用，理解用户视角
3. **DataFrame.java** - 理解惰性逻辑计划，每个操作返回新的 DataFrame
4. **catalyst/expressions/** - 理解表达式树：Attribute、Literal、GreaterThan 等
5. **catalyst/plans/logical/** - 理解逻辑计划节点：Scan、Filter、Project、Aggregate
6. **catalyst/optimizer/** - 理解优化规则：谓词下推、列裁剪、Filter 合并
7. **execution/** - 理解物理执行：把逻辑计划翻译回 RDD 操作

## 运行

```bash
mvn -pl ch12-dataframe-future package
java -Dfile.encoding=UTF-8 -cp ch12-dataframe-future/target/classes com.sparklearn.sql.Main
```

## 运行测试

```bash
mvn -pl ch12-dataframe-future test
```