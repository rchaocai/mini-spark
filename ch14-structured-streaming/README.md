# 第 12 章 · 从 RDD 到 DataFrame

## 1. 从一个具体问题开始

假设我们有一批员工数据，每条记录包含 `id`、`name`、`department`、`salary` 四个字段。

### 问题：查出薪水超过 50000 的员工姓名，并计算调整后薪水（涨薪 25%）

#### 用纯 RDD 怎么写？

```java
rdd.filter(row -> ((Number) row.get("salary")).doubleValue() > 50000)
   .map(row -> {
       String name = (String) row.get("name");
       double salary = ((Number) row.get("salary")).doubleValue();
       double adjusted = salary * 1.25;
       return new Pair(name, adjusted);
   });
```

这段代码有什么问题？

1. **类型不安全**：每次都要强制类型转换，容易出错
2. **重复代码**：每个查询都要写类似的转换逻辑
3. **无法优化**：系统不知道你想做什么，只能按你的指令执行
4. **可读性差**：代码里充满了 `get()`、强制转换，业务逻辑被淹没

#### 用 DataFrame 怎么写？

```java
df.where(col("salary").gt(50000))
  .select(col("name"), col("salary").multiply(1.25).as("adjusted_salary"));
```

差别一目了然。DataFrame 让你**声明式地描述"要什么"**，而不是命令式地告诉"怎么做"。

---

## 2. DataFrame 是什么？

DataFrame 就是**带 Schema 的惰性逻辑计划**。

让我们拆解这个定义：

### 2.1 什么是 Schema？

Schema 是数据的"说明书"，告诉系统每一列叫什么名字、是什么类型：

```
Schema:
┌──────┬──────┬──────────┬────────┐
│ id   │ name │ department │ salary │
│ int  │ str  │ str       │ int    │
└──────┴──────┴──────────┴────────┘
```

有了 Schema，系统就能理解你的数据结构。

### 2.2 什么是逻辑计划？

逻辑计划是一棵**描述查询操作的树**，比如上面的查询会生成这样的树：

```
Project(name, salary * 1.25 AS adjusted_salary)
  └── Filter(salary > 50000)
      └── Scan(employees, columns=[id, name, department, salary])
```

- `Scan`：从数据源读取数据
- `Filter`：过滤薪水大于 50000 的行
- `Project`：选择 name 列，并计算调整后薪水

### 2.3 为什么是"惰性"的？

逻辑计划只是一个**描述**，不会立即执行。只有当你调用 `collect()`、`show()` 等 action 操作时，系统才会：

1. 优化逻辑计划（Catalyst 优化器）
2. 转换为物理计划（翻译成 RDD 操作）
3. 真正执行计算

---

## 3. Catalyst 优化器：让查询跑得更快

Catalyst 是 Spark SQL 的核心优化引擎。它的工作原理很简单：

```
原始逻辑计划 → 优化规则 → 优化后逻辑计划 → 物理计划
```

### 3.1 优化规则演示

让我们看看优化器如何改进上面的查询：

**优化前**：
```
Project(name, salary * 1.25 AS adjusted_salary)
  └── Filter(salary > 50000)
      └── Scan(employees, columns=[id, name, department, salary])
```

**优化后**：
```
Project(name, salary * 1.25 AS adjusted_salary)
  └── Scan(employees, columns=[name, salary], pushedFilters=[salary > 50000])
```

发生了两件神奇的事：

#### 优化一：谓词下推（PushFilterIntoScan）

把 `Filter(salary > 50000)` 推进到 `Scan` 里面。这样数据源读取时就直接过滤，只读取符合条件的数据。

**收益**：减少数据读取量 → 减少内存占用 → 加快查询速度

#### 优化二：列裁剪（PruneScanColumns）

Scan 原本读取所有 4 列，但后续操作只用到 `name` 和 `salary`，所以优化器让 Scan 只读这两列。

**收益**：减少数据传输 → 减少内存占用 → 加快查询速度

### 3.2 优化器的工作方式

Catalyst 使用**规则批次 + fixed point** 的方式工作：

1. 把优化规则分成多个批次（Batch）
2. 对逻辑计划树应用所有规则
3. 如果计划发生变化，就重复这个过程
4. 直到计划不再变化（fixed point）或达到最大迭代次数

---

## 4. 表达式树：让系统"看懂"你的查询

DataFrame API 构建的不是直接的操作，而是**表达式树**。

比如 `col("salary").gt(50000)` 会生成：

```
GreaterThan
├── Attribute("salary")
└── Literal(50000)
```

而 `col("salary").multiply(1.25).as("adjusted_salary")` 会生成：

```
Alias(name="adjusted_salary")
└── Multiply
    ├── Attribute("salary")
    └── Literal(1.25)
```

表达式树有什么用？

- **类型推断**：系统可以推断每个表达式的返回类型
- **优化分析**：系统可以分析表达式的依赖关系，进行列裁剪等优化
- **代码生成**：最终可以把表达式树翻译成高效的执行代码

---

## 5. 物理执行：落回 RDD

优化后的逻辑计划最终会被翻译成物理计划，而物理计划的执行就是调用 RDD 的操作。

比如 `groupBy("department").count()` 会被翻译成：

```
HashAggregateExec(groupBy=[department], count(*))
  └── ScanExec(employees, columns=[department])
```

底层执行时，`HashAggregateExec` 会调用 `RDD.reduceByKey()`，触发 Shuffle，最终还是由我们之前实现的 DAGScheduler 来划分 Stage。

---

## 6. 代码阅读路径

建议按以下顺序阅读代码，层层递进理解 DataFrame 的实现：

### 第一步：理解数据模型

**Row.java + Schema.java**

- `Row`：一行数据，内部用 `Object[]` 存储值序列
- `Schema`：数据结构描述，包含列名和类型

### 第二步：理解用户 API

**Main.java**

- 看 DataFrame API 怎么用，理解用户视角
- 注意 `explainString()` 的输出，这是理解查询计划的关键

### 第三步：理解惰性逻辑计划

**DataFrame.java**

- 每个操作（`where`、`select`、`groupBy`）都返回新的 DataFrame
- DataFrame 只是逻辑计划的包装

### 第四步：理解表达式树

**catalyst/expressions/**

- `Expression`：表达式接口，所有表达式的基础
- `Attribute`：列引用，代表输入数据中的一列
- `Literal`：常量表达式
- `GreaterThan`、`EqualTo`、`Multiply`：操作表达式
- `Alias`：给表达式起名字

### 第五步：理解逻辑计划

**catalyst/plans/logical/**

- `LogicalPlan`：逻辑计划接口
- `Scan`：数据源扫描
- `Filter`：过滤操作
- `Project`：投影操作（选择列）
- `Aggregate`：聚合操作

### 第六步：理解优化器

**catalyst/optimizer/**

- `RuleExecutor`：规则执行器，负责应用优化规则
- `PushFilterIntoScan`：谓词下推规则
- `PruneScanColumns`：列裁剪规则
- `CombineFilters`：合并相邻 Filter

### 第七步：理解物理执行

**execution/**

- `PhysicalPlan`：物理计划接口
- `PhysicalPlanner`：把逻辑计划翻译成物理计划
- `ScanExec`、`FilterExec`、`ProjectExec`、`HashAggregateExec`：具体的物理执行器

---

## 7. 运行代码

```bash
mvn -pl ch12-dataframe-future package
java -Dfile.encoding=UTF-8 -cp ch12-dataframe-future/target/classes com.sparklearn.sql.Main
```

运行后你会看到：

1. **原始逻辑计划**：展示未经优化的查询树
2. **优化后逻辑计划**：展示优化器处理后的查询树（注意谓词下推和列裁剪的效果）
3. **物理计划**：展示最终要执行的 RDD 操作
4. **执行结果**：展示查询的最终输出

---

## 8. 关键设计思想回顾

| 设计思想 | 说明 | 在代码中的体现 |
|---------|------|--------------|
| **声明式 API** | 描述"要什么"，不是"怎么做" | `df.where(...)` 不立即执行 |
| **惰性求值** | 逻辑计划只描述，action 才执行 | `collect()` 触发执行 |
| **不可变树** | 逻辑计划是不可变的，优化时创建新节点 | `withNewChildren()` 返回新节点 |
| **规则优化** | 通过规则批次优化计划 | `RuleExecutor.execute()` |
| **类型推断** | 表达式知道自己的返回类型 | `Expression.dataType()` |
| **最终落回 RDD** | 所有操作最终翻译成 RDD | `PhysicalPlanner` 生成 RDD 操作 |

---

## 9. 运行测试

```bash
mvn -pl ch12-dataframe-future test
```

测试用例覆盖了核心场景：
- `filterAndProjectRunThroughOptimizedScan`：测试谓词下推和列裁剪
- `groupByCountFallsBackToReduceByKey`：测试聚合落回 RDD