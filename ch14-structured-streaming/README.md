# 第 12 章 · 从 RDD 到 DataFrame

## 前言

前面 11 章，我们从零实现了 RDD 内核，包括分区、惰性迭代器、Shuffle、DAG 调度、容错等核心功能。

但是有个问题——直接用 RDD 写业务代码，实在太麻烦了。

比如我们要查"薪水超过 50000 的员工"，用 RDD 得这么写：

```java
rdd.filter(row -> ((Number) row.get("salary")).doubleValue() > 50000)
   .map(row -> new Pair(row.get("name"), row.get("salary")));
```

这段代码充满了类型转换、重复的 `get()` 调用，可读性差，还容易出错。

能不能让代码更简洁、更安全？当然可以——这就是 DataFrame 要解决的问题。

在这一章，我们会一起实现一个简化版的 DataFrame 引擎，理解它背后的核心思想。

---

## 第一步：让数据"结构化"起来

要让系统理解我们的数据，首先得告诉它数据的结构。

### 1.1 什么是 Schema？

假设我们有员工数据：

```
1, Alice, eng, 72000
2, Bob, ops, 45000
3, Cathy, eng, 83000
```

每条记录有四个字段：`id`、`name`、`department`、`salary`。

我们需要一个"说明书"来描述这个结构——这就是 **Schema**：

```
Schema:
┌──────┬──────┬──────────┬────────┐
│ id   │ name │ department │ salary │
│ int  │ str  │ str       │ int    │
└──────┴──────┴──────────┴────────┘
```

有了 Schema，系统就知道：
- 第一列叫 `id`，是整数
- 第二列叫 `name`，是字符串
- ...

### 1.2 什么是 Row？

Row 就是一行数据。在 Spark 中，Row 是一个**纯值序列**，它本身不知道列名——列名由 Schema 来描述。

比如 `Row(1, "Alice", "eng", 72000)` 这一行，配合上面的 Schema，系统就知道：
- 第 0 个位置的值 `1` 对应 `id` 列
- 第 1 个位置的值 `"Alice"` 对应 `name` 列

这种设计的好处是：
1. **存储紧凑**：用数组存储，比 Map 省空间
2. **访问高效**：按位置访问，O(1) 时间复杂度
3. **类型安全**：结合 Schema 可以做类型检查

---

## 第二步：写一个简单的查询

现在，让我们用 DataFrame API 写一个查询：

```java
df.where(col("salary").gt(50000))
  .select(col("name"), col("salary").multiply(1.25).as("adjusted_salary"));
```

这段代码看起来很简洁，但背后发生了什么？

### 2.1 表达式树

`col("salary").gt(50000)` 这个调用，并没有立即执行过滤操作。它创建了一个**表达式树**：

```
GreaterThan
├── Attribute("salary")
└── Literal(50000)
```

同样，`col("salary").multiply(1.25).as("adjusted_salary")` 创建了：

```
Alias(name="adjusted_salary")
└── Multiply
    ├── Attribute("salary")
    └── Literal(1.25)
```

表达式树是 DataFrame 的核心——它让系统"看懂"你要做什么。

### 2.2 逻辑计划

当你链式调用 `where()` 和 `select()` 时，DataFrame 会构建一棵**逻辑计划树**：

```
Project(name, salary * 1.25 AS adjusted_salary)
  └── Filter(salary > 50000)
      └── Scan(employees, columns=[id, name, department, salary])
```

这棵树描述了查询的步骤：
1. 从数据源扫描所有列（Scan）
2. 过滤薪水大于 50000 的行（Filter）
3. 选择 name 列，并计算调整后薪水（Project）

### 2.3 惰性求值

关键在于——这棵树只是一个**描述**，不会立即执行。

只有当你调用 `collect()` 或 `show()` 时，系统才会真正开始计算。这种设计叫**惰性求值**，它给了系统优化的机会。

---

## 第三步：让查询跑得更快——Catalyst 优化器

现在，假设你是 Spark 的开发者，拿到了上面的逻辑计划，你会怎么优化它？

### 3.1 优化一：谓词下推

原始计划中，Filter 在 Scan 之后。这意味着：
1. 先读取所有数据到内存
2. 再在内存中过滤

如果把 Filter 推进到 Scan 里面呢？这样数据源读取时就直接过滤，只读取符合条件的数据。

**优化后**：
```
Project(name, salary * 1.25 AS adjusted_salary)
  └── Scan(employees, columns=[id, name, department, salary], pushedFilters=[salary > 50000])
```

收益很明显：减少数据读取量 → 减少内存占用 → 查询更快。

### 3.2 优化二：列裁剪

原始计划中，Scan 读取了所有 4 列。但后续操作只用到了 `name` 和 `salary`，其他两列完全是浪费。

让 Scan 只读取需要的列：

**优化后**：
```
Project(name, salary * 1.25 AS adjusted_salary)
  └── Scan(employees, columns=[name, salary], pushedFilters=[salary > 50000])
```

收益：减少数据传输 → 减少内存占用 → 查询更快。

### 3.3 优化器是怎么工作的？

Catalyst 优化器的工作原理很简单：

1. 定义一批优化规则（比如"谓词下推"、"列裁剪"）
2. 对逻辑计划树应用这些规则
3. 如果计划发生了变化，就重复这个过程
4. 直到计划不再变化（fixed point）

这个过程就像给计划树"做按摩"，让它变得更高效。

---

## 第四步：把逻辑计划变成物理执行

优化后的逻辑计划，最终要翻译成具体的执行操作。

### 4.1 物理计划

逻辑计划 `Scan` 会被翻译成物理计划 `ScanExec`，`Project` 会被翻译成 `ProjectExec`：

```
ProjectExec(name, salary * 1.25 AS adjusted_salary)
  └── ScanExec(employees, columns=[name, salary], pushedFilters=[salary > 50000])
```

### 4.2 落回 RDD

物理计划的执行，最终就是调用我们之前实现的 RDD 操作。

比如 `ScanExec` 会创建一个 `ListRDD`，`ProjectExec` 会调用 `map()`，`HashAggregateExec` 会调用 `reduceByKey()`。

看到了吗？DataFrame 并没有取代 RDD，而是在 RDD 之上加了一层"智能"。

---

## 第五步：代码阅读指南

现在，让我们一起看看代码是怎么实现这些功能的。

### 5.1 先看入口：Main.java

打开 `Main.java`，看看 DataFrame API 是怎么用的：

```java
DataFrame adjustedSalary = employees
        .where(col("salary").gt(50_000))
        .select(col("name"), col("salary").multiply(1.25).as("adjusted_salary"));

System.out.println(adjustedSalary.explainString());
```

注意 `explainString()` 方法，它会打印出逻辑计划和物理计划。运行代码后，你可以看到优化前后的变化。

### 5.2 理解数据模型：Row.java + Schema.java

- `Row.java`：一行数据，内部用 `Object[]` 存储值
- `Schema.java`：描述数据结构，包含字段名和类型

这两个类是整个 DataFrame 的基础。

### 5.3 理解惰性计划：DataFrame.java

DataFrame 类很简单，它就是逻辑计划的包装：

```java
public final class DataFrame {
    private final LogicalPlan logicalPlan;
    
    public DataFrame where(Expression condition) {
        return new DataFrame(new Filter(condition, logicalPlan));
    }
    
    public DataFrame select(NamedExpression... expressions) {
        return new DataFrame(new Project(List.of(expressions), logicalPlan));
    }
}
```

每个操作都返回一个新的 DataFrame，这就是惰性求值的实现方式。

### 5.4 理解表达式：catalyst/expressions/

这里定义了所有的表达式类型：
- `Expression`：表达式接口
- `Attribute`：列引用（比如 `col("salary")`）
- `Literal`：常量（比如 `50000`）
- `GreaterThan`、`Multiply`：操作表达式
- `Alias`：给表达式起名字

### 5.5 理解逻辑计划：catalyst/plans/logical/

这里定义了逻辑计划的节点：
- `Scan`：数据源扫描
- `Filter`：过滤
- `Project`：投影（选择列）
- `Aggregate`：聚合

每个节点都有 `schema()` 方法，返回该节点输出的 Schema。

### 5.6 理解优化器：catalyst/optimizer/

这里定义了优化规则：
- `PushFilterIntoScan`：谓词下推
- `PruneScanColumns`：列裁剪
- `CombineFilters`：合并相邻 Filter

规则的实现很简单，就是检查计划的结构，然后返回优化后的计划。

### 5.7 理解物理执行：execution/

这里定义了物理执行器：
- `ScanExec`：实际读取数据
- `FilterExec`：执行过滤
- `ProjectExec`：执行投影
- `HashAggregateExec`：执行聚合

每个执行器都有 `execute()` 方法，返回一个 RDD。

---

## 运行代码

```bash
mvn -pl ch12-dataframe-future package
java -Dfile.encoding=UTF-8 -cp ch12-dataframe-future/target/classes com.sparklearn.sql.Main
```

运行后，你会看到：

1. **原始逻辑计划**：未经优化的查询树
2. **优化后逻辑计划**：优化器处理后的查询树（注意 Scan 的 columns 和 pushedFilters）
3. **物理计划**：最终要执行的 RDD 操作
4. **执行结果**：查询的最终输出

试着修改 `Main.java` 中的查询，看看优化器会怎么处理不同的情况。

---

## 小结

在这一章，我们一起实现了一个简化版的 DataFrame 引擎。核心思想是：

1. **Schema + Row**：让数据结构化，系统能理解列名和类型
2. **表达式树**：让系统"看懂"查询条件
3. **逻辑计划**：用树结构描述查询步骤
4. **惰性求值**：给系统优化的机会
5. **Catalyst 优化器**：自动优化查询计划
6. **物理执行**：最终翻译成 RDD 操作

DataFrame 不是取代 RDD，而是在 RDD 之上加了一层"智能"——让你用更简洁、更安全的方式写代码，同时享受自动优化带来的性能提升。

下一章，我们会在此基础上实现 Structured Streaming。