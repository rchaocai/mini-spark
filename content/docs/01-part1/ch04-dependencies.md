---
title: "第 4 章 · 血缘与依赖"
weight: 4
date: 2026-07-16
tags: ["RDD", "Partition", "Dependency", "Lineage", "窄依赖"]
summary: "给 RDD 装上正式骨架：分区、依赖和按分区计算。把第 3 章里无名的 parent 字段命名为 Dependency，并通过 OneToOneDependency 看清血缘链如何只记录配方、不保存数据。"
---

# 第 4 章 · 血缘与依赖

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies)
>
> 构建运行：`mvn -pl ch04-dependencies package && java -cp ch04-dependencies/target/classes com.sparklearn.Main`

到上一章为止，我们已经有了能跑通惰性流水线的几个零件：

```java
new ListRDD<>(data)
        .map(...)
        .filter(...)
        .flatMap(...)
        .collect();
```

`MapPartitionsRDD` 会保存父 RDD，并在 `collect()` 触发时把父迭代器包成新的迭代器。程序已经能运行，但如果回头看它的字段，会发现一件重要的事还没有名字：

```java
private final RDD<T> parent;
```

这个 `parent` 串起了两层 RDD。上一章我们只是把它当作一个普通字段使用，还没有正式回答：

> 一个 RDD 和它的父 RDD 到底是什么关系？

这一章，我们给这段关系命名：**Dependency（依赖）**。有了依赖，沿着依赖一路往回走，就得到一条完整的血缘（Lineage）链。

同时，第 1 章埋下的“分块计算”也要正式进入代码。`compute()` 不再只是“给我整条数据流”，而是变成“请计算第几个分区”。

## 4.1 把“第几块”落成 Partition

第 1 章里，我们用 WordCount 看到了两个朴素事实：数据太大时，必须切块；想并行时，也必须按块分工。

前 3 章为了把注意力放在迭代器和惰性流水线上，暂时把“分块”藏了起来。现在要把它补回来。代码只有几行：

```java
public record Partition(int index) implements Serializable {
}
```

完整实现见 [`Partition.java`](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies/src/main/java/com/sparklearn/Partition.java)。

这里要把第 1 章的直觉和本章的代码表示区分开。第 1 章说的“分区”是一块被切出来的数据；到了代码里，`Partition` 先只表示这块数据的身份，也就是“第几块”。真正的数据在哪里、怎么读，以后会挂到 RDD 和调度层上。

所以，`Partition` 只是一个带编号的值。它不保存数据，也不负责计算，只告诉外界：

```text
我是第 0 块
我是第 1 块
我是第 2 块
```

在当前章节里，`ListRDD` 仍然只有一个分区：

```java
this.partitions = List.of(new Partition(0));
```

这看起来像“还没有真正分布式”。没错。本章的目标不是马上把数据拆成多份，而是先把 Spark 最核心的接口形状立起来：**RDD 的计算必须以分区为单位发生。**

因此，`RDD.compute()` 的签名从上一章的：

```java
public abstract Iterator<T> compute();
```

变成：

```java
public abstract Iterator<T> compute(Partition partition);
```

这个变化很小，但语义完全不同。以前是“算这个 RDD”。现在是“算这个 RDD 的某个分区”。

## 4.2 把 parent 命名为 Dependency

再看第 3 章的 `MapPartitionsRDD`：

```java
private final RDD<T> parent;
```

`parent` 不是随便存的引用。它表达的是：“当前 RDD 是从这个父 RDD 变换来的。”

Spark 给这种关系取名为 `Dependency`。我们用 Java 17 的 `sealed interface` 把它写出来：

```java
public sealed interface Dependency<T> permits NarrowDependency, ShuffleDependency {
    RDD<T> rdd();
}
```

完整实现见 [`Dependency.java`](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies/src/main/java/com/sparklearn/Dependency.java)。

如果你还没用过 `sealed`，先不用被这个语法绊住。它的意思只是：`Dependency` 只允许下面列出的几种子类型。把它暂时看成普通 `interface`，也不影响理解本章主线。

这里有两个重点。

第一，`rdd()` 返回这条依赖指向的父 RDD。也就是说，只要拿到一个 `Dependency`，就能继续往上游走。

第二，`permits NarrowDependency, ShuffleDependency` 表示依赖只有两大类：

| 依赖类型 | 含义 | 本章状态 |
|---|---|---|
| `NarrowDependency` | 子分区只依赖父 RDD 的有限几个分区 | 正式实现 |
| `ShuffleDependency` | 子分区可能依赖父 RDD 的许多分区，需要 shuffle | 先占位 |

为什么要把这两类依赖在代码结构上写清楚？因为后面第 7 章划分 Stage 时，核心判断就是：

> 遇到宽依赖就切开，窄依赖可以继续放在同一个 Stage 里。

现在还不到实现 shuffle 的时候，所以 [`ShuffleDependency.java`](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies/src/main/java/com/sparklearn/ShuffleDependency.java) 只是一个占位壳。它存在的意义，是让“依赖分窄和宽”这件事先在代码里明确下来。

后面的路线是这样的：第 5 章先把“遍历分区”改成提交 Task；第 6 章实现 shuffle 读写；第 7 章再利用窄依赖和宽依赖来切分 Stage。

## 4.3 窄依赖：子分区 i 依赖父分区 i

`map`、`filter`、`flatMap` 都不会改变分区结构。父 RDD 有几个分区，子 RDD 也有几个分区；子分区 0 只需要读父分区 0，子分区 1 只需要读父分区 1。

这种关系叫**一对一窄依赖**：

```java
public final class OneToOneDependency<T> extends NarrowDependency<T> {

    public OneToOneDependency(RDD<T> rdd) {
        super(rdd);
    }

    @Override
    public List<Integer> getParents(int outputPartition) {
        return List.of(outputPartition);
    }
}
```

完整实现见 [`OneToOneDependency.java`](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies/src/main/java/com/sparklearn/OneToOneDependency.java)。

`getParents(0)` 返回 `[0]`，意思是：

```text
当前 RDD 的第 0 个分区
  依赖父 RDD 的第 0 个分区
```

如果将来有第 5 个分区，`getParents(5)` 也会返回 `[5]`，意思是：

```text
当前 RDD 的第 5 个分区
  依赖父 RDD 的第 5 个分区
```

这就是“一对一”。没有跨分区搬数据，也不需要等待所有父分区都准备好。将来真正并行时，这种关系可以在同一个任务里一路向上计算。

对应的抽象父类是 `NarrowDependency`：

```java
public abstract non-sealed class NarrowDependency<T> implements Dependency<T> {
    private final RDD<T> rdd;

    protected NarrowDependency(RDD<T> rdd) {
        this.rdd = rdd;
    }

    @Override
    public RDD<T> rdd() {
        return rdd;
    }

    public abstract List<Integer> getParents(int outputPartition);
}
```

完整实现见 [`NarrowDependency.java`](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies/src/main/java/com/sparklearn/NarrowDependency.java)。

`NarrowDependency` 负责保存父 RDD，具体“子分区依赖哪些父分区”交给子类回答。`OneToOneDependency` 的回答最简单：同号分区一一对应。

## 4.4 RDD 的正式骨架

有了 `Partition` 和 `Dependency`，`RDD` 就可以从“能返回一个迭代器”升级成更完整的抽象。

一个 RDD 至少要回答三个问题：

| 问题 | 方法 |
|---|---|
| 你有几个分区？ | `partitions()` |
| 某个分区怎么算？ | `compute(Partition partition)` |
| 你依赖哪些父 RDD？ | `dependencies()` |

代码如下：

```java
public abstract class RDD<T> {
    public abstract List<Partition> partitions();

    public abstract Iterator<T> compute(Partition partition);

    public abstract List<Dependency<?>> dependencies();

    public final Iterator<T> iterator(Partition partition) {
        return compute(partition);
    }
}
```

完整实现见 [`RDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies/src/main/java/com/sparklearn/RDD.java)。

这里多了一个看起来有点绕的 `iterator(partition)`。它现在只是直接调用 `compute(partition)`，好像没什么用。但这个方法会在第 10 章变得关键：缓存逻辑会插在这里。

概念上，它会变成：

```text
iterator(partition):
    如果这个分区已经缓存，就读缓存
    否则调用 compute(partition) 重新计算
```

所以本章先把钩子留好。当前版本仍然只有最朴素的一行：

```java
return compute(partition);
```

这也说明了一个重要边界：`compute` 是“如何计算这个分区”；`iterator` 是“外部读取这个分区的统一入口”。现在两者相同，后面会逐渐分开。

> [!INFO]
> **真实 Spark 里 RDD 还回答哪些问题？**
>
> 一个 RDD 的核心信息可以概括成五个问题：有哪些分区、依赖谁、每个分区怎么算、key-value 数据按什么规则分区、数据更适合在哪些机器上计算。本章先收敛前三个。`partitioner` 会在第 6 章 shuffle 时出现，`preferredLocations` 会在第 9 章跨进程执行任务时出现。

## 4.5 ListRDD 和 MapPartitionsRDD 归位

`RDD` 抽象变了，两个具体 RDD 也要归位。

先看 `ListRDD`。它是数据源头，所以有两个特点：

1. 当前只有一个分区：`Partition(0)`。
2. 没有父 RDD，所以 `dependencies()` 返回空列表。

```java
public final class ListRDD<T> extends RDD<T> {
    private final Supplier<Iterator<T>> supplier;
    private final List<Partition> partitions;

    public ListRDD(List<T> data) {
        this.supplier = data::iterator;
        this.partitions = List.of(new Partition(0));
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    @Override
    public Iterator<T> compute(Partition partition) {
        if (partition.index() != 0) {
            throw new IllegalArgumentException("unknown partition: " + partition);
        }
        return supplier.get();
    }

    @Override
    public List<Dependency<?>> dependencies() {
        return List.of();
    }
}
```

完整实现见 [`ListRDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies/src/main/java/com/sparklearn/ListRDD.java)。

再看 `MapPartitionsRDD`。它是从父 RDD 变换来的，所以三个问题的答案分别是：

| 问题 | 答案 |
|---|---|
| 有哪些分区？ | 和父 RDD 一样 |
| 某个分区怎么算？ | 先读父 RDD 的同号分区，再包装迭代器 |
| 依赖谁？ | 依赖父 RDD，且是一对一窄依赖 |

```java
public final class MapPartitionsRDD<T, U> extends RDD<U> {
    private final RDD<T> parent;
    private final Function<Iterator<T>, Iterator<U>> iteratorTransform;
    private final List<Partition> partitions;
    private final List<Dependency<?>> dependencies;

    public MapPartitionsRDD(
            RDD<T> parent,
            Function<Iterator<T>, Iterator<U>> iteratorTransform) {
        this.parent = parent;
        this.iteratorTransform = iteratorTransform;
        this.partitions = parent.partitions();
        this.dependencies = List.of(new OneToOneDependency<>(parent));
    }

    @Override
    public List<Partition> partitions() {
        return partitions;
    }

    @Override
    public Iterator<U> compute(Partition partition) {
        Iterator<T> parentIterator = parent.iterator(partition);
        return iteratorTransform.apply(parentIterator);
    }

    @Override
    public List<Dependency<?>> dependencies() {
        return dependencies;
    }
}
```

完整实现见 [`MapPartitionsRDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch04-dependencies/src/main/java/com/sparklearn/MapPartitionsRDD.java)。

第 3 章里的 `parent` 字段还在，但它不再是一个无名关系。现在 `dependencies()` 会把它正式暴露成：

```java
new OneToOneDependency<>(parent)
```

这意味着任何人拿到一个 `MapPartitionsRDD`，都能问它：“你是从谁来的？”然后继续沿着父 RDD 往上走。

## 4.6 血缘：只保存配方，不保存数据

现在构造一条流水线：

```java
RDD<Integer> rdd = new ListRDD<>(Arrays.asList(1, 2, 3, 4, 5))
        .map(number -> number + 1)
        .filter(number -> number % 2 == 0)
        .map(number -> number * 10);
```

从最外层开始，沿着 `dependencies()` 往回走，会得到这样的结构：

```text
MapPartitionsRDD
  依赖: OneToOneDependency
    MapPartitionsRDD
      依赖: OneToOneDependency
        MapPartitionsRDD
          依赖: OneToOneDependency
            ListRDD  <- 源头
```

这条链就是血缘。

更准确地说，血缘不保存 `map`、`filter` 这些 transformation 产生的中间结果。源头 RDD 仍然会保存或引用自己的读取入口，比如本章的 `ListRDD` 持有能创建原始迭代器的 `Supplier`。血缘链上保存的是：

```text
当前 RDD 依赖哪个父 RDD
当前 RDD 如何从父 RDD 的分区迭代器算出来
```

换句话说，血缘不是“结果表”，而是“配方链”。

这正好接上第 3 章最后提到的粗粒度变换。`map` 记录的不是“第 1 条记录改成什么、第 2 条记录改成什么”，而是“这个分区里的每个元素都应用同一个函数”。因为变换是粗粒度的，血缘才足够轻：保存一次函数定义，就能描述整个分区的重算方式。

> [!WARNING]
> **本章还不做容错重算**
>
> 血缘让“数据可以重算”成为可能，但本章只负责把血缘链记录出来。真正什么时候发现分区丢了、如何沿血缘重新计算，会放到第 8 章。

我们可以在代码里打印这条链。`Main.java` 里有一个递归方法：

```java
private static void printLineage(RDD<?> rdd, String indent) {
    List<Dependency<?>> dependencies = rdd.dependencies();
    if (dependencies.isEmpty()) {
        System.out.println(indent + rdd.getClass().getSimpleName() + "  <- 源头");
        return;
    }

    System.out.println(indent + rdd.getClass().getSimpleName());
    for (Dependency<?> dependency : dependencies) {
        System.out.println(indent + "  依赖: " + dependency.getClass().getSimpleName());
        printLineage(dependency.rdd(), indent + "    ");
    }
}
```

注意，这段打印血缘的代码没有调用 `collect()`，也没有读取任何真实元素。它只是在 RDD 对象之间沿着依赖关系回溯。

## 4.7 Action 变成“遍历所有分区”

第 3 章的 `collect()` 只处理一条迭代器：

```java
Iterator<T> iterator = compute();
while (iterator.hasNext()) {
    result.add(iterator.next());
}
```

现在有了 `Partition`，action 的形状要变成：

```text
遍历所有分区
  读取当前分区的迭代器
  消费这个迭代器
  把分区结果合并到最终结果
```

`collect()` 的实现如下：

```java
public List<T> collect() {
    List<T> result = new ArrayList<>();
    for (Partition partition : partitions()) {
        Iterator<T> iterator = iterator(partition);
        while (iterator.hasNext()) {
            T element = iterator.next();
            result.add(element);
        }
    }
    return result;
}
```

`count()` 也是同一个骨架，只是不保存元素，每读到一个元素就加一：

```java
public long count() {
    long count = 0;
    for (Partition partition : partitions()) {
        Iterator<T> iterator = iterator(partition);
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
    }
    return count;
}
```

`reduce()` 把所有元素用一个二元函数归并起来：

```java
public T reduce(BinaryOperator<T> operator) {
    T result = null;
    boolean hasResult = false;
    for (Partition partition : partitions()) {
        Iterator<T> iterator = iterator(partition);
        while (iterator.hasNext()) {
            if (!hasResult) {
                result = iterator.next();
                hasResult = true;
            } else {
                result = operator.apply(result, iterator.next());
            }
        }
    }

    if (!hasResult) {
        throw new NoSuchElementException("reduce on empty RDD");
    }
    return result;
}
```

这里的 `for (Partition partition : partitions())` 是下一章的入口。现在它只是单线程循环；下一章把“遍历分区”改成“为每个分区提交一个任务”，并行计算就会自然出现。

## 4.8 本章小结

这一章，我们把前 3 章的零件装成了更正式的 RDD 骨架。

新增的核心概念有四个：

1. **`Partition`**：把“第几块数据”变成代码里的值。
2. **`Dependency`**：把 `parent` 字段命名为 RDD 之间的依赖关系。
3. **`OneToOneDependency`**：描述 `map`、`filter`、`flatMap` 这种不改变分区结构的窄依赖。
4. **血缘（Lineage）**：沿 `dependencies()` 从当前 RDD 回溯到源头得到的配方链。

`RDD` 也从一个只有 `compute()` 的抽象，扩展成了三个必须回答的问题：有哪些分区、每个分区怎么算、依赖哪些父 RDD。`collect()`、`count()`、`reduce()` 则统一成了同一个 action 模式：遍历所有分区，消费每个分区的迭代器，再归并结果。

到这里，第一部分的单机 RDD 内核已经成形。但它还没有真正并行。下一章，我们会把“遍历分区”改造成“提交多个 Task”，让每个分区可以由不同线程同时计算。
