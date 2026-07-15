---
title: "第 2 章 · 数据流与延迟迭代"
weight: 2
date: 2026-07-15
tags: ["RDD", "Iterator", "惰性求值", "ListRDD"]
summary: '造出Spark的第一个抽象——RDD：它不复制整份数据，也不预先物化结果，只记录"怎么访问"。从Iterator、Supplier到RDD、ListRDD，一步步拼出RDD最核心的骨架。'
---

# 第 2 章 · 数据流与延迟迭代

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator)

上一章结尾，我们手里握着两件法宝——"分块"和"并行"。要让计算最终在分区上铺开，代码里首先需要一个对象来代表这批数据，并说明每次该怎么访问它们。至于任务最终跑在哪台机器上，是调度层根据"代码发给数据"这条策略去决定的事。

这一章，我们先造出这个代表的最小骨架：暂时不碰分区和调度，只抓住一件事——**数据怎么被访问**。

![RDD 作为数据代表的概念图](/mini-spark/images/ch02/rdd-data-representative.png)

*图 2-1：RDD 描述一批数据及其访问方式；真正运行时，任务会在具体分区上执行计算逻辑。*

## 2.1 一个被忽略的直觉：数据本身，还是数据的访问方式？

先看一段再普通不过的 Java 代码：

```java
List<String> words = Arrays.asList("hello", "spark", "hello", "world");
for (String w : words) {
    System.out.println(w);
}
```

你脑子里可能自动浮现出 `words` 里装着四个字符串。这个直觉没错——`ArrayList` 确实在内存里实实在在存着这四条数据，你可以反复遍历它，每次看到的都一样。

但如果我跟你说，在 Spark 的世界里，**"数据的代表"通常不复制整份数据，也不急着把结果算出来**，你会不会觉得有点反直觉？

别急，我们从一个你每天都在用、却可能没认真想过的东西开始——`Iterator`。本节的几个小片段都放在 [`IteratorExamples.java`](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator/src/main/java/com/sparklearn/IteratorExamples.java) 里，可以直接运行。

### Iterator：一道门，不是一间房

在 Java 里，`Iterator` 是一个接口，它只回答两个问题：

- `hasNext()`："还有下一个吗？"
- `next()`："把下一个给我。"

光看接口定义看不出什么。我们直接上代码，把 `ArrayList` 和 `Iterator` 放在一起对比：

```java
List<String> room = Arrays.asList("hello", "spark", "hello", "world");

// ArrayList 是一间"房"——数据实实在在存在里面
System.out.println("大小：" + room.size());     // 能问"有多少"
System.out.println("第2个：" + room.get(1));    // 能直接跳到任意位置
// 能反复遍历，每次看到的都一样
for (String w : room) System.out.println(w);
for (String w : room) System.out.println(w);    // 再走一遍，完全没问题

// Iterator 只是一道"门"——你只能往前走
Iterator<String> door = room.iterator();
// door.size()  ← 编译都过不了！没有这个方法
// door.get(1)  ← 也没有。只能 next()、next()、next()...
while (door.hasNext()) {
    System.out.println(door.next());
}
// 走到头了，想再走一遍？对不起——没有"倒带"键，只能换扇新门
```

这个对比把两者的区别砸实了：**`ArrayList` 是储存数据的房间，`Iterator` 是通向数据的门。** 房间能随便逛——问大小、跳到任意位置、反复走几遍都行。门只有两个动作：问"还有吗"、拿"下一个"。走过就过了，没法回头。

更妙的是：门后面甚至**不需要**有一间房。比如——

```java
// 一个永远数不到头的数字流——内存里并不真的存着"所有自然数"
Iterator<Integer> endless = new Iterator<>() {
    int n = 0;
    public boolean hasNext() { return true; }   // 永远有下一个
    public Integer next() { return n++; }
};

System.out.println(endless.next());  // 0
System.out.println(endless.next());  // 1
System.out.println(endless.next());  // 2
// 只要你不喊停，它能数到地老天荒——而内存占用只有 int n 一个变量
```

`ArrayList` 做不到这件事：你不可能在内存里装下"所有自然数"。但 `Iterator` 可以——因为它不存数据，只负责每次现算一个。

这个区别，在后面所有章节都会反复回来。现在你只需要把这件事记在心里：**房间拥有数据，门只访问数据。**

![Iterator vs ArrayList 概念对照图](/mini-spark/images/ch02/arraylist-iterator-comparison.png)

*图 2-2：同一个数据源可以被看成"拥有数据"的房间，也可以被看成"访问数据"的一道门。*

## 2.2 把"待执行的计算"装进一个对象

既然 `Iterator` 是一道门——那么能不能在一切开始之前，先把"这扇门怎么打开"这件事，也包装成一个对象？

这就要请出 Java 8 的一个老朋友了。你大概率见过 `Supplier`：

```java
@FunctionalInterface
public interface Supplier<T> {
    T get();
}
```

`Supplier` 就是一个**还没执行的计算**。你把它当成一张欠条——"现在不兑现，等你真要的时候我再给你"。它不存结果，只存"怎么算"。

光看接口定义可能没什么感觉。我们动手写一个叫 `Deferred` 的小东西，让它把"延迟"这件事变得肉眼可见（对应代码 [`Deferred.java`](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator/src/main/java/com/sparklearn/Deferred.java)）：

```java
System.out.println("构造 Deferred...");
Deferred<String> lazy = new Deferred<>(() -> {
    // 大括号里的重计算——但构造时不会执行
    return "expensive result (" + System.currentTimeMillis() + ")";
});
System.out.println("Deferred 已构造，但计算还没发生。");

System.out.println("第一次 get(): " + lazy.get());  // 第一次 get()——触发计算
System.out.println("第二次 get(): " + lazy.get());  // 第二次 get()——直接用缓存值，不重算
```

运行这段代码，控制台输出如下。下面整段都来自程序的 `stdout`；时间戳每次运行都会不同，但两次 `get()` 返回的是同一个值：

```text
=== 1. Deferred：感受「延迟」===
构造 Deferred...
Deferred 已构造，但计算还没发生。
  [Deferred] 第一次 get()——触发计算...
第一次 get(): expensive result (<timestamp>)
  [Deferred] 已缓存，直接返回，不再重复计算
第二次 get(): expensive result (<timestamp>)
```

构造 `Deferred` 时什么都没发生，第一次 `get()` 才触发计算；第二次 `get()` 只打印"已缓存"，没有重新执行那段 `Supplier`。**延迟**这个词，现在你亲眼看见了。

不过 Spark 的 RDD 比 `Deferred` 走得更远：这里的 `Deferred` 会缓存第一次计算结果，而本章的 RDD 骨架不会缓存数据；每次需要数据时，它都会重新生成一个迭代器。眼下你只要先感受"构造一个东西 ≠ 马上执行"就够了。

## 2.3 RDD 的雏形：一个最小约定

有了 `Iterator` 和 `Supplier`，我们已经有了两块积木：

- `Iterator`：真正读数据时用的那扇门。
- `Supplier`：把"怎么开门"这件事先存起来，等需要时再执行。

现在，我们给"数据的代表"定一个最小约定。

先不要把 RDD 想得很复杂。现在它只回答一个问题：

> 如果有人要读数据，你能不能给他一个 `Iterator`？

这个问题，在代码里就叫 `compute()`（对应代码 [`RDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator/src/main/java/com/sparklearn/RDD.java)）：

```java
public abstract class RDD<T> {
    public abstract Iterator<T> compute();
}
```

就这么简单。`RDD<T>` 暂时只是一个约定：**任何 RDD，都必须能在需要数据时返回一个 `Iterator<T>`。**

这个定义看起来近乎"什么都没有"，但它正好抓住了本章要的那根骨架：RDD 先不急着保存一整份数据，也不急着算出结果；它先保存一张"怎么访问数据"的配方。

至于分区、依赖、缓存、优先位置……这些都先不碰。现在我们只让 RDD 学会一件事：需要数据时，交出一扇能往前读的门。

## 2.4 ListRDD：不复制数据，只存"怎么访问"

有了 `RDD` 这个最小约定，就可以搭出第一个具体实现了——`ListRDD`（对应代码 [`ListRDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator/src/main/java/com/sparklearn/ListRDD.java)）。

从这里开始的示例可以直接运行 [`Main.java`](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator/src/main/java/com/sparklearn/Main.java)。它会依次演示 `Deferred`、`ListRDD` 和 `compute()`。

看到这里，你可能会问：`List` 本来就有 `data.iterator()`，为什么还要绕一层 `Supplier`？

这个问题问得对。如果只看 `ListRDD`，下面这种写法也能工作：

```java
private final List<T> data;

public Iterator<T> compute() {
    return data.iterator();
}
```

而且要注意：这段代码每次调用 `compute()`，也会创建一个新的 `Iterator`。所以关键区别不是"直接 `data.iterator()` 不行"。

关键区别在于：`Supplier` 把"怎么创建 Iterator"这件事抽象成一个字段。

也就是说，`ListRDD` 保存的不是某个固定写死的读取动作，而是一个更一般的东西：

```java
Supplier<Iterator<T>>
```

它表达的是一句话：

> 等 `compute()` 被调用时，你能不能给我一个新的 `Iterator`？

今天这个 `Supplier` 是：

```java
() -> data.iterator()
```

以后它也可以是：

```java
() -> Files.lines(path).iterator()
```

或者：

```java
() -> readFromPartition(partitionId)
```

所以，在本章这个最小例子里，直接保存 `List<T> data` 并在 `compute()` 中 `return data.iterator()` 完全可以，而且更容易理解。我们使用 `Supplier`，不是因为 `List` 做不到，而是为了提前采用一个更通用的形状：RDD 保存的是**生成数据流的方法**，不一定保存某个具体容器。

再换个角度看：`ListRDD` 不保存一个已经创建好的 `Iterator`，而是保存一个**创建 Iterator 的方法**。原因很简单：`Iterator` 是一次性的，走到末尾就没了；但"创建 Iterator 的方法"可以反复调用。每次调用 `compute()`，都能重新打开一扇从头开始的门。

所以 `ListRDD extends RDD<T>` 的意思很朴素：`ListRDD` 遵守 RDD 的约定，它能在 `compute()` 被调用时给出一个 `Iterator<T>`。

构造函数拿到一个 `List`，但它不会把这个 `List` 复制一份，也不会提前遍历。它只是把"以后怎么创建迭代器"保存下来。注意，这个 `Supplier` 闭包仍然引用着传入的 `List`；数据还在原地，`ListRDD` 保存的是访问入口：

```java
public class ListRDD<T> extends RDD<T> {
    private final Supplier<Iterator<T>> supplier;

    public ListRDD(List<T> data) {
        // 这里没有读取 data。
        // 这里只是记住：以后需要数据时，调用 data.iterator()。
        this.supplier = () -> data.iterator();
    }

    @Override
    public Iterator<T> compute() {
        // 到这里才真正创建 Iterator。
        return supplier.get();
    }
}
```

看到关键的那一行了吗？`() -> data.iterator()`——不是 `data.iterator()` 本身，更不是 `new ArrayList<>(data)`。它保存的是"到时候怎么拿"，不是现在就拿。

然后，等到你真的需要数据的时候：

```java
List<String> words = Arrays.asList("hello", "spark", "hello", "world", "spark", "is", "fun");
ListRDD<String> rdd = new ListRDD<>(words);       // 构造：什么也没遍历
Iterator<String> it1 = rdd.compute();             // 拿到一个全新的迭代器
Iterator<String> it2 = rdd.compute();             // 再拿一个，全新的

System.out.println("it1 == it2 ? " + (it1 == it2) + "  ← false，说明每次都是新的！");
System.out.println("第一次遍历:");
it1.forEachRemaining(w -> System.out.print(" " + w));
System.out.println();
System.out.println("第二次遍历（独立的迭代器，从头开始）:");
it2.forEachRemaining(w -> System.out.print(" " + w));
System.out.println();
```

每次调用 `compute()`，`Supplier` 都会调用 `list.iterator()`，给你一个**全新的、从头开始的迭代器**。运行验证一下：

```
=== 3. 每次 compute() 返回全新迭代器 ===
it1 == it2 ? false  ← false，说明每次都是新的！

第一次遍历:
 hello spark hello world spark is fun
第二次遍历（独立的迭代器，从头开始）:
 hello spark hello world spark is fun
```

同一个 `ListRDD` 可以反复消费，每次都是独立的。这里先记住一个很小但很关键的事实：**RDD 本身不是那个已经走到末尾的迭代器；RDD 是能重新生成迭代器的对象。**

### 回到第 1 章：为什么不能复制一整份数据？

到这里，`Supplier` 的作用已经清楚了：它让 `ListRDD` 保存"以后怎么生成 Iterator"。现在再回到第 1 章的问题：数据一旦变大，为什么我们这么在意"不要复制一整份数据"？

先看反面。如果把数据复制一份存进去——

```java
// 错：犯了"持有副本"的毛病
this.data = new ArrayList<>(list);  // 把原始数据全拷了一份
```

记不记得第 1 章的 50GB 日志场景。朴素 WordCount 的第一堵墙，就是数据太大，单机内存装不下。现在如果每构造一个"数据的代表"，又把底层数据完整复制一遍，那就等于在原来的问题上再加一份内存压力。`ListRDD` 的做法是：数据还在原地，RDD 只保存访问路径。

这就是本章要建立的直觉：**RDD 不必总是"物化"成一份已经算好的数据。它可以先保存访问路径，等真正需要时再生成数据流。** 我们这个 `ListRDD` 只是最小例子，但它已经把方向摆出来了：RDD 描述的是"怎么算、怎么读"，而不是"算完后存在这里的一整份结果"。

## 2.5 本章小结

回顾一下，这一章我们只做了一件事：**把"数据的代表"造出来。**

具体的零件有四个：

1. **`Iterator`** —— 一道访问的门，不是一间储存数据的房。
2. **`Supplier` / `Deferred`** —— 把"待执行的计算"包装成对象，让你直观感受什么是"延迟"。
3. **`RDD`** —— 一个最小约定：需要数据时，必须能通过 `compute()` 返回一个 `Iterator`。
4. **`ListRDD`** —— 第一个遵守这个约定的实现：不复制数据、不预先物化结果，只保存访问方式。

这四个零件拼在一起，回答了第 1 章结尾留下的那个问题——怎么把数据变成代码里能拿来算的东西？答案是 RDD。它不是把数据复制进自己身体里，而是保存一张"怎么算、怎么访问"的配方。

下一章，我们要在这张配方上加一个关键能力：**变换**。给 RDD 套一层 `map`，它能返回一个新的 RDD——而新 RDD 的 `compute()` 里，又嵌套着老 RDD 的迭代器。等你消费最外层数据的时候，数据会像流水线一样一层层流过来。

别急，我们一步一步来。
