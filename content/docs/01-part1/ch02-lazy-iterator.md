---
title: "第 2 章 · 数据流与延迟迭代"
weight: 2
date: 2026-07-15
tags: ["RDD", "Iterator", "惰性求值", "ListRDD"]
summary: '造出Spark的第一个抽象——RDD：它不存数据，只存"怎么访问"。从Iterator、Supplier到ListRDD，用三个零件拼出RDD最核心的那根骨架。'
---

# 第 2 章 · 数据流与延迟迭代

> 💻 本章完整代码：[GitHub 查看](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator)

上一章结尾，我们手里握着两件法宝——"分块"和"并行"。而要让计算在分区上并行铺开，mini-spark 选择了存算一体的经典路线：把代码发给数据。真要动手实现它，第一个问题就跳出来了：**发给谁？** 换句话说，你的代码到了那台存着数据的机器上，需要有一个"数据的代表"来接住它。

这一章，我们就造出这个代表。

![RDD 作为数据代表的概念图](/mini-spark/images/ch02/rdd-data-representative.png)

*图 2-1：代码被发送到数据节点后，由 RDD 作为"数据的代表"接住计算。*

## 2.1 一个被忽略的直觉：数据本身，还是数据的访问方式？

先看一段再普通不过的 Java 代码：

```java
List<String> words = Arrays.asList("hello", "spark", "hello", "world");
for (String w : words) {
    System.out.println(w);
}
```

你脑子里可能自动浮现出 `words` 里装着四个字符串。这个直觉没错——`ArrayList` 确实在内存里实实在在存着这四条数据，你可以反复遍历它，每次看到的都一样。

但如果我跟你说，在 Spark 的世界里，**"数据的代表"通常不存数据本身**，你会不会觉得有点反直觉？

别急，我们从一个你每天都在用、却可能没认真想过的东西开始——`Iterator`。

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
System.out.println("看到了吗——第二次 get() 不重新算，直接返回缓存值。");
```

运行这段代码，控制台会清清楚楚告诉你（时间戳每次运行都会不同，但两次 `get()` 返回的是同一个值）：

```
=== 1. Deferred：感受「延迟」===
构造 Deferred...
Deferred 已构造，但计算还没发生。
  [Deferred] 第一次 get()——触发计算...
第一次 get(): expensive result (<timestamp>)
  [Deferred] 已缓存，直接返回，不再重复计算
第二次 get(): expensive result (<timestamp>)
看到了吗——第二次 get() 不重新算，直接返回缓存值。
```

构造 `Deferred` 时什么都没发生，第一次 `get()` 才触发计算——**延迟**这个词，现在你亲眼看见了。

不过 Spark 的 RDD 比 `Deferred` 走得更远：RDD 压根不缓存计算结果，每次调用都要重新算。这件事我们留到第 8 章容错的时候再深聊，眼下你只要先感受"构造一个东西 ≠ 马上执行"就够了。

## 2.3 ListRDD：不存数据，只存"怎么访问"

有了 `Supplier` 和 `Iterator` 这两块积木，就可以搭出本章的主角了——`ListRDD`（对应代码 [`ListRDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator/src/main/java/com/sparklearn/ListRDD.java)）。

它的设计简单到让你失望：**`ListRDD` 不持有数据，只持有一个 `Supplier<Iterator<T>>`**。

构造函数拿到一个 `List`，但它立刻把"怎么拿到这个 `List` 的迭代器"这件事包装成一个 `Supplier` 存下来——绝不复制数据，也绝不提前遍历：

```java
public class ListRDD<T> extends RDD<T> {
    private final Supplier<Iterator<T>> supplier;

    public ListRDD(List<T> data) {
        // 关键：不是 new ArrayList<>(data)，而是用 Supplier 包裹 iterator()
        this.supplier = () -> data.iterator();
    }

    @Override
    public Iterator<T> compute() {
        return supplier.get();
    }
}
```

看到关键的那一行了吗？`() -> data.iterator()`——不是 `new ArrayList<>(data)`。没有复制，没有拷贝，只有一行"到时候再拿"的承诺。

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

同一个 `ListRDD` 可以反复消费，每次都是独立的。这一点对后面第 8 章的容错重算至关重要：如果分区丢了，再调一次 `compute()`，就能从头重算——不会因为迭代器"已经走到末尾"就无法回滚。当然这是后话。

### 不存数据，真的有用吗？

你可能会问：不存数据，就存一个 `Supplier`——这到底有什么好处？

先看反面。如果把数据复制一份存进去——

```java
// 错：犯了"持有副本"的毛病
this.data = new ArrayList<>(list);  // 把原始数据全拷了一份
```

记不记得第 1 章的场景——100TB。如果每次构造一个"数据的代表"都要**复制一整份数据**，你的内存瞬间就炸了，连分块都来不及。

`ListRDD` 的做法是：**不存数据，只存"怎么访问"。** 就像你不会把图书馆所有的书都复印一份带回家——你只记下"哪个书架、第几本"，要找的时候再去翻。

到了这一步，我们触及了 RDD 的一个核心属性，它反直觉但极其重要：**RDD 不必总是"物化"——它只要知道怎么从原始数据里读出来，就够了。** 我们这个只存访问方式、不存数据的 `ListRDD`，正是这个性质的化身。它描述的是"怎么算"，而不是"算完的结果"。

## 2.4 RDD 的雏形：一个轻量抽象

有了 `ListRDD` 这个具体实现，我们可以往上提一层，给所有"数据的代表"找一个共同的名字——就叫它 RDD（对应代码 [`RDD.java`](https://github.com/rchaocai/mini-spark/tree/main/ch02-lazy-iterator/src/main/java/com/sparklearn/RDD.java)）：

```java
public abstract class RDD<T> {
    public abstract Iterator<T> compute();
}
```

就这么简单。任何 RDD，只要你能回答"怎么算出你的数据"——也就是实现 `compute()`——那你就是一个合格的 RDD。

现在这个定义看起来近乎"什么都没有"，但先别急着给它填空。本章的任务只是让你认识 RDD 最基本的那根骨架：**RDD 是数据的代表，但它不存数据——它存的是"怎么访问"。**

至于分区（`splits`）、依赖（`dependencies`）、缓存、优先位置……后面的章节会一步一步加上去。一章只加一个新概念——这是我们说好的。

## 2.5 本章小结

回顾一下，这一章我们只做了一件事：**把"数据的代表"造出来。**

具体的零件有三个：

1. **`Iterator`** —— 一道访问的门，不是一间储存数据的房。
2. **`Supplier` / `Deferred`** —— 把"待执行的计算"包装成对象，让你直观感受什么是"延迟"。
3. **`ListRDD`** —— 持有 `Supplier<Iterator<T>>`，不存数据只存访问方式。这是全书第一个具体的 RDD。

这三个零件拼在一起，回答了第 1 章结尾留下的那个问题——怎么把数据变成代码里能拿来算的东西？答案是 RDD。而 RDD 不存数据——它存的是一张"怎么算"的配方。

下一章，我们要在这张配方上加一个关键能力：**变换**。给 RDD 套一层 `map`，它能返回一个新的 RDD——而新 RDD 的 `compute()` 里，又嵌套着老 RDD 的迭代器。等你消费最外层数据的时候，数据会像流水线一样一层层流过来。

那将是第一个"顿悟时刻"。

别急，我们一步一步来。
