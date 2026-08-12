package com.sparklearn.sql.execution;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;

import java.util.List;

/**
 * 能参与 whole-stage codegen 的物理算子实现这个接口。
 *
 * <p>核心是 produce / consume 一对方法：
 * <ul>
 *   <li>produce 生成"从哪里读数据"的框架代码（叶子算子写 while 循环，非叶子算子转给 child）</li>
 *   <li>consume  生成"拿到一行后怎么处理"的代码（Filter 写 if-continue，Project 写表达式求值）</li>
 * </ul>
 *
 * <p>调用链：WholeStageCodegenExec 调 child.produce(ctx, null)；
 * 叶子的 doProduce 生成循环，循环体内调 this.consume(...)；
 * consume 查到自己的消费者（parent），调 parent.doConsume(...)；
 * parent.doConsume 处理完一行后再调 this.consume(...) 把新行传给更上层；
 * 直到根算子，consume 发现 parent 为 null，生成 output.add(row);。
 */
public non-sealed interface CodegenSupport extends PhysicalPlan {

    /**
     * 是否参与 whole-stage codegen。默认 true。
     */
    default boolean supportCodegen() {
        return true;
    }

    /**
     * 提供输入 RDD。只有叶子算子（ScanExec）真正实现；非叶子算子转给 child。
     */
    List<RDD<Row>> inputRDDs();

    /**
     * 本算子输出行的字段名列表，供下游算子把 Attribute 解析成索引。
     */
    List<String> outputFieldNames();

    /**
     * 生成 produce 代码。consumer 是本算子的消费者。
     * 默认实现：登记 consumer，再调 doProduce。
     */
    default String produce(CodegenContext ctx, CodegenSupport consumer) {
        ctx.setParent(this, consumer);
        return doProduce(ctx);
    }

    /**
     * 生成 produce 代码的算子专属部分。叶子算子写循环，非叶子算子调 child.produce。
     */
    String doProduce(CodegenContext ctx);

    /**
     * 生成 consume 代码：把一行传给本算子的消费者。
     * 默认实现：查 consumer，调 consumer.doConsume；如果没有 consumer（根算子），输出到缓冲。
     */
    default String consume(CodegenContext ctx, String rowVar, List<String> rowFields) {
        CodegenSupport consumer = ctx.parentOf(this);
        if (consumer == null) {
            return "output.add(" + rowVar + ");";
        }
        return consumer.doConsume(ctx, rowVar, rowFields);
    }

    /**
     * 生成 consume 代码的算子专属部分。rowVar 是输入行变量名，rowFields 是该行的字段名列表。
     * 实现应该在处理完一行后调 this.consume(...) 把结果传给更上层。
     */
    String doConsume(CodegenContext ctx, String rowVar, List<String> rowFields);
}
