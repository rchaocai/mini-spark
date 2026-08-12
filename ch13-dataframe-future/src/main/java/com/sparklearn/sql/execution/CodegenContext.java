package com.sparklearn.sql.execution;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * 代码生成上下文：在一次 whole-stage codegen 过程里收集变量名、可变状态和算子之间的父子关系。
 *
 * <p>每个 {@link WholeStageCodegenExec} 在 doExecute 时新建一个 CodegenContext，
 * 然后调用 child 的 produce，produce 内部会往 ctx 里登记各种信息。
 */
public final class CodegenContext {

    /**
     * 生成的迭代器在模板代码里固定叫 input，所有叶子算子的 doProduce 都从这里读数据。
     */
    public static final String INPUT_ITERATOR = "input";

    private int freshNameId = 0;
    private final List<String> mutableStateDecls = new ArrayList<>();
    private final Map<CodegenSupport, CodegenSupport> parents = new IdentityHashMap<>();

    /**
     * 生成一个不会重复的变量名，比如 scanRow_0、projectOutput_1。
     */
    public String freshName(String prefix) {
        return prefix + "_" + (freshNameId++);
    }

    /**
     * 登记一个算子的消费者（父算子），供 consume 时回查。
     */
    public void setParent(CodegenSupport node, CodegenSupport parent) {
        parents.put(node, parent);
    }

    /**
     * 取一个算子的消费者。返回 null 表示这是 codegen 阶段的根，行直接写入输出缓冲。
     */
    public CodegenSupport parentOf(CodegenSupport node) {
        return parents.get(node);
    }

    /**
     * 收集生成的可变状态声明（字段）。本教学实现目前没有用到，但保留接口和 Spark 对齐。
     */
    public void addMutableState(String javaType, String name, String init) {
        String decl = javaType + " " + name + (init == null ? "" : " = " + init) + ";";
        mutableStateDecls.add(decl);
    }

    public String declareMutableStates() {
        if (mutableStateDecls.isEmpty()) {
            return "";
        }
        return String.join("\n        ", mutableStateDecls) + "\n        ";
    }
}
