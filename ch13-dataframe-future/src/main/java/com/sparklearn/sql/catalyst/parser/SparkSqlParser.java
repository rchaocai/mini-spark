package com.sparklearn.sql.catalyst.parser;

import com.sparklearn.sql.catalyst.analysis.FunctionRegistry;

/**
 * 基于 ANTLR4 的 SQL 解析器。
 *
 * <p>本类只是 {@link AbstractSqlParser} 的一个薄封装，所有解析流程都在基类里，
 * 自己只负责提供 {@link AstBuilder}。解析阶段产出"未解析"的计划（表名和列名只是
 * 字符串），元数据绑定交给后面的
 * {@link com.sparklearn.sql.catalyst.analysis.Analyzer Analyzer}。
 *
 * <p>构造时传入 {@link FunctionRegistry}，解析函数调用时通过注册表查找构造器，
 * 而非硬编码 switch。这样用户注册自定义函数后即可在 SQL 中使用。
 */
public final class SparkSqlParser extends AbstractSqlParser {

    private final AstBuilder astBuilder;

    public SparkSqlParser(FunctionRegistry functionRegistry) {
        this.astBuilder = new AstBuilder(functionRegistry);
    }

    @Override
    protected AstBuilder astBuilder() {
        return astBuilder;
    }
}
