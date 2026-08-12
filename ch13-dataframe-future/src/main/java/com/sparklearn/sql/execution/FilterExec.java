package com.sparklearn.sql.execution;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.Expression;

import java.util.List;

/**
 * 没有被下推的数据过滤。
 *
 * <p>codegen 时：doProduce 直接转给 child；doConsume 生成 if (!(cond)) continue; 再把原行交给上游。
 */
public record FilterExec(Expression condition, PhysicalPlan child) implements CodegenSupport {

    @Override
    public RDD<Row> execute() {
        return child.execute().filter(row -> Boolean.TRUE.equals(condition.eval(row)));
    }

    @Override
    public List<RDD<Row>> inputRDDs() {
        return ((CodegenSupport) child).inputRDDs();
    }

    @Override
    public List<String> outputFieldNames() {
        // Filter 不改 schema，输出字段和 child 一致
        return ((CodegenSupport) child).outputFieldNames();
    }

    @Override
    public String doProduce(CodegenContext ctx) {
        return ((CodegenSupport) child).produce(ctx, this);
    }

    @Override
    public String doConsume(CodegenContext ctx, String rowVar, List<String> rowFields) {
        String condCode = condition.genCode(ctx, rowVar, rowFields);
        return "if (!(" + condCode + ")) continue;\n"
                + "    " + consume(ctx, rowVar, rowFields);
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    @Override
    public String nodeName() {
        return "FilterExec";
    }

    @Override
    public String detailString() {
        return condition.sql();
    }
}
