package com.sparklearn.sql.execution;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.Expression;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 结构化数据源扫描。
 *
 * <p>作为 codegen 阶段的叶子节点，它的 doProduce 负责生成读数据的 while 循环，
 * 循环里依次应用下推过滤、投影裁剪，再把行交给上游算子。
 */
public record ScanExec(
        String relationName,
        RDD<Row> rdd,
        Schema sourceSchema,
        List<String> requiredColumns,
        List<Expression> pushedFilters) implements CodegenSupport {

    public ScanExec {
        requiredColumns = List.copyOf(requiredColumns);
        pushedFilters = List.copyOf(pushedFilters);
    }

    @Override
    public RDD<Row> execute() {
        // 绑定 schema：原始 Row 只有 nameToIndex，绑上 sourceSchema 后才有 ExprId→index 映射。
        // 下游 Attribute.eval(row) 用 ExprId 精确取值，JOIN 同名列场景关键。
        RDD<Row> current = rdd.map(row -> row.withSchema(sourceSchema));
        for (Expression filter : pushedFilters) {
            current = current.filter(row -> Boolean.TRUE.equals(filter.eval(row)));
        }
        if (!requiredColumns.isEmpty()) {
            Schema outputSchema = sourceSchema.select(requiredColumns);
            current = current.map(row -> row.select(requiredColumns).withSchema(outputSchema));
        }
        return current;
    }

    @Override
    public List<RDD<Row>> inputRDDs() {
        return List.of(rdd);
    }

    @Override
    public List<String> outputFieldNames() {
        if (requiredColumns.isEmpty()) {
            return sourceSchema.fieldNames();
        }
        return requiredColumns;
    }

    @Override
    public String doProduce(CodegenContext ctx) {
        String scanRow = ctx.freshName("scanRow");
        // 原始行的字段名列表，用来把下推过滤里的列名解析成索引
        List<String> sourceFields = sourceSchema.fieldNames();

        StringBuilder sb = new StringBuilder();
        sb.append("while (").append(CodegenContext.INPUT_ITERATOR).append(".hasNext()) {\n");
        sb.append("    Row ").append(scanRow)
                .append(" = (Row) ").append(CodegenContext.INPUT_ITERATOR).append(".next();\n");

        // 下推过滤：每条都生成一个 if (!cond) continue;
        for (Expression filter : pushedFilters) {
            String condCode = filter.genCode(ctx, scanRow, sourceFields);
            sb.append("    if (!(").append(condCode).append(")) continue;\n");
        }

        // 列裁剪：如果 requiredColumns 非空，生成 row.select(...)
        String outputRowVar;
        List<String> outputFields;
        if (requiredColumns.isEmpty()) {
            outputRowVar = scanRow;
            outputFields = sourceFields;
        } else {
            outputRowVar = ctx.freshName("scanOutput");
            String columnsArgs = requiredColumns.stream()
                    .map(c -> "\"" + c + "\"")
                    .collect(Collectors.joining(", "));
            sb.append("    Row ").append(outputRowVar)
                    .append(" = ").append(scanRow)
                    .append(".select(java.util.Arrays.asList(").append(columnsArgs).append("));\n");
            outputFields = requiredColumns;
        }

        // 把这一行交给上游算子
        sb.append("    ").append(consume(ctx, outputRowVar, outputFields)).append("\n");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String doConsume(CodegenContext ctx, String rowVar, List<String> rowFields) {
        // ScanExec 是叶子，不会有人调它的 consume
        throw new UnsupportedOperationException("ScanExec.doConsume should not be called");
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of();
    }

    @Override
    public String nodeName() {
        return "ScanExec";
    }

    @Override
    public String detailString() {
        String columns = requiredColumns.isEmpty()
                ? "*"
                : String.join(", ", requiredColumns);
        String filters = pushedFilters.isEmpty()
                ? ""
                : ", pushedFilters=["
                + String.join(", ", pushedFilters.stream().map(Expression::sql).toList())
                + "]";
        return "columns=[" + columns + "]" + filters;
    }
}
