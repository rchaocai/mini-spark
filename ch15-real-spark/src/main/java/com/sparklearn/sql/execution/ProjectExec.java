package com.sparklearn.sql.execution;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.NamedExpression;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 投影执行，落成 RDD.map。
 *
 * <p>codegen 时：doProduce 转给 child；doConsume 为每个投影表达式生成一行求值代码，
 * 再用 Row.of(name, value, ...) 组装成新行交给上游。
 */
public record ProjectExec(List<NamedExpression> projectList, PhysicalPlan child)
        implements CodegenSupport {

    public ProjectExec {
        projectList = List.copyOf(projectList);
    }

    @Override
    public RDD<Row> execute() {
        return child.execute().map(row -> {
            LinkedHashMap<String, Object> values = new LinkedHashMap<>();
            for (NamedExpression expression : projectList) {
                values.put(expression.name(), expression.eval(row));
            }
            return Row.of(values);
        });
    }

    @Override
    public List<RDD<Row>> inputRDDs() {
        return ((CodegenSupport) child).inputRDDs();
    }

    @Override
    public List<String> outputFieldNames() {
        return projectList.stream().map(NamedExpression::name).toList();
    }

    @Override
    public String doProduce(CodegenContext ctx) {
        return ((CodegenSupport) child).produce(ctx, this);
    }

    @Override
    public String doConsume(CodegenContext ctx, String rowVar, List<String> rowFields) {
        StringBuilder sb = new StringBuilder();
        String outputVar = ctx.freshName("projectOutput");
        List<String> outputFields = outputFieldNames();

        // 第一步：为每个投影表达式生成求值语句，并记下 value 变量名
        List<String> valueVars = new ArrayList<>();
        for (int i = 0; i < projectList.size(); i++) {
            NamedExpression expr = projectList.get(i);
            String valueVar = ctx.freshName("projectValue");
            valueVars.add(valueVar);
            String exprCode = expr.genCode(ctx, rowVar, rowFields);
            sb.append("Object ").append(valueVar)
                    .append(" = ").append(exprCode).append(";\n    ");
        }

        // 第二步：用 Row.of(name1, value1, name2, value2, ...) 组装新行
        sb.append("Row ").append(outputVar).append(" = Row.of(");
        for (int i = 0; i < projectList.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("\"").append(projectList.get(i).name()).append("\", ")
                    .append(valueVars.get(i));
        }
        sb.append(");\n    ");

        // 第三步：把新行交给上游
        sb.append(consume(ctx, outputVar, outputFields));
        return sb.toString();
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    @Override
    public String nodeName() {
        return "ProjectExec";
    }

    @Override
    public String detailString() {
        return String.join(", ", projectList.stream().map(NamedExpression::sql).toList());
    }
}
