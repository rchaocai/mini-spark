package com.sparklearn.sql.catalyst.expressions;

import com.sparklearn.sql.DataType;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.execution.CodegenContext;

import java.util.List;

/**
 * 聚合函数接口。
 *
 * <p>参考 Spark 的 {@code AggregateFunction}（sql/catalyst/.../expressions/aggregate/AggregateFunction.scala）
 * 和 {@code AggregateExpression}（包装 AggregateFunction + mode）。
 *
 * <p>Spark 的聚合函数有两个关键方法：
 * <ul>
 *   <li>{@code update(buffer, input)}：对一行数据更新聚合缓冲区（Partial 阶段）</li>
 *   <li>{@code merge(buffer1, buffer2)}：合并两个缓冲区（Partial → Final 之间）</li>
 * </ul>
 *
 * <p>mini-spark 用 {@link #initialize(Row)} 代替 update：每行直接产生一个"单行状态"，
 * 再用 {@link #merge(Object, Object)} 反复合并。{@code reduceByKey} 内部的 map-side combine
 * 做的就是反复 merge——等价于 Spark 的 Partial 聚合。
 *
 * <p>聚合函数不能对单行 {@code eval()}，也不能参与 whole-stage codegen（它需要跨行聚合，
 * HashAggregateExec 走的是 {@code reduceByKey} 路径，不走 codegen 循环）。
 */
public non-sealed interface AggregateFunction extends Expression {

    /** 输出列名，例如 "count"。 */
    String name();

    /**
     * 对一行数据产生初始聚合状态。
     *
     * <p>count(*) 每行返回 1L；sum(salary) 每行返回 salary 的值。
     * 这个状态会被 {@link #merge} 反复合并。
     */
    Object initialize(Row row);

    /**
     * 合并两个聚合状态。
     *
     * <p>对应 Spark 的 {@code merge(buffer1, buffer2)}。
     * {@code reduceByKey} 在 map-side combine 和 reduce-side 都调用这个方法。
     */
    Object merge(Object state1, Object state2);

    /**
     * 将最终聚合状态转为输出值。
     *
     * <p>对应 Spark 的 {@code evaluateExpression}：从聚合 buffer 计算最终结果。
     * count 的状态是 Long，直接输出；sum 的状态是 Double，直接输出；
     * avg 的状态是 {@code double[]{sum, count}}，输出 {@code sum / count}。
     *
     * @param state 最终合并后的聚合状态
     * @return 输出到结果行中的值
     */
    Object evaluate(Object state);

    @Override
    default Object eval(Row row) {
        throw new UnsupportedOperationException(
                "聚合函数不能对单行求值，它需要跨一组行计算");
    }

    @Override
    default String genCode(CodegenContext ctx, String rowVar, List<String> inputFields) {
        throw new UnsupportedOperationException(
                "聚合函数不参与 whole-stage codegen，HashAggregateExec 走 reduceByKey 路径");
    }

    @Override
    default Expression transform(ExpressionRule rule) {
        return rule.apply(this);
    }
}
