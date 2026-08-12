package com.sparklearn.sql.execution;

import com.sparklearn.core.rdd.RDD;
import com.sparklearn.core.storage.Broadcast;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.Schema;
import com.sparklearn.sql.catalyst.expressions.And;
import com.sparklearn.sql.catalyst.expressions.EqualTo;
import com.sparklearn.sql.catalyst.expressions.Expression;
import com.sparklearn.sql.catalyst.parser.ParseException;
import com.sparklearn.sql.catalyst.plans.logical.JoinType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 哈希连接物理算子。
 *
 * <p>执行流程（参考 Spark 的 {@code HashJoin} trait / {@code BroadcastHashJoinExec}）：
 * <ol>
 *   <li>把右表 RDD collect 到 driver，按 join key 值列表建 hash map</li>
 *   <li>把 hash map 通过 {@link Broadcast} 广播：Broadcast 句柄只持有 broadcastId，
 *       Task 闭包捕获它时序列化链路只传 id，不传 map 本身</li>
 *   <li>遍历左表 RDD，每个 Task 从广播变量取 hash map，对每行用 join key 值列表查 map，
 *       匹配的右表行与左表行合并输出</li>
 *   <li>LEFT OUTER：左行无匹配时输出左行 + NULL 右列</li>
 * </ol>
 *
 * <p>实现说明：
 * <ul>
 *   <li>右表整体 collect 到 driver 建 hash map，再用广播变量分发——
 *       无论左表有多少分区/Task，rightMap 在序列化链路上只传一次</li>
 *   <li>只支持等值连接（{@code ON a = b [AND c = d ...]}），非等值条件抛异常</li>
 *   <li>不实现 {@link CodegenSupport}，切断 whole-stage codegen stage，
 *       左右子树各自独立走 codegen——Join 算子通常是 codegen stage 的边界</li>
 * </ul>
 *
 * <p><b>join key 侧别判定</b>：Spark 的 {@code HashJoin} trait 通过 {@code buildSide} 枚举
 * 显式指定哪侧建 hash map。本章固定右表建 map、左表 probe，但 ON 条件里等号
 * 左右两侧的列可能来自任一表（{@code ON b.x = a.x} 反序也能写），所以需要按列名
 * 判定每个 EqualTo 的哪一侧属于左表、哪一侧属于右表。
 */
public record HashJoinExec(
        Expression condition,
        JoinType joinType,
        Schema leftSchema,
        Schema rightSchema,
        PhysicalPlan left,
        PhysicalPlan right) implements PhysicalPlan {

    public HashJoinExec {
        Objects.requireNonNull(condition, "condition");
        Objects.requireNonNull(joinType, "joinType");
        Objects.requireNonNull(leftSchema, "leftSchema");
        Objects.requireNonNull(rightSchema, "rightSchema");
        Objects.requireNonNull(left, "left");
        Objects.requireNonNull(right, "right");
    }

    @Override
    public RDD<Row> execute() {
        RDD<Row> leftRdd = left.execute();
        RDD<Row> rightRdd = right.execute();
        List<JoinKey> keys = extractKeys(condition);

        // 1. 对 right RDD collect 到 driver，按 join key 值列表建 hash map
        List<Row> rightRows = rightRdd.collect();
        Map<List<Object>, List<Row>> rightMap = new HashMap<>();
        for (Row row : rightRows) {
            List<Object> keyValues = new ArrayList<>();
            for (JoinKey key : keys) {
                keyValues.add(key.rightKey().eval(row));
            }
            rightMap.computeIfAbsent(keyValues, k -> new ArrayList<>()).add(row);
        }

        // 2. 把 hash map 广播出去。Broadcast 句柄只持有 broadcastId（一个 long），
        //    Task 闭包捕获它时序列化链路只传 id；executor 端调 broadcast.value() 凭 id 取 map。
        //    这样无论左表有多少分区/Task，rightMap 在序列化链路上只传一次（driver 存一份，
        //    executor 按 id 取）。对应真实 Spark 的 BroadcastHashJoin：BroadcastExchangeExec
        //    把右表 collect 成 Broadcast 对象，BroadcastHashJoinExec 从广播变量拿右表建 map。
        Broadcast<Map<List<Object>, List<Row>>> broadcast =
                rightRdd.sparkContext().broadcast(rightMap);

        // 3. 遍历 left RDD，每个 Task 从广播变量取 rightMap 来 probe
        Row nullRight = Row.nullRow(rightSchema);
        return leftRdd.flatMap(leftRow -> {
            Map<List<Object>, List<Row>> map = broadcast.value();
            List<Object> keyValues = new ArrayList<>();
            for (JoinKey key : keys) {
                keyValues.add(key.leftKey().eval(leftRow));
            }
            List<Row> matches = map.get(keyValues);
            List<Row> output = new ArrayList<>();
            if (matches != null && !matches.isEmpty()) {
                for (Row rightRow : matches) {
                    output.add(Row.merge(leftRow, rightRow));
                }
            } else if (joinType == JoinType.LEFT_OUTER) {
                output.add(Row.merge(leftRow, nullRight));
            }
            return output;
        });
    }

    /**
     * 从 condition 提取等值 join key 对。非等值条件抛异常。
     *
     * <p>每个 {@link EqualTo} 的左右两侧可能来自任一表（用户可写 {@code ON b.x = a.x}），
     * {@link #assignSides} 按列名所属 schema 判定哪侧是左表 key、哪侧是右表 key。
     */
    private List<JoinKey> extractKeys(Expression expr) {
        List<JoinKey> keys = new ArrayList<>();
        collectKeys(expr, keys);
        if (keys.isEmpty()) {
            throw new ParseException(
                    "JOIN condition must be equi-join (a = b [AND ...]), got: " + expr.sql());
        }
        return keys;
    }

    private void collectKeys(Expression expr, List<JoinKey> keys) {
        if (expr instanceof EqualTo eq) {
            keys.add(assignSides(eq));
        } else if (expr instanceof And and) {
            collectKeys(and.left(), keys);
            collectKeys(and.right(), keys);
        } else {
            throw new ParseException(
                    "JOIN condition must be equi-join, unsupported: " + expr.sql());
        }
    }

    /**
     * 判定 EqualTo 两侧哪边属于左表、哪边属于右表，返回正确朝向的 JoinKey。
     *
     * <p>判定逻辑（按列名匹配 schema）：
     * <ul>
     *   <li>列名只在左表 → 来自左表</li>
     *   <li>列名只在右表 → 来自右表</li>
     *   <li>列名两表都有（同名限定列 {@code ON a.id = b.id}）→ 两侧都解析为
     *       Attribute("id")，按名求值时各自行取各列，朝向不影响正确性，默认 a→left, b→right</li>
     * </ul>
     */
    private JoinKey assignSides(EqualTo eq) {
        Expression a = eq.left();
        Expression b = eq.right();
        boolean aOnlyLeft = isOnlyFromSide(a, leftSchema, rightSchema);
        boolean aOnlyRight = isOnlyFromSide(a, rightSchema, leftSchema);
        boolean bOnlyLeft = isOnlyFromSide(b, leftSchema, rightSchema);
        boolean bOnlyRight = isOnlyFromSide(b, rightSchema, leftSchema);

        if (aOnlyLeft && bOnlyRight) {
            return new JoinKey(a, b);
        }
        if (aOnlyRight && bOnlyLeft) {
            return new JoinKey(b, a);
        }
        // 同名列或无法明确判定侧别时，默认 a→left, b→right
        //（同名列场景下 Attribute.eval(row) 按名取值，两侧各自的行都有该列，朝向不影响结果）
        return new JoinKey(a, b);
    }

    /** expr 引用的列全在 targetSchema 且不在 otherSchema → true。 */
    private boolean isOnlyFromSide(Expression expr, Schema targetSchema, Schema otherSchema) {
        Set<String> refs = expr.references();
        if (refs.isEmpty()) {
            return false;
        }
        for (String ref : refs) {
            if (!hasField(targetSchema, ref) || hasField(otherSchema, ref)) {
                return false;
            }
        }
        return true;
    }

    private boolean hasField(Schema schema, String name) {
        for (String n : schema.fieldNames()) {
            if (n.equals(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(left, right);
    }

    @Override
    public String nodeName() {
        return "HashJoinExec";
    }

    @Override
    public String detailString() {
        return joinType.sql() + ", " + condition.sql();
    }

    /** 一对 join key：左表 key 表达式 + 右表 key 表达式。 */
    private record JoinKey(Expression leftKey, Expression rightKey) {
    }
}
