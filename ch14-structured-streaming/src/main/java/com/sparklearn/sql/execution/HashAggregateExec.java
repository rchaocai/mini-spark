package com.sparklearn.sql.execution;

import com.sparklearn.core.KeyValuePair;
import com.sparklearn.core.RDD;
import com.sparklearn.sql.Row;
import com.sparklearn.sql.catalyst.expressions.Attribute;

import java.util.LinkedHashMap;
import java.util.List;
import java.io.Serializable;

/**
 * groupBy(...).count() 的物理执行，底层用 reduceByKey 触发 shuffle。
 */
public record HashAggregateExec(List<Attribute> groupingExpressions, PhysicalPlan child)
        implements PhysicalPlan {

    private static final int DEFAULT_REDUCE_PARTITIONS = 2;

    public HashAggregateExec {
        groupingExpressions = List.copyOf(groupingExpressions);
    }

    @Override
    public RDD<Row> execute() {
        RDD<KeyValuePair<GroupKey, Long>> keyed = child.execute()
                .map(row -> new KeyValuePair<>(GroupKey.from(row, groupingExpressions), 1L));
        RDD<KeyValuePair<GroupKey, Long>> counts =
                keyed.reduceByKey(Long::sum, DEFAULT_REDUCE_PARTITIONS);
        return counts.map(pair -> pair.key().toRow(pair.value()));
    }

    @Override
    public List<PhysicalPlan> children() {
        return List.of(child);
    }

    @Override
    public String nodeName() {
        return "HashAggregateExec";
    }

    @Override
    public String detailString() {
        return "groupBy=[" + String.join(", ",
                groupingExpressions.stream().map(Attribute::sql).toList()) + "], count(*)";
    }

    private record GroupKey(List<String> names, List<Object> values) implements Serializable {

        static GroupKey from(Row row, List<Attribute> groupingExpressions) {
            return new GroupKey(
                    groupingExpressions.stream().map(Attribute::name).toList(),
                    groupingExpressions.stream().map(attribute -> attribute.eval(row)).toList());
        }

        Row toRow(long count) {
            LinkedHashMap<String, Object> result = new LinkedHashMap<>();
            for (int index = 0; index < names.size(); index++) {
                result.put(names.get(index), values.get(index));
            }
            result.put("count", count);
            return Row.of(result);
        }
    }
}
