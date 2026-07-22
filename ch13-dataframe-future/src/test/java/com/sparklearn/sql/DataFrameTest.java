package com.sparklearn.sql;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.catalyst.plans.logical.Scan;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.sparklearn.sql.catalyst.expressions.Expressions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DataFrameTest {

    @Test
    void filterAndProjectRunThroughOptimizedScan() {
        try (SparkContext spark = new SparkContext(2)) {
            DataFrame result = employees(spark)
                    .where(col("salary").gt(50_000))
                    .select(
                            col("name"),
                            col("salary").multiply(1.1).as("adjusted_salary"));

            List<Row> rows = result.collect();

            assertEquals(List.of("Alice", "Cathy", "David", "Frank"),
                    rows.stream().map(row -> row.get("name")).toList());
            assertEquals(List.of("name", "adjusted_salary"), rows.get(0).fieldNames());

            Scan scan = assertInstanceOf(
                    Scan.class,
                    result.queryExecution().optimized().children().get(0));
            assertEquals(List.of("name", "salary"), scan.requiredColumns());
            assertEquals(1, scan.pushedFilters().size());
            assertTrue(scan.detailString().contains("salary > 50000"));
        }
    }

    @Test
    void groupByCountFallsBackToReduceByKey() {
        try (SparkContext spark = new SparkContext(2)) {
            DataFrame result = employees(spark)
                    .where(col("salary").gt(50_000))
                    .groupBy("department")
                    .count();

            Map<String, Long> counts = result.collect().stream()
                    .collect(Collectors.toMap(
                            row -> (String) row.get("department"),
                            row -> (Long) row.get("count")));

            assertEquals(Map.of("eng", 2L, "sales", 1L, "ops", 1L), counts);
            assertTrue(result.explainString().contains("HashAggregateExec"));
        }
    }

    private static DataFrame employees(SparkContext spark) {
        SQLContext sql = new SQLContext(spark);
        return sql.createDataFrame("employees", List.of(
                Row.of("id", 1, "name", "Alice", "department", "eng", "salary", 72_000),
                Row.of("id", 2, "name", "Bob", "department", "ops", "salary", 45_000),
                Row.of("id", 3, "name", "Cathy", "department", "eng", "salary", 83_000),
                Row.of("id", 4, "name", "David", "department", "sales", "salary", 51_000),
                Row.of("id", 5, "name", "Eva", "department", "sales", "salary", 39_000),
                Row.of("id", 6, "name", "Frank", "department", "ops", "salary", 67_000)), 2);
    }
}
