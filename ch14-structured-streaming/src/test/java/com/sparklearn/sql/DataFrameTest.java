package com.sparklearn.sql;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.catalyst.plans.logical.Scan;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class DataFrameTest {

    @Test
    void sqlSelectWhereBuildsProjectAndOptimizedScan() {
        try (SparkContext spark = new SparkContext(2)) {
            SQLContext sql = new SQLContext(spark);
            DataFrame employees = sql.createDataFrame("employees", employeeRows(), 2);
            sql.registerTable("employees", employees.logicalPlan());

            DataFrame result = sql.sql(
                    "SELECT name, salary FROM employees WHERE department = 'eng'");

            List<Row> rows = result.collect();

            assertEquals(List.of("Alice", "Cathy"),
                    rows.stream().map(row -> row.get("name")).toList());
            assertEquals(List.of("name", "salary"), rows.get(0).fieldNames());

            Scan scan = assertInstanceOf(
                    Scan.class,
                    result.queryExecution().optimized().children().get(0));
            assertEquals(List.of("name", "salary", "department"), scan.requiredColumns());
            assertEquals(1, scan.pushedFilters().size());
            assertTrue(result.explainString().contains("Project(name, salary)"));
        }
    }

    private static List<Row> employeeRows() {
        return List.of(
                Row.of("id", 1, "name", "Alice", "department", "eng", "salary", 72_000),
                Row.of("id", 2, "name", "Bob", "department", "ops", "salary", 45_000),
                Row.of("id", 3, "name", "Cathy", "department", "eng", "salary", 83_000),
                Row.of("id", 4, "name", "David", "department", "sales", "salary", 51_000),
                Row.of("id", 5, "name", "Eva", "department", "sales", "salary", 39_000),
                Row.of("id", 6, "name", "Frank", "department", "ops", "salary", 67_000));
    }
}
