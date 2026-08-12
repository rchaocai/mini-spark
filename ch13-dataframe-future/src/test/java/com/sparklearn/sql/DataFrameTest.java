package com.sparklearn.sql;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.catalyst.plans.logical.Scan;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.sparklearn.sql.catalyst.expressions.Expressions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    @Test
    void sqlSelectWhereBuildsTheSamePlanShapeAsDataFrameApi() {
        try (SparkContext spark = new SparkContext(2)) {
            SQLContext sql = new SQLContext(spark);
            DataFrame employees = sql.createDataFrame(employeeRows(), employeesSchema());
            employees.createOrReplaceTempView("employees");

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

    @Test
    void sqlInnerJoinProducesCrossTableRows() {
        try (SparkContext spark = new SparkContext(2)) {
            SQLContext sql = new SQLContext(spark);
            DataFrame employees = sql.createDataFrame(employeeRows(), employeesSchema());
            employees.createOrReplaceTempView("employees");
            DataFrame departments = sql.createDataFrame(departmentRows(), departmentsSchema());
            departments.createOrReplaceTempView("departments");

            DataFrame result = sql.sql(
                    "SELECT name, dept_name FROM employees JOIN departments ON department = dept_code");

            List<Row> rows = result.collect();
            assertEquals(6, rows.size());

            Map<String, String> nameToDept = new HashMap<>();
            for (Row row : rows) {
                nameToDept.put((String) row.get("name"), (String) row.get("dept_name"));
            }
            assertEquals("Engineering", nameToDept.get("Alice"));
            assertEquals("Operations", nameToDept.get("Bob"));
            assertEquals("Engineering", nameToDept.get("Cathy"));
            assertEquals("Sales", nameToDept.get("David"));
            assertEquals("Sales", nameToDept.get("Eva"));
            assertEquals("Operations", nameToDept.get("Frank"));
            assertTrue(result.explainString().contains("HashJoinExec"));
        }
    }

    @Test
    void sqlLeftJoinKeepsUnmatchedLeftRowsWithNulls() {
        try (SparkContext spark = new SparkContext(2)) {
            SQLContext sql = new SQLContext(spark);
            DataFrame employees = sql.createDataFrame(employeeRows(), employeesSchema());
            employees.createOrReplaceTempView("employees");
            // 右表只注册 eng 和 ops，不注册 sales → sales 员工 LEFT JOIN 后右列为 NULL
            DataFrame departments = sql.createDataFrame(List.of(
                    Row.of("dept_code", "eng", "dept_name", "Engineering"),
                    Row.of("dept_code", "ops", "dept_name", "Operations")), departmentsSchema());
            departments.createOrReplaceTempView("departments");

            DataFrame result = sql.sql(
                    "SELECT name, dept_name FROM employees LEFT JOIN departments ON department = dept_code");

            List<Row> rows = result.collect();
            assertEquals(6, rows.size());

            Map<String, Object> nameToDept = new HashMap<>();
            for (Row row : rows) {
                nameToDept.put((String) row.get("name"), row.get("dept_name"));
            }
            assertEquals("Engineering", nameToDept.get("Alice"));
            assertNull(nameToDept.get("David")); // sales → 无匹配
            assertNull(nameToDept.get("Eva"));   // sales → 无匹配
        }
    }

    @Test
    void sqlJoinWithQualifiedColumnNamesResolvesCorrectly() {
        try (SparkContext spark = new SparkContext(2)) {
            SQLContext sql = new SQLContext(spark);
            DataFrame employees = sql.createDataFrame(employeeRows(), employeesSchema());
            employees.createOrReplaceTempView("employees");
            DataFrame departments = sql.createDataFrame(departmentRows(), departmentsSchema());
            departments.createOrReplaceTempView("departments");

            // 限定列名 table.col：明确指定列来自哪张表
            DataFrame result = sql.sql(
                    "SELECT name, dept_name FROM employees JOIN departments "
                            + "ON employees.department = departments.dept_code");

            List<Row> rows = result.collect();
            assertEquals(6, rows.size());

            Map<String, String> nameToDept = new HashMap<>();
            for (Row row : rows) {
                nameToDept.put((String) row.get("name"), (String) row.get("dept_name"));
            }
            assertEquals("Engineering", nameToDept.get("Alice"));
            assertEquals("Operations", nameToDept.get("Bob"));
            assertEquals("Sales", nameToDept.get("David"));
        }
    }

    @Test
    void sqlJoinWithReversedConditionStillWorks() {
        try (SparkContext spark = new SparkContext(2)) {
            SQLContext sql = new SQLContext(spark);
            DataFrame employees = sql.createDataFrame(employeeRows(), employeesSchema());
            employees.createOrReplaceTempView("employees");
            DataFrame departments = sql.createDataFrame(departmentRows(), departmentsSchema());
            departments.createOrReplaceTempView("departments");

            // 反序条件：右表列在等号左边、左表列在等号右边
            // ResolveAttributes 按列名独立解析（不依赖等号位置），HashJoinExec 按列名判定侧别
            DataFrame result = sql.sql(
                    "SELECT name, dept_name FROM employees JOIN departments ON dept_code = department");

            List<Row> rows = result.collect();
            assertEquals(6, rows.size());

            Map<String, String> nameToDept = new HashMap<>();
            for (Row row : rows) {
                nameToDept.put((String) row.get("name"), (String) row.get("dept_name"));
            }
            assertEquals("Engineering", nameToDept.get("Alice"));
            assertEquals("Operations", nameToDept.get("Frank"));
            assertEquals("Sales", nameToDept.get("Eva"));
        }
    }

    private static DataFrame employees(SparkContext spark) {
        SQLContext sql = new SQLContext(spark);
        return sql.createDataFrame(employeeRows(), employeesSchema());
    }

    private static Schema employeesSchema() {
        return Schema.of(
                new Field("id", DataType.INTEGER),
                new Field("name", DataType.STRING),
                new Field("department", DataType.STRING),
                new Field("salary", DataType.INTEGER));
    }

    private static Schema departmentsSchema() {
        return Schema.of(
                new Field("dept_code", DataType.STRING),
                new Field("dept_name", DataType.STRING));
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

    private static List<Row> departmentRows() {
        return List.of(
                Row.of("dept_code", "eng", "dept_name", "Engineering"),
                Row.of("dept_code", "ops", "dept_name", "Operations"),
                Row.of("dept_code", "sales", "dept_name", "Sales"));
    }
}
