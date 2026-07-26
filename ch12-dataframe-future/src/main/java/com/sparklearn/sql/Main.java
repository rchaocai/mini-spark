package com.sparklearn.sql;

import com.sparklearn.core.SparkContext;

import java.util.List;

import static com.sparklearn.sql.catalyst.expressions.Expressions.col;

/**
 * 第 12 章 · 从 RDD 到 DataFrame 演示入口。
 *
 * <p>通过两个典型需求展示 DataFrame API 的使用：
 * <ul>
 *   <li>需求一：过滤 + 投影，演示谓词下推和列裁剪</li>
 *   <li>需求二：分组聚合，演示 Aggregate 落回 RDD.reduceByKey</li>
 * </ul>
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        System.out.println("=".repeat(72));
        System.out.println("第 12 章 · 从 RDD 到 DataFrame");
        System.out.println("=".repeat(72));

        try (SparkContext spark = new SparkContext(2, true)) {
            SQLContext sql = new SQLContext(spark);
            DataFrame employees = sql.createDataFrame("employees", employees(), 2);

            System.out.println("\n--- 需求一：查出薪水 > 50000 的员工姓名和调整后薪水 ---\n");

            DataFrame adjustedSalary = employees
                    .where(col("salary").gt(50_000))
                    .select(
                            col("name"),
                            col("salary").multiply(1.25).as("adjusted_salary"));

            System.out.println(adjustedSalary.explainString());
            System.out.println("结果：");
            adjustedSalary.show();
            // 预期结果：Alice(90000)、Cathy(103750)、David(63750)、Frank(83750)
            // Bob(45000) 和 Eva(39000) 因薪水低于 50000 被过滤

            System.out.println("\n" + "=".repeat(72));
            System.out.println("需求二：统计薪水 > 50000 的员工，按部门分组计数。");
            System.out.println("=".repeat(72));

            DataFrame departmentCounts = employees
                    .where(col("salary").gt(50_000))
                    .groupBy("department")
                    .count();

            System.out.println(departmentCounts.explainString());
            System.out.println("结果：");
            departmentCounts.show();
            // 预期结果：eng=2, sales=1, ops=1
            // 底层使用 RDD.reduceByKey，会触发 Shuffle

            // ---- SQL 演示 ----
            System.out.println("\n" + "=".repeat(72));
            System.out.println("SQL 演示：用 SQL 字符串做同样的查询");
            System.out.println("=".repeat(72));

            demoSQL(sql);
        }
    }

    private static void demoSQL(SQLContext sql) {
        // 注册表，让 SQL 能引用
        DataFrame employees = sql.createDataFrame("employees", employees(), 2);
        sql.registerTable("employees", employees.logicalPlan());

        System.out.println("\nSQL 查询：SELECT department, count(*) FROM employees GROUP BY department\n");

        DataFrame result = sql.sql("SELECT department, count(*) FROM employees GROUP BY department");
        System.out.println(result.explainString());
        System.out.println("结果：");
        result.show();
        // 预期结果：每个部门 2 人（未过滤薪水）
        // SQL 解析后生成与 DataFrame API 相同的 LogicalPlan

        System.out.println("\nSQL 查询（带 WHERE）：SELECT department, count(*) FROM employees WHERE salary > 50000 GROUP BY department\n");

        DataFrame filteredResult = sql.sql("SELECT department, count(*) FROM employees WHERE salary > 50000 GROUP BY department");
        System.out.println(filteredResult.explainString());
        System.out.println("结果：");
        filteredResult.show();
        // 预期结果：eng=2, sales=1, ops=1（过滤掉薪水低于 50000 的员工）

        System.out.println("\nSQL 和 DataFrame API 会汇到同一种逻辑计划树。");
    }

    /**
     * 测试数据：6 名员工，包含 id、name、department、salary 四个字段。
     */
    private static List<Row> employees() {
        return List.of(
                Row.of("id", 1, "name", "Alice", "department", "eng", "salary", 72_000),
                Row.of("id", 2, "name", "Bob", "department", "ops", "salary", 45_000),
                Row.of("id", 3, "name", "Cathy", "department", "eng", "salary", 83_000),
                Row.of("id", 4, "name", "David", "department", "sales", "salary", 51_000),
                Row.of("id", 5, "name", "Eva", "department", "sales", "salary", 39_000),
                Row.of("id", 6, "name", "Frank", "department", "ops", "salary", 67_000));
    }
}
