package com.sparklearn.sql;

import com.sparklearn.core.SparkContext;

import java.util.List;

import static com.sparklearn.sql.catalyst.expressions.Expressions.col;

/**
 * 第 12 章 · 从 RDD 到 DataFrame 演示入口。
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

        System.out.println("\nSQL 和 DataFrame API 生成的逻辑计划一模一样。");
    }

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
