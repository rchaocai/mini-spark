package com.sparklearn.sql;

import com.sparklearn.core.SparkContext;

import java.util.List;

import static com.sparklearn.sql.catalyst.expressions.Expressions.col;

/**
 * 第 13 章 · 从 RDD 到 DataFrame 演示入口。
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) {
        System.out.println("=".repeat(72));
        System.out.println("第 13 章 · 从 RDD 到 DataFrame");
        System.out.println("=".repeat(72));

        try (SparkContext spark = new SparkContext(2, true)) {
            SQLContext sql = new SQLContext(spark);
            DataFrame employees = sql.createDataFrame("employees", employees(), 2);

            System.out.println("\n需求一：查出薪水 > 50000 的员工姓名和调整后薪水。\n");

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
        }
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
