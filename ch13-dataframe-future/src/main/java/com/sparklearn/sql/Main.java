package com.sparklearn.sql;

import com.sparklearn.core.SparkContext;
import com.sparklearn.sql.execution.WholeStageCodegenExec;

import java.util.List;
import java.util.function.Function;

import static com.sparklearn.sql.catalyst.expressions.Expressions.col;

/**
 * 第 13 章 · 从 RDD 到 DataFrame 演示入口。
 *
 * <p>通过几个典型需求展示 DataFrame API 的使用：
 * <ul>
 *   <li>需求一：过滤 + 投影，演示谓词下推和列裁剪</li>
 *   <li>需求二：分组聚合，演示 Aggregate 落回 RDD.reduceByKey</li>
 *   <li>需求三：JOIN，演示 DataFrame API 和 SQL 汇到同一种逻辑计划</li>
 *   <li>需求四：whole-stage codegen，演示把多个算子融合成一段编译出来的 Java 代码</li>
 * </ul>
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
            DataFrame employees = sql.createDataFrame(employees(), employeesSchema());

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

            System.out.println("\n" + "=".repeat(72));
            System.out.println("需求三：用 DataFrame API 做 JOIN");
            System.out.println("=".repeat(72));

            DataFrame departments = sql.createDataFrame(departments(), departmentsSchema());

            DataFrame joined = employees
                    .join(departments, col("department").equalTo(col("dept_code")))
                    .select(col("name"), col("dept_name"));
            System.out.println(joined.explainString());
            System.out.println("结果：");
            joined.show();
            // 预期结果：每个员工关联到部门名
            // Alice→Engineering, Bob→Operations, Cathy→Engineering,
            // David→Sales, Eva→Sales, Frank→Operations

            // ---- SQL 演示 ----
            System.out.println("\n" + "=".repeat(72));
            System.out.println("SQL 演示：用 SQL 字符串做同样的查询");
            System.out.println("=".repeat(72));

            demoSQL(sql);

            // ---- 代码生成演示 ----
            System.out.println("\n" + "=".repeat(72));
            System.out.println("代码生成演示：whole-stage codegen");
            System.out.println("=".repeat(72));

            demoCodegen(sql);

            // ---- UDF + 内置聚合函数演示 ----
            System.out.println("\n" + "=".repeat(72));
            System.out.println("UDF + 内置聚合函数演示：sum / avg / 自定义函数");
            System.out.println("=".repeat(72));

            demoUDF(sql);
        }
    }

    private static void demoSQL(SQLContext sql) {
        // 显式指定 schema，和数据一起包进 Scan 节点
        DataFrame employees = sql.createDataFrame(employees(), employeesSchema());
        employees.createOrReplaceTempView("employees");

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

        // ---- JOIN 演示 ----
        System.out.println("\nSQL 查询（JOIN）：SELECT name, dept_name FROM employees JOIN departments ON department = dept_code\n");

        DataFrame departments = sql.createDataFrame(departments(), departmentsSchema());
        departments.createOrReplaceTempView("departments");

        DataFrame joined = sql.sql(
                "SELECT name, dept_name FROM employees JOIN departments ON department = dept_code");
        System.out.println(joined.explainString());
        System.out.println("结果：");
        joined.show();
        // 预期结果：每个员工关联到部门名
        // Alice→Engineering, Bob→Operations, Cathy→Engineering,
        // David→Sales, Eva→Sales, Frank→Operations

        System.out.println("\nSQL 查询（LEFT JOIN + WHERE）：SELECT name, dept_name FROM employees LEFT JOIN departments ON department = dept_code WHERE salary > 50000\n");

        DataFrame leftJoined = sql.sql(
                "SELECT name, dept_name FROM employees LEFT JOIN departments ON department = dept_code WHERE salary > 50000");
        System.out.println(leftJoined.explainString());
        System.out.println("结果：");
        leftJoined.show();

        System.out.println("\nSQL 查询（限定列名）：SELECT name, dept_name FROM employees JOIN departments ON employees.department = departments.dept_code\n");

        DataFrame qualifiedJoined = sql.sql(
                "SELECT name, dept_name FROM employees JOIN departments ON employees.department = departments.dept_code");
        System.out.println(qualifiedJoined.explainString());
        System.out.println("结果：");
        qualifiedJoined.show();
        // 限定列名 table.col：JOIN 同名列歧义时用，结果和第一个 JOIN 一致

        System.out.println("\nSQL 和 DataFrame API 会汇到同一种逻辑计划树。");
    }

    /**
     * 演示 whole-stage codegen：物理计划里出现了 WholeStageCodegenExec，
     * 它把 ScanExec + ProjectExec 融成一段编译出来的 Java 代码。
     */
    private static void demoCodegen(SQLContext sql) {
        DataFrame employees = sql.createDataFrame(employees(), employeesSchema());

        DataFrame adjustedSalary = employees
                .where(col("salary").gt(50_000))
                .select(
                        col("name"),
                        col("salary").multiply(1.25).as("adjusted_salary"));

        System.out.println("\n物理计划（注意 WholeStageCodegenExec 包住了 ScanExec 和 ProjectExec）：");
        QueryExecution queryExecution = adjustedSalary.queryExecution();
        System.out.println(queryExecution.executed().treeString());

        // 打印生成的 Java 源码
        if (queryExecution.executed() instanceof WholeStageCodegenExec wsce) {
            System.out.println("生成的 Java 源码（由 Janino 编译后执行）：");
            System.out.println("-".repeat(72));
            System.out.println(wsce.generatedSource());
            System.out.println("-".repeat(72));
        }

        System.out.println("执行结果（和需求一一致，但走的是编译出来的代码）：");
        adjustedSalary.show();
    }

    /**
     * 演示 UDF（用户自定义函数）和内置聚合函数 sum / avg。
     *
     * <p>UDF 注册流程：用户传入 Lambda → 包装成 FunctionBuilder → 存入 FunctionRegistry。
     * SQL 中出现函数名时，解析器通过 FunctionRegistry.lookupFunction 构造 UserDefinedFunction 表达式。
     *
     * <p>内置聚合函数 sum / avg 在 SimpleFunctionRegistry 构造时自动注册，
     * 走与 count 完全相同的 HashAggregateExec → reduceByKey 路径。
     */
    private static void demoUDF(SQLContext sql) {
        DataFrame employees = sql.createDataFrame(employees(), employeesSchema());
        employees.createOrReplaceTempView("employees");

        // 注册 UDF：按薪水分级
        Function<Object, Object> salaryLevel = salary -> {
            int s = ((Number) salary).intValue();
            if (s >= 70000) return "high";
            if (s >= 50000) return "mid";
            return "low";
        };
        sql.registerFunction("salaryLevel", salaryLevel, DataType.STRING);

        // UDF + GROUP BY：按薪水分级统计人数
        System.out.println("\nSQL 查询（UDF）：SELECT salaryLevel(salary), count(*) FROM employees GROUP BY salaryLevel(salary)\n");

        DataFrame levelCounts = sql.sql(
                "SELECT salaryLevel(salary), count(*) FROM employees GROUP BY salaryLevel(salary)");
        System.out.println(levelCounts.explainString());
        System.out.println("结果：");
        levelCounts.show();
        // 预期结果：high=2(Alice,Cathy), mid=2(David,Frank), low=2(Bob,Eva)

        // 内置聚合函数 sum：按部门统计薪水总和
        System.out.println("\nSQL 查询（sum）：SELECT department, sum(salary) FROM employees GROUP BY department\n");

        DataFrame sumByDept = sql.sql(
                "SELECT department, sum(salary) FROM employees GROUP BY department");
        System.out.println(sumByDept.explainString());
        System.out.println("结果：");
        sumByDept.show();
        // 预期结果：eng=155000, ops=112000, sales=90000

        // 内置聚合函数 avg：按部门统计平均薪水
        System.out.println("\nSQL 查询（avg）：SELECT department, avg(salary) FROM employees GROUP BY department\n");

        DataFrame avgByDept = sql.sql(
                "SELECT department, avg(salary) FROM employees GROUP BY department");
        System.out.println(avgByDept.explainString());
        System.out.println("结果：");
        avgByDept.show();
        // 预期结果：eng=77500.0, ops=56000.0, sales=45000.0

        System.out.println("\nUDF 和内置聚合函数都通过 FunctionRegistry 注册，走相同的解析和执行链路。");
    }

    /**
     * 员工表 schema：id、name、department、salary 四个字段。
     */
    private static Schema employeesSchema() {
        return Schema.of(
                new Field("id", DataType.INTEGER),
                new Field("name", DataType.STRING),
                new Field("department", DataType.STRING),
                new Field("salary", DataType.INTEGER));
    }

    /**
     * 部门表 schema：dept_code、dept_name 两个字段。
     */
    private static Schema departmentsSchema() {
        return Schema.of(
                new Field("dept_code", DataType.STRING),
                new Field("dept_name", DataType.STRING));
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

    /**
     * 测试数据：3 个部门，包含 dept_code、dept_name 两个字段。
     * dept_code 与 employees.department 对应，用于 JOIN。
     */
    private static List<Row> departments() {
        return List.of(
                Row.of("dept_code", "eng", "dept_name", "Engineering"),
                Row.of("dept_code", "ops", "dept_name", "Operations"),
                Row.of("dept_code", "sales", "dept_name", "Sales"));
    }
}
