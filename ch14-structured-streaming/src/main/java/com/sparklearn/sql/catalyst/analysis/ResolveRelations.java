package com.sparklearn.sql.catalyst.analysis;

import com.sparklearn.sql.catalyst.catalog.SessionCatalog;
import com.sparklearn.sql.catalyst.parser.ParseException;
import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.catalyst.plans.logical.UnresolvedRelation;
import com.sparklearn.sql.catalyst.rules.PlanRule;

/**
 * 解析关系规则：把 {@link UnresolvedRelation}（只有表名）换成 Catalog 里注册的逻辑计划。
 *
 * <p>这条规则是 Analyzer 的第一步——SQL 解析器产出 {@code UnresolvedRelation("employees")}，
 * 本规则查 {@link SessionCatalog} 把它替换成注册过的 {@code Scan(employees, schema, rdd)}。
 * 替换之后，上游节点（Filter / Project / Aggregate）才能从 child 的 schema 里拿到列的类型信息，
 * {@link ResolveAttributes} 才能正常工作。
 *
 * <p>参考 Spark 的 {@code ResolveRelations} 规则——Spark 调用
 * {@code catalog.lookupRelation(tableIdentifier)} 查找表，mini-spark 调用
 * {@code catalog.lookupRelation(tableName)}，没有 database 限定。
 *
 * <p>表名找不到时抛 {@link ParseException}，和解析阶段的其他错误保持一致。
 */
public final class ResolveRelations implements PlanRule {

    private final SessionCatalog catalog;

    public ResolveRelations(SessionCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan instanceof UnresolvedRelation ur) {
            if (!catalog.tableExists(ur.tableName())) {
                throw new ParseException("table not found: " + ur.tableName());
            }
            return catalog.lookupRelation(ur.tableName());
        }
        return plan;
    }
}
