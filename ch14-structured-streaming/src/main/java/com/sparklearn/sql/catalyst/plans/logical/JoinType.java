package com.sparklearn.sql.catalyst.plans.logical;

/**
 * JOIN 类型。
 *
 * <p>本章只覆盖最常用的两种：
 * <ul>
 *   <li>{@link #INNER}：内连接，只输出两侧都匹配的行</li>
 *   <li>{@link #LEFT_OUTER}：左外连接，左表无匹配时补 NULL 右列</li>
 * </ul>
 */
public enum JoinType {

    INNER,
    LEFT_OUTER;

    public String sql() {
        return switch (this) {
            case INNER -> "INNER";
            case LEFT_OUTER -> "LEFT OUTER";
        };
    }
}
