package com.sparklearn.sql;

import com.sparklearn.sql.catalyst.plans.logical.LogicalPlan;
import com.sparklearn.sql.execution.PhysicalPlan;

/**
 * 一次 DataFrame action 背后的查询执行过程。
 */
public record QueryExecution(
        LogicalPlan logical,
        LogicalPlan optimized,
        PhysicalPlan executed) {

    public String explainString() {
        return """
                == Logical Plan ==
                %s

                == Optimized Logical Plan ==
                %s

                == Physical Plan ==
                %s
                """.formatted(
                logical.treeString(),
                optimized.treeString(),
                executed.treeString());
    }
}
