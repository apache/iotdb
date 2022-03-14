package org.apache.iotdb.db.query.mpp.plan;

import org.apache.iotdb.db.query.mpp.common.QueryContext;
import org.apache.iotdb.db.query.mpp.common.TsBlock;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNode;

/**
 * LogicalQueryPlan represents a logical query plan. It stores the root node of corresponding query plan node tree.
 */
public class LogicalQueryPlan {
    private QueryContext context;
    private PlanNode<TsBlock> rootNode;
}
