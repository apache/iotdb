package org.apache.iotdb.db.query.mpp.plan.optimzation;

import org.apache.iotdb.db.query.mpp.common.QueryContext;
import org.apache.iotdb.db.query.mpp.common.TsBlock;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNode;

public interface PlanOptimizer {
    PlanNode<TsBlock> optimize(PlanNode<TsBlock> plan, QueryContext context);
}
