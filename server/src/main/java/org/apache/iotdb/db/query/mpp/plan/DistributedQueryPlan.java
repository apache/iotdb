package org.apache.iotdb.db.query.mpp.plan;

import org.apache.iotdb.db.query.mpp.common.QueryContext;
import org.apache.iotdb.db.query.mpp.common.TsBlock;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNode;

import java.util.List;

public class DistributedQueryPlan {
    private QueryContext context;
    private PlanNode<TsBlock> rootNode;
    private PlanFragment rootFragment;

    //TODO: consider whether this field is necessary when do the implementation
    private List<PlanFragment> fragments;
}
