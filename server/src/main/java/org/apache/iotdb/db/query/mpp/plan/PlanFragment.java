package org.apache.iotdb.db.query.mpp.plan;

import org.apache.iotdb.db.query.mpp.common.TsBlock;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNode;

//TODO: consider whether it is necessary to make PlanFragment as a TreeNode
/**
 * PlanFragment contains a sub-query of distributed query.
 */
public class PlanFragment {
    private PlanFragmentId id;
    private PlanNode<TsBlock> root;
}
