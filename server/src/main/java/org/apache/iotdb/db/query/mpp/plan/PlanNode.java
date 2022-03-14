package org.apache.iotdb.db.query.mpp.plan;


import org.apache.iotdb.db.query.mpp.common.TreeNode;

/**
 * @author xingtanzjr
 * The base class of query executable operators, which is used to compose logical query plan.
 * TODO: consider how to restrict the children type for each type of ExecOperator
 */
public abstract class PlanNode<T> extends TreeNode<PlanNode<T>> {
    private PlanNodeId id;
    public PlanNode(PlanNodeId id) {
        this.id = id;
    }
}
