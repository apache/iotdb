package org.apache.iotdb.db.query.mpp.plan.node.process;

import org.apache.iotdb.db.query.mpp.common.WithoutPolicy;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

/**
 * WithoutNode is used to discard specific rows from upstream node.
 */
public class WithoutNode extends ProcessNode {

    // The policy to discard the result from upstream operator
    private WithoutPolicy discardPolicy;

    public WithoutNode(PlanNodeId id) {
        super(id);
    }

    public WithoutNode(PlanNodeId id, WithoutPolicy discardPolicy) {
        this(id);
        this.discardPolicy = discardPolicy;
    }
}
