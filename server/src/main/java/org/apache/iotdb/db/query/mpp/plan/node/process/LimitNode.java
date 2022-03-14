package org.apache.iotdb.db.query.mpp.plan.node.process;

import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

/**
 * LimitNode is used to select top n result. It uses the default order of upstream nodes
 *
 */
public class LimitNode extends ProcessNode {

    // The limit count
    private int limit;

    public LimitNode(PlanNodeId id) {
        super(id);
    }

    public LimitNode(PlanNodeId id, int limit) {
        this(id);
        this.limit = limit;
    }
}
