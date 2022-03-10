package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.plan.PlanNodeId;

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
