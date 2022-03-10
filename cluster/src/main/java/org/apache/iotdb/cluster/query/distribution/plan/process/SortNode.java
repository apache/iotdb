package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.OrderBy;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNodeId;

/**
 * In general, the parameter in sortNode should be pushed down to the upstream operators.
 * In our optimized logical query plan, the sortNode should not appear.
 */
public class SortNode extends ProcessNode {

    private OrderBy sortOrder;

    public SortNode(PlanNodeId id) {
        super(id);
    }

    public SortNode(PlanNodeId id, OrderBy sortOrder) {
        this(id);
        this.sortOrder = sortOrder;
    }
}
