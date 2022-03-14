package org.apache.iotdb.db.query.mpp.plan.node.process;

import org.apache.iotdb.db.query.mpp.common.OrderBy;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

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
