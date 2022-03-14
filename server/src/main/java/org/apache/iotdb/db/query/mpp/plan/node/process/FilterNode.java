package org.apache.iotdb.db.query.mpp.plan.node.process;

import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

/**
 * The FilterNode is responsible to filter the RowRecord from TsBlock.
 */
public class FilterNode extends ProcessNode {

    // The filter
    private FilterOperator rowFilter;

    public FilterNode(PlanNodeId id) {
        super(id);
    }

    public FilterNode(PlanNodeId id, FilterOperator rowFilter) {
        this(id);
        this.rowFilter = rowFilter;
    }
}
