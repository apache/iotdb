package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.plan.PlanNodeId;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;

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
