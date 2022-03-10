package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;

/**
 * (We use FilterExecOperator to distinguish itself from the FilterOperator used in single-node IoTDB)
 * The FilterExecOperator is responsible to filter the RowRecord from Tablet.
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class FilterNode extends ProcessNode<TsBlock> {

    // The filter
    private FilterOperator rowFilter;
}
