package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;

/**
 * LimitOperator is used to select top n result. It uses the default order of upstream operators
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class LimitNode extends ProcessNode<TsBlock> {

    // The limit count
    private int limit;
}
