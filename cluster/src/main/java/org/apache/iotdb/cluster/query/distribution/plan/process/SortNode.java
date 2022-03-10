package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.common.TraversalOrder;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;

/**
 * In general, the parameter in sortOperator should be pushed down to the upstream operators.
 * In our optimized logical query plan, the sortOperator should not appear.
 */
public class SortNode extends ProcessNode<TsBlock> {

    private TraversalOrder sortOrder;
    
}
