package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.common.TraversalOrder;

/**
 * In general, the parameter in sortOperator should be pushed down to the upstream operators.
 * In our optimized logical query plan, the sortOperator should not appear.
 */
public class SortNode extends PlanNode<TsBlock> {

    private TraversalOrder sortOrder;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public TsBlock getNextBatch() {
        return null;
    }
}
