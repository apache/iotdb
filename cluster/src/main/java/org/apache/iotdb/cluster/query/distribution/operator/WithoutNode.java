package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.common.WithoutPolicy;

/**
 * WithoutOperator is used to discard specific result from upstream operators.
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class WithoutNode extends PlanNode<TsBlock> {

    // The policy to discard the result from upstream operator
    private WithoutPolicy discardPolicy;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public TsBlock getNextBatch() {
        return null;
    }
}
