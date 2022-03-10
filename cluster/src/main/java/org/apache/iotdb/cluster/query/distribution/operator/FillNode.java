package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.FillPolicy;
import org.apache.iotdb.cluster.query.distribution.common.TsBlock;

/**
 * FillOperator is used to fill the empty field in one row.
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class FillNode extends PlanNode<TsBlock> {

    // The policy to discard the result from upstream operator
    private FillPolicy fillPolicy;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public TsBlock getNextBatch() {
        return null;
    }
}
