package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;

/**
 * LimitOperator is used to select top n result. It uses the default order of upstream operators
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class LimitNode extends PlanNode<TsBlock> {

    // The limit count
    private int limit;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public TsBlock getNextBatch() {
        return null;
    }
}
