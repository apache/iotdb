package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.TsBlock;

/**
 * OffsetOperator is used to skip top n result from upstream operators. It uses the default order of upstream operators
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class OffsetNode extends PlanNode<TsBlock> {

    // The limit count
    private int offset;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public TsBlock getNextBatch() {
        return null;
    }
}
