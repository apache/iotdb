package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.Tablet;

/**
 * LimitOperator is used to select top n result. It uses the default order of upstream operators
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class LimitOperator extends ExecOperator<Tablet> {

    // The limit count
    private int limit;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tablet getNextBatch() {
        return null;
    }
}
