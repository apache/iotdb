package org.apache.iotdb.db.query.distribution.operator;

import org.apache.iotdb.db.query.distribution.common.Tablet;

/**
 * OffsetOperator is used to skip top n result from upstream operators. It uses the default order of upstream operators
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class OffsetOperator extends ExecOperator<Tablet> {

    // The limit count
    private int offset;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tablet getNextBatch() {
        return null;
    }
}
