package org.apache.iotdb.db.query.distribution.operator;

import org.apache.iotdb.db.query.distribution.common.Tablet;
import org.apache.iotdb.db.query.distribution.common.WithoutPolicy;

/**
 * WithoutOperator is used to discard specific result from upstream operators.
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class WithoutOperator extends ExecOperator<Tablet> {

    // The policy to discard the result from upstream operator
    private WithoutPolicy discardPolicy;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tablet getNextBatch() {
        return null;
    }
}
