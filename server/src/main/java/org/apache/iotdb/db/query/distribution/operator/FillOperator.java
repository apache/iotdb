package org.apache.iotdb.db.query.distribution.operator;

import org.apache.iotdb.db.query.distribution.common.FillPolicy;
import org.apache.iotdb.db.query.distribution.common.Tablet;
import org.apache.iotdb.db.query.distribution.common.WithoutPolicy;

/**
 * FillOperator is used to fill the empty field in one row.
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class FillOperator extends ExecOperator<Tablet> {

    // The policy to discard the result from upstream operator
    private FillPolicy fillPolicy;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tablet getNextBatch() {
        return null;
    }
}
