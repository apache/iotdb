package org.apache.iotdb.db.query.distribution.operator;

import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.query.distribution.common.Tablet;

/**
 * (We use FilterExecOperator to distinguish itself from the FilterOperator used in single-node IoTDB)
 * The FilterExecOperator is responsible to filter the RowRecord from Tablet.
 *
 * Children type: [All the operators whose result set is Tablet]
 */
public class FilterExecOperator extends ExecOperator<Tablet> {

    // The filter
    private FilterOperator rowFilter;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Tablet getNextBatch() {
        return null;
    }
}
