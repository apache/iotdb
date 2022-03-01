package org.apache.iotdb.db.query.distribution.operator;

import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * TimeJoinOperator is responsible for join two or more series.
 * The join algorithm is like outer join by timestamp column.
 * The output result of TimeJoinOperator is sorted by timestamp
 *
 * Children type: [SeriesScanOperator]
 */
public class TimeJoinOperator extends ExecOperator<RowRecord> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public RowRecord getNextBatch() {
        return null;
    }
}
