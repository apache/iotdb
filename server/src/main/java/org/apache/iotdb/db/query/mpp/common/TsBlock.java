package org.apache.iotdb.db.query.mpp.common;

import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * Intermediate result for most of ExecOperators.
 * The Tablet contains data from one or more columns and constructs them as a row based view
 * The columns can be series, aggregation result for one series or scalar value (such as deviceName).
 * The Tablet also contains the metadata to describe the columns.
 *
 * TODO: consider the detailed data store model in memory. (using column based or row based ?)
 */
public class TsBlock {

    // Describe the column info
    private TsBlockMetadata metadata;

    public boolean hasNext() {
        return false;
    }

    // Get next row in current tablet
    public RowRecord getNext() {
        return null;
    }

    public TsBlockMetadata getMetadata() {
        return metadata;
    }
}
