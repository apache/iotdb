package org.apache.iotdb.cluster.query.distribution.common;

import org.apache.iotdb.tsfile.read.common.RowRecord;

/**
 * Intermediate result for most of ExecOperators.
 * The Tablet contains data from one or more series and constructs them as a row based view
 * The Tablet constructed with n series has n+1 columns where one column is timestamp and the other n columns are values
 * from each series.
 * The Tablet also contains the metadata of owned series.
 *
 * TODO: consider the detailed data store model in memory. (using column based or row based ?)
 */
public class Tablet {

    private TabletMetadata metadata;

    public boolean hasNext() {
        return false;
    }

    public RowRecord getNext() {
        return null;
    }

    public TabletMetadata getMetadata() {
        return metadata;
    }
}
