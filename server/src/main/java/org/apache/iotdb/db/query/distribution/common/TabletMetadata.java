package org.apache.iotdb.db.query.distribution.common;

import org.apache.iotdb.tsfile.read.common.Path;

import java.util.List;

public class TabletMetadata {
    // list of all columns in current Tablet
    // The column list not only contains the series column, but also contains other column to construct the final result
    // set such as timestamp and deviceName
    private List<String> columnList;

    // Indicate whether the result set should be aligned by device. This parameter can be used for downstream operators
    // when processing data from current Tablet. The RowRecord produced by Tablet with `alignedByDevice = true` will contain
    // n + 1 fields which are n series field and 1 deviceName field.
    // For example, when the FilterOperator execute the filter operation, it may need the deviceName field when matching
    // the series with corresponding column in Tablet
    //
    // If alignedByDevice is true, the owned series should belong to one device
    private boolean alignedByDevice;
}
