package org.apache.iotdb.cluster.query.distribution.common;

import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;

/**
 * SeriesBatchAggInfo is the "batch" result of SeriesAggregateOperator when its getNextBatch() is invoked.
 */
public class SeriesBatchAggInfo {
    // Path of the series.
    // Path will be used in the downstream operators.
    // GroupByLevelOperator will use it to divide the data into different buckets to do the rollup operation.
    private Path path;

    // Time range of current statistic.
    private TimeRange timeRange;

    // Statistics for the series in current time range
    private Statistics<?> statistics;
}
