package org.apache.iotdb.cluster.query.distribution.operator;

import org.apache.iotdb.cluster.query.distribution.common.SeriesBatchData;
import org.apache.iotdb.cluster.query.distribution.common.TraversalOrder;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * SeriesScanOperator is responsible for read data and pre-aggregated statistic for a specific series.
 * When reading data, the SeriesScanOperator can read the raw data batch by batch. And also, it can leverage
 * the filter and other info to decrease the result set.
 * Besides, the SeriesScanOperator can read the pre-aggregated statistic in TsFile. And return the statistic with
 * a fix time range one by one. If the time range is narrower than the smallest pre-aggregated statistic or has overlap
 * with pre-aggregated statistic, the SeriesScanOperator will read the raw data and calculate the aggregation result for
 * specific time range.
 *
 * Children type: []
 */
public class SeriesScanNode extends PlanNode<SeriesBatchData> {

    // The path of the target series which will be scanned.
    private Path seriesPath;

    // The order to traverse the data.
    // Currently, we only support TIMESTAMP_ASC and TIMESTAMP_DESC here.
    // The default order is TIMESTAMP_ASC, which means "order by timestamp asc"
    private TraversalOrder scanOrder = TraversalOrder.TIMESTAMP_ASC;

    // Filter data in current series.
    private Filter filter;

    // Limit for result set. The default value is -1, which means no limit
    private int limit = -1;

    // offset for result set. The default value is 0
    private int offset;

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public SeriesBatchData getNextBatch() {
        return null;
    }

    // This method will only be invoked by SeriesAggregateOperator
    // It will return the statistics of the series in given time range
    // When calculate the statistics, the operator should use the most optimized way to do that. In other words, using
    // raw data is the final way to do that.
    public Statistics<?> getNextStatisticBetween(TimeRange timeRange) {
        return null;
    }
}
