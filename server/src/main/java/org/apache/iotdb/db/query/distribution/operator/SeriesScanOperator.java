package org.apache.iotdb.db.query.distribution.operator;

import org.apache.iotdb.db.query.distribution.common.SeriesBatchData;
import org.apache.iotdb.db.query.distribution.common.TraversalOrder;
import org.apache.iotdb.tsfile.read.common.Path;
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
public class SeriesScanOperator extends ExecOperator<SeriesBatchData> {

    // The path of the target series which will be scanned.
    private Path seriesPath;

    // The order to traverse the series by timestamp.
    // The default order is ASC, which means "order by timestamp asc"
    private TraversalOrder scanOrder = TraversalOrder.ASC;

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
}
