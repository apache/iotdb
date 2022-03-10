package org.apache.iotdb.cluster.query.distribution.plan.process;

import org.apache.iotdb.cluster.query.distribution.common.GroupByTimeParameter;
import org.apache.iotdb.cluster.query.distribution.common.SeriesBatchAggInfo;
import org.apache.iotdb.cluster.query.distribution.common.TsBlock;
import org.apache.iotdb.cluster.query.distribution.plan.PlanNode;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;

/**
 * SeriesAggregateOperator is responsible to do the aggregation calculation for one series.
 * This operator will split data in one series into many groups by time range and do the aggregation calculation for each
 * group.
 * If there is no split parameter, it will return one result which is the aggregation result of all data in current series.
 *
 * Children type: [SeriesScanOperator]
 */
public class SeriesAggregateNode extends ProcessNode<TsBlock> {

    // The parameter of `group by time`
    // Its value will be null if there is no `group by time` clause,
    private GroupByTimeParameter groupByTimeParameter;

    // TODO: need consider how to represent the aggregation function and corresponding implementation
    // We use a String to indicate the parameter temporarily
    private String aggregationFunc;

    // This method will only be invoked by SeriesAggregateOperator
    // It will return the statistics of the series in given time range
    // When calculate the statistics, the operator should use the most optimized way to do that. In
    // other words, using
    // raw data is the final way to do that.
    public Statistics<?> getNextStatisticBetween(TimeRange timeRange) {
        return null;
    }
}
