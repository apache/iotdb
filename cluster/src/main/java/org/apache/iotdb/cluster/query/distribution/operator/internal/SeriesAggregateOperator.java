package org.apache.iotdb.cluster.query.distribution.operator.internal;

import org.apache.iotdb.cluster.query.distribution.common.GroupByTimeParameter;
import org.apache.iotdb.cluster.query.distribution.common.SeriesBatchAggInfo;

/**
 * SeriesAggregateOperator is responsible to do the aggregation calculation for one series. This
 * operator will split data in one series into many groups by time range and do the aggregation
 * calculation for each group. If there is no split parameter, it will return one result which is
 * the aggregation result of all data in current series.
 *
 * <p>Children type: [SeriesScanOperator]
 */
public class SeriesAggregateOperator extends InternalOperator<SeriesBatchAggInfo> {

  // The parameter of `group by time`
  // Its value will be null if there is no `group by time` clause,
  private GroupByTimeParameter groupByTimeParameter;

  // TODO: need consider how to represent the aggregation function and corresponding implementation
  // We use a String to indicate the parameter temporarily
  private String aggregationFunc;

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public SeriesBatchAggInfo getNextBatch() {
    return null;
  }
}
