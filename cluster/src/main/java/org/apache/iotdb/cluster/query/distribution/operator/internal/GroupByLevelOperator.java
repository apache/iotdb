package org.apache.iotdb.cluster.query.distribution.operator.internal;

import org.apache.iotdb.cluster.query.distribution.common.GroupByTimeParameter;
import org.apache.iotdb.cluster.query.distribution.common.LevelBucketInfo;
import org.apache.iotdb.cluster.query.distribution.common.Tablet;

/**
 * This operator is responsible for the final aggregation merge operation. It will arrange the data
 * by time range firstly. And inside each time range, the data from same measurement and different
 * devices will be rolled up by corresponding level into different buckets. If the bucketInfo is
 * empty, the data from `same measurement and different devices` won't be rolled up. If the
 * groupByTimeParameter is null, the data won't be split by time range.
 *
 * <p>Children type: [SeriesAggregateOperator]
 */
public class GroupByLevelOperator extends InternalOperator<Tablet> {

  // All the buckets that the SeriesBatchAggInfo from upstream will be divided into.
  private LevelBucketInfo bucketInfo;

  // The parameter of `group by time`
  // The GroupByLevelOperator also need GroupByTimeParameter
  private GroupByTimeParameter groupByTimeParameter;

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Tablet getNextBatch() {
    return null;
  }
}
