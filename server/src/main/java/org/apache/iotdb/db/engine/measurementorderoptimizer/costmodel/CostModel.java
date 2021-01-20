package org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel;

import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;

import java.util.List;

public class CostModel {

  /*
   * TODO: Approximate the cost of a set of query over a specified measurement order.
   */
  public static float approximateAggregationQueryCostWithoutTimeRange(List<QueryRecord> queryRecords, List<String> measurements, List<Long> chunkSize) {
    return SeekCostModel.approximate(queryRecords, measurements, chunkSize) +
            ReadCostModel.approximate(queryRecords, measurements, chunkSize);
  }
}
