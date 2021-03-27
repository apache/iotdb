package org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel;

import org.apache.iotdb.db.engine.measurementorderoptimizer.MeasurePointEstimator;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.AggregationQueryRecord;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.GroupByQueryRecord;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CostModel {

  static float sc = 0;
  static float rc = 0;

  /**
   * Approximate the cost of a set of query over a specified measurement order.
   *
   * @param queryRecords: The optimization target query records.
   * @param measurements: The order of measurements
   * @param chunkSize:    The chunk size for each chunk in the chunk group.
   * @return The approximated cost in milliseconds
   */
  public static float approximateAggregationQueryCostWithoutTimeRange(List<QueryRecord> queryRecords, List<String> measurements, List<Long> chunkSize, int chunkGroupNum) {
    return SeekCostModel.approximate(queryRecords, measurements, chunkSize, chunkGroupNum) +
            ReadCostModel.approximate(queryRecords, measurements, chunkSize, chunkGroupNum);
  }

  /**
   * TODO: Approximate the query cost for aggregation with time range
   *
   * @param queryRecords: The optimization target query
   * @param measurements: The order of the measurements
   * @param chunkSize:    The chunk size of the measurements
   * @return The approximated cost in milliseconds
   */
  public static float approximateAggregationQueryCostWithTimeRange(List<QueryRecord> queryRecords, List<String> measurements, List<Long> chunkSize) {
    // Get the average chunk size
    long totalChunkSize = 0l;
    sc = 0;
    rc = 0;
    for (int i = 0; i < chunkSize.size(); ++i) {
      totalChunkSize += chunkSize.get(i);
    }
    long averageChunkSize = totalChunkSize / chunkSize.size();
    float tmp = approximateAggregationQueryCostWithTimeRange(queryRecords, measurements, averageChunkSize);
    return tmp;
  }

  public static float approximateAggregationQueryCostWithTimeRange(List<QueryRecord> queryRecords, List<String> measurements, long averageChunkSize) {
    // Get the point num according to the chunk size
    long chunkLength = MeasurePointEstimator.getInstance().getMeasurePointNum(averageChunkSize);
    sc = 0;
    rc = 0;
    float cost = 0.0f;
    for (QueryRecord queryRecord : queryRecords) {
      cost += approximateSingleAggregationQueryCostWithTimeRange(queryRecord, measurements, averageChunkSize);
    }
    return cost;
  }

  public static float approximateSingleAggregationQueryCostWithTimeRange(QueryRecord queryRecord, List<String> measurements, long averageChunkSize) {
    // Get the point num according to the chunk size
    long chunkLength = MeasurePointEstimator.getInstance().getMeasurePointNum(averageChunkSize);

    float cost = 0.0f;
    GroupByQueryRecord groupByRecord = (GroupByQueryRecord) queryRecord;
    long visitLength = groupByRecord.getVisitLength();
    int visitCnt = (int) Math.ceil(((double) visitLength / (double) chunkLength)) + 1;
    // Calculate the cost in single chunk group
    List<String> visitMeasurements = sortInSameOrderAndDeduplicate(queryRecord.getSensors(), measurements);
    int k = 1;
    int intervalCnt = 0;
    int startIdx = -1;
    float initSeekCost = 0.0f;
    List<Long> seekLength = new ArrayList<>();
    // Calculate the init seek cost
    for (int i = 0; i < measurements.size(); ++i) {
      if (measurements.get(i).equals(visitMeasurements.get(0))) {
        initSeekCost = SeekCostModel.getSeekCost(intervalCnt * averageChunkSize);
        seekLength.add(intervalCnt * averageChunkSize);
        cost += ReadCostModel.getReadCost(averageChunkSize);
        startIdx = i;
        intervalCnt = 0;
        break;
      } else {
        intervalCnt++;
      }
    }
    int i = startIdx + 1;
    for (; i < measurements.size() && k < visitMeasurements.size(); ++i) {
      if (measurements.get(i).equals(visitMeasurements.get(k))) {
        k++;
        cost += SeekCostModel.getSeekCost(intervalCnt * averageChunkSize);
        seekLength.add(intervalCnt * averageChunkSize);
        cost += ReadCostModel.getReadCost(averageChunkSize);
        intervalCnt = 0;
      } else {
        intervalCnt++;
      }
    }
    seekLength.add((long)(measurements.size() - i) * averageChunkSize + (long)(startIdx) * averageChunkSize);
    List<Float> seekCost = new ArrayList<>();
    for (int j = 0; j < seekLength.size(); ++j) {
      seekCost.add(SeekCostModel.getSeekCost(seekLength.get(j)));
    }
    cost += SeekCostModel.getSeekCost((long)(measurements.size() - i) * averageChunkSize + (long)(startIdx) * averageChunkSize);
    cost *= visitCnt;
    cost += initSeekCost;
    rc += ReadCostModel.getReadCost(averageChunkSize) * visitMeasurements.size() * visitCnt;
    sc += cost - ReadCostModel.getReadCost(averageChunkSize) * visitMeasurements.size() * visitCnt;
    return cost;
  }

  /**
   * Sort the order of query measurements so that the order of them are the same as the physical order of measurements.
   */
  public static List<String> sortInSameOrderAndDeduplicate(List<String> queryMeasurements, List<String> measurements) {
    List<String> sortedQueryMeasurements = new ArrayList<>();
    Set<String> measurementSet = new HashSet<>();
    for (int i = 0; i < queryMeasurements.size(); ++i) {
      measurementSet.add(queryMeasurements.get(i));
    }
    int k = 0;
    for (int i = 0; i < measurements.size() && k < queryMeasurements.size(); ++i) {
      if (measurementSet.contains(measurements.get(i))) {
        sortedQueryMeasurements.add(measurements.get(i));
        k ++;
      }
    }
    return sortedQueryMeasurements;
  }

}
