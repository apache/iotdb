package org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel;

import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;

import java.util.List;

public class ReadCostModel {
  private static float readSpeed = 80645.0f / 1.1f;
  public static float approximate(List<QueryRecord> queryRecords, List<String> measurements, List<Long> chunkSize, int chunkGroupNum) {
    float totalCost = 0;
    for(QueryRecord queryRecord : queryRecords) {
      List<String> queryMeasurements = queryRecord.getSensors();
      List<String> sortedAndDeduplicatedQueryMeasurements = CostModel.sortInSameOrderAndDeduplicate(queryMeasurements, measurements);
      for(int i = 0, k = 0; i < sortedAndDeduplicatedQueryMeasurements.size(); ++i) {
        while(!sortedAndDeduplicatedQueryMeasurements.get(i).equals(measurements.get(k))) {
          k++;
        }
        totalCost += getReadCost(chunkSize.get(k));
      }
    }
    return totalCost * chunkGroupNum;
  }

  public static float getReadCost(long chunkSize) {
    return ((float)chunkSize)/readSpeed;
  }
}
