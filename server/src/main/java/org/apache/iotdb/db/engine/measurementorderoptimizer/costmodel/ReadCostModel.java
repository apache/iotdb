package org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel;

import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;

import java.util.List;

public class ReadCostModel {
  public static float approximate(List<QueryRecord> queryRecords, List<String> measurements, List<Long> chunkSize) {
    return 0;
  }
}
