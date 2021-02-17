package org.apache.iotdb.db.engine.divergentdesign;

import org.apache.iotdb.db.engine.measurementorderoptimizer.MeasurementOrderOptimizer;
import org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel.CostModel;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.AggregationQueryRecord;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;

import java.util.ArrayList;
import java.util.List;

public class Replica {
  private String deviceId;
  private List<String> measurements;
  private List<Long> chunkSize;
  private int totalChunkGroupNum;

  public Replica(String deviceId) {
    this.deviceId = deviceId;
    this.measurements = new ArrayList<>(MeasurementOrderOptimizer.getInstance()
            .getMeasurementsOrder(deviceId));
    this.chunkSize = MeasurementOrderOptimizer.getInstance().getChunkSize(deviceId);
  }

  public Replica(String deviceId, List<String> measurements, long averageChunkSize) {
    this.deviceId = deviceId;
    this.measurements = measurements;
    chunkSize = new ArrayList<Long>();
    for(int i = 0; i < measurements.size(); ++i) {
      chunkSize.add(averageChunkSize);
    }
  }

  public float calCostForQuery(QueryRecord query) {
    float cost = 0.0f;
    List<QueryRecord> recordList = new ArrayList<>();
    recordList.add(query);
    if (query instanceof AggregationQueryRecord) {
      cost = CostModel.approximateAggregationQueryCostWithoutTimeRange(recordList, measurements, chunkSize, totalChunkGroupNum);
    } else {
      cost = CostModel.approximateAggregationQueryCostWithTimeRange(recordList, measurements, chunkSize);
    }
    return cost;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public String getDeviceId() {
    return deviceId;
  }
}
