package org.apache.iotdb.db.engine.divergentdesign;

import org.apache.iotdb.db.engine.measurementorderoptimizer.MeasurementOrderOptimizer;
import org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel.CostModel;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.AggregationQueryRecord;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class Replica {
  private String deviceId;
  private List<String> measurements;
  private List<Long> chunkSize;
  private int totalChunkGroupNum;
  private long averageChunkSize;

  public Replica(Replica replica) {
    this.deviceId = replica.deviceId;
    this.measurements = new ArrayList<>(replica.measurements);
    this.chunkSize = new ArrayList<>(replica.chunkSize);
    this.totalChunkGroupNum = replica.totalChunkGroupNum;
    this.averageChunkSize = replica.averageChunkSize;
  }

  public Replica(String deviceId) {
    this.deviceId = deviceId;
    this.measurements = new ArrayList<>(MeasurementOrderOptimizer.getInstance()
            .getMeasurementsOrder(deviceId));
    this.chunkSize = new ArrayList<>(MeasurementOrderOptimizer.getInstance().getChunkSize(deviceId));
  }

  public Replica(String deviceId, List<String> measurements) {
    this.deviceId = deviceId;
    this.measurements = new ArrayList<>(measurements);
    chunkSize = new ArrayList<>(MeasurementOrderOptimizer.getInstance().getChunkSize(deviceId));
    BigInteger totalChunkSize = new BigInteger("0");
    for(int i = 0; i < chunkSize.size(); ++i) {
      totalChunkSize = totalChunkSize.add(new BigInteger(String.valueOf(chunkSize.get(i))));
    }
    averageChunkSize = totalChunkSize.divide(new BigInteger(String.valueOf(chunkSize.size()))).longValue();
  }

  public Replica(String deviceId, List<String> measurements, long averageChunkSize) {
    this.deviceId = deviceId;
    this.averageChunkSize = averageChunkSize;
    this.measurements = new ArrayList<>(measurements);
    chunkSize = new ArrayList<Long>();
    for(int i = 0; i < measurements.size(); ++i) {
      chunkSize.add(averageChunkSize);
    }
  }

  public float calCostForQuery(QueryRecord query) {
    float cost = 0.0f;
    if (query instanceof AggregationQueryRecord) {
      List<QueryRecord> recordList = new ArrayList<>();
      recordList.add(query);
      cost = CostModel.approximateAggregationQueryCostWithoutTimeRange(recordList, measurements, chunkSize, totalChunkGroupNum);
    } else {
      cost = CostModel.approximateSingleAggregationQueryCostWithTimeRange(query, measurements, averageChunkSize);
    }
    return cost;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public List<Long> getChunkSize() {
    return chunkSize;
  }

  public void swapMeasurementPos(int left, int right) {
    String tempMeasurement = measurements.get(left);
    measurements.set(left, measurements.get(right));
    measurements.set(right, tempMeasurement);
    long tempChunkSize = chunkSize.get(left);
    chunkSize.set(left, chunkSize.get(right));
    chunkSize.set(right, tempChunkSize);
  }

  public void setAverageChunkSize(long averageChunkSize) {
    this.averageChunkSize = averageChunkSize;
  }

  public long getAverageChunkSize() {
    return averageChunkSize;
  }
}
