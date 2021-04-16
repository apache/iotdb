package org.apache.iotdb.db.layoutoptimize.layoutoptimizer;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;

import java.util.ArrayList;
import java.util.List;

public class LayoutOptimizer {
  private List<QueryRecord> records;
  // device id should be the full path
  private String deviceId;
  private long averageChunkSize;
  private List<String> measurementOrder;
  public int recordSampleNum = 100;

  public LayoutOptimizer(String deviceId) {
    this.deviceId = deviceId;
    records =
        new ArrayList<>(
            WorkloadManager.getInstance().getSampledQueryRecord(deviceId, recordSampleNum));
  }
}
