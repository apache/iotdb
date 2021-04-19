package org.apache.iotdb.db.layoutoptimize.layoutoptimizer;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public abstract class LayoutOptimizer {
  protected List<QueryRecord> records;
  // device id should be the full path
  protected String deviceId;
  protected long averageChunkSize;
  protected List<String> measurementOrder;
  public int recordSampleNum = 100;

  public LayoutOptimizer(String deviceId) {
    this.deviceId = deviceId;
    records =
        new ArrayList<>(
            WorkloadManager.getInstance().getSampledQueryRecord(deviceId, recordSampleNum));
  }

  public abstract Pair<List<String>, Long> optimize();
}
