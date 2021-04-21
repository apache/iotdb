package org.apache.iotdb.db.layoutoptimize.layoutoptimizer;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public abstract class LayoutOptimizer {
  protected List<QueryRecord> records;
  // device id should be the full path
  protected PartialPath device;
  protected long averageChunkSize;
  protected List<String> measurementOrder;
  protected OptimizeConfig config = new OptimizeConfig();

  public LayoutOptimizer(PartialPath device) {
    this.device = device;
    records =
        new ArrayList<>(
            WorkloadManager.getInstance()
                .getSampledQueryRecord(device.getDevice(), config.getRecordSampleNum()));
  }

  public abstract Pair<List<String>, Long> optimize();
}
