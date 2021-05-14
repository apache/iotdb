package org.apache.iotdb.db.layoutoptimize.layoutoptimizer;

import org.apache.iotdb.db.exception.layoutoptimize.LayoutNotExistException;
import org.apache.iotdb.db.layoutoptimize.layoutholder.LayoutHolder;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.WorkloadManager;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public abstract class LayoutOptimizer {
  protected List<QueryRecord> records;
  // device id should be the full path
  protected PartialPath device;
  protected long averageChunkSize;
  protected List<String> measurementOrder;
  protected OptimizeConfig config;

  public LayoutOptimizer(PartialPath device) {
    this.device = device;
    this.config = new OptimizeConfig();
    LayoutHolder holder = LayoutHolder.getInstance();
    if (!holder.hasLayoutForDevice(device.getFullPath())) {
      holder.updateMetadata();
    }
    try {
      measurementOrder = holder.getMeasurementForDevice(device.getFullPath());
      averageChunkSize = holder.getChunkSize(device.getFullPath());
    } catch (LayoutNotExistException e) {
      e.printStackTrace();
    }
  }

  public LayoutOptimizer(PartialPath device, OptimizeConfig config) {
    this.device = device;
    this.config = config;
  }

  public final void invoke() {
    if (measurementOrder == null) {
      return;
    }
    WorkloadManager manager = WorkloadManager.getInstance();
    if (manager.isWorkloadChanged(device.getFullPath())) {
      this.records =
          manager.getSampledQueryRecord(device.getFullPath(), config.getRecordSampleNum());
      Pair<List<String>, Long> optimizedLayout = optimize();
      LayoutHolder.getInstance()
          .setLayout(device.getFullPath(), optimizedLayout.left, optimizedLayout.right);
      LayoutHolder.getInstance();
    }
  }

  public abstract Pair<List<String>, Long> optimize();
}
