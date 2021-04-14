package org.apache.iotdb.db.layoutoptimize.workloadmanager;

public class WorkloadManager {
  private static final WorkloadManager INSTANCE = new WorkloadManager();

  public static WorkloadManager getInstance() {
    return INSTANCE;
  }

  private WorkloadManager() {}
}
