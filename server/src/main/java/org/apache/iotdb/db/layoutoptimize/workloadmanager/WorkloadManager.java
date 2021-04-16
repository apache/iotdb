package org.apache.iotdb.db.layoutoptimize.workloadmanager;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.WorkloadList;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadManager {
  private static final WorkloadManager INSTANCE = new WorkloadManager();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
  private final WorkloadList workloadList = new WorkloadList();

  public static WorkloadManager getInstance() {
    return INSTANCE;
  }

  private WorkloadManager() {}

  public void addQueryRecord(String deviceID, List<String> sensors, long interval) {
    writeLock.lock();
    try {
      workloadList.addRecord(deviceID, sensors, interval);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * TODO: using statistic info to test if the query workload has change
   *
   * @param deviceId
   * @return
   */
  public boolean isWorkloadChanged(String deviceId) {
    workloadList.dropExpiredRecord();
    return false;
  }

  /**
   * TODO: return a sample of query record
   *
   * @param deviceId
   * @param sampleNum
   * @return
   */
  public List<QueryRecord> getSampledQueryRecord(String deviceId, int sampleNum) {
    return null;
  }
}
