package org.apache.iotdb.db.layoutoptimize.workloadmanager;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.WorkloadInfo;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.WorkloadList;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.statisitc.ListStatistic;

import java.util.LinkedList;
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
   * using the statistic info to judge if the workload is changed
   *
   * @param deviceId the id of the device to judge on
   * @return true if the workload changes else false
   */
  public boolean isWorkloadChanged(String deviceId) {
    readLock.lock();
    try {
      ListStatistic oriStatistic = workloadList.getStatistic();
      workloadList.dropExpiredRecord();
      workloadList.updateStatistic();
      ListStatistic newStatistic = workloadList.getStatistic();
      return !oriStatistic.isTheSame(newStatistic);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * generate a sample of query record according to the collected info in workload manager
   *
   * @param deviceId the id of the device to sample on
   * @param sampleNum the number of the sampled query record
   * @return the list of the query record
   */
  public List<QueryRecord> getSampledQueryRecord(String deviceId, int sampleNum) {
    readLock.lock();
    try {
      WorkloadInfo info = workloadList.getWorkloadInfo(deviceId);
      List<QueryRecord> records = new LinkedList<>();
      for (int i = 0; i < sampleNum; i++) {
        records.add(info.sample());
      }
      return records;
    } finally {
      readLock.unlock();
    }
  }
}
