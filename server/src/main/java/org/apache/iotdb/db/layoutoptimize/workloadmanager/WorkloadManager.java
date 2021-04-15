package org.apache.iotdb.db.layoutoptimize.workloadmanager;

import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadManager {
  private static final WorkloadManager INSTANCE = new WorkloadManager();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
  private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

  public static WorkloadManager getInstance() {
    return INSTANCE;
  }

  private WorkloadManager() {}

  public void addQueryRecord(String deviceID, List<String> sensors, long interval) {
    writeLock.lock();
    try {

    } finally {
      writeLock.unlock();
    }
  }
}
