package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadItem {
  // the record start time and end time of the item
  private long startTime;
  private long endTime;
  // record the query measurements
  private List<QueryRecord> queryList = new ArrayList<>();
  // record the query interval, interval -> query frequency
  private Map<Long, Long> intervalMap = new HashMap<>();
  private Map<QueryRecord, Long> measurementMap = new HashMap<>();
  private ExecutorService threadPool;
  private long timeGrainSize;
  private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
  private ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
  private final int RECORD_THRESHOLD = 100000;

  public WorkloadItem(
      long startTime, long endTime, long timeGrainSize, ExecutorService threadPool) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.timeGrainSize = timeGrainSize;
    this.threadPool = threadPool;
  }

  public void addRecord(String device, List<String> measurements, long interval) {
    long grainedInterval =
        interval < timeGrainSize ? timeGrainSize : interval / timeGrainSize * timeGrainSize;
    QueryRecord record = new QueryRecord(device, measurements);
    writeLock.lock();
    try {
      if (!intervalMap.containsKey(grainedInterval)) {
        intervalMap.put(grainedInterval, 0L);
      }
      intervalMap.replace(grainedInterval, intervalMap.get(grainedInterval) + 1L);

      queryList.add(record);
      if (queryList.size() >= RECORD_THRESHOLD) {
        threadPool.submit(new ListToMapTask(measurementMap, queryList));
        this.queryList = new ArrayList<>();
      }
    } finally {
      writeLock.unlock();
    }
  }

  public boolean isExpired() {
    return endTime < System.currentTimeMillis();
  }

  public void encapsulate() {
    writeLock.lock();
    try {
      threadPool.submit(new ListToMapTask(measurementMap, queryList));
      this.queryList = null;
    } finally {
      writeLock.unlock();
    }
  }

  private static class ListToMapTask implements Runnable {
    Map<QueryRecord, Long> measurementMap;
    List<QueryRecord> measurementList;
    private static Lock transferLock = new ReentrantLock();

    public ListToMapTask(Map<QueryRecord, Long> measurementMap, List<QueryRecord> measurementList) {
      this.measurementMap = measurementMap;
      this.measurementList = measurementList;
    }

    @Override
    public void run() {
      transferLock.lock();
      try {
        for (QueryRecord record : measurementList) {
          record.calHashCode();
          if (!measurementMap.containsKey(record)) {
            measurementMap.put(record, 1L);
          } else {
            measurementMap.replace(record, measurementMap.get(record) + 1L);
          }
        }
      } finally {
        transferLock.unlock();
      }
    }
  }
}
