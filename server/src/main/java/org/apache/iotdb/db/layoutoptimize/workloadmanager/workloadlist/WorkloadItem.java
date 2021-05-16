package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.VisitedMeasurements;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.statisitc.ItemStatistic;

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
  private List<VisitedMeasurements> queryList = new ArrayList<>();
  // record the query span, span -> query frequency
  private Map<String, Map<Long, Long>> spanMap = new HashMap<>();
  private Map<VisitedMeasurements, Long> measurementMap = new HashMap<>();
  private ExecutorService threadPool;
  private long timeGrainSize;
  private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
  private ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
  private final long RECORD_THRESHOLD = 49L;
  private final ItemStatistic statistic = new ItemStatistic();

  public WorkloadItem(
      long startTime, long endTime, long timeGrainSize, ExecutorService threadPool) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.timeGrainSize = timeGrainSize;
    this.threadPool = threadPool;
  }

  public void addRecord(String device, List<String> measurements, long span) {
    long grainedSpan = span < timeGrainSize ? timeGrainSize : span / timeGrainSize * timeGrainSize;
    VisitedMeasurements record = new VisitedMeasurements(device, measurements);
    writeLock.lock();
    try {
      statistic.addSpan(span);
      if (!spanMap.containsKey(device)) {
        spanMap.put(device, new HashMap<>());
      }
      if (!spanMap.get(device).containsKey(grainedSpan)) {
        spanMap.get(device).put(grainedSpan, 0L);
      }
      spanMap.get(device).replace(grainedSpan, spanMap.get(device).get(grainedSpan) + 1L);

      statistic.addVisitedMeasurement(device, measurements);
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
      this.endTime = System.currentTimeMillis();
      this.queryList = null;
    } finally {
      writeLock.unlock();
    }
  }

  public long getEndTime() {
    return endTime;
  }

  public ItemStatistic getStatistic() {
    return statistic;
  }

  public Map<VisitedMeasurements, Long> getMeasurementMap() {
    return measurementMap;
  }

  public Map<Long, Long> getSpanMap(String deviceId) {
    return spanMap.get(deviceId);
  }

  private static class ListToMapTask implements Runnable {
    Map<VisitedMeasurements, Long> measurementMap;
    List<VisitedMeasurements> measurementList;
    private static Lock transferLock = new ReentrantLock();

    public ListToMapTask(
        Map<VisitedMeasurements, Long> measurementMap, List<VisitedMeasurements> measurementList) {
      this.measurementMap = measurementMap;
      this.measurementList = measurementList;
    }

    @Override
    public void run() {
      transferLock.lock();
      try {
        for (VisitedMeasurements record : measurementList) {
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
