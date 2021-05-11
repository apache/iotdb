package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.VisitedMeasurements;
import org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.statisitc.ListStatistic;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkloadList {
  private final LinkedList<WorkloadItem> workloadItems = new LinkedList<>();
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  // the range of each item
  private final long ITEM_RANGE = 1L * 24L * 60L * 60L * 1000L;
  private final long ITEM_VALID_PERIOD = 30 * ITEM_RANGE;
  private long timeGrainSize = 10L;
  ListStatistic statistic;
  WorkloadItem curItem;

  public WorkloadList() {
    long curTimestamp = System.currentTimeMillis();
    curItem = new WorkloadItem(curTimestamp, curTimestamp + ITEM_RANGE, timeGrainSize, threadPool);
    statistic = new ListStatistic();
  }

  public void addRecord(String deviceId, List<String> measurement, long span) {
    if (curItem.isExpired()) {
      curItem.encapsulate();
      workloadItems.add(curItem);
      long curTime = System.currentTimeMillis();
      curItem = new WorkloadItem(curTime, curTime + ITEM_RANGE, timeGrainSize, threadPool);
    }
    curItem.addRecord(deviceId, measurement, span);
  }

  /** Drop the records that are expired */
  public void dropExpiredRecord() {
    long curTime = System.currentTimeMillis();
    while (true && workloadItems.size() > 0) {
      WorkloadItem item = workloadItems.getFirst();
      if (curTime - item.getEndTime() > ITEM_VALID_PERIOD) {
        workloadItems.removeFirst();
      } else {
        break;
      }
    }
  }

  public ListStatistic getStatistic() {
    return statistic;
  }

  /** update the statistic info of the workload list */
  public void updateStatistic() {
    statistic = new ListStatistic();
    statistic.addItemStatistic(curItem.getStatistic());
    for (WorkloadItem item : workloadItems) {
      statistic.addItemStatistic(item.getStatistic());
    }
  }

  /**
   * return the workload info of the valid workload item
   *
   * @param deviceId the id of the device to get the info
   * @return
   */
  public WorkloadInfo getWorkloadInfo(String deviceId) {
    WorkloadInfo info = new WorkloadInfo(deviceId);
    dropExpiredRecord();
    Map<VisitedMeasurements, Long> measurementMap = curItem.getMeasurementMap();
    for (Map.Entry<VisitedMeasurements, Long> measurementEntry : measurementMap.entrySet()) {
      if (measurementEntry.getKey().getDeviceId().equals(deviceId)) {
        for (String measurement : measurementEntry.getKey().getMeasurements()) {
          info.addVisitedMeasurement(measurement);
        }
      }
    }

    Map<Long, Long> spanMap = curItem.getSpanMap(deviceId);
    for (Map.Entry<Long, Long> spanEntry : spanMap.entrySet()) {
      info.addVisitedSpan(spanEntry.getKey(), spanEntry.getValue());
    }

    for (WorkloadItem item : workloadItems) {
      measurementMap = item.getMeasurementMap();
      for (Map.Entry<VisitedMeasurements, Long> measurementEntry : measurementMap.entrySet()) {
        if (measurementEntry.getKey().getDeviceId().equals(deviceId)) {
          for (String measurement : measurementEntry.getKey().getMeasurements()) {
            info.addVisitedMeasurement(measurement);
          }
        }
      }

      spanMap = item.getSpanMap(deviceId);
      for (Map.Entry<Long, Long> spanEntry : spanMap.entrySet()) {
        info.addVisitedSpan(spanEntry.getKey(), spanEntry.getValue());
      }
    }
    return info;
  }
}
