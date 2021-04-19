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
  private long timeGrainSize = 1000L * 60L;
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

  /**
   * Drop the records that are expired
   *
   * @return true if some records has been drop else false
   */
  public boolean dropExpiredRecord() {
    long curTime = System.currentTimeMillis();
    boolean flag = false;
    while (true) {
      WorkloadItem item = workloadItems.getFirst();
      if (curTime - item.getEndTime() > ITEM_VALID_PERIOD) {
        workloadItems.removeFirst();
        flag = true;
      } else {
        break;
      }
    }
    return flag;
  }

  public ListStatistic getStatistic() {
    return statistic;
  }

  public void updateStatistic() {
    statistic = new ListStatistic();
    for (WorkloadItem item : workloadItems) {
      statistic.addItemStatistic(item.getStatistic());
    }
  }

  /**
   * return the workload info in valid
   *
   * @param deviceId
   * @return
   */
  public WorkloadInfo getWorkloadInfo(String deviceId) {
    WorkloadInfo info = new WorkloadInfo(deviceId);
    dropExpiredRecord();
    for (WorkloadItem item : workloadItems) {
      Map<VisitedMeasurements, Long> measurementMap = item.getMeasurementMap();
      for (Map.Entry<VisitedMeasurements, Long> measurementEntry : measurementMap.entrySet()) {
        if (measurementEntry.getKey().getDeviceId().equals(deviceId)) {
          for (String measurement : measurementEntry.getKey().getMeasurements()) {
            info.addMeasurementVisit(measurement);
          }
        }
      }

      Map<Long, Long> spanMap = item.getSpanMap(deviceId);
      for (Map.Entry<Long, Long> spanEntry : spanMap.entrySet()) {
        info.addSpanVisit(spanEntry.getKey(), spanEntry.getValue());
      }
    }
    return info;
  }
}
