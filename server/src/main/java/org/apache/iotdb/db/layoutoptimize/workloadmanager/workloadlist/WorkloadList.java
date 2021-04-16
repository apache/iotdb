package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkloadList {
  private final List<WorkloadItem> workloadItems = new LinkedList<>();
  private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
  // the range of each item
  private final long ITEM_RANGE = 1L * 24L * 60L * 60L * 1000L;
  private final long ITEM_VALID_PERIOD = 30 * ITEM_RANGE;
  private long timeGrainSize = 1000L * 60L;
  WorkloadItem curItem;

  public WorkloadList() {
    long curTimestamp = System.currentTimeMillis();
    curItem = new WorkloadItem(curTimestamp, curTimestamp + ITEM_RANGE, timeGrainSize, threadPool);
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
}
