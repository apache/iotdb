package org.apache.iotdb.metrics.metricsets.disk;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractDiskMetricsManager implements IDiskMetricsManager {

  public static final double BYTES_PER_KB = 1024.0;
  // Disk IO status structure
  protected Map<String, Long> lastReadOperationCountForDisk;
  protected Map<String, Long> lastWriteOperationCountForDisk;
  protected Map<String, Long> lastReadSizeForDisk;
  protected Map<String, Long> lastWriteSizeForDisk;
  protected Map<String, Long> lastReadTimeCostForDisk;
  protected Map<String, Long> lastWriteTimeCostForDisk;
  protected Map<String, Long> lastMergedReadCountForDisk;
  protected Map<String, Long> lastMergedWriteCountForDisk;
  protected Map<String, Long> lastReadSectorCountForDisk;
  protected Map<String, Long> lastWriteSectorCountForDisk;
  protected Map<String, Long> lastIoBusyTimeForDisk;
  protected Map<String, Long> lastTimeInQueueForDisk;
  protected Map<String, Long> incrementReadOperationCountForDisk;
  protected Map<String, Long> incrementWriteOperationCountForDisk;
  protected Map<String, Long> incrementReadSizeForDisk;
  protected Map<String, Long> incrementWriteSizeForDisk;
  protected Map<String, Long> incrementMergedReadOperationCountForDisk;
  protected Map<String, Long> incrementMergedWriteOperationCountForDisk;
  protected Map<String, Long> incrementReadTimeCostForDisk;
  protected Map<String, Long> incrementWriteTimeCostForDisk;
  protected Map<String, Long> incrementReadSectorCountForDisk;
  protected Map<String, Long> incrementWriteSectorCountForDisk;
  protected Map<String, Long> incrementIoBusyTimeForDisk;
  protected Map<String, Long> incrementTimeInQueueForDisk;
  protected long lastUpdateTime = 0L;
  protected long updateInterval = 1L;
  protected Set<String> diskIdSet;

  public AbstractDiskMetricsManager() {}

  protected void init() {
    collectDiskId();
    lastReadOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastWriteOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastReadSizeForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastWriteSizeForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastReadTimeCostForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastWriteTimeCostForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastMergedReadCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastMergedWriteCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastReadSectorCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastWriteSectorCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastIoBusyTimeForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    lastTimeInQueueForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementReadOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementWriteOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementReadSizeForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementWriteSizeForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementMergedReadOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementMergedWriteOperationCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementReadTimeCostForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementWriteTimeCostForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementReadSectorCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementWriteSectorCountForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementIoBusyTimeForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
    incrementTimeInQueueForDisk = new HashMap<>(diskIdSet.size() + 1, 1);
  }

  protected void checkUpdate() {
    if (System.currentTimeMillis() - lastUpdateTime
        > IDiskMetricsManager.UPDATE_SMALLEST_INTERVAL) {
      updateInfo();
    }
  }

  @Override
  public Map<String, Double> getIoUtilsPercentage() {
    checkUpdate();
    Map<String, Double> utilsMap = new HashMap<>(incrementIoBusyTimeForDisk.size());
    for (Map.Entry<String, Long> entry : incrementIoBusyTimeForDisk.entrySet()) {
      utilsMap.put(entry.getKey(), ((double) entry.getValue()) / updateInterval);
    }
    return utilsMap;
  }

  protected void updateInfo() {
    long currentTime = System.currentTimeMillis();
    updateInterval = currentTime - lastUpdateTime;
    lastUpdateTime = currentTime;
  }

  protected void updateSingleDiskInfo(
      String diskId, long currentValue, Map<String, Long> lastMap, Map<String, Long> incrementMap) {
    if (incrementMap != null) {
      long lastValue = lastMap.getOrDefault(diskId, 0L);
      if (lastValue != 0) {
        incrementMap.put(diskId, currentValue - lastValue);
      } else {
        incrementMap.put(diskId, 0L);
      }
    }
    lastMap.put(diskId, currentValue);
  }

  @Override
  public Set<String> getDiskIds() {
    return diskIdSet;
  }

  protected abstract void collectDiskId();
}
