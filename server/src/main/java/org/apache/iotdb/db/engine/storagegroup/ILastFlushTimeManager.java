package org.apache.iotdb.db.engine.storagegroup;

import java.util.Map;

/**
 * This interface manages last time and flush time for sequence and unsequence determination This
 * class
 */
public interface ILastFlushTimeManager {

  // region set
  void setLastTimeAll(long timePartitionId, Map<String, Long> lastTimeMap);

  void setLastTime(long timePartitionId, String path, long time);

  void setFlushedTimeAll(long timePartitionId, Map<String, Long> flushedTimeMap);

  void setFlushedTime(long timePartitionId, String path, long time);

  void setGlobalFlushedTimeAll(Map<String, Long> globalFlushedTimeMap);

  void setGlobalFlushedTime(String path, long time);
  // endregion

  // region update
  void updateLastTime(long timePartitionId, String path, long time);

  void updateFlushedTime(long timePartitionId, String path, long time);

  void updateGlobalFlushedTime(String path, long time);

  void updateNewlyFlushedPartitionLatestFlushedTimeForEachDevice(
      long partitionId, String deviceId, long time);
  // endregion

  // region ensure
  void ensureLastTimePartition(long timePartitionId);

  void ensureFlushedTimePartition(long timePartitionId);

  long ensureFlushedTimePartitionAndInit(long timePartitionId, String path, long initTime);
  // endregion

  // region support upgrade methods
  void applyNewlyFlushedTimeToFlushedTime();

  /**
   * update latest flush time for partition id
   *
   * @param partitionId partition id
   * @param latestFlushTime lastest flush time
   * @return true if update latest flush time success
   */
  boolean updateLatestFlushTimeToPartition(long partitionId, long latestFlushTime);

  boolean updateLatestFlushTime(long partitionId);
  // endregion

  // region query
  long getFlushedTime(long timePartitionId, String path);

  long getLastTime(long timePartitionId, String path);

  long getGlobalFlushedTime(String path);
  // endregion

  // region clear
  void clearLastTime();

  void clearFlushedTime();

  void clearGlobalFlushedTime();
  // endregion
}
