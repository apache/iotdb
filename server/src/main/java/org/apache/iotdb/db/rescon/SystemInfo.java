/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.rescon;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupInfo;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.exception.WriteProcessRejectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;

public class SystemInfo {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);

  private long totalStorageGroupMemCost = 0L;
  private volatile boolean rejected = false;

  private static long memorySizeForWrite = config.getAllocateMemoryForWrite();
  private Map<StorageGroupInfo, Long> reportedStorageGroupMemCostMap = new HashMap<>();

  private long flushingMemTablesCost = 0L;

  private ExecutorService flushTaskSubmitThreadPool =
      IoTDBThreadPoolFactory.newSingleThreadExecutor("FlushTask-Submit-Pool");
  private static double FLUSH_THERSHOLD = memorySizeForWrite * config.getFlushProportion();
  private static double REJECT_THERSHOLD = memorySizeForWrite * config.getRejectProportion();

  private volatile boolean isEncodingFasterThanIo = true;

  /**
   * Report current mem cost of storage group to system. Called when the memory of storage group
   * newly accumulates to IoTDBConfig.getStorageGroupSizeReportThreshold()
   *
   * @param storageGroupInfo storage group
   * @throws WriteProcessRejectException
   */
  public synchronized boolean reportStorageGroupStatus(
      StorageGroupInfo storageGroupInfo, TsFileProcessor tsFileProcessor)
      throws WriteProcessRejectException {
    long delta =
        storageGroupInfo.getMemCost()
            - reportedStorageGroupMemCostMap.getOrDefault(storageGroupInfo, 0L);
    totalStorageGroupMemCost += delta;
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Report Storage Group Status to the system. "
              + "After adding {}, current sg mem cost is {}.",
          delta,
          totalStorageGroupMemCost);
    }
    reportedStorageGroupMemCostMap.put(storageGroupInfo, storageGroupInfo.getMemCost());
    storageGroupInfo.setLastReportedSize(storageGroupInfo.getMemCost());
    if (totalStorageGroupMemCost < FLUSH_THERSHOLD) {
      return true;
    } else if (totalStorageGroupMemCost >= FLUSH_THERSHOLD
        && totalStorageGroupMemCost < REJECT_THERSHOLD) {
      logger.debug(
          "The total storage group mem costs are too large, call for flushing. "
              + "Current sg cost is {}",
          totalStorageGroupMemCost);
      chooseMemTablesToMarkFlush(tsFileProcessor);
      return true;
    } else {
      logger.info(
          "Change system to reject status. Triggered by: logical SG ({}), mem cost delta ({}), totalSgMemCost ({}).",
          storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
          delta,
          totalStorageGroupMemCost);
      rejected = true;
      if (chooseMemTablesToMarkFlush(tsFileProcessor)) {
        if (totalStorageGroupMemCost < memorySizeForWrite) {
          return true;
        } else {
          throw new WriteProcessRejectException(
              "Total Storage Group MemCost "
                  + totalStorageGroupMemCost
                  + " is over than memorySizeForWriting "
                  + memorySizeForWrite);
        }
      } else {
        return false;
      }
    }
  }

  /**
   * Report resetting the mem cost of sg to system. It will be called after flushing, closing and
   * failed to insert
   *
   * @param storageGroupInfo storage group
   */
  public synchronized void resetStorageGroupStatus(StorageGroupInfo storageGroupInfo) {
    long delta = 0;

    if (reportedStorageGroupMemCostMap.containsKey(storageGroupInfo)) {
      delta = reportedStorageGroupMemCostMap.get(storageGroupInfo) - storageGroupInfo.getMemCost();
      this.totalStorageGroupMemCost -= delta;
      storageGroupInfo.setLastReportedSize(storageGroupInfo.getMemCost());
      reportedStorageGroupMemCostMap.put(storageGroupInfo, storageGroupInfo.getMemCost());
    }

    if (totalStorageGroupMemCost >= FLUSH_THERSHOLD
        && totalStorageGroupMemCost < REJECT_THERSHOLD) {
      logger.debug(
          "SG ({}) released memory (delta: {}) but still exceeding flush proportion (totalSgMemCost: {}), call flush.",
          storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
          delta,
          totalStorageGroupMemCost);
      if (rejected) {
        logger.info(
            "SG ({}) released memory (delta: {}), set system to normal status (totalSgMemCost: {}).",
            storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
            delta,
            totalStorageGroupMemCost);
      }
      logCurrentTotalSGMemory();
      rejected = false;
    } else if (totalStorageGroupMemCost >= REJECT_THERSHOLD) {
      logger.warn(
          "SG ({}) released memory (delta: {}), but system is still in reject status (totalSgMemCost: {}).",
          storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
          delta,
          totalStorageGroupMemCost);
      logCurrentTotalSGMemory();
      rejected = true;
    } else {
      logger.debug(
          "SG ({}) released memory (delta: {}), system is in normal status (totalSgMemCost: {}).",
          storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
          delta,
          totalStorageGroupMemCost);
      logCurrentTotalSGMemory();
      rejected = false;
    }
  }

  public synchronized void addFlushingMemTableCost(long flushingMemTableCost) {
    this.flushingMemTablesCost += flushingMemTableCost;
  }

  public synchronized void resetFlushingMemTableCost(long flushingMemTableCost) {
    this.flushingMemTablesCost -= flushingMemTableCost;
  }

  private void logCurrentTotalSGMemory() {
    logger.debug("Current Sg cost is {}", totalStorageGroupMemCost);
  }

  /**
   * Order all working memtables in system by memory cost of actual data points in memtable. Mark
   * the top K TSPs as to be flushed, so that after flushing the K TSPs, the memory cost should be
   * less than FLUSH_THRESHOLD
   */
  private boolean chooseMemTablesToMarkFlush(TsFileProcessor currentTsFileProcessor) {
    // If invoke flush by replaying logs, do not flush now!
    if (reportedStorageGroupMemCostMap.size() == 0) {
      return false;
    }
    PriorityQueue<TsFileProcessor> allTsFileProcessors =
        new PriorityQueue<>(
            (o1, o2) -> Long.compare(o2.getWorkMemTableRamCost(), o1.getWorkMemTableRamCost()));
    for (StorageGroupInfo storageGroupInfo : reportedStorageGroupMemCostMap.keySet()) {
      allTsFileProcessors.addAll(storageGroupInfo.getAllReportedTsp());
    }
    boolean isCurrentTsFileProcessorSelected = false;
    long memCost = 0;
    long activeMemSize = totalStorageGroupMemCost - flushingMemTablesCost;
    while (activeMemSize - memCost > FLUSH_THERSHOLD) {
      if (allTsFileProcessors.isEmpty()
          || allTsFileProcessors.peek().getWorkMemTableRamCost() == 0) {
        return false;
      }
      TsFileProcessor selectedTsFileProcessor = allTsFileProcessors.peek();
      memCost += selectedTsFileProcessor.getWorkMemTableRamCost();
      selectedTsFileProcessor.setWorkMemTableShouldFlush();
      flushTaskSubmitThreadPool.submit(
          () -> {
            selectedTsFileProcessor.submitAFlushTask();
          });
      if (selectedTsFileProcessor == currentTsFileProcessor) {
        isCurrentTsFileProcessorSelected = true;
      }
      allTsFileProcessors.poll();
    }
    return isCurrentTsFileProcessorSelected;
  }

  public boolean isRejected() {
    return rejected;
  }

  public void setEncodingFasterThanIo(boolean isEncodingFasterThanIo) {
    this.isEncodingFasterThanIo = isEncodingFasterThanIo;
  }

  public boolean isEncodingFasterThanIo() {
    return isEncodingFasterThanIo;
  }

  public void close() {
    reportedStorageGroupMemCostMap.clear();
    totalStorageGroupMemCost = 0;
    rejected = false;
  }

  public static SystemInfo getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static SystemInfo instance = new SystemInfo();
  }

  public synchronized void applyTemporaryMemoryForFlushing(long estimatedTemporaryMemSize) {
    memorySizeForWrite -= estimatedTemporaryMemSize;
    FLUSH_THERSHOLD = memorySizeForWrite * config.getFlushProportion();
    REJECT_THERSHOLD = memorySizeForWrite * config.getRejectProportion();
  }

  public synchronized void releaseTemporaryMemoryForFlushing(long estimatedTemporaryMemSize) {
    memorySizeForWrite += estimatedTemporaryMemSize;
    FLUSH_THERSHOLD = memorySizeForWrite * config.getFlushProportion();
    REJECT_THERSHOLD = memorySizeForWrite * config.getRejectProportion();
  }

  public long getTotalMemTableSize() {
    return totalStorageGroupMemCost;
  }

  public double getFlushThershold() {
    return FLUSH_THERSHOLD;
  }

  public double getRejectThershold() {
    return REJECT_THERSHOLD;
  }

  public int flushingMemTableNum() {
    return FlushManager.getInstance().getNumberOfWorkingTasks();
  }
}
