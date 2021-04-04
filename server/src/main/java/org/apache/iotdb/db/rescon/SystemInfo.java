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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupInfo;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class SystemInfo {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);

  private long totalSgMemCost = 0L;
  private volatile boolean rejected = false;

  private static long memorySizeForWrite = config.getAllocateMemoryForWrite();
  private Map<StorageGroupInfo, Long> reportedSgMemCostMap = new HashMap<>();

  private static double FLUSH_THERSHOLD = memorySizeForWrite * config.getFlushProportion();
  private static double REJECT_THERSHOLD = memorySizeForWrite * config.getRejectProportion();

  private boolean isEncodingFasterThanIo = true;

  /**
   * Report current mem cost of storage group to system. Called when the memory of storage group
   * newly accumulates to IoTDBConfig.getStorageGroupSizeReportThreshold()
   *
   * @param storageGroupInfo storage group
   */
  public synchronized void reportStorageGroupStatus(StorageGroupInfo storageGroupInfo) {
    long delta =
        storageGroupInfo.getMemCost() - reportedSgMemCostMap.getOrDefault(storageGroupInfo, 0L);
    totalSgMemCost += delta;
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Report Storage Group Status to the system. "
              + "After adding {}, current sg mem cost is {}.",
          delta,
          totalSgMemCost);
    }
    reportedSgMemCostMap.put(storageGroupInfo, storageGroupInfo.getMemCost());
    storageGroupInfo.setLastReportedSize(storageGroupInfo.getMemCost());
    if (totalSgMemCost >= FLUSH_THERSHOLD) {
      logger.debug(
          "The total storage group mem costs are too large, call for flushing. "
              + "Current sg cost is {}",
          totalSgMemCost);
      chooseTSPToMarkFlush();
    }
    if (totalSgMemCost >= REJECT_THERSHOLD) {
      logger.info(
          "Change system to reject status. Triggered by: logical SG ({}), mem cost delta ({}), totalSgMemCost ({}).",
          storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
          delta,
          totalSgMemCost);
      rejected = true;
    }
  }

  /**
   * Report resetting the mem cost of sg to system. It will be called after flushing, closing and
   * failed to insert
   *
   * @param storageGroupInfo storage group
   */
  public void resetStorageGroupStatus(
      StorageGroupInfo storageGroupInfo, boolean shouldInvokeFlush) {
    boolean needForceAsyncFlush = false;
    synchronized (this) {
      long delta = 0;

      if (reportedSgMemCostMap.containsKey(storageGroupInfo)) {
        delta = reportedSgMemCostMap.get(storageGroupInfo) - storageGroupInfo.getMemCost();
        this.totalSgMemCost -= delta;
        storageGroupInfo.setLastReportedSize(storageGroupInfo.getMemCost());
        reportedSgMemCostMap.put(storageGroupInfo, storageGroupInfo.getMemCost());
      }

      if (totalSgMemCost >= FLUSH_THERSHOLD && totalSgMemCost < REJECT_THERSHOLD) {
        logger.debug(
            "SG ({}) released memory (delta: {}) but still exceeding flush proportion (totalSgMemCost: {}), call flush.",
            storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
            delta,
            totalSgMemCost);
        if (rejected) {
          logger.info(
              "SG ({}) released memory (delta: {}), set system to normal status (totalSgMemCost: {}).",
              storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
              delta,
              totalSgMemCost);
        }
        logCurrentTotalSGMemory();
        rejected = false;
        needForceAsyncFlush = true;
      } else if (totalSgMemCost >= REJECT_THERSHOLD) {
        logger.warn(
            "SG ({}) released memory (delta: {}), but system is still in reject status (totalSgMemCost: {}).",
            storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
            delta,
            totalSgMemCost);
        logCurrentTotalSGMemory();
        rejected = true;
        needForceAsyncFlush = true;
      } else {
        logger.debug(
            "SG ({}) released memory (delta: {}), system is in normal status (totalSgMemCost: {}).",
            storageGroupInfo.getStorageGroupProcessor().getLogicalStorageGroupName(),
            delta,
            totalSgMemCost);
        logCurrentTotalSGMemory();
        rejected = false;
      }
    }
    if (shouldInvokeFlush && needForceAsyncFlush) {
      forceAsyncFlush();
    }
  }

  private void logCurrentTotalSGMemory() {
    logger.debug("Current Sg cost is {}", totalSgMemCost);
  }

  /**
   * Order all tsfileProcessors in system by memory cost of actual data points in memtable. Mark the
   * top K TSPs as to be flushed, so that after flushing the K TSPs, the memory cost should be less
   * than FLUSH_THRESHOLD
   */
  private void chooseTSPToMarkFlush() {
    if (FlushManager.getInstance().getNumberOfWorkingTasks() > 0) {
      return;
    }
    // If invoke flush by replaying logs, do not flush now!
    if (reportedSgMemCostMap.size() == 0) {
      return;
    }
    // get the tsFile processors which has the max work MemTable size
    List<TsFileProcessor> processors = getTsFileProcessorsToFlush();
    for (TsFileProcessor processor : processors) {
      if (processor != null) {
        processor.setFlush();
      }
    }
  }

  /** Be Careful!! This method can only be called by flush thread! */
  private void forceAsyncFlush() {
    if (FlushManager.getInstance().getNumberOfWorkingTasks() > 1) {
      return;
    }
    List<TsFileProcessor> processors = getTsFileProcessorsToFlush();
    if (logger.isDebugEnabled()) {
      logger.debug("[mem control] get {} tsp to flush", processors.size());
    }
    for (TsFileProcessor processor : processors) {
      if (processor != null) {
        processor.startAsyncFlush();
      }
    }
  }

  private List<TsFileProcessor> getTsFileProcessorsToFlush() {
    PriorityQueue<TsFileProcessor> tsps =
        new PriorityQueue<>(
            (o1, o2) -> Long.compare(o2.getWorkMemTableRamCost(), o1.getWorkMemTableRamCost()));
    for (StorageGroupInfo sgInfo : reportedSgMemCostMap.keySet()) {
      tsps.addAll(sgInfo.getAllReportedTsp());
    }
    List<TsFileProcessor> processors = new ArrayList<>();
    long memCost = 0;
    while (totalSgMemCost - memCost > FLUSH_THERSHOLD / 2) {
      if (tsps.isEmpty() || tsps.peek().getWorkMemTableRamCost() == 0) {
        return processors;
      }
      processors.add(tsps.peek());
      memCost += tsps.peek().getWorkMemTableRamCost();
      tsps.poll();
    }
    return processors;
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
    reportedSgMemCostMap.clear();
    totalSgMemCost = 0;
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
}
