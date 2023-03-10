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

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.storagegroup.DataRegionInfo;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.exception.WriteProcessRejectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class SystemInfo {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);

  private long totalStorageGroupMemCost = 0L;
  private volatile boolean rejected = false;

  private long memorySizeForMemtable;
  private long memorySizeForCompaction;
  private Map<DataRegionInfo, Long> reportedStorageGroupMemCostMap = new HashMap<>();

  private long flushingMemTablesCost = 0L;
  private AtomicLong compactionMemoryCost = new AtomicLong(0L);

  private ExecutorService flushTaskSubmitThreadPool =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(ThreadName.FLUSH_TASK_SUBMIT.getName());
  private double FLUSH_THERSHOLD = memorySizeForMemtable * config.getFlushProportion();
  private double REJECT_THERSHOLD = memorySizeForMemtable * config.getRejectProportion();

  private volatile boolean isEncodingFasterThanIo = true;

  private SystemInfo() {
    allocateWriteMemory();
  }

  /**
   * Report current mem cost of database to system. Called when the memory of database newly
   * accumulates to IoTDBConfig.getStorageGroupSizeReportThreshold()
   *
   * @param dataRegionInfo database
   * @throws WriteProcessRejectException
   */
  public synchronized boolean reportStorageGroupStatus(
      DataRegionInfo dataRegionInfo, TsFileProcessor tsFileProcessor)
      throws WriteProcessRejectException {
    long currentDataRegionMemCost = dataRegionInfo.getMemCost();
    long delta =
        currentDataRegionMemCost - reportedStorageGroupMemCostMap.getOrDefault(dataRegionInfo, 0L);
    totalStorageGroupMemCost += delta;
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Report database Status to the system. " + "After adding {}, current sg mem cost is {}.",
          delta,
          totalStorageGroupMemCost);
    }
    reportedStorageGroupMemCostMap.put(dataRegionInfo, currentDataRegionMemCost);
    dataRegionInfo.setLastReportedSize(currentDataRegionMemCost);
    if (totalStorageGroupMemCost < FLUSH_THERSHOLD) {
      return true;
    } else if (totalStorageGroupMemCost >= FLUSH_THERSHOLD
        && totalStorageGroupMemCost < REJECT_THERSHOLD) {
      logger.debug(
          "The total database mem costs are too large, call for flushing. "
              + "Current sg cost is {}",
          totalStorageGroupMemCost);
      chooseMemTablesToMarkFlush(tsFileProcessor);
      return true;
    } else {
      logger.info(
          "Change system to reject status. Triggered by: logical SG ({}), mem cost delta ({}), totalSgMemCost ({}), REJECT_THERSHOLD ({})",
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalStorageGroupMemCost,
          REJECT_THERSHOLD);
      rejected = true;
      if (chooseMemTablesToMarkFlush(tsFileProcessor)) {
        if (totalStorageGroupMemCost < memorySizeForMemtable) {
          return true;
        } else {
          throw new WriteProcessRejectException(
              "Total database MemCost "
                  + totalStorageGroupMemCost
                  + " is over than memorySizeForWriting "
                  + memorySizeForMemtable);
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
   * @param dataRegionInfo database
   */
  public synchronized void resetStorageGroupStatus(DataRegionInfo dataRegionInfo) {
    long currentDataRegionMemCost = dataRegionInfo.getMemCost();
    long delta = 0;
    if (reportedStorageGroupMemCostMap.containsKey(dataRegionInfo)) {
      delta = reportedStorageGroupMemCostMap.get(dataRegionInfo) - currentDataRegionMemCost;
      this.totalStorageGroupMemCost -= delta;
      dataRegionInfo.setLastReportedSize(currentDataRegionMemCost);
      // report after reset sg status, because slow write may not reach the report threshold
      dataRegionInfo.setNeedToReportToSystem(true);
      reportedStorageGroupMemCostMap.put(dataRegionInfo, currentDataRegionMemCost);
    }

    if (totalStorageGroupMemCost >= FLUSH_THERSHOLD
        && totalStorageGroupMemCost < REJECT_THERSHOLD) {
      logger.debug(
          "SG ({}) released memory (delta: {}) but still exceeding flush proportion (totalSgMemCost: {}), call flush.",
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalStorageGroupMemCost);
      if (rejected) {
        logger.info(
            "SG ({}) released memory (delta: {}), set system to normal status (totalSgMemCost: {}).",
            dataRegionInfo.getDataRegion().getDatabaseName(),
            delta,
            totalStorageGroupMemCost);
      }
      logCurrentTotalSGMemory();
      rejected = false;
    } else if (totalStorageGroupMemCost >= REJECT_THERSHOLD) {
      logger.warn(
          "SG ({}) released memory (delta: {}), but system is still in reject status (totalSgMemCost: {}).",
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalStorageGroupMemCost);
      logCurrentTotalSGMemory();
      rejected = true;
    } else {
      logger.debug(
          "SG ({}) released memory (delta: {}), system is in normal status (totalSgMemCost: {}).",
          dataRegionInfo.getDataRegion().getDatabaseName(),
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

  public void addCompactionMemoryCost(long memoryCost) throws InterruptedException {
    if (config.isEnableMemControl()) {
      long originSize = this.compactionMemoryCost.get();
      while (originSize + memoryCost > memorySizeForCompaction
          || !compactionMemoryCost.compareAndSet(originSize, originSize + memoryCost)) {
        Thread.sleep(100);
        originSize = this.compactionMemoryCost.get();
      }
    }
  }

  public synchronized void resetCompactionMemoryCost(long compactionMemoryCost) {
    this.compactionMemoryCost.addAndGet(-compactionMemoryCost);
  }

  public long getMemorySizeForCompaction() {
    if (config.isEnableMemControl()) {
      return memorySizeForCompaction;
    } else {
      return Long.MAX_VALUE;
    }
  }

  public void allocateWriteMemory() {
    memorySizeForMemtable =
        (long)
            (config.getAllocateMemoryForStorageEngine() * config.getWriteProportionForMemtable());
    memorySizeForCompaction =
        (long) (config.getAllocateMemoryForStorageEngine() * config.getCompactionProportion());
    FLUSH_THERSHOLD = memorySizeForMemtable * config.getFlushProportion();
    REJECT_THERSHOLD = memorySizeForMemtable * config.getRejectProportion();
  }

  @TestOnly
  public void setMemorySizeForCompaction(long size) {
    memorySizeForCompaction = size;
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
    for (DataRegionInfo dataRegionInfo : reportedStorageGroupMemCostMap.keySet()) {
      allTsFileProcessors.addAll(dataRegionInfo.getAllReportedTsp());
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
    memorySizeForMemtable -= estimatedTemporaryMemSize;
    FLUSH_THERSHOLD = memorySizeForMemtable * config.getFlushProportion();
    REJECT_THERSHOLD = memorySizeForMemtable * config.getRejectProportion();
  }

  public synchronized void releaseTemporaryMemoryForFlushing(long estimatedTemporaryMemSize) {
    memorySizeForMemtable += estimatedTemporaryMemSize;
    FLUSH_THERSHOLD = memorySizeForMemtable * config.getFlushProportion();
    REJECT_THERSHOLD = memorySizeForMemtable * config.getRejectProportion();
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
