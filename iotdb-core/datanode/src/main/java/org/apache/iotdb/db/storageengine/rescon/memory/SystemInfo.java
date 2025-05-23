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

package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.memory.IMemoryBlock;
import org.apache.iotdb.commons.memory.MemoryBlockType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.WriteProcessRejectException;
import org.apache.iotdb.db.service.metrics.WritingMetrics;
import org.apache.iotdb.db.storageengine.dataregion.DataRegionInfo;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionFileCountExceededException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionMemoryNotEnoughException;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SystemInfo {
  private static final Logger logger = LoggerFactory.getLogger(SystemInfo.class);

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final DataNodeMemoryConfig memoryConfig =
      IoTDBDescriptor.getInstance().getMemoryConfig();

  private long totalDatabaseMemCost = 0L;
  private volatile boolean rejected = false;

  private long memorySizeForMemtable;
  private final Map<DataRegionInfo, Long> reportedDatabaseMemCostMap = new HashMap<>();

  private long flushingMemTablesCost = 0L;
  private IMemoryBlock walBufferQueueMemoryBlock;
  private IMemoryBlock directBufferMemoryBlock;
  private IMemoryBlock compactionMemoryBlock;
  private final AtomicLong seqInnerSpaceCompactionMemoryCost = new AtomicLong(0L);
  private final AtomicLong unseqInnerSpaceCompactionMemoryCost = new AtomicLong(0L);
  private final AtomicLong crossSpaceCompactionMemoryCost = new AtomicLong(0L);
  private final AtomicLong settleCompactionMemoryCost = new AtomicLong(0L);

  private final AtomicInteger compactionFileNumCost = new AtomicInteger(0);

  private int totalFileLimitForCompactionTask = config.getTotalFileLimitForCompactionTask();

  private final ExecutorService flushTaskSubmitThreadPool =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(ThreadName.FLUSH_TASK_SUBMIT.getName());
  private double FLUSH_THRESHOLD = memorySizeForMemtable * config.getFlushProportion();
  private double REJECT_THRESHOLD = memorySizeForMemtable * memoryConfig.getRejectProportion();

  private volatile boolean isEncodingFasterThanIo = true;

  private SystemInfo() {
    compactionMemoryBlock =
        memoryConfig
            .getCompactionMemoryManager()
            .exactAllocate("Compaction", MemoryBlockType.DYNAMIC);
    walBufferQueueMemoryBlock =
        memoryConfig
            .getWalBufferQueueMemoryManager()
            .exactAllocate("WalBufferQueue", MemoryBlockType.DYNAMIC);
    directBufferMemoryBlock =
        memoryConfig
            .getDirectBufferMemoryManager()
            .exactAllocate("DirectBuffer", MemoryBlockType.DYNAMIC);
    loadWriteMemory();
  }

  /**
   * Report current mem cost of database to system. Called when the memory of database newly
   * accumulates to IoTDBConfig.getDatabaseSizeReportThreshold()
   *
   * @param dataRegionInfo database
   * @throws WriteProcessRejectException
   */
  public synchronized boolean reportDatabaseStatus(
      DataRegionInfo dataRegionInfo, TsFileProcessor tsFileProcessor)
      throws WriteProcessRejectException {
    long currentDataRegionMemCost = dataRegionInfo.getMemCost();
    long delta =
        currentDataRegionMemCost - reportedDatabaseMemCostMap.getOrDefault(dataRegionInfo, 0L);
    totalDatabaseMemCost += delta;
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Report database Status to the system. " + "After adding {}, current sg mem cost is {}.",
          delta,
          totalDatabaseMemCost);
    }
    reportedDatabaseMemCostMap.put(dataRegionInfo, currentDataRegionMemCost);
    dataRegionInfo.setLastReportedSize(currentDataRegionMemCost);
    if (totalDatabaseMemCost < FLUSH_THRESHOLD) {
      return true;
    } else if (totalDatabaseMemCost < REJECT_THRESHOLD) {
      logger.debug(
          "The total database mem costs are too large, call for flushing. "
              + "Current sg cost is {}",
          totalDatabaseMemCost);
      chooseMemTablesToMarkFlush(tsFileProcessor);
      return true;
    } else {
      logger.info(
          "Change system to reject status. Triggered by: logical SG ({}), mem cost delta ({}), totalSgMemCost ({}), REJECT_THRESHOLD ({})",
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalDatabaseMemCost,
          REJECT_THRESHOLD);
      rejected = true;
      if (chooseMemTablesToMarkFlush(tsFileProcessor)) {
        if (totalDatabaseMemCost < memorySizeForMemtable) {
          return true;
        } else {
          throw new WriteProcessRejectException(
              "Total database MemCost "
                  + totalDatabaseMemCost
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
  public synchronized void resetDatabaseStatus(DataRegionInfo dataRegionInfo) {
    long currentDataRegionMemCost = dataRegionInfo.getMemCost();
    long delta = 0;
    if (reportedDatabaseMemCostMap.containsKey(dataRegionInfo)) {
      delta = reportedDatabaseMemCostMap.get(dataRegionInfo) - currentDataRegionMemCost;
      this.totalDatabaseMemCost -= delta;
      dataRegionInfo.setLastReportedSize(currentDataRegionMemCost);
      // report after reset sg status, because slow write may not reach the report threshold
      dataRegionInfo.setNeedToReportToSystem(true);
      reportedDatabaseMemCostMap.put(dataRegionInfo, currentDataRegionMemCost);
    }

    if (totalDatabaseMemCost >= FLUSH_THRESHOLD && totalDatabaseMemCost < REJECT_THRESHOLD) {
      logger.debug(
          "SG ({}) released memory (delta: {}) but still exceeding flush proportion (totalSgMemCost: {}), call flush.",
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalDatabaseMemCost);
      if (rejected) {
        logger.info(
            "SG ({}) released memory (delta: {}), set system to normal status (totalSgMemCost: {}).",
            dataRegionInfo.getDataRegion().getDatabaseName(),
            delta,
            totalDatabaseMemCost);
      }
      logCurrentTotalSGMemory();
      rejected = false;
    } else if (totalDatabaseMemCost >= REJECT_THRESHOLD) {
      logger.warn(
          "SG ({}) released memory (delta: {}), but system is still in reject status (totalSgMemCost: {}).",
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalDatabaseMemCost);
      logCurrentTotalSGMemory();
      rejected = true;
    } else {
      logger.debug(
          "SG ({}) released memory (delta: {}), system is in normal status (totalSgMemCost: {}).",
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalDatabaseMemCost);
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

  public boolean addDirectBufferMemoryCost(long size) {
    return directBufferMemoryBlock.allocate(size);
  }

  public void decreaseDirectBufferMemoryCost(long size) {
    directBufferMemoryBlock.release(size);
  }

  public long getTotalDirectBufferMemorySizeLimit() {
    return memoryConfig.getDirectBufferMemoryManager().getTotalMemorySizeInBytes();
  }

  public long getDirectBufferMemoryCost() {
    return directBufferMemoryBlock.getUsedMemoryInBytes();
  }

  public boolean addCompactionFileNum(int fileNum, long timeOutInSecond)
      throws InterruptedException, CompactionFileCountExceededException {
    if (fileNum > totalFileLimitForCompactionTask) {
      // source file num is greater than the max file num for compaction
      throw new CompactionFileCountExceededException(
          String.format(
              "Required file num %d is greater than the max file num %d for compaction.",
              fileNum, totalFileLimitForCompactionTask));
    }
    long startTime = System.currentTimeMillis();
    int originFileNum = this.compactionFileNumCost.get();
    while (originFileNum + fileNum > totalFileLimitForCompactionTask
        || !compactionFileNumCost.compareAndSet(originFileNum, originFileNum + fileNum)) {
      if (System.currentTimeMillis() - startTime >= timeOutInSecond * 1000L) {
        throw new CompactionFileCountExceededException(
            String.format(
                "Failed to allocate %d files for compaction after %d seconds, max file num for compaction module is %d, %d files is used.",
                fileNum, timeOutInSecond, totalFileLimitForCompactionTask, originFileNum));
      }
      Thread.sleep(100);
      originFileNum = this.compactionFileNumCost.get();
    }
    return true;
  }

  public void addCompactionFileNum(int fileNum, boolean waitUntilAcquired)
      throws CompactionFileCountExceededException, InterruptedException {
    if (fileNum > totalFileLimitForCompactionTask) {
      // source file num is greater than the max file num for compaction
      throw new CompactionFileCountExceededException(
          String.format(
              "Required file num %d is greater than the max file num %d for compaction.",
              fileNum, totalFileLimitForCompactionTask));
    }
    int originFileNum = this.compactionFileNumCost.get();
    while (true) {
      boolean canUpdate = originFileNum + fileNum <= totalFileLimitForCompactionTask;
      if (!canUpdate && !waitUntilAcquired) {
        throw new CompactionFileCountExceededException(
            String.format(
                "Failed to allocate %d files for compaction, max file num for compaction module is %d, %d files is used.",
                fileNum, totalFileLimitForCompactionTask, originFileNum));
      }
      if (canUpdate
          && compactionFileNumCost.compareAndSet(originFileNum, originFileNum + fileNum)) {
        return;
      }
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      originFileNum = this.compactionFileNumCost.get();
    }
  }

  public void addCompactionMemoryCost(
      CompactionTaskType taskType, long memoryCost, boolean waitUntilAcquired)
      throws CompactionMemoryNotEnoughException, InterruptedException {
    if (memoryCost > compactionMemoryBlock.getTotalMemorySizeInBytes()) {
      // required memory cost is greater than the total memory budget for compaction
      throw new CompactionMemoryNotEnoughException(
          String.format(
              "Required memory cost %d bytes is greater than "
                  + "the total memory budget for compaction %d bytes",
              memoryCost, compactionMemoryBlock.getTotalMemorySizeInBytes()));
    }
    boolean allocateResult =
        waitUntilAcquired
            ? compactionMemoryBlock.allocateUntilAvailable(memoryCost, 100)
            : compactionMemoryBlock.allocate(memoryCost);
    if (!allocateResult) {
      throw new CompactionMemoryNotEnoughException(
          String.format(
              "Failed to allocate %d bytes memory for compaction, "
                  + "total memory budget for compaction module is %d bytes, %d bytes is used",
              memoryCost,
              compactionMemoryBlock.getTotalMemorySizeInBytes(),
              compactionMemoryBlock.getUsedMemoryInBytes()));
    }
    switch (taskType) {
      case INNER_SEQ:
        seqInnerSpaceCompactionMemoryCost.addAndGet(memoryCost);
        break;
      case INNER_UNSEQ:
        unseqInnerSpaceCompactionMemoryCost.addAndGet(memoryCost);
        break;
      case CROSS:
        crossSpaceCompactionMemoryCost.addAndGet(memoryCost);
        break;
      case SETTLE:
        settleCompactionMemoryCost.addAndGet(memoryCost);
        break;
      default:
    }
  }

  public synchronized void resetCompactionMemoryCost(
      CompactionTaskType taskType, long compactionMemoryCost) {
    this.compactionMemoryBlock.release(compactionMemoryCost);
    switch (taskType) {
      case INNER_SEQ:
        seqInnerSpaceCompactionMemoryCost.addAndGet(-compactionMemoryCost);
        break;
      case INNER_UNSEQ:
        unseqInnerSpaceCompactionMemoryCost.addAndGet(-compactionMemoryCost);
        break;
      case CROSS:
        crossSpaceCompactionMemoryCost.addAndGet(-compactionMemoryCost);
        break;
      case SETTLE:
        settleCompactionMemoryCost.addAndGet(-compactionMemoryCost);
        break;
      default:
        break;
    }
  }

  public synchronized void decreaseCompactionFileNumCost(int fileNum) {
    this.compactionFileNumCost.addAndGet(-fileNum);
  }

  public long getMemorySizeForCompaction() {
    return compactionMemoryBlock.getTotalMemorySizeInBytes();
  }

  public void loadWriteMemory() {
    memorySizeForMemtable = memoryConfig.getMemtableMemoryManager().getTotalMemorySizeInBytes();
    FLUSH_THRESHOLD = memorySizeForMemtable * config.getFlushProportion();
    REJECT_THRESHOLD = memorySizeForMemtable * memoryConfig.getRejectProportion();
    WritingMetrics.getInstance().recordFlushThreshold(FLUSH_THRESHOLD);
    WritingMetrics.getInstance().recordRejectThreshold(REJECT_THRESHOLD);
    WritingMetrics.getInstance()
        .recordWALQueueMaxMemorySize(walBufferQueueMemoryBlock.getTotalMemorySizeInBytes());
  }

  @TestOnly
  public void setMemorySizeForCompaction(long size) {
    compactionMemoryBlock.setTotalMemorySizeInBytes(size);
  }

  @TestOnly
  public void setTotalFileLimitForCompactionTask(int totalFileLimitForCompactionTask) {
    this.totalFileLimitForCompactionTask = totalFileLimitForCompactionTask;
  }

  public int getTotalFileLimitForCompaction() {
    return totalFileLimitForCompactionTask;
  }

  public IMemoryBlock getCompactionMemoryBlock() {
    return compactionMemoryBlock;
  }

  public AtomicLong getSeqInnerSpaceCompactionMemoryCost() {
    return seqInnerSpaceCompactionMemoryCost;
  }

  public AtomicLong getUnseqInnerSpaceCompactionMemoryCost() {
    return unseqInnerSpaceCompactionMemoryCost;
  }

  public AtomicLong getCrossSpaceCompactionMemoryCost() {
    return crossSpaceCompactionMemoryCost;
  }

  public AtomicLong getSettleCompactionMemoryCost() {
    return settleCompactionMemoryCost;
  }

  @TestOnly
  public AtomicInteger getCompactionFileNumCost() {
    return compactionFileNumCost;
  }

  private void logCurrentTotalSGMemory() {
    logger.debug("Current Sg cost is {}", totalDatabaseMemCost);
  }

  /**
   * Order all working memtables in system by memory cost of actual data points in memtable. Mark
   * the top K TSPs as to be flushed, so that after flushing the K TSPs, the memory cost should be
   * less than FLUSH_THRESHOLD
   */
  private boolean chooseMemTablesToMarkFlush(TsFileProcessor currentTsFileProcessor) {
    // If invoke flush by replaying logs, do not flush now!
    if (reportedDatabaseMemCostMap.isEmpty()) {
      return false;
    }
    PriorityQueue<TsFileProcessor> allTsFileProcessors =
        new PriorityQueue<>(
            (o1, o2) -> Long.compare(o2.getWorkMemTableRamCost(), o1.getWorkMemTableRamCost()));
    for (DataRegionInfo dataRegionInfo : reportedDatabaseMemCostMap.keySet()) {
      allTsFileProcessors.addAll(dataRegionInfo.getAllReportedTsp());
    }
    boolean isCurrentTsFileProcessorSelected = false;
    long memCost = 0;
    long activeMemSize = totalDatabaseMemCost - flushingMemTablesCost;
    while (activeMemSize - memCost > FLUSH_THRESHOLD) {
      if (allTsFileProcessors.isEmpty()
          || allTsFileProcessors.peek().getWorkMemTableRamCost() == 0) {
        return false;
      }
      TsFileProcessor selectedTsFileProcessor = allTsFileProcessors.peek();
      memCost += selectedTsFileProcessor.getWorkMemTableRamCost();
      selectedTsFileProcessor.setWorkMemTableShouldFlush();
      flushTaskSubmitThreadPool.submit(selectedTsFileProcessor::submitAFlushTask);
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
    reportedDatabaseMemCostMap.clear();
    totalDatabaseMemCost = 0;
    rejected = false;
  }

  public static SystemInfo getInstance() {
    return InstanceHolder.instance;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final SystemInfo instance = new SystemInfo();
  }

  public synchronized void applyTemporaryMemoryForFlushing(long estimatedTemporaryMemSize) {
    memorySizeForMemtable -= estimatedTemporaryMemSize;
    FLUSH_THRESHOLD = memorySizeForMemtable * config.getFlushProportion();
    REJECT_THRESHOLD = memorySizeForMemtable * memoryConfig.getRejectProportion();
    WritingMetrics.getInstance().recordFlushThreshold(FLUSH_THRESHOLD);
    WritingMetrics.getInstance().recordRejectThreshold(REJECT_THRESHOLD);
    WritingMetrics.getInstance()
        .recordWALQueueMaxMemorySize(walBufferQueueMemoryBlock.getTotalMemorySizeInBytes());
  }

  public synchronized void releaseTemporaryMemoryForFlushing(long estimatedTemporaryMemSize) {
    memorySizeForMemtable += estimatedTemporaryMemSize;
    FLUSH_THRESHOLD = memorySizeForMemtable * config.getFlushProportion();
    REJECT_THRESHOLD = memorySizeForMemtable * memoryConfig.getRejectProportion();
    WritingMetrics.getInstance().recordFlushThreshold(FLUSH_THRESHOLD);
    WritingMetrics.getInstance().recordRejectThreshold(REJECT_THRESHOLD);
    WritingMetrics.getInstance()
        .recordWALQueueMaxMemorySize(walBufferQueueMemoryBlock.getTotalMemorySizeInBytes());
  }

  public long getTotalMemTableSize() {
    return totalDatabaseMemCost;
  }

  public double getFlushThreshold() {
    return FLUSH_THRESHOLD;
  }

  public double getRejectThreshold() {
    return REJECT_THRESHOLD;
  }

  public IMemoryBlock getWalBufferQueueMemoryBlock() {
    return walBufferQueueMemoryBlock;
  }
}
