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
import org.apache.iotdb.db.i18n.StorageEngineMessages;
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

  private long totalStorageGroupMemCost = 0L;
  private volatile boolean rejected = false;

  private long memorySizeForMemtable;
  private final Map<DataRegionInfo, Long> reportedStorageGroupMemCostMap = new HashMap<>();

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
          StorageEngineMessages
              .STORAGE_LOG_REPORT_DATABASE_STATUS_TO_THE_SYSTEM_AFTER_ADDING_CURRENT_8982BBD7,
          delta,
          totalStorageGroupMemCost);
    }
    reportedStorageGroupMemCostMap.put(dataRegionInfo, currentDataRegionMemCost);
    dataRegionInfo.setLastReportedSize(currentDataRegionMemCost);
    if (totalStorageGroupMemCost < FLUSH_THRESHOLD) {
      return true;
    } else if (totalStorageGroupMemCost < REJECT_THRESHOLD) {
      logger.debug(
          StorageEngineMessages
              .STORAGE_LOG_THE_TOTAL_DATABASE_MEM_COSTS_ARE_TOO_LARGE_CALL_FOR_FLUSHING_26AD8CDF,
          totalStorageGroupMemCost);
      chooseMemTablesToMarkFlush(tsFileProcessor);
      return true;
    } else {
      logger.info(
          StorageEngineMessages
              .STORAGE_LOG_CHANGE_SYSTEM_TO_REJECT_STATUS_TRIGGERED_BY_LOGICAL_SG_MEM_6F9BCBD3,
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalStorageGroupMemCost,
          REJECT_THRESHOLD);
      rejected = true;
      if (chooseMemTablesToMarkFlush(tsFileProcessor)) {
        if (totalStorageGroupMemCost < memorySizeForMemtable) {
          return true;
        } else {
          throw new WriteProcessRejectException(
              String.format(
                  StorageEngineMessages
                      .STORAGE_EXCEPTION_TOTAL_DATABASE_MEMCOST_S_IS_OVER_THAN_MEMORYSIZEFORWRITING_C63E4D72,
                  totalStorageGroupMemCost,
                  memorySizeForMemtable));
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

    if (totalStorageGroupMemCost >= FLUSH_THRESHOLD
        && totalStorageGroupMemCost < REJECT_THRESHOLD) {
      logger.debug(
          StorageEngineMessages
              .STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_BUT_STILL_EXCEEDING_FLUSH_PROPORTION_DB68D9D5,
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalStorageGroupMemCost);
      if (rejected) {
        logger.info(
            StorageEngineMessages
                .STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_SET_SYSTEM_TO_NORMAL_STATUS_TOTALSGMEMCOST_0F714668,
            dataRegionInfo.getDataRegion().getDatabaseName(),
            delta,
            totalStorageGroupMemCost);
      }
      logCurrentTotalSGMemory();
      rejected = false;
    } else if (totalStorageGroupMemCost >= REJECT_THRESHOLD) {
      logger.warn(
          StorageEngineMessages
              .STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_BUT_SYSTEM_IS_STILL_IN_REJECT_STATUS_AD5E475C,
          dataRegionInfo.getDataRegion().getDatabaseName(),
          delta,
          totalStorageGroupMemCost);
      logCurrentTotalSGMemory();
      rejected = true;
    } else {
      logger.debug(
          StorageEngineMessages
              .STORAGE_LOG_SG_RELEASED_MEMORY_DELTA_SYSTEM_IS_IN_NORMAL_STATUS_TOTALSGMEMCOST_600A4A8D,
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
              StorageEngineMessages
                  .STORAGE_EXCEPTION_REQUIRED_FILE_NUM_D_IS_GREATER_THAN_THE_MAX_FILE_NUM_D_FOR_AB6DE95B,
              fileNum,
              totalFileLimitForCompactionTask));
    }
    long startTime = System.currentTimeMillis();
    int originFileNum = this.compactionFileNumCost.get();
    while (originFileNum + fileNum > totalFileLimitForCompactionTask
        || !compactionFileNumCost.compareAndSet(originFileNum, originFileNum + fileNum)) {
      if (System.currentTimeMillis() - startTime >= timeOutInSecond * 1000L) {
        throw new CompactionFileCountExceededException(
            String.format(
                StorageEngineMessages
                    .STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_FILES_FOR_COMPACTION_AFTER_D_SECONDS_C701F750,
                fileNum,
                timeOutInSecond,
                totalFileLimitForCompactionTask,
                originFileNum));
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
              StorageEngineMessages
                  .STORAGE_EXCEPTION_REQUIRED_FILE_NUM_D_IS_GREATER_THAN_THE_MAX_FILE_NUM_D_FOR_AB6DE95B,
              fileNum,
              totalFileLimitForCompactionTask));
    }
    int originFileNum = this.compactionFileNumCost.get();
    while (true) {
      boolean canUpdate = originFileNum + fileNum <= totalFileLimitForCompactionTask;
      if (!canUpdate && !waitUntilAcquired) {
        throw new CompactionFileCountExceededException(
            String.format(
                StorageEngineMessages
                    .STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_FILES_FOR_COMPACTION_MAX_FILE_NUM_FOR_9B954F8C,
                fileNum,
                totalFileLimitForCompactionTask,
                originFileNum));
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
              StorageEngineMessages
                  .STORAGE_EXCEPTION_REQUIRED_MEMORY_COST_D_BYTES_IS_GREATER_THAN_THE_TOTAL_MEMORY_444D8FE4,
              memoryCost,
              compactionMemoryBlock.getTotalMemorySizeInBytes()));
    }
    boolean allocateResult =
        waitUntilAcquired
            ? compactionMemoryBlock.allocateUntilAvailable(memoryCost, 100)
            : compactionMemoryBlock.allocate(memoryCost);
    if (!allocateResult) {
      throw new CompactionMemoryNotEnoughException(
          String.format(
              StorageEngineMessages
                  .STORAGE_EXCEPTION_FAILED_TO_ALLOCATE_D_BYTES_MEMORY_FOR_COMPACTION_TOTAL_MEMORY_33BE3C71,
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
    logger.debug(StorageEngineMessages.CURRENT_SG_COST, totalStorageGroupMemCost);
  }

  /**
   * Order all working memtables in system by memory cost of actual data points in memtable. Mark
   * the top K TSPs as to be flushed, so that after flushing the K TSPs, the memory cost should be
   * less than FLUSH_THRESHOLD
   */
  private boolean chooseMemTablesToMarkFlush(TsFileProcessor currentTsFileProcessor) {
    // If invoke flush by replaying logs, do not flush now!
    if (reportedStorageGroupMemCostMap.isEmpty()) {
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
    reportedStorageGroupMemCostMap.clear();
    totalStorageGroupMemCost = 0;
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
    return totalStorageGroupMemCost;
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
