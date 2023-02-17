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

package org.apache.iotdb.db.engine.compaction.execute.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.TsFileMetricManager;
import org.apache.iotdb.db.engine.compaction.execute.exception.CompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.metrics.recorder.CompactionMetricsManager;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_DELETED_TARGET_FILES;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.engine.compaction.execute.utils.log.CompactionLogger.STR_TARGET_FILES;

public class CrossSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected List<TsFileResource> selectedSequenceFiles;
  protected List<TsFileResource> selectedUnsequenceFiles;
  protected TsFileResourceList seqTsFileResourceList;
  protected TsFileResourceList unseqTsFileResourceList;
  private File logFile;
  protected List<TsFileResource> targetTsfileResourceList;
  protected List<TsFileResource> holdReadLockList = new ArrayList<>();
  protected List<TsFileResource> holdWriteLockList = new ArrayList<>();
  protected double selectedSeqFileSize = 0;
  protected double selectedUnseqFileSize = 0;
  protected long memoryCost = 0L;

  public CrossSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSequenceFiles,
      List<TsFileResource> selectedUnsequenceFiles,
      ICrossCompactionPerformer performer,
      AtomicInteger currentTaskNum,
      long memoryCost,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        currentTaskNum,
        serialId);
    this.selectedSequenceFiles = selectedSequenceFiles;
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
    this.seqTsFileResourceList =
        tsFileManager.getOrCreateSequenceListByTimePartition(timePartition);
    this.unseqTsFileResourceList =
        tsFileManager.getOrCreateUnsequenceListByTimePartition(timePartition);
    this.performer = performer;
    this.hashCode = this.toString().hashCode();
    this.memoryCost = memoryCost;
    this.crossTask = true;
    this.innerSeqTask = false;
    createSummary();
  }

  @Override
  public void doCompaction() {
    try {
      SystemInfo.getInstance().addCompactionMemoryCost(memoryCost);
    } catch (InterruptedException e) {
      LOGGER.error("Interrupted when allocating memory for compaction", e);
      return;
    }
    try {
      if (!tsFileManager.isAllowCompaction()) {
        return;
      }
      long startTime = System.currentTimeMillis();
      targetTsfileResourceList =
          TsFileNameGenerator.getCrossCompactionTargetFileResources(selectedSequenceFiles);

      if (targetTsfileResourceList.isEmpty()
          || selectedSequenceFiles.isEmpty()
          || selectedUnsequenceFiles.isEmpty()) {
        LOGGER.info(
            "{}-{} [Compaction] Cross space compaction file list is empty, end it",
            storageGroupName,
            dataRegionId);
        return;
      }

      for (TsFileResource resource : selectedSequenceFiles) {
        selectedSeqFileSize += resource.getTsFileSize();
      }

      for (TsFileResource resource : selectedUnsequenceFiles) {
        selectedUnseqFileSize += resource.getTsFileSize();
      }

      LOGGER.info(
          "{}-{} [Compaction] CrossSpaceCompaction task starts with {} seq files and {} unsequence files. Sequence files : {}, unsequence files : {} . Sequence files size is {} MB, unsequence file size is {} MB, total size is {} MB",
          storageGroupName,
          dataRegionId,
          selectedSequenceFiles.size(),
          selectedUnsequenceFiles.size(),
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          selectedSeqFileSize / 1024 / 1024,
          selectedUnseqFileSize / 1024 / 1024,
          (selectedSeqFileSize + selectedUnseqFileSize) / 1024 / 1024);

      logFile =
          new File(
              selectedSequenceFiles.get(0).getTsFile().getParent()
                  + File.separator
                  + targetTsfileResourceList.get(0).getTsFile().getName()
                  + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);

      try (CompactionLogger compactionLogger = new CompactionLogger(logFile)) {
        // print the path of the temporary file first for priority check during recovery
        compactionLogger.logFiles(selectedSequenceFiles, STR_SOURCE_FILES);
        compactionLogger.logFiles(selectedUnsequenceFiles, STR_SOURCE_FILES);
        compactionLogger.logFiles(targetTsfileResourceList, STR_TARGET_FILES);

        performer.setSourceFiles(selectedSequenceFiles, selectedUnsequenceFiles);
        performer.setTargetFiles(targetTsfileResourceList);
        performer.setSummary(summary);
        performer.perform();

        CompactionUtils.moveTargetFile(
            targetTsfileResourceList, false, storageGroupName + "-" + dataRegionId);
        CompactionUtils.combineModsInCrossCompaction(
            selectedSequenceFiles, selectedUnsequenceFiles, targetTsfileResourceList);

        // update tsfile resource in memory
        tsFileManager.replace(
            selectedSequenceFiles,
            selectedUnsequenceFiles,
            targetTsfileResourceList,
            timePartition,
            true);

        // find empty target files and add log
        for (TsFileResource targetResource : targetTsfileResourceList) {
          if (targetResource.isDeleted()) {
            compactionLogger.logFile(targetResource, STR_DELETED_TARGET_FILES);
          }
        }

        if (IoTDBDescriptor.getInstance().getConfig().isEnableCompactionValidation()
            && !CompactionUtils.validateTsFileResources(
                tsFileManager, storageGroupName, timePartition)) {
          LOGGER.error(
              "Failed to pass compaction validation, source sequence files is: {}, unsequence files is {}, target files is {}",
              selectedSequenceFiles,
              selectedUnsequenceFiles,
              targetTsfileResourceList);
          throw new RuntimeException("Failed to pass compaction validation");
        }

        releaseReadAndLockWrite(selectedSequenceFiles);
        releaseReadAndLockWrite(selectedUnsequenceFiles);

        for (TsFileResource sequenceResource : selectedSequenceFiles) {
          if (sequenceResource.getModFile().exists()) {
            TsFileMetricManager.getInstance().decreaseModFileNum(1);
            TsFileMetricManager.getInstance()
                .decreaseModFileSize(sequenceResource.getModFile().getSize());
          }
        }

        for (TsFileResource unsequenceResource : selectedUnsequenceFiles) {
          if (unsequenceResource.getModFile().exists()) {
            TsFileMetricManager.getInstance().decreaseModFileNum(1);
            TsFileMetricManager.getInstance()
                .decreaseModFileSize(unsequenceResource.getModFile().getSize());
          }
        }

        long sequenceFileSize = deleteOldFiles(selectedSequenceFiles);
        long unsequenceFileSize = deleteOldFiles(selectedUnsequenceFiles);
        CompactionUtils.deleteCompactionModsFile(selectedSequenceFiles, selectedUnsequenceFiles);

        if (logFile.exists()) {
          FileUtils.delete(logFile);
        }

        // update the metrics finally in case of any exception occurs
        for (TsFileResource targetResource : targetTsfileResourceList) {
          if (!targetResource.isDeleted()) {
            TsFileMetricManager.getInstance().addFile(targetResource.getTsFileSize(), true);

            // set target resources to CLOSED, so that they can be selected to compact
            targetResource.setStatus(TsFileResourceStatus.CLOSED);
          } else {
            // target resource is empty after compaction, then delete it
            targetResource.remove();
          }
        }
        TsFileMetricManager.getInstance()
            .deleteFile(sequenceFileSize, true, selectedSequenceFiles.size());
        TsFileMetricManager.getInstance()
            .deleteFile(unsequenceFileSize, false, selectedUnsequenceFiles.size());

        CompactionMetricsManager.getInstance().updateSummary(summary);

        long costTime = (System.currentTimeMillis() - startTime) / 1000;

        LOGGER.info(
            "{}-{} [Compaction] CrossSpaceCompaction task finishes successfully, time cost is {} s, compaction speed is {} MB/s, {}",
            storageGroupName,
            dataRegionId,
            costTime,
            (selectedSeqFileSize + selectedUnseqFileSize) / 1024 / 1024 / costTime,
            summary);
      }
    } catch (Throwable throwable) {
      // catch throwable to handle OOM errors
      if (!(throwable instanceof InterruptedException)) {
        LOGGER.error(
            "{}-{} [Compaction] Meet errors in cross space compaction.",
            storageGroupName,
            dataRegionId,
            throwable);
      } else {
        LOGGER.warn("{}-{} [Compaction] Compaction interrupted", storageGroupName, dataRegionId);
        // clean the interrupted flag
        Thread.interrupted();
      }

      // handle exception
      CompactionExceptionHandler.handleException(
          storageGroupName + "-" + dataRegionId,
          logFile,
          targetTsfileResourceList,
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          tsFileManager,
          timePartition,
          false,
          true);
    } finally {
      SystemInfo.getInstance().resetCompactionMemoryCost(memoryCost);
      releaseAllLock();
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof CrossSpaceCompactionTask)) {
      return false;
    }
    CrossSpaceCompactionTask otherCrossCompactionTask = (CrossSpaceCompactionTask) otherTask;
    return this.selectedSequenceFiles.equals(otherCrossCompactionTask.selectedSequenceFiles)
        && this.selectedUnsequenceFiles.equals(otherCrossCompactionTask.selectedUnsequenceFiles)
        && this.performer.getClass().isInstance(otherCrossCompactionTask.performer);
  }

  private void releaseAllLock() {
    selectedSequenceFiles.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
    selectedUnsequenceFiles.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
    for (TsFileResource tsFileResource : holdReadLockList) {
      tsFileResource.readUnlock();
      tsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    }
    for (TsFileResource tsFileResource : holdWriteLockList) {
      tsFileResource.writeUnlock();
      tsFileResource.setStatus(TsFileResourceStatus.CLOSED);
    }
    holdReadLockList.clear();
    holdWriteLockList.clear();
  }

  public List<TsFileResource> getSelectedSequenceFiles() {
    return selectedSequenceFiles;
  }

  @Override
  public void setSourceFilesToCompactionCandidate() {
    this.selectedSequenceFiles.forEach(x -> x.setStatus(TsFileResourceStatus.COMPACTION_CANDIDATE));
    this.selectedUnsequenceFiles.forEach(
        x -> x.setStatus(TsFileResourceStatus.COMPACTION_CANDIDATE));
  }

  public List<TsFileResource> getSelectedUnsequenceFiles() {
    return selectedUnsequenceFiles;
  }

  @Override
  public String toString() {
    return storageGroupName
        + "-"
        + dataRegionId
        + "-"
        + timePartition
        + " task seq files are "
        + selectedSequenceFiles.toString()
        + " , unseq files are "
        + selectedUnsequenceFiles.toString();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof CrossSpaceCompactionTask)) {
      return false;
    }

    return equalsOtherTask((CrossSpaceCompactionTask) other);
  }

  @Override
  public void resetCompactionCandidateStatusForAllSourceFiles() {
    selectedSequenceFiles.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
    selectedUnsequenceFiles.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
  }

  private long deleteOldFiles(List<TsFileResource> tsFileResourceList) throws IOException {
    long totalSize = 0;
    for (TsFileResource tsFileResource : tsFileResourceList) {
      totalSize += tsFileResource.getTsFileSize();
      tsFileResource.remove();
      LOGGER.info(
          "[CrossSpaceCompaction] Delete TsFile :{}.",
          tsFileResource.getTsFile().getAbsolutePath());
    }
    return totalSize;
  }

  private void releaseReadAndLockWrite(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.readUnlock();
      holdReadLockList.remove(tsFileResource);
      tsFileResource.writeLock();
      holdWriteLockList.add(tsFileResource);
    }
  }

  @Override
  public boolean checkValidAndSetMerging() {
    return addReadLock(selectedSequenceFiles) && addReadLock(selectedUnsequenceFiles);
  }

  private boolean addReadLock(List<TsFileResource> tsFileResourceList) {
    if (!tsFileManager.isAllowCompaction()) {
      return false;
    }
    try {
      for (TsFileResource tsFileResource : tsFileResourceList) {
        tsFileResource.readLock();
        holdReadLockList.add(tsFileResource);
        if (tsFileResource.isCompacting()
            || !tsFileResource.isClosed()
            || !tsFileResource.getTsFile().exists()
            || tsFileResource.isDeleted()) {
          releaseAllLock();
          return false;
        }
        tsFileResource.setStatus(TsFileResourceStatus.COMPACTING);
      }
    } catch (Throwable e) {
      releaseAllLock();
      throw e;
    }
    return true;
  }

  @Override
  protected void createSummary() {
    if (performer instanceof FastCompactionPerformer) {
      this.summary = new FastCompactionTaskSummary();
    } else {
      this.summary = new CompactionTaskSummary();
    }
  }
}
