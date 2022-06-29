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

package org.apache.iotdb.db.engine.compaction.cross;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.log.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.query.control.FileReaderManager;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.engine.compaction.log.CompactionLogger.STR_TARGET_FILES;

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
  protected long selectedFileSize = 0;

  public CrossSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSequenceFiles,
      List<TsFileResource> selectedUnsequenceFiles,
      ICrossCompactionPerformer performer,
      AtomicInteger currentTaskNum,
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
    this.seqTsFileResourceList = tsFileManager.getSequenceListByTimePartition(timePartition);
    this.unseqTsFileResourceList = tsFileManager.getUnsequenceListByTimePartition(timePartition);
    this.performer = performer;
    this.hashCode = this.toString().hashCode();
  }

  @Override
  protected void doCompaction() {
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
        selectedFileSize += resource.getTsFileSize();
      }
      for (TsFileResource resource : selectedUnsequenceFiles) {
        selectedFileSize += resource.getTsFileSize();
      }

      LOGGER.info(
          "{}-{} [Compaction] CrossSpaceCompactionTask start. Sequence files : {}, unsequence files : {}, total size is {} MB",
          storageGroupName,
          dataRegionId,
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          ((double) selectedFileSize) / 1024.0 / 1024.0);
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
        // indicates that the cross compaction is complete and the result can be reused during a
        // restart recovery
        compactionLogger.close();

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

        releaseReadAndLockWrite(selectedSequenceFiles);
        releaseReadAndLockWrite(selectedUnsequenceFiles);

        deleteOldFiles(selectedSequenceFiles);
        deleteOldFiles(selectedUnsequenceFiles);
        CompactionUtils.deleteCompactionModsFile(selectedSequenceFiles, selectedUnsequenceFiles);

        if (logFile.exists()) {
          FileUtils.delete(logFile);
        }
        long costTime = (System.currentTimeMillis() - startTime) / 1000;
        LOGGER.info(
            "{}-{} [Compaction] CrossSpaceCompactionTask Costs {} s, compaction speed is {} MB/s",
            storageGroupName,
            dataRegionId,
            costTime,
            ((double) selectedFileSize) / 1024.0d / 1024.0d / costTime);
      }
    } catch (Throwable throwable) {
      // catch throwable to handle OOM errors
      if (!(throwable instanceof InterruptedException)) {
        LOGGER.error(
            "{}-{} [Compaction] Meet errors in cross space compaction.",
            storageGroupName,
            dataRegionId);
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

  private void deleteOldFiles(List<TsFileResource> tsFileResourceList) throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
      tsFileResource.setStatus(TsFileResourceStatus.DELETED);
      tsFileResource.remove();
      LOGGER.info(
          "[CrossSpaceCompaction] Delete TsFile :{}.",
          tsFileResource.getTsFile().getAbsolutePath());
    }
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
}
