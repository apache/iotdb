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

public abstract class AbstractCrossSpaceCompactionTask extends AbstractCompactionTask {
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

  public AbstractCrossSpaceCompactionTask(
      String fullStorageGroupName,
      long timePartition,
      AtomicInteger currentTaskNum,
      List<TsFileResource> selectedSequenceFiles,
      List<TsFileResource> selectedUnsequenceFiles,
      TsFileManager tsFileManager) {
    super(fullStorageGroupName, timePartition, tsFileManager, currentTaskNum);
    this.selectedSequenceFiles = selectedSequenceFiles;
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
    this.seqTsFileResourceList = tsFileManager.getSequenceListByTimePartition(timePartition);
    this.unseqTsFileResourceList = tsFileManager.getUnsequenceListByTimePartition(timePartition);
  }

  @Override
  protected void doCompaction() throws Exception {
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
            "{} [Compaction] Cross space compaction file list is empty, end it",
            fullStorageGroupName);
        return;
      }

      LOGGER.info(
          "{} [Compaction] CrossSpaceCompactionTask start. Sequence files : {}, unsequence files : {}",
          fullStorageGroupName,
          selectedSequenceFiles,
          selectedUnsequenceFiles);
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

        performCompaction();

        CompactionUtils.moveTargetFile(targetTsfileResourceList, false, fullStorageGroupName);
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
        LOGGER.info(
            "{} [Compaction] CrossSpaceCompactionTask Costs {} s",
            fullStorageGroupName,
            (System.currentTimeMillis() - startTime) / 1000);
      }
    } catch (Throwable throwable) {
      // catch throwable instead of exception to handle OOM errors
      LOGGER.error("Meet errors in cross space compaction, {}", throwable.getMessage());
      CompactionExceptionHandler.handleException(
          fullStorageGroupName,
          logFile,
          targetTsfileResourceList,
          selectedSequenceFiles,
          selectedUnsequenceFiles,
          tsFileManager,
          timePartition,
          false,
          true);
      throw throwable;
    } finally {
      releaseAllLock();
    }
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
    return new StringBuilder()
        .append(fullStorageGroupName)
        .append("-")
        .append(timePartition)
        .append(" task seq files are ")
        .append(selectedSequenceFiles.toString())
        .append(" , unseq files are ")
        .append(selectedUnsequenceFiles.toString())
        .toString();
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
    return true;
  }

  public String getStorageGroupName() {
    return fullStorageGroupName;
  }
}
