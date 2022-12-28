/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.CompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.engine.compaction.utils.log.CompactionLogger.STR_SOURCE_FILES;
import static org.apache.iotdb.db.engine.compaction.utils.log.CompactionLogger.STR_TARGET_FILES;

public class RewriteCrossSpaceCompactionTask extends AbstractCrossSpaceCompactionTask {
  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected TsFileResourceList seqTsFileResourceList;
  protected TsFileResourceList unseqTsFileResourceList;
  private File logFile;
  private long memoryCost = -1;

  private List<TsFileResource> targetTsfileResourceList;
  private List<TsFileResource> holdReadLockList = new ArrayList<>();
  private List<TsFileResource> holdWriteLockList = new ArrayList<>();

  public RewriteCrossSpaceCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartitionId,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSeqTsFileResourceList,
      List<TsFileResource> selectedUnSeqTsFileResourceList,
      AtomicInteger currentTaskNum,
      long memoryCost) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartitionId,
        currentTaskNum,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList,
        tsFileManager);
    this.selectedSeqTsFileResourceList = selectedSeqTsFileResourceList;
    this.selectedUnSeqTsFileResourceList = selectedUnSeqTsFileResourceList;
    this.seqTsFileResourceList = tsFileManager.getSequenceListByTimePartition(timePartition);
    this.unseqTsFileResourceList = tsFileManager.getUnsequenceListByTimePartition(timePartition);
    this.memoryCost = memoryCost;
  }

  @Override
  protected void doCompaction() throws Exception {
    try {
      SystemInfo.getInstance().addCompactionMemoryCost(memoryCost);
    } catch (InterruptedException e) {
      logger.error("Thread get interrupted when allocating memory for compaction", e);
      return;
    }
    try {
      executeCompaction();
    } catch (Throwable throwable) {
      // catch throwable instead of exception to handle OOM errors
      logger.error("Meet errors in cross space compaction", throwable);
      CompactionExceptionHandler.handleException(
          fullStorageGroupName,
          logFile,
          targetTsfileResourceList,
          selectedSeqTsFileResourceList,
          selectedUnSeqTsFileResourceList,
          tsFileManager,
          timePartition,
          false,
          true);
      throw throwable;
    } finally {
      SystemInfo.getInstance().resetCompactionMemoryCost(memoryCost);
      releaseAllLock();
    }
  }

  private void executeCompaction()
      throws IOException, StorageEngineException, MetadataException, InterruptedException,
          WriteProcessException {
    if (!tsFileManager.isAllowCompaction()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    targetTsfileResourceList =
        TsFileNameGenerator.getCrossCompactionTargetFileResources(selectedSeqTsFileResourceList);

    if (targetTsfileResourceList.isEmpty()
        || selectedSeqTsFileResourceList.isEmpty()
        || selectedUnSeqTsFileResourceList.isEmpty()) {
      logger.info(
          "{} [Compaction] Cross space compaction file list is empty, end it",
          fullStorageGroupName);
      return;
    }

    double totalSize = 0;
    double seqSize = 0;
    double unseqSize = 0;
    for (TsFileResource resource : selectedSeqTsFileResourceList) {
      totalSize += resource.getTsFileSize();
    }
    seqSize = totalSize;
    for (TsFileResource resource : selectedUnSeqTsFileResourceList) {
      totalSize += resource.getTsFileSize();
    }
    unseqSize = totalSize - seqSize;

    logger.info(
        "{} [Compaction] CrossSpaceCompactionTask start. Sequence files : {}, unsequence files : {},"
            + " seq files size is {} MB, unseq file size is {} MB, total size is {} MB",
        fullStorageGroupName,
        selectedSeqTsFileResourceList,
        selectedUnSeqTsFileResourceList,
        seqSize / 1024 / 1024,
        unseqSize / 1024 / 1024,
        totalSize / 1024 / 1024);
    logFile =
        new File(
            selectedSeqTsFileResourceList.get(0).getTsFile().getParent()
                + File.separator
                + targetTsfileResourceList.get(0).getTsFile().getName()
                + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);

    try (CompactionLogger compactionLogger = new CompactionLogger(logFile)) {
      // print the path of the temporary file first for priority check during recovery
      compactionLogger.logFiles(selectedSeqTsFileResourceList, STR_SOURCE_FILES);
      compactionLogger.logFiles(selectedUnSeqTsFileResourceList, STR_SOURCE_FILES);
      compactionLogger.logFiles(targetTsfileResourceList, STR_TARGET_FILES);
      // indicates that the cross compaction is complete and the result can be reused during a
      // restart recovery
      compactionLogger.close();

      CompactionUtils.compact(
          selectedSeqTsFileResourceList, selectedUnSeqTsFileResourceList, targetTsfileResourceList);

      CompactionUtils.moveTargetFile(targetTsfileResourceList, false, fullStorageGroupName);
      CompactionUtils.combineModsInCompaction(
          selectedSeqTsFileResourceList, selectedUnSeqTsFileResourceList, targetTsfileResourceList);

      // update tsfile resource in memory
      tsFileManager.replace(
          selectedSeqTsFileResourceList,
          selectedUnSeqTsFileResourceList,
          targetTsfileResourceList,
          timePartition,
          true);

      releaseReadAndLockWrite(selectedSeqTsFileResourceList);
      releaseReadAndLockWrite(selectedUnSeqTsFileResourceList);

      deleteOldFiles(selectedSeqTsFileResourceList);
      deleteOldFiles(selectedUnSeqTsFileResourceList);
      CompactionUtils.deleteCompactionModsFile(
          selectedSeqTsFileResourceList, selectedUnSeqTsFileResourceList);

      if (logFile.exists()) {
        FileUtils.delete(logFile);
      }
      double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
      logger.info(
          "{} [Compaction] CrossSpaceCompactionTask Costs {} s, compaction speed is {} MB/s",
          fullStorageGroupName,
          costTime,
          totalSize / 1024.0d / 1024.0d / costTime);
    }
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

  @Override
  public boolean checkValidAndSetMerging() {
    return addReadLock(selectedSeqTsFileResourceList)
        && addReadLock(selectedUnSeqTsFileResourceList);
  }

  private void releaseReadAndLockWrite(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.readUnlock();
      holdReadLockList.remove(tsFileResource);
      tsFileResource.writeLock();
      holdWriteLockList.add(tsFileResource);
    }
  }

  private void releaseAllLock() {
    selectedSeqTsFileResourceList.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
    selectedUnSeqTsFileResourceList.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
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

  private void deleteOldFiles(List<TsFileResource> tsFileResourceList) throws IOException {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
      tsFileResource.setStatus(TsFileResourceStatus.DELETED);
      tsFileResource.remove();
      logger.info(
          "[CrossSpaceCompaction] Delete TsFile :{}.",
          tsFileResource.getTsFile().getAbsolutePath());
    }
  }

  public String getStorageGroupName() {
    return fullStorageGroupName;
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof RewriteCrossSpaceCompactionTask) {
      RewriteCrossSpaceCompactionTask otherTask = (RewriteCrossSpaceCompactionTask) other;
      return otherTask.selectedSeqTsFileResourceList.equals(selectedSeqTsFileResourceList)
          && otherTask.selectedUnSeqTsFileResourceList.equals(selectedUnSeqTsFileResourceList);
    }
    return false;
  }
}
