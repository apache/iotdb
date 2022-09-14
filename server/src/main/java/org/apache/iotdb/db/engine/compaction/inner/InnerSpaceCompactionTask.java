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

package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.log.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.performer.ICompactionPerformer;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class InnerSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  protected List<TsFileResource> selectedTsFileResourceList;
  protected TsFileResource targetTsFileResource;
  protected boolean sequence;
  protected long selectedFileSize;
  protected int sumOfCompactionCount;
  protected long maxFileVersion;
  protected int maxCompactionCount;

  protected TsFileResourceList tsFileResourceList;
  protected List<TsFileResource> targetTsFileList;
  protected boolean[] isHoldingReadLock;
  protected boolean[] isHoldingWriteLock;

  public InnerSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      ICompactionPerformer performer,
      AtomicInteger currentTaskNum,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        currentTaskNum,
        serialId);
    this.selectedTsFileResourceList = selectedTsFileResourceList;
    this.sequence = sequence;
    this.performer = performer;
    isHoldingReadLock = new boolean[selectedTsFileResourceList.size()];
    isHoldingWriteLock = new boolean[selectedTsFileResourceList.size()];
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      isHoldingWriteLock[i] = false;
      isHoldingReadLock[i] = false;
    }
    if (sequence) {
      tsFileResourceList = tsFileManager.getSequenceListByTimePartition(timePartition);
    } else {
      tsFileResourceList = tsFileManager.getUnsequenceListByTimePartition(timePartition);
    }
    this.hashCode = this.toString().hashCode();
    collectSelectedFilesInfo();
  }

  @Override
  protected void doCompaction() {
    if (!tsFileManager.isAllowCompaction()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    // get resource of target file
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    LOGGER.info(
        "{}-{} [Compaction] starting compaction task with {} files",
        storageGroupName,
        dataRegionId,
        selectedTsFileResourceList.size());
    try {
      targetTsFileResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(
              selectedTsFileResourceList, sequence);
    } catch (IOException e) {
      LOGGER.error("Failed to get target file for {}", selectedTsFileResourceList, e);
      return;
    }
    File logFile =
        new File(
            dataDirectory
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    try (CompactionLogger compactionLogger = new CompactionLogger(logFile)) {
      // Here is tmpTargetFile, which is xxx.target
      targetTsFileList = new ArrayList<>(Collections.singletonList(targetTsFileResource));
      compactionLogger.logFiles(selectedTsFileResourceList, CompactionLogger.STR_SOURCE_FILES);
      compactionLogger.logFiles(targetTsFileList, CompactionLogger.STR_TARGET_FILES);
      LOGGER.info(
          "{}-{} [InnerSpaceCompactionTask] Close the logger", storageGroupName, dataRegionId);
      compactionLogger.close();
      LOGGER.info(
          "{}-{} [Compaction] compaction with {}",
          storageGroupName,
          dataRegionId,
          selectedTsFileResourceList);

      // carry out the compaction
      performer.setSourceFiles(selectedTsFileResourceList);
      // As elements in targetFiles may be removed in ReadPointCompactionPerformer, we should use a
      // mutable list instead of Collections.singletonList()
      performer.setTargetFiles(targetTsFileList);
      performer.setSummary(summary);
      performer.perform();

      CompactionUtils.moveTargetFile(targetTsFileList, true, storageGroupName + "-" + dataRegionId);

      LOGGER.info(
          "{}-{} [InnerSpaceCompactionTask] start to rename mods file",
          storageGroupName,
          dataRegionId);
      CompactionUtils.combineModsInInnerCompaction(
          selectedTsFileResourceList, targetTsFileResource);

      if (Thread.currentThread().isInterrupted() || summary.isCancel()) {
        throw new InterruptedException(
            String.format("%s-%s [Compaction] abort", storageGroupName, dataRegionId));
      }

      // replace the old files with new file, the new is in same position as the old
      if (sequence) {
        tsFileManager.replace(
            selectedTsFileResourceList,
            Collections.emptyList(),
            targetTsFileList,
            timePartition,
            true);
      } else {
        tsFileManager.replace(
            Collections.emptyList(),
            selectedTsFileResourceList,
            targetTsFileList,
            timePartition,
            false);
      }

      LOGGER.info(
          "{}-{} [Compaction] Compacted target files, try to get the write lock of source files",
          storageGroupName,
          dataRegionId);

      // release the read lock of all source files, and get the write lock of them to delete them
      for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
        selectedTsFileResourceList.get(i).readUnlock();
        isHoldingReadLock[i] = false;
        selectedTsFileResourceList.get(i).writeLock();
        isHoldingWriteLock[i] = true;
      }

      if (targetTsFileResource.getTsFile().exists()
          && targetTsFileResource.getTsFile().length()
              < TSFileConfig.MAGIC_STRING.getBytes().length * 2L + Byte.BYTES) {
        // the file size is smaller than magic string and version number
        throw new TsFileNotCompleteException(
            String.format(
                "target file %s is smaller than magic string and version number size",
                targetTsFileResource));
      }

      LOGGER.info(
          "{}-{} [Compaction] compaction finish, start to delete old files",
          storageGroupName,
          dataRegionId);
      // delete the old files
      CompactionUtils.deleteTsFilesInDisk(
          selectedTsFileResourceList, storageGroupName + "-" + dataRegionId);
      CompactionUtils.deleteModificationForSourceFile(
          selectedTsFileResourceList, storageGroupName + "-" + dataRegionId);

      double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
      LOGGER.info(
          "{}-{} [InnerSpaceCompactionTask] all compaction task finish, target file is {},"
              + "time cost is {} s, compaction speed is {} MB/s",
          storageGroupName,
          dataRegionId,
          targetTsFileResource.getTsFile().getName(),
          costTime,
          ((double) selectedFileSize) / 1024.0d / 1024.0d / costTime);

      if (logFile.exists()) {
        FileUtils.delete(logFile);
      }
    } catch (Throwable throwable) {
      // catch throwable to handle OOM errors
      if (!(throwable instanceof InterruptedException)) {
        LOGGER.error(
            "{}-{} [Compaction] Meet errors in inner space compaction.",
            storageGroupName,
            dataRegionId,
            throwable);
      } else {
        // clean the interrupt flag
        LOGGER.warn("{}-{} [Compaction] Compaction interrupted", storageGroupName, dataRegionId);
        Thread.interrupted();
      }

      // handle exception
      if (isSequence()) {
        CompactionExceptionHandler.handleException(
            storageGroupName + "-" + dataRegionId,
            logFile,
            targetTsFileList,
            selectedTsFileResourceList,
            Collections.emptyList(),
            tsFileManager,
            timePartition,
            true,
            isSequence());
      } else {
        CompactionExceptionHandler.handleException(
            storageGroupName + "-" + dataRegionId,
            logFile,
            targetTsFileList,
            Collections.emptyList(),
            selectedTsFileResourceList,
            tsFileManager,
            timePartition,
            true,
            isSequence());
      }
    } finally {
      releaseFileLocksAndResetMergingStatus();
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof InnerSpaceCompactionTask)) {
      return false;
    }
    InnerSpaceCompactionTask task = (InnerSpaceCompactionTask) otherTask;
    return this.selectedTsFileResourceList.equals(task.selectedTsFileResourceList)
        && this.performer.getClass().isInstance(task.performer);
  }

  @Override
  public void setSourceFilesToCompactionCandidate() {
    selectedTsFileResourceList.forEach(x -> x.setStatus(TsFileResourceStatus.COMPACTION_CANDIDATE));
  }

  private void collectSelectedFilesInfo() {
    selectedFileSize = 0L;
    sumOfCompactionCount = 0;
    maxFileVersion = -1L;
    maxCompactionCount = -1;
    if (selectedTsFileResourceList == null) {
      return;
    }
    for (TsFileResource resource : selectedTsFileResourceList) {
      try {
        selectedFileSize += resource.getTsFileSize();
        TsFileNameGenerator.TsFileName fileName =
            TsFileNameGenerator.getTsFileName(resource.getTsFile().getName());
        sumOfCompactionCount += fileName.getInnerCompactionCnt();
        if (fileName.getInnerCompactionCnt() > maxCompactionCount) {
          maxCompactionCount = fileName.getInnerCompactionCnt();
        }
        if (fileName.getVersion() > maxFileVersion) {
          maxFileVersion = fileName.getVersion();
        }
      } catch (IOException e) {
        LOGGER.warn("Fail to get the tsfile name of {}", resource.getTsFile(), e);
      }
    }
  }

  public List<TsFileResource> getSelectedTsFileResourceList() {
    return selectedTsFileResourceList;
  }

  public boolean isSequence() {
    return sequence;
  }

  public long getSelectedFileSize() {
    return selectedFileSize;
  }

  public int getSumOfCompactionCount() {
    return sumOfCompactionCount;
  }

  public long getMaxFileVersion() {
    return maxFileVersion;
  }

  @Override
  public String toString() {
    return storageGroupName
        + "-"
        + dataRegionId
        + "-"
        + timePartition
        + " task file num is "
        + selectedTsFileResourceList.size()
        + ", files is "
        + selectedTsFileResourceList
        + ", total compaction count is "
        + sumOfCompactionCount;
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof InnerSpaceCompactionTask)) {
      return false;
    }
    return equalsOtherTask((InnerSpaceCompactionTask) other);
  }

  @Override
  public void resetCompactionCandidateStatusForAllSourceFiles() {
    selectedTsFileResourceList.forEach(x -> x.setStatus(TsFileResourceStatus.CLOSED));
  }

  /**
   * release the read lock and write lock of files if it is held, and set the merging status of
   * selected files to false
   */
  protected void releaseFileLocksAndResetMergingStatus() {
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      TsFileResource resource = selectedTsFileResourceList.get(i);
      if (isHoldingReadLock[i]) {
        resource.readUnlock();
      }
      if (isHoldingWriteLock[i]) {
        resource.writeUnlock();
      }
      try {
        if (!resource.isDeleted()) {
          selectedTsFileResourceList.get(i).setStatus(TsFileResourceStatus.CLOSED);
        }
      } catch (Throwable e) {
        LOGGER.error("Exception occurs when resetting resource status", e);
      }
    }
  }

  @Override
  public boolean checkValidAndSetMerging() {
    if (!tsFileManager.isAllowCompaction()) {
      return false;
    }
    try {
      for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
        TsFileResource resource = selectedTsFileResourceList.get(i);
        resource.readLock();
        isHoldingReadLock[i] = true;
        if (resource.isCompacting()
            || !resource.isClosed()
            || !resource.getTsFile().exists()
            || resource.isDeleted()) {
          // this source file cannot be compacted
          // release the lock of locked files, and return
          releaseFileLocksAndResetMergingStatus();
          return false;
        }
      }

      for (TsFileResource resource : selectedTsFileResourceList) {
        resource.setStatus(TsFileResourceStatus.COMPACTING);
      }
    } catch (Throwable e) {
      releaseFileLocksAndResetMergingStatus();
      throw e;
    }
    return true;
  }
}
