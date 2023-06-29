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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionExceptionHandler;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionValidationFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.validator.CompactionValidator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceList;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
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
  private File logFile;

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
      tsFileResourceList = tsFileManager.getOrCreateSequenceListByTimePartition(timePartition);
    } else {
      tsFileResourceList = tsFileManager.getOrCreateUnsequenceListByTimePartition(timePartition);
    }
    this.hashCode = this.toString().hashCode();
    this.innerSeqTask = sequence;
    this.crossTask = false;
    collectSelectedFilesInfo();
    createSummary();
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  protected boolean doCompaction() {
    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    long startTime = System.currentTimeMillis();
    // get resource of target file
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    LOGGER.info(
        "{}-{} [Compaction] {} InnerSpaceCompaction task starts with {} files, "
            + "total file size is {} MB.",
        storageGroupName,
        dataRegionId,
        sequence ? "Sequence" : "Unsequence",
        selectedTsFileResourceList.size(),
        selectedFileSize / 1024 / 1024);
    boolean isSuccess = true;

    try {
      targetTsFileResource =
          TsFileNameGenerator.getInnerCompactionTargetFileResource(
              selectedTsFileResourceList, sequence);
      logFile =
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
            "{}-{} [Compaction] compaction with {}",
            storageGroupName,
            dataRegionId,
            selectedTsFileResourceList);

        // carry out the compaction
        performer.setSourceFiles(selectedTsFileResourceList);
        // As elements in targetFiles may be removed in ReadPointCompactionPerformer, we should use
        // a
        // mutable list instead of Collections.singletonList()
        performer.setTargetFiles(targetTsFileList);
        performer.setSummary(summary);
        performer.perform();

        CompactionUtils.updateProgressIndex(
            targetTsFileList, selectedTsFileResourceList, Collections.emptyList());
        CompactionUtils.moveTargetFile(
            targetTsFileList, true, storageGroupName + "-" + dataRegionId);

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

        if (targetTsFileResource.isDeleted()) {
          compactionLogger.logFile(targetTsFileResource, CompactionLogger.STR_DELETED_TARGET_FILES);
        }

        CompactionValidator validator = CompactionValidator.getInstance();
        if (!validator.validateCompaction(
            tsFileManager, targetTsFileList, storageGroupName, timePartition)) {
          LOGGER.error(
              "Failed to pass compaction validation, source files is: {}, target files is {}",
              selectedTsFileResourceList,
              targetTsFileList);
          throw new CompactionValidationFailedException("Failed to pass compaction validation");
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
        long[] sizeList = new long[selectedTsFileResourceList.size()];
        for (int i = 0, size = selectedTsFileResourceList.size(); i < size; ++i) {
          sizeList[i] = selectedTsFileResourceList.get(i).getTsFileSize();
        }
        CompactionUtils.deleteTsFilesInDisk(
            selectedTsFileResourceList, storageGroupName + "-" + dataRegionId);
        CompactionUtils.deleteModificationForSourceFile(
            selectedTsFileResourceList, storageGroupName + "-" + dataRegionId);

        // inner space compaction task has only one target file
        if (!targetTsFileResource.isDeleted()) {
          FileMetrics.getInstance()
              .addFile(
                  targetTsFileResource.getTsFile().length(),
                  sequence,
                  targetTsFileResource.getTsFile().getName());

          // set target resource to CLOSED, so that it can be selected to compact
          targetTsFileResource.setStatus(TsFileResourceStatus.NORMAL);
        } else {
          // target resource is empty after compaction, then delete it
          targetTsFileResource.remove();
        }
        List<String> fileNames = new ArrayList<>();
        for (TsFileResource resource : selectedTsFileResourceList) {
          fileNames.add(resource.getTsFile().getName());
        }
        FileMetrics.getInstance().deleteFile(sizeList, sequence, fileNames);

        CompactionMetrics.getInstance().recordSummaryInfo(summary);

        double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
        LOGGER.info(
            "{}-{} [Compaction] {} InnerSpaceCompaction task finishes successfully, "
                + "target file is {},"
                + "time cost is {} s, "
                + "compaction speed is {} MB/s, {}",
            storageGroupName,
            dataRegionId,
            sequence ? "Sequence" : "Unsequence",
            targetTsFileResource.getTsFile().getName(),
            costTime,
            selectedFileSize / 1024.0d / 1024.0d / costTime,
            summary);
      }
      if (logFile.exists()) {
        FileUtils.delete(logFile);
      }
    } catch (Exception e) {
      isSuccess = false;
      // catch throwable to handle OOM errors
      if (!(e instanceof InterruptedException)) {
        LOGGER.error(
            "{}-{} [Compaction] Meet errors in inner space compaction.",
            storageGroupName,
            dataRegionId,
            e);
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
      releaseAllLocksAndResetStatus();
    }
    return isSuccess;
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
  protected List<TsFileResource> getAllSourceTsFiles() {
    return this.selectedTsFileResourceList;
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

  /**
   * release the read lock and write lock of files if it is held, and set the merging status of
   * selected files to false.
   */
  private void releaseAllLocksAndResetStatus() {
    resetCompactionCandidateStatusForAllSourceFiles();
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      TsFileResource resource = selectedTsFileResourceList.get(i);
      if (isHoldingReadLock[i]) {
        resource.readUnlock();
      }
      if (isHoldingWriteLock[i]) {
        resource.writeUnlock();
      }
    }
  }

  @Override
  public boolean checkValidAndSetMerging() {
    if (!tsFileManager.isAllowCompaction()) {
      resetCompactionCandidateStatusForAllSourceFiles();
      return false;
    }
    try {
      for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
        TsFileResource resource = selectedTsFileResourceList.get(i);
        resource.readLock();
        isHoldingReadLock[i] = true;
        if (!resource.setStatus(TsFileResourceStatus.COMPACTING)) {
          releaseAllLocksAndResetStatus();
          return false;
        }
      }
    } catch (Exception e) {
      releaseAllLocksAndResetStatus();
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
