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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICrossCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.apache.tsfile.utils.TsFileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CrossSpaceCompactionTask extends AbstractCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected List<TsFileResource> selectedSequenceFiles;
  protected List<TsFileResource> selectedUnsequenceFiles;
  private File logFile;
  protected List<TsFileResource> targetTsfileResourceList;
  private List<TsFileResource> emptyTargetTsFileResourceList;
  protected List<TsFileResource> holdWriteLockList = new ArrayList<>();
  protected double selectedSeqFileSize = 0;
  protected double selectedUnseqFileSize = 0;

  @SuppressWarnings("squid:S107")
  public CrossSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedSequenceFiles,
      List<TsFileResource> selectedUnsequenceFiles,
      ICrossCompactionPerformer performer,
      long memoryCost,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    this.selectedSequenceFiles = selectedSequenceFiles;
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
    for (TsFileResource resource : selectedSequenceFiles) {
      selectedSeqFileSize += resource.getTsFileSize();
    }
    for (TsFileResource resource : selectedUnsequenceFiles) {
      selectedUnseqFileSize += resource.getTsFileSize();
    }
    this.emptyTargetTsFileResourceList = new ArrayList<>();
    this.performer = performer;
    this.hashCode = this.toString().hashCode();
    this.memoryCost = memoryCost;
    createSummary();
  }

  public CrossSpaceCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, 0L, tsFileManager, 0L);
    this.logFile = logFile;
    this.needRecoverTaskInfoFromLogFile = true;
  }

  private void recoverTaskInfoFromLogFile() throws IOException {
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    this.selectedSequenceFiles = new ArrayList<>();
    sourceFileIdentifiers.stream()
        .filter(TsFileIdentifier::isSequence)
        .forEach(f -> this.selectedSequenceFiles.add(new TsFileResource(f.getFileFromDataDirs())));
    sourceFileIdentifiers.stream()
        .filter(f -> !f.isSequence())
        .forEach(
            f -> this.selectedUnsequenceFiles.add(new TsFileResource(f.getFileFromDataDirs())));

    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    List<TsFileIdentifier> deletedTargetFileIdentifiers = logAnalyzer.getDeletedTargetFileInfos();
    for (TsFileIdentifier f : targetFileIdentifiers) {
      File targetFileOnDisk = getRealTargetFile(f, IoTDBConstant.CROSS_COMPACTION_TMP_FILE_SUFFIX);
      // The targetFileOnDisk may be null, but it won't impact the task recover stage
      TsFileResource targetTsFile = new TsFileResource(targetFileOnDisk);
      this.targetTsfileResourceList.add(targetTsFile);
      if (deletedTargetFileIdentifiers.contains(f)) {
        this.emptyTargetTsFileResourceList.add(targetTsFile);
      }
    }
    this.taskStage = logAnalyzer.getTaskStage();
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  public boolean doCompaction() {
    recoverMemoryStatus = true;
    boolean isSuccess = true;
    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    if (!IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction()) {
      return true;
    }
    if (compactionConfigVersion
        < CompactionTaskManager.getInstance().getCurrentCompactionConfigVersion()) {
      return true;
    }

    if (selectedSequenceFiles.isEmpty() || selectedUnsequenceFiles.isEmpty()) {
      LOGGER.info(
          "{}-{} [Compaction] Cross space compaction file list is empty, end it",
          storageGroupName,
          dataRegionId);
      return true;
    }
    LOGGER.info(
        "{}-{} [Compaction] CrossSpaceCompaction task starts with {} seq files "
            + "and {} unsequence files. "
            + "Sequence files : {}, unsequence files : {} . "
            + "Sequence files size is {} MB, "
            + "unsequence file size is {} MB, "
            + "total size is {} MB",
        storageGroupName,
        dataRegionId,
        selectedSequenceFiles.size(),
        selectedUnsequenceFiles.size(),
        selectedSequenceFiles,
        selectedUnsequenceFiles,
        selectedSeqFileSize / 1024 / 1024,
        selectedUnseqFileSize / 1024 / 1024,
        (selectedSeqFileSize + selectedUnseqFileSize) / 1024 / 1024);
    try {
      long startTime = System.currentTimeMillis();
      targetTsfileResourceList =
          TsFileNameGenerator.getCrossCompactionTargetFileResources(selectedSequenceFiles);

      logFile =
          new File(
              selectedSequenceFiles.get(0).getTsFile().getParent()
                  + File.separator
                  + targetTsfileResourceList.get(0).getTsFile().getName()
                  + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX);

      try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
        // print the path of the temporary file first for priority check during recovery
        compactionLogger.logSourceFiles(selectedSequenceFiles);
        compactionLogger.logSourceFiles(selectedUnsequenceFiles);
        compactionLogger.logTargetFiles(targetTsfileResourceList);
        compactionLogger.force();

        performer.setSourceFiles(selectedSequenceFiles, selectedUnsequenceFiles);
        performer.setTargetFiles(targetTsfileResourceList);
        performer.setSummary(summary);
        performer.perform();

        CompactionUtils.updateProgressIndex(
            targetTsfileResourceList, selectedSequenceFiles, selectedUnsequenceFiles);
        CompactionUtils.moveTargetFile(
            targetTsfileResourceList,
            CompactionTaskType.CROSS,
            storageGroupName + "-" + dataRegionId);
        CompactionUtils.combineModsInCrossCompaction(
            selectedSequenceFiles, selectedUnsequenceFiles, targetTsfileResourceList);

        validateCompactionResult(
            selectedSequenceFiles, selectedUnsequenceFiles, targetTsfileResourceList);

        // update tsfile resource in memory
        tsFileManager.replace(
            selectedSequenceFiles,
            selectedUnsequenceFiles,
            targetTsfileResourceList,
            timePartition);

        // find empty target files and add log
        for (TsFileResource targetResource : targetTsfileResourceList) {
          if (targetResource.isDeleted()) {
            emptyTargetTsFileResourceList.add(targetResource);
            compactionLogger.logEmptyTargetFile(targetResource);
            compactionLogger.force();
          }
        }

        lockWrite(selectedSequenceFiles);
        lockWrite(selectedUnsequenceFiles);

        CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(
            selectedSequenceFiles, selectedUnsequenceFiles);

        for (TsFileResource targetResource : targetTsfileResourceList) {
          if (!targetResource.isDeleted()) {
            CompactionUtils.addFilesToFileMetrics(targetResource);
          } else {
            // target resource is empty after compaction, then delete it
            targetResource.remove();
          }
        }

        CompactionMetrics.getInstance().recordSummaryInfo(summary);

        double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;

        LOGGER.info(
            "{}-{} [Compaction] CrossSpaceCompaction task finishes successfully, "
                + "time cost is {} s, "
                + "compaction speed is {} MB/s, {}",
            storageGroupName,
            dataRegionId,
            String.format("%.2f", costTime),
            String.format(
                "%.2f",
                (selectedSeqFileSize + selectedUnseqFileSize) / 1024.0d / 1024.0d / costTime),
            summary);
      }
    } catch (Exception e) {
      isSuccess = false;
      handleException(LOGGER, e);
      recover();
    } finally {
      releaseAllLocks();
      try {
        if (logFile != null) {
          Files.deleteIfExists(logFile.toPath());
        }
      } catch (IOException e) {
        handleException(LOGGER, e);
      }
      for (TsFileResource resource : targetTsfileResourceList) {
        // may failed to set status if the status of current resource is DELETED
        resource.setStatus(TsFileResourceStatus.NORMAL);
      }
    }
    return isSuccess;
  }

  public void recover() {
    try {
      if (needRecoverTaskInfoFromLogFile) {
        recoverTaskInfoFromLogFile();
      }
      if (shouldRollback()) {
        rollback();
      } else {
        // That finishTask() is revoked means
        finishTask();
      }
    } catch (Exception e) {
      handleRecoverException(e);
    }
  }

  private boolean shouldRollback() {
    return checkAllSourceFileExists(selectedSequenceFiles)
        && checkAllSourceFileExists(selectedUnsequenceFiles);
  }

  private void rollback() throws IOException {
    // if the task has started,
    targetTsfileResourceList =
        targetTsfileResourceList == null ? Collections.emptyList() : targetTsfileResourceList;
    if (recoverMemoryStatus) {
      replaceTsFileInMemory(
          targetTsfileResourceList,
          Stream.concat(selectedSequenceFiles.stream(), selectedUnsequenceFiles.stream())
              .collect(Collectors.toList()));
    }
    deleteCompactionModsFile(selectedSequenceFiles);
    deleteCompactionModsFile(selectedUnsequenceFiles);
    // delete target file
    if (targetTsfileResourceList != null && !deleteTsFilesOnDisk(targetTsfileResourceList)) {
      throw new CompactionRecoverException("failed to delete target file %s");
    }
  }

  private void finishTask() throws IOException {
    for (TsFileResource target : targetTsfileResourceList) {
      if (target.isDeleted() || emptyTargetTsFileResourceList.contains(target)) {
        // it means the target file is empty after compaction
        if (!target.remove()) {
          throw new CompactionRecoverException(
              String.format("failed to delete empty target file %s", target));
        }
      } else {
        File targetFile = target.getTsFile();
        if (targetFile == null || !TsFileUtils.isTsFileComplete(target.getTsFile())) {
          throw new CompactionRecoverException(
              String.format("Target file is not completed. %s", targetFile));
        }
        if (recoverMemoryStatus) {
          target.setStatus(TsFileResourceStatus.NORMAL);
        }
      }
    }
    if (!deleteTsFilesOnDisk(selectedSequenceFiles)
        || !deleteTsFilesOnDisk(selectedUnsequenceFiles)) {
      throw new CompactionRecoverException("source files cannot be deleted successfully");
    }
    if (recoverMemoryStatus) {
      FileMetrics.getInstance().deleteTsFile(true, selectedSequenceFiles);
      FileMetrics.getInstance().deleteTsFile(false, selectedUnsequenceFiles);
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

  private void releaseAllLocks() {
    for (TsFileResource tsFileResource : holdWriteLockList) {
      tsFileResource.writeUnlock();
    }
    holdWriteLockList.clear();
  }

  public List<TsFileResource> getSelectedSequenceFiles() {
    return selectedSequenceFiles;
  }

  @Override
  public List<TsFileResource> getAllSourceTsFiles() {
    List<TsFileResource> allRelatedFiles = new ArrayList<>();
    allRelatedFiles.addAll(selectedSequenceFiles);
    allRelatedFiles.addAll(selectedUnsequenceFiles);
    return allRelatedFiles;
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

  private void lockWrite(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.writeLock();
      holdWriteLockList.add(tsFileResource);
    }
  }

  @Override
  public long getEstimatedMemoryCost() {
    return memoryCost;
  }

  @Override
  public int getProcessedFileNum() {
    return selectedSequenceFiles.size() + selectedUnsequenceFiles.size();
  }

  @Override
  protected void createSummary() {
    if (performer instanceof FastCompactionPerformer) {
      this.summary = new FastCompactionTaskSummary();
    } else {
      this.summary = new CompactionTaskSummary();
    }
  }

  @Override
  public CompactionTaskType getCompactionTaskType() {
    return CompactionTaskType.CROSS;
  }

  @Override
  public long getCompactionConfigVersion() {
    return this.compactionConfigVersion;
  }

  @Override
  public void setCompactionConfigVersion(long compactionConfigVersion) {
    this.compactionConfigVersion = Math.min(this.compactionConfigVersion, compactionConfigVersion);
  }

  @Override
  public long getSelectedFileSize() {
    return (long) (selectedSeqFileSize + selectedUnseqFileSize);
  }
}
