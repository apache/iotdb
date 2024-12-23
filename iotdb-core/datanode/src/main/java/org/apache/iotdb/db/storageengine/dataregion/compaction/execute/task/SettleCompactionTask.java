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

import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * This settle task contains fully_dirty files and partially_dirty files. This task will do the
 * following two things respectively: 1. Settle all fully_dirty files by deleting them directly. 2.
 * Settle partially_dirty files by cleaning them with inner space compaction task. The
 * partially_dirty files will be compacted into one target file.
 */
public class SettleCompactionTask extends InnerSpaceCompactionTask {
  private List<TsFileResource> fullyDirtyFiles;
  private double fullyDirtyFileSize = 0;
  private double partiallyDirtyFileSize = 0;

  private int fullyDeletedSuccessNum = 0;

  private long totalModsFileSize;

  public SettleCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> fullyDirtyFiles,
      List<TsFileResource> partiallyDirtyFiles,
      boolean isSequence,
      ICompactionPerformer performer,
      long serialId) {
    super(timePartition, tsFileManager, partiallyDirtyFiles, isSequence, performer, serialId);
    this.fullyDirtyFiles = fullyDirtyFiles;
    fullyDirtyFiles.forEach(x -> fullyDirtyFileSize += x.getTsFileSize());
    partiallyDirtyFiles.forEach(
        x -> {
          partiallyDirtyFileSize += x.getTsFileSize();
          totalModsFileSize += x.getTotalModSizeInByte();
        });
    this.hashCode = this.toString().hashCode();
  }

  public SettleCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, tsFileManager, logFile);
  }

  @Override
  public List<TsFileResource> getAllSourceTsFiles() {
    List<TsFileResource> allSourceFiles = new ArrayList<>(fullyDirtyFiles);
    allSourceFiles.addAll(filesView.sourceFilesInCompactionPerformer);
    return allSourceFiles;
  }

  @Override
  protected void calculateSourceFilesAndTargetFiles() throws IOException {
    filesView.renamedTargetFiles = Collections.emptyList();
    filesView.targetFilesInLog =
        filesView.sourceFilesInCompactionPerformer.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(
                TsFileNameGenerator.getSettleCompactionTargetFileResources(
                    filesView.sourceFilesInCompactionPerformer, filesView.sequence));
    filesView.targetFilesInPerformer = filesView.targetFilesInLog;
  }

  @Override
  protected boolean doCompaction() {
    recoverMemoryStatus = true;
    boolean isSuccess;

    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    if (fullyDirtyFiles.isEmpty() && filesView.sourceFilesInCompactionPerformer.isEmpty()) {
      LOGGER.info(
          "{}-{} [Compaction] Settle compaction file list is empty, end it",
          storageGroupName,
          dataRegionId);
    }
    long startTime = System.currentTimeMillis();

    LOGGER.info(
        "{}-{} [Compaction] SettleCompaction task starts with {} fully_dirty files "
            + "and {} partially_dirty files. "
            + "Fully_dirty files : {}, partially_dirty files : {} . "
            + "Fully_dirty files size is {} MB, "
            + "partially_dirty file size is {} MB. "
            + "Memory cost is {} MB.",
        storageGroupName,
        dataRegionId,
        fullyDirtyFiles.size(),
        filesView.sourceFilesInCompactionPerformer.size(),
        fullyDirtyFiles,
        filesView.sourceFilesInCompactionPerformer,
        fullyDirtyFileSize / 1024 / 1024,
        partiallyDirtyFileSize / 1024 / 1024,
        memoryCost == 0 ? 0 : (double) memoryCost / 1024 / 1024);

    List<TsFileResource> allSourceFiles = getAllSourceTsFiles();
    logFile =
        new File(
            allSourceFiles.get(0).getTsFile().getAbsolutePath()
                + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      calculateSourceFilesAndTargetFiles();
      isHoldingWriteLock = new boolean[this.filesView.sourceFilesInLog.size()];
      Arrays.fill(isHoldingWriteLock, false);
      compactionLogger.logSourceFiles(fullyDirtyFiles);
      compactionLogger.logEmptyTargetFiles(fullyDirtyFiles);
      compactionLogger.logSourceFiles(filesView.sourceFilesInCompactionPerformer);
      compactionLogger.logTargetFiles(filesView.targetFilesInLog);
      compactionLogger.force();

      isSuccess = settleWithFullyDirtyFiles();
      // In order to prevent overlap of sequence files after settle task, partially_dirty files can
      // only be settled after fully_dirty files are settled successfully, because multiple
      // partially_dirty files will be settled into one file.
      if (isSuccess) {
        settleWithPartiallyDirtyFiles(compactionLogger);
      }

      double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
      if (isSuccess) {
        if (partiallyDirtyFileSize == 0) {
          LOGGER.info(
              "{}-{} [Compaction] SettleCompaction task finishes successfully, time cost is {} s."
                  + "Fully_dirty files num is {}.",
              storageGroupName,
              dataRegionId,
              String.format("%.2f", costTime),
              fullyDirtyFiles.size());
        } else {
          LOGGER.info(
              "{}-{} [Compaction] SettleCompaction task finishes successfully, time cost is {} s, compaction speed is {} MB/s."
                  + "Fully_dirty files num is {} and partially_dirty files num is {}.",
              storageGroupName,
              dataRegionId,
              String.format("%.2f", costTime),
              String.format("%.2f", (partiallyDirtyFileSize) / 1024.0d / 1024.0d / costTime),
              fullyDirtyFiles.size(),
              filesView.sourceFilesInCompactionPerformer.size());
        }
      } else {
        LOGGER.info(
            "{}-{} [Compaction] SettleCompaction task finishes with some error, time cost is {} s."
                + "Fully_dirty files num is {} and there are {} files fail to delete.",
            storageGroupName,
            dataRegionId,
            String.format("%.2f", costTime),
            fullyDirtyFiles.size(),
            allSourceFiles.size() - fullyDeletedSuccessNum);
      }
    } catch (Exception e) {
      isSuccess = false;
      handleException(LOGGER, e);
      recover();
    } finally {
      releaseAllLocks();
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        handleException(LOGGER, e);
      }
      // may fail to set status if the status of target resource is DELETED
      for (TsFileResource resource : filesView.targetFilesInLog) {
        resource.setStatus(TsFileResourceStatus.NORMAL);
      }
    }
    return isSuccess;
  }

  public boolean settleWithFullyDirtyFiles() {
    if (fullyDirtyFiles.isEmpty()) {
      return true;
    }
    boolean isSuccess = true;
    for (TsFileResource resource : fullyDirtyFiles) {
      if (recoverMemoryStatus) {
        tsFileManager.remove(resource, resource.isSeq());
      }
      boolean res = deleteTsFileOnDisk(resource);
      if (res) {
        fullyDeletedSuccessNum++;
        LOGGER.debug(
            "Settle task deletes fully_dirty tsfile {} successfully.",
            resource.getTsFile().getAbsolutePath());
        if (recoverMemoryStatus) {
          FileMetrics.getInstance()
              .deleteTsFile(resource.isSeq(), Collections.singletonList(resource));
        }
      } else {
        LOGGER.error(
            "Settle task fail to delete fully_dirty tsfile {}.",
            resource.getTsFile().getAbsolutePath());
      }
      isSuccess = isSuccess && res;
    }
    return isSuccess;
  }

  /** Use inner compaction task to compact the partially_dirty files. */
  private void settleWithPartiallyDirtyFiles(SimpleCompactionLogger logger) throws Exception {
    if (filesView.sourceFilesInCompactionPerformer.isEmpty()) {
      return;
    }
    LOGGER.info(
        "{}-{} [Compaction] Start to settle {} {} partially_dirty files, "
            + "total file size is {} MB",
        storageGroupName,
        dataRegionId,
        filesView.sourceFilesInCompactionPerformer.size(),
        filesView.sequence ? "Sequence" : "Unsequence",
        filesView.selectedFileSize / 1024 / 1024);
    long startTime = System.currentTimeMillis();
    compact(logger);
    double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
    LOGGER.info(
        "{}-{} [Compaction] Finish to settle {} {} partially_dirty files successfully , "
            + "target file is {},"
            + "time cost is {} s, "
            + "compaction speed is {} MB/s, {}",
        storageGroupName,
        dataRegionId,
        filesView.sourceFilesInCompactionPerformer.size(),
        filesView.sequence ? "Sequence" : "Unsequence",
        filesView.targetFilesInLog.get(0).getTsFile().getName(),
        String.format("%.2f", costTime),
        String.format("%.2f", filesView.selectedFileSize / 1024.0d / 1024.0d / costTime),
        summary);
  }

  @Override
  public void recover() {
    LOGGER.info(
        "{}-{} [Compaction][Recover] Start to recover settle compaction.",
        storageGroupName,
        dataRegionId);
    try {
      if (needRecoverTaskInfoFromLogFile) {
        recoverSettleTaskInfoFromLogFile();
      }
      recoverFullyDirtyFiles();
      recoverPartiallyDirtyFiles();
      LOGGER.info(
          "{}-{} [Compaction][Recover] Finish to recover settle compaction successfully.",
          storageGroupName,
          dataRegionId);
      if (needRecoverTaskInfoFromLogFile) {
        Files.deleteIfExists(logFile.toPath());
      }
    } catch (Exception e) {
      handleRecoverException(e);
    }
  }

  public void recoverFullyDirtyFiles() {
    if (!settleWithFullyDirtyFiles()) {
      throw new CompactionRecoverException("Failed to delete fully_dirty source file.");
    }
  }

  private void recoverPartiallyDirtyFiles() throws IOException {
    if (shouldRollback()) {
      rollback();
    } else {
      finishTask();
    }
  }

  public void recoverSettleTaskInfoFromLogFile() throws IOException {
    LOGGER.info(
        "{}-{} [Compaction][Recover] compaction log is {}",
        storageGroupName,
        dataRegionId,
        logFile);
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    List<TsFileIdentifier> deletedTargetFileIdentifiers = logAnalyzer.getDeletedTargetFileInfos();

    fullyDirtyFiles = new ArrayList<>();
    List<TsFileResource> selectedTsFileResourceList = new ArrayList<>();
    // recover source files, including fully_dirty files and partially_dirty files
    for (TsFileIdentifier x : sourceFileIdentifiers) {
      File sourceFile = x.getFileFromDataDirsIfAnyAdjuvantFileExists();
      TsFileResource resource;
      if (sourceFile == null) {
        // source file has been deleted, create empty resource
        resource = new TsFileResource(new File(x.getFilePath()));
      } else {
        resource = new TsFileResource(sourceFile);
      }
      if (deletedTargetFileIdentifiers.contains(x)) {
        fullyDirtyFiles.add(resource);
      } else {
        selectedTsFileResourceList.add(resource);
      }
    }

    filesView.setSourceFilesForRecover(selectedTsFileResourceList);
    // recover target file
    recoverTargetResource(targetFileIdentifiers, deletedTargetFileIdentifiers);
  }

  @Override
  public CompactionTaskType getCompactionTaskType() {
    return CompactionTaskType.SETTLE;
  }

  public List<TsFileResource> getFullyDirtyFiles() {
    return fullyDirtyFiles;
  }

  public List<TsFileResource> getPartiallyDirtyFiles() {
    return filesView.sourceFilesInCompactionPerformer;
  }

  public double getFullyDirtyFileSize() {
    return fullyDirtyFileSize;
  }

  public double getPartiallyDirtyFileSize() {
    return partiallyDirtyFileSize;
  }

  public long getTotalModsSize() {
    return totalModsFileSize;
  }

  @Override
  public long getEstimatedMemoryCost() {
    if (filesView.sourceFilesInCompactionPerformer.isEmpty()) {
      return 0;
    }
    return super.getEstimatedMemoryCost();
  }

  @Override
  public String toString() {
    return storageGroupName
        + "-"
        + dataRegionId
        + "-"
        + timePartition
        + " fully_dirty file num is "
        + fullyDirtyFiles.size()
        + ", partially_dirty file num is "
        + filesView.sourceFilesInCompactionPerformer.size()
        + ", fully_dirty files is "
        + fullyDirtyFiles
        + ", partially_dirty files is "
        + filesView.sourceFilesInCompactionPerformer;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SettleCompactionTask)) {
      return false;
    }

    return equalsOtherTask((SettleCompactionTask) other);
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof SettleCompactionTask)) {
      return false;
    }
    SettleCompactionTask otherSettleCompactionTask = (SettleCompactionTask) otherTask;
    return this.fullyDirtyFiles.equals(otherSettleCompactionTask.fullyDirtyFiles)
        && filesView.sourceFilesInCompactionPerformer.equals(
            otherSettleCompactionTask.filesView.sourceFilesInCompactionPerformer)
        && this.performer.getClass().isInstance(otherSettleCompactionTask.performer);
  }

  @Override
  public long getSelectedFileSize() {
    return (long) (partiallyDirtyFileSize + fullyDirtyFileSize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullyDirtyFiles, filesView.sourceFilesInCompactionPerformer, performer);
  }
}
