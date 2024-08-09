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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractInnerSpaceEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.CompactionEstimateUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCompactionInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.ReadChunkInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.StopReadTsFileByInterruptException;
import org.apache.tsfile.utils.TsFileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InnerSpaceCompactionTask extends AbstractCompactionTask {

  protected List<TsFileResource> selectedTsFileResourceList;
  protected TsFileResource targetTsFileResource;
  protected boolean sequence;
  protected long selectedFileSize;
  protected int sumOfCompactionCount;
  protected long maxFileVersion;
  protected int maxCompactionCount;
  protected File logFile;
  protected List<TsFileResource> targetTsFileList;
  protected boolean[] isHoldingWriteLock;
  protected AbstractInnerSpaceEstimator innerSpaceEstimator;

  public InnerSpaceCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, 0L, tsFileManager, 0L);
    this.logFile = logFile;
    this.needRecoverTaskInfoFromLogFile = true;
  }

  private void recoverTaskInfoFromLogFile() throws IOException {
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    List<TsFileIdentifier> deletedTargetFileIdentifiers = logAnalyzer.getDeletedTargetFileInfos();

    // recover source files
    this.selectedTsFileResourceList = new ArrayList<>();
    sourceFileIdentifiers.forEach(
        f -> this.selectedTsFileResourceList.add(new TsFileResource(f.getFileFromDataDirs())));

    // recover target files
    recoverTargetResource(targetFileIdentifiers, deletedTargetFileIdentifiers);
    this.taskStage = logAnalyzer.getTaskStage();
  }

  protected void recoverTargetResource(
      List<TsFileIdentifier> targetFileIdentifiers,
      List<TsFileIdentifier> deletedTargetFileIdentifiers) {
    if (targetFileIdentifiers.isEmpty()) {
      return;
    }
    TsFileIdentifier targetIdentifier = targetFileIdentifiers.get(0);
    File tmpTargetFile = targetIdentifier.getFileFromDataDirsIfAnyAdjuvantFileExists();
    targetIdentifier.setFilename(
        targetIdentifier
            .getFilename()
            .replace(
                CompactionUtils.getTmpFileSuffix(getCompactionTaskType()),
                TsFileConstant.TSFILE_SUFFIX));
    File targetFile = targetIdentifier.getFileFromDataDirsIfAnyAdjuvantFileExists();
    if (tmpTargetFile != null) {
      targetTsFileResource = new TsFileResource(tmpTargetFile);
    } else if (targetFile != null) {
      targetTsFileResource = new TsFileResource(targetFile);
    } else {
      // target file does not exist, then create empty resource
      targetTsFileResource = new TsFileResource(new File(targetIdentifier.getFilePath()));
    }
    // check if target file is deleted after compaction or not
    if (deletedTargetFileIdentifiers.contains(targetIdentifier)) {
      // target file is deleted after compaction
      targetTsFileResource.forceMarkDeleted();
    }
  }

  public InnerSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      ICompactionPerformer performer,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    this.selectedTsFileResourceList = selectedTsFileResourceList;
    this.sequence = sequence;
    this.performer = performer;
    isHoldingWriteLock = new boolean[selectedTsFileResourceList.size()];
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      isHoldingWriteLock[i] = false;
    }
    this.hashCode = this.toString().hashCode();
    collectSelectedFilesInfo();
    createSummary();
  }

  public InnerSpaceCompactionTask(
      TsFileManager tsFileManager,
      long timePartition,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      ICompactionPerformer performer,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    this.selectedTsFileResourceList = selectedTsFileResourceList;
    this.sequence = sequence;
    this.performer = performer;
    isHoldingWriteLock = new boolean[selectedTsFileResourceList.size()];
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      isHoldingWriteLock[i] = false;
    }
    collectSelectedFilesInfo();
    createSummary();
  }

  protected void prepare() throws IOException, DiskSpaceInsufficientException {
    targetTsFileResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(
            selectedTsFileResourceList, sequence);
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    String logSuffix =
        CompactionLogger.getLogSuffix(
            isSequence() ? CompactionTaskType.INNER_SEQ : CompactionTaskType.INNER_UNSEQ);
    logFile =
        new File(
            dataDirectory
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + logSuffix);
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  protected boolean doCompaction() {
    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    if ((sequence && !IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction())
        || (!sequence
            && !IoTDBDescriptor.getInstance().getConfig().isEnableUnseqSpaceCompaction())) {
      return true;
    }
    if (this.compactionConfigVersion
        < CompactionTaskManager.getInstance().getCurrentCompactionConfigVersion()) {
      return true;
    }
    long startTime = System.currentTimeMillis();
    // get resource of target file
    recoverMemoryStatus = true;
    LOGGER.info(
        "{}-{} [Compaction] {} InnerSpaceCompaction task starts with {} files, "
            + "total file size is {} MB, estimated memory cost is {} MB",
        storageGroupName,
        dataRegionId,
        sequence ? "Sequence" : "Unsequence",
        selectedTsFileResourceList.size(),
        selectedFileSize / 1024 / 1024,
        memoryCost == 0 ? 0 : (double) memoryCost / 1024 / 1024);
    boolean isSuccess = true;

    try {
      prepare();
      try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
        // Here is tmpTargetFile, which is xxx.target
        compactionLogger.logSourceFiles(selectedTsFileResourceList);
        compactionLogger.logTargetFile(targetTsFileResource);
        compactionLogger.force();
        LOGGER.info(
            "{}-{} [Compaction] compaction with {}",
            storageGroupName,
            dataRegionId,
            selectedTsFileResourceList);
        compact(compactionLogger);
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
            String.format("%.2f", costTime),
            String.format("%.2f", selectedFileSize / 1024.0d / 1024.0d / costTime),
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
      // may fail to set status if the status of target resource is DELETED
      targetTsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    }
    return isSuccess;
  }

  protected void compact(SimpleCompactionLogger compactionLogger) throws Exception {
    // carry out the compaction
    targetTsFileList = new ArrayList<>(Collections.singletonList(targetTsFileResource));
    performer.setSourceFiles(selectedTsFileResourceList);
    // As elements in targetFiles may be removed in performer, we should use a mutable list
    // instead of Collections.singletonList()
    performer.setTargetFiles(targetTsFileList);
    performer.setSummary(summary);
    performer.perform();

    prepareTargetFiles();

    if (Thread.currentThread().isInterrupted() || summary.isCancel()) {
      throw new InterruptedException(
          String.format("%s-%s [Compaction] abort", storageGroupName, dataRegionId));
    }

    validateCompactionResult(
        sequence ? selectedTsFileResourceList : Collections.emptyList(),
        sequence ? Collections.emptyList() : selectedTsFileResourceList,
        targetTsFileList);

    // replace the old files with new file, the new is in same position as the old
    tsFileManager.replace(
        sequence ? selectedTsFileResourceList : Collections.emptyList(),
        sequence ? Collections.emptyList() : selectedTsFileResourceList,
        targetTsFileList,
        timePartition);

    // get the write lock of them to delete them
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      selectedTsFileResourceList.get(i).writeLock();
      isHoldingWriteLock[i] = true;
    }

    CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(selectedTsFileResourceList, sequence);

    // inner space compaction task has only one target file
    if (!targetTsFileResource.isDeleted()) {
      FileMetrics.getInstance()
          .addTsFile(
              targetTsFileResource.getDatabaseName(),
              targetTsFileResource.getDataRegionId(),
              targetTsFileResource.getTsFile().length(),
              sequence,
              targetTsFileResource.getTsFile().getName());
    } else {
      // target resource is empty after compaction, then delete it
      compactionLogger.logEmptyTargetFile(targetTsFileResource);
      compactionLogger.force();
      targetTsFileResource.remove();
    }
    CompactionMetrics.getInstance().recordSummaryInfo(summary);
  }

  protected void prepareTargetFiles() throws IOException {
    CompactionUtils.updateProgressIndex(
        targetTsFileList, selectedTsFileResourceList, Collections.emptyList());
    CompactionUtils.moveTargetFile(
        targetTsFileList, getCompactionTaskType(), storageGroupName + "-" + dataRegionId);

    CompactionUtils.combineModsInInnerCompaction(selectedTsFileResourceList, targetTsFileResource);
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

  protected void rollback() throws IOException {
    // if the task has started,
    if (recoverMemoryStatus) {
      replaceTsFileInMemory(
          Collections.singletonList(targetTsFileResource), selectedTsFileResourceList);
    }
    deleteCompactionModsFile(selectedTsFileResourceList);
    // delete target file
    if (targetTsFileResource != null && !deleteTsFileOnDisk(targetTsFileResource)) {
      throw new CompactionRecoverException(
          String.format("failed to delete target file %s", targetTsFileResource));
    }
  }

  protected void finishTask() throws IOException {
    if (targetTsFileResource.isDeleted()) {
      // it means the target file is empty after compaction
      if (!targetTsFileResource.remove()) {
        throw new CompactionRecoverException(
            String.format("failed to delete empty target file %s", targetTsFileResource));
      }
    } else {
      File targetFile = targetTsFileResource.getTsFile();
      if (targetFile == null
          || !targetFile.exists()
          || !TsFileUtils.isTsFileComplete(targetTsFileResource.getTsFile())) {
        throw new CompactionRecoverException(
            String.format("Target file is not completed. %s", targetFile));
      }
      if (recoverMemoryStatus) {
        targetTsFileResource.setStatus(TsFileResourceStatus.NORMAL);
      }
    }
    if (!deleteTsFilesOnDisk(selectedTsFileResourceList)) {
      throw new CompactionRecoverException("source files cannot be deleted successfully");
    }
    if (recoverMemoryStatus) {
      FileMetrics.getInstance().deleteTsFile(true, selectedTsFileResourceList);
    }
  }

  protected boolean shouldRollback() {
    return checkAllSourceFileExists(selectedTsFileResourceList);
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
  public List<TsFileResource> getAllSourceTsFiles() {
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
  protected void releaseAllLocks() {
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      TsFileResource resource = selectedTsFileResourceList.get(i);
      if (isHoldingWriteLock[i]) {
        resource.writeUnlock();
      }
    }
  }

  @Override
  public long getEstimatedMemoryCost() {
    if (innerSpaceEstimator == null) {
      if (this.performer instanceof ReadChunkCompactionPerformer) {
        innerSpaceEstimator = new ReadChunkInnerCompactionEstimator();
      } else if (this.performer instanceof FastCompactionPerformer) {
        innerSpaceEstimator = new FastCompactionInnerCompactionEstimator();
      }
    }
    if (innerSpaceEstimator != null && memoryCost == 0L) {
      try {
        long roughEstimatedMemoryCost =
            innerSpaceEstimator.roughEstimateInnerCompactionMemory(selectedTsFileResourceList);
        memoryCost =
            CompactionEstimateUtils.shouldAccurateEstimate(roughEstimatedMemoryCost)
                ? roughEstimatedMemoryCost
                : innerSpaceEstimator.estimateInnerCompactionMemory(selectedTsFileResourceList);
      } catch (Exception e) {
        if (e instanceof StopReadTsFileByInterruptException || Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return -1;
        }
        innerSpaceEstimator.cleanup();
        // This exception may be caused by drop database
        if (!tsFileManager.isAllowCompaction()) {
          return -1;
        }
        LOGGER.error("Meet error when estimate inner compaction memory", e);
        return -1;
      }
    }
    return memoryCost;
  }

  @Override
  public int getProcessedFileNum() {
    return selectedTsFileResourceList.size();
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
    if (sequence) {
      return CompactionTaskType.INNER_SEQ;
    } else {
      return CompactionTaskType.INNER_UNSEQ;
    }
  }

  @TestOnly
  public void setTargetTsFileResource(TsFileResource targetTsFileResource) {
    this.targetTsFileResource = targetTsFileResource;
  }

  @Override
  public long getCompactionConfigVersion() {
    return this.compactionConfigVersion;
  }

  @Override
  public void setCompactionConfigVersion(long compactionConfigVersion) {
    this.compactionConfigVersion = Math.min(this.compactionConfigVersion, compactionConfigVersion);
  }
}
