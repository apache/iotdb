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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionFileCountExceededException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionLastTimeCheckFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionMemoryNotEnoughException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionValidationFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.FileCannotTransitToCompactingException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionTaskStage;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.RepairDataFileScanUtil;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.validate.TsFileValidator;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.StopReadTsFileByInterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * AbstractCompactionTask is the base class for all compaction task, it carries out the execution of
 * compaction. AbstractCompactionTask uses a template method, it executes the abstract function
 * {@link AbstractCompactionTask#doCompaction()} implemented by subclass, and decrease the
 * currentTaskNum in CompactionScheduler when the {@link AbstractCompactionTask#doCompaction()} is
 * finished. The future returns the {@link CompactionTaskSummary} of this task execution.
 */
public abstract class AbstractCompactionTask {
  protected static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  protected String dataRegionId;
  protected String storageGroupName;
  protected long timePartition;
  protected final TsFileManager tsFileManager;
  protected ICompactionPerformer performer;
  protected int hashCode = -1;
  protected CompactionTaskSummary summary;
  protected long serialId;
  protected CompactionTaskStage taskStage;
  protected long memoryCost = 0L;

  protected boolean recoverMemoryStatus;

  protected boolean needRecoverTaskInfoFromLogFile;

  private boolean memoryAcquired = false;
  private boolean fileHandleAcquired = false;
  protected long compactionConfigVersion = Long.MAX_VALUE;

  protected AbstractCompactionTask(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      long serialId) {
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
    this.serialId = serialId;
  }

  public abstract List<TsFileResource> getAllSourceTsFiles();

  public long getCompactionConfigVersion() {
    // This parameter should not take effect by default unless it is overridden by a subclass
    return Long.MAX_VALUE;
  }

  public void setCompactionConfigVersion(long compactionConfigVersion) {
    // This parameter should not take effect by default unless it is overridden by a subclass
    this.compactionConfigVersion = Long.MAX_VALUE;
  }

  /**
   * This method will try to set the files to COMPACTION_CANDIDATE. If failed, it should roll back
   * all status to original value
   *
   * @return set status successfully or not
   */
  public boolean setSourceFilesToCompactionCandidate() {
    List<TsFileResource> files = getAllSourceTsFiles();
    for (int i = 0; i < files.size(); i++) {
      if (!files.get(i).transformStatus(TsFileResourceStatus.COMPACTION_CANDIDATE)) {
        // rollback status to NORMAL
        for (int j = 0; j < i; j++) {
          files.get(j).setStatus(TsFileResourceStatus.NORMAL);
        }
        return false;
      }
    }
    return true;
  }

  protected abstract boolean doCompaction();

  protected abstract void recover();

  public void handleTaskCleanup() {}

  protected void handleException(Logger logger, Exception e) {
    if (e instanceof CompactionLastTimeCheckFailedException
        || e instanceof CompactionValidationFailedException) {
      logger.error(
          "{}-{} [Compaction] {} task meets error: {}.",
          storageGroupName,
          dataRegionId,
          getCompactionTaskType(),
          e.getMessage());
      List<TsFileResource> unsortedTsFileResources = new ArrayList<>();
      if (e instanceof CompactionLastTimeCheckFailedException) {
        unsortedTsFileResources.addAll(getAllSourceTsFiles());
      } else {
        CompactionValidationFailedException validationException =
            (CompactionValidationFailedException) e;
        List<TsFileResource> overlappedTsFileResource =
            validationException.getOverlappedTsFileResources();
        unsortedTsFileResources =
            overlappedTsFileResource == null ? unsortedTsFileResources : overlappedTsFileResource;
      }
      // these exceptions generally caused by unsorted data, mark all source files as NEED_TO_REPAIR
      for (TsFileResource resource : unsortedTsFileResources) {
        if (resource.getTsFileRepairStatus() != TsFileRepairStatus.CAN_NOT_REPAIR) {
          resource.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_CHECK);
        }
      }
    } else if (e instanceof InterruptedException
        || Thread.interrupted()
        || e instanceof StopReadTsFileByInterruptException
        || !tsFileManager.isAllowCompaction()) {
      logger.warn(
          "{}-{} [Compaction] {} task interrupted",
          storageGroupName,
          dataRegionId,
          getCompactionTaskType());
      Thread.currentThread().interrupt();
    } else {
      logger.error(
          "{}-{} [Compaction] {} task meets error: {}.",
          storageGroupName,
          dataRegionId,
          getCompactionTaskType(),
          e);
    }
  }

  public boolean tryOccupyResourcesForRunning() {
    if (!isDiskSpaceCheckPassed()) {
      return false;
    }
    boolean blockUntilCanExecute = false;
    long estimatedMemoryCost = getEstimatedMemoryCost();
    try {
      SystemInfo.getInstance()
          .addCompactionMemoryCost(
              getCompactionTaskType(), estimatedMemoryCost, blockUntilCanExecute);
      memoryAcquired = true;
      SystemInfo.getInstance().addCompactionFileNum(getProcessedFileNum(), blockUntilCanExecute);
      fileHandleAcquired = true;
    } catch (CompactionMemoryNotEnoughException | CompactionFileCountExceededException ignored) {
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      if (!memoryAcquired || !fileHandleAcquired) {
        releaseOccupiedResources();
      }
    }
    return memoryAcquired && fileHandleAcquired;
  }

  public void releaseOccupiedResources() {
    if (memoryAcquired) {
      SystemInfo.getInstance()
          .resetCompactionMemoryCost(getCompactionTaskType(), getEstimatedMemoryCost());
      memoryAcquired = false;
    }
    if (fileHandleAcquired) {
      SystemInfo.getInstance().decreaseCompactionFileNumCost(getProcessedFileNum());
      fileHandleAcquired = false;
    }
  }

  public boolean start() {
    boolean isSuccess = false;
    summary.start();
    try {
      isSuccess = doCompaction();
    } finally {
      resetCompactionCandidateStatusForAllSourceFiles();
      handleTaskCleanup();
      releaseOccupiedResources();
      summary.finish(isSuccess);
      CompactionTaskManager.getInstance().removeRunningTaskFuture(this);
      CompactionMetrics.getInstance()
          .recordTaskFinishOrAbort(getCompactionTaskType(), summary.getTimeCost());
    }
    return isSuccess;
  }

  public String getStorageGroupName() {
    return this.storageGroupName;
  }

  public String getDataRegionId() {
    return this.dataRegionId;
  }

  public long getTimePartition() {
    return timePartition;
  }

  public abstract boolean equalsOtherTask(AbstractCompactionTask otherTask);

  public void transitSourceFilesToMerging() throws FileCannotTransitToCompactingException {
    for (TsFileResource f : getAllSourceTsFiles()) {
      if (!f.transformStatus(TsFileResourceStatus.COMPACTING)) {
        throw new FileCannotTransitToCompactingException(f);
      }
    }
  }

  public abstract long getEstimatedMemoryCost();

  public abstract int getProcessedFileNum();

  public boolean isCompactionAllowed() {
    return tsFileManager.isAllowCompaction();
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof AbstractCompactionTask) {
      return equalsOtherTask((AbstractCompactionTask) other);
    }
    return false;
  }

  public void resetCompactionCandidateStatusForAllSourceFiles() {
    List<TsFileResource> resources = getAllSourceTsFiles();
    // only reset status of the resources whose status is COMPACTING and COMPACTING_CANDIDATE
    resources.forEach(x -> x.setStatus(TsFileResourceStatus.NORMAL));
  }

  public long getTimeCost() {
    return summary.getTimeCost();
  }

  protected void checkInterrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted()) {
      throw new InterruptedException(
          String.format("%s-%s [Compaction] abort", storageGroupName, dataRegionId));
    }
  }

  protected void replaceTsFileInMemory(
      List<TsFileResource> removedTsFiles, List<TsFileResource> addedTsFiles) throws IOException {
    tsFileManager.writeLock("compactionRollBack");
    try {
      removeTsFileInMemory(removedTsFiles);
      insertFilesToTsFileManager(addedTsFiles);
    } finally {
      tsFileManager.writeUnlock();
    }
  }

  protected boolean checkAllSourceFileExists(List<TsFileResource> tsFileResources) {
    for (TsFileResource tsFileResource : tsFileResources) {
      if (!tsFileResource.tsFileExists() || !tsFileResource.resourceFileExists()) {
        return false;
      }
    }
    return true;
  }

  protected void handleRecoverException(Exception e) {
    // all files in this data region may be deleted directly without acquire lock or transfer
    // TsFileResourceStatus
    if (!tsFileManager.isAllowCompaction()) {
      return;
    }
    LOGGER.error(
        "{} [Compaction][Recover] Failed to recover compaction. TaskInfo: {}, Exception: {}",
        dataRegionId,
        this,
        e);
    // Do not set allow compaction to false here. To keep the error environment, mark all source
    // files in memory to avoid compaction.
    for (TsFileResource sourceTsFileResource : getAllSourceTsFiles()) {
      sourceTsFileResource.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
    }
  }

  protected void insertFilesToTsFileManager(List<TsFileResource> tsFiles) throws IOException {
    for (TsFileResource tsFileResource : tsFiles) {
      if (!tsFileResource.isFileInList()) {
        tsFileManager.keepOrderInsert(tsFileResource, tsFileResource.isSeq());
      }
    }
  }

  protected void removeTsFileInMemory(List<TsFileResource> resourceList) {
    for (TsFileResource targetTsFile : resourceList) {
      if (targetTsFile == null) {
        // target file has been deleted due to empty after compaction
        continue;
      }
      tsFileManager.remove(targetTsFile, targetTsFile.isSeq());
    }
  }

  public File getRealTargetFile(TsFileIdentifier targetFileIdentifier, String suffix) {
    File tmpTargetFile = targetFileIdentifier.getFileFromDataDirs();
    File targetFile =
        getFileFromDataDirs(
            targetFileIdentifier.getFilePath().replace(suffix, TsFileConstant.TSFILE_SUFFIX));
    return tmpTargetFile != null ? tmpTargetFile : targetFile;
  }

  /**
   * This method find the File object of given filePath by searching it in every data directory. If
   * the file is not found, it will return null.
   */
  public File getFileFromDataDirs(String filePath) {
    String[] dataDirs = IoTDBDescriptor.getInstance().getConfig().getLocalDataDirs();
    for (String dataDir : dataDirs) {
      File f = new File(dataDir, filePath);
      if (f.exists()) {
        return f;
      }
    }
    return null;
  }

  protected void deleteCompactionModsFile(List<TsFileResource> tsFileResourceList)
      throws IOException {
    for (TsFileResource tsFile : tsFileResourceList) {
      tsFile.removeCompactionModFile();
    }
  }

  protected boolean deleteTsFilesOnDisk(List<TsFileResource> tsFiles) {
    for (TsFileResource resource : tsFiles) {
      if (!deleteTsFileOnDisk(resource)) {
        return false;
      }
    }
    return true;
  }

  protected boolean deleteTsFileOnDisk(TsFileResource tsFileResource) {
    tsFileResource.writeLock();
    try {
      return tsFileResource.remove();
    } finally {
      tsFileResource.writeUnlock();
    }
  }

  public void setTaskStage(CompactionTaskStage stage) {
    this.taskStage = stage;
  }

  public boolean isTaskRan() {
    return summary.isRan();
  }

  public void cancel() {
    summary.cancel();
  }

  public boolean isSuccess() {
    return summary.isSuccess();
  }

  public CompactionTaskSummary getSummary() {
    return summary;
  }

  public boolean isTaskFinished() {
    return summary.isFinished();
  }

  public long getSerialId() {
    return serialId;
  }

  protected abstract void createSummary();

  public long getTemporalFileSize() {
    return summary.getTemporalFileSize();
  }

  public boolean isDiskSpaceCheckPassed() {
    if (getCompactionTaskType() == CompactionTaskType.SETTLE) {
      return true;
    }
    return CompactionUtils.isDiskHasSpace();
  }

  protected void validateCompactionResult(
      List<TsFileResource> sourceSeqFiles,
      List<TsFileResource> sourceUnseqFiles,
      List<TsFileResource> targetFiles)
      throws CompactionValidationFailedException {
    // skip TsFileResource which is marked as DELETED status
    List<TsFileResource> validTargetFiles =
        targetFiles.stream().filter(resource -> !resource.isDeleted()).collect(Collectors.toList());
    CompactionTaskType taskType = getCompactionTaskType();
    boolean needToValidateTsFileCorrectness = taskType != CompactionTaskType.INSERTION;
    boolean needToValidatePartitionSeqSpaceOverlap =
        !targetFiles.isEmpty() && targetFiles.get(0).isSeq();

    TsFileValidator validator = TsFileValidator.getInstance();
    if (needToValidatePartitionSeqSpaceOverlap) {
      checkSequenceSpaceOverlap(sourceSeqFiles, sourceUnseqFiles, targetFiles, validTargetFiles);
    }
    if (needToValidateTsFileCorrectness && !validator.validateTsFiles(validTargetFiles)) {
      LOGGER.error(
          "Failed to pass compaction validation, source seq files: {}, source unseq files: {}, target files: {}",
          sourceSeqFiles,
          sourceUnseqFiles,
          targetFiles);
      throw new CompactionValidationFailedException(
          "Failed to pass compaction validation, .resources file or tsfile data is wrong");
    }
  }

  protected void checkSequenceSpaceOverlap(
      List<TsFileResource> sourceSeqFiles,
      List<TsFileResource> sourceUnseqFiles,
      List<TsFileResource> targetFiles,
      List<TsFileResource> validTargetFiles) {
    List<TsFileResource> timePartitionSeqFiles =
        new ArrayList<>(tsFileManager.getOrCreateSequenceListByTimePartition(timePartition));
    timePartitionSeqFiles.removeAll(sourceSeqFiles);
    timePartitionSeqFiles.addAll(validTargetFiles);
    timePartitionSeqFiles.sort(
        (f1, f2) -> {
          int timeDiff =
              Long.compareUnsigned(
                  Long.parseLong(f1.getTsFile().getName().split("-")[0]),
                  Long.parseLong(f2.getTsFile().getName().split("-")[0]));
          return timeDiff == 0
              ? Long.compareUnsigned(
                  Long.parseLong(f1.getTsFile().getName().split("-")[1]),
                  Long.parseLong(f2.getTsFile().getName().split("-")[1]))
              : timeDiff;
        });
    if (this instanceof InnerSpaceCompactionTask
        || this instanceof InsertionCrossSpaceCompactionTask) {
      timePartitionSeqFiles =
          filterResourcesByFileTimeIndexInOverlapValidation(
              timePartitionSeqFiles, validTargetFiles);
    }
    List<TsFileResource> overlapFilesInTimePartition =
        RepairDataFileScanUtil.checkTimePartitionHasOverlap(timePartitionSeqFiles, true);
    if (!overlapFilesInTimePartition.isEmpty()) {
      LOGGER.error(
          "Failed to pass compaction overlap validation, source seq files: {}, source unseq files: {}, target files: {}",
          sourceSeqFiles,
          sourceUnseqFiles,
          targetFiles);
      for (TsFileResource resource : overlapFilesInTimePartition) {
        if (resource.getTsFileRepairStatus() != TsFileRepairStatus.CAN_NOT_REPAIR) {
          resource.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_CHECK);
        }
      }
    }
  }

  private List<TsFileResource> filterResourcesByFileTimeIndexInOverlapValidation(
      List<TsFileResource> timePartitionSeqFiles, List<TsFileResource> targetFiles) {
    if (targetFiles.isEmpty()) {
      return timePartitionSeqFiles;
    }
    TsFileResource firstTargetResource = targetFiles.get(0);
    TsFileResource lastTargetResource = targetFiles.get(targetFiles.size() - 1);
    long minStartTimeInTargetFiles =
        targetFiles.stream().mapToLong(TsFileResource::getFileStartTime).min().getAsLong();
    long maxEndTimeInTargetFiles =
        targetFiles.stream().mapToLong(TsFileResource::getFileEndTime).max().getAsLong();
    List<TsFileResource> result = new ArrayList<>(timePartitionSeqFiles.size());
    int idx;
    for (idx = 0; idx < timePartitionSeqFiles.size(); idx++) {
      TsFileResource resource = timePartitionSeqFiles.get(idx);
      if (resource == firstTargetResource) {
        break;
      }
      if (resource.getFileEndTime() >= minStartTimeInTargetFiles) {
        result.add(resource);
      }
    }
    for (; idx < timePartitionSeqFiles.size(); idx++) {
      TsFileResource resource = timePartitionSeqFiles.get(idx);
      result.add(resource);
      if (resource == lastTargetResource) {
        break;
      }
    }
    for (idx += 1; idx < timePartitionSeqFiles.size(); idx++) {
      TsFileResource resource = timePartitionSeqFiles.get(idx);
      if (resource.getFileStartTime() <= maxEndTimeInTargetFiles) {
        result.add(resource);
      }
    }
    return result;
  }

  public abstract CompactionTaskType getCompactionTaskType();

  @TestOnly
  public void setRecoverMemoryStatus(boolean recoverMemoryStatus) {
    this.recoverMemoryStatus = recoverMemoryStatus;
  }

  public abstract long getSelectedFileSize();
}
