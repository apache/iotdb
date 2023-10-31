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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.FileCannotTransitToCompactingException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionTaskStage;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
  protected boolean crossTask;
  protected boolean innerSeqTask;
  protected CompactionTaskStage taskStage;
  protected long memoryCost = 0L;

  protected boolean recoverMemoryStatus;
  protected CompactionTaskPriorityType compactionTaskPriorityType;

  protected AbstractCompactionTask(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      long serialId) {
    this(
        storageGroupName,
        dataRegionId,
        timePartition,
        tsFileManager,
        serialId,
        CompactionTaskPriorityType.NORMAL);
  }

  protected AbstractCompactionTask(
      String storageGroupName,
      String dataRegionId,
      long timePartition,
      TsFileManager tsFileManager,
      long serialId,
      CompactionTaskPriorityType compactionTaskPriorityType) {
    this.storageGroupName = storageGroupName;
    this.dataRegionId = dataRegionId;
    this.timePartition = timePartition;
    this.tsFileManager = tsFileManager;
    this.serialId = serialId;
    this.compactionTaskPriorityType = compactionTaskPriorityType;
  }

  protected abstract List<TsFileResource> getAllSourceTsFiles();

  /**
   * This method will try to set the files to COMPACTION_CANDIDATE. If failed, it should roll back
   * all status to original value
   *
   * @return set status successfully or not
   */
  public boolean setSourceFilesToCompactionCandidate() {
    List<TsFileResource> files = getAllSourceTsFiles();
    for (int i = 0; i < files.size(); i++) {
      if (!files.get(i).setStatus(TsFileResourceStatus.COMPACTION_CANDIDATE)) {
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

  protected void printLogWhenException(Logger logger, Exception e) {
    if (e instanceof InterruptedException) {
      logger.warn("{}-{} [Compaction] Compaction interrupted", storageGroupName, dataRegionId);
      Thread.currentThread().interrupt();
    } else {
      logger.error(
          "{}-{} [Compaction] Meet errors {}.",
          getCompactionTaskType(),
          storageGroupName,
          dataRegionId,
          e);
    }
  }

  public boolean start() {
    boolean isSuccess = false;
    summary.start();
    try {
      isSuccess = doCompaction();
    } finally {
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
      if (!f.setStatus(TsFileResourceStatus.COMPACTING)) {
        throw new FileCannotTransitToCompactingException(f);
      }
    }
  }

  public abstract long getEstimatedMemoryCost() throws IOException;

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
      if (!tsFileResource.getTsFile().exists() || !tsFileResource.resourceFileExists()) {
        return false;
      }
    }
    return true;
  }

  protected void handleRecoverException(Exception e) {
    LOGGER.error(
        "{} [Compaction][Recover] Failed to recover compaction. TaskInfo: {}, Exception: {}",
        dataRegionId,
        this,
        e);
    tsFileManager.setAllowCompaction(false);
    LOGGER.error("stop compaction because of exception during recovering");
    CommonDescriptor.getInstance().getConfig().setNodeStatus(NodeStatus.ReadOnly);
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
      ModificationFile modificationFile = tsFile.getCompactionModFile();
      if (modificationFile.exists()) {
        modificationFile.remove();
      }
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

  public boolean isCrossTask() {
    return crossTask;
  }

  public long getTemporalFileSize() {
    return summary.getTemporalFileSize();
  }

  public boolean isInnerSeqTask() {
    return innerSeqTask;
  }

  public CompactionTaskPriorityType getCompactionTaskPriorityType() {
    return compactionTaskPriorityType;
  }

  public boolean isDiskSpaceCheckPassed() {
    if (compactionTaskPriorityType == CompactionTaskPriorityType.MOD_SETTLE) {
      return true;
    }
    return CompactionUtils.isDiskHasSpace();
  }

  public CompactionTaskType getCompactionTaskType() {
    if (this instanceof CrossSpaceCompactionTask) {
      return CompactionTaskType.CROSS;
    } else if (this instanceof InsertionCrossSpaceCompactionTask) {
      return CompactionTaskType.INSERTION;
    } else if (innerSeqTask) {
      return CompactionTaskType.INNER_SEQ;
    } else {
      return CompactionTaskType.INNER_UNSEQ;
    }
  }
}
