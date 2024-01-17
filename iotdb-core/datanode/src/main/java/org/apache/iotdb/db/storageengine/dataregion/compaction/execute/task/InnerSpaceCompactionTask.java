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
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractInnerSpaceEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCompactionInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.ReadChunkInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class InnerSpaceCompactionTask extends AbstractCompactionTask {

  protected List<TsFileResource> selectedTsFileResourceList;
  protected TsFileResource targetTsFileResource;
  protected boolean isTargetTsFileEmpty;
  protected boolean sequence;
  protected long selectedFileSize;
  protected int sumOfCompactionCount;
  protected long maxFileVersion;
  protected int maxCompactionCount;
  private File logFile;
  protected List<TsFileResource> targetTsFileList;
  protected boolean[] isHoldingWriteLock;
  protected long maxModsFileSize;
  protected AbstractInnerSpaceEstimator innerSpaceEstimator;
  protected boolean needRecoverTaskInfoFromLogFile;

  public InnerSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      ICompactionPerformer performer,
      long serialId) {
    this(
        timePartition,
        tsFileManager,
        selectedTsFileResourceList,
        sequence,
        performer,
        serialId,
        CompactionTaskPriorityType.NORMAL);
  }

  public InnerSpaceCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, 0L, tsFileManager, 0L, CompactionTaskPriorityType.NORMAL);
    this.logFile = logFile;
    this.needRecoverTaskInfoFromLogFile = true;
  }

  private void recoverTaskInfoFromLogFile() throws IOException {
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    this.selectedTsFileResourceList = new ArrayList<>();
    sourceFileIdentifiers.forEach(
        f -> this.selectedTsFileResourceList.add(new TsFileResource(f.getFileFromDataDirs())));

    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    List<TsFileIdentifier> deletedTargetFileIdentifiers = logAnalyzer.getDeletedTargetFileInfos();
    if (!targetFileIdentifiers.isEmpty()) {
      File targetFileOnDisk =
          getRealTargetFile(
              targetFileIdentifiers.get(0), IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX);
      // The targetFileOnDisk may be null, but it won't impact the task recover stage
      this.targetTsFileResource = new TsFileResource(targetFileOnDisk);
    }
    this.isTargetTsFileEmpty = !deletedTargetFileIdentifiers.isEmpty();
    this.taskStage = logAnalyzer.getTaskStage();
  }

  public InnerSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      ICompactionPerformer performer,
      long serialId,
      CompactionTaskPriorityType compactionTaskPriorityType) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId,
        compactionTaskPriorityType);
    this.selectedTsFileResourceList = selectedTsFileResourceList;
    this.sequence = sequence;
    this.performer = performer;
    if (IoTDBDescriptor.getInstance().getConfig().isEnableCompactionMemControl()) {
      if (this.performer instanceof ReadChunkCompactionPerformer) {
        innerSpaceEstimator = new ReadChunkInnerCompactionEstimator();
      } else if (!sequence && this.performer instanceof FastCompactionPerformer) {
        innerSpaceEstimator = new FastCompactionInnerCompactionEstimator();
      }
    }
    isHoldingWriteLock = new boolean[selectedTsFileResourceList.size()];
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      isHoldingWriteLock[i] = false;
    }
    this.hashCode = this.toString().hashCode();
    this.innerSeqTask = sequence;
    this.crossTask = false;
    collectSelectedFilesInfo();
    createSummary();
  }

  private void prepare() throws IOException {
    targetTsFileResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource(
            selectedTsFileResourceList, sequence);
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    logFile =
        new File(
            dataDirectory
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  protected boolean doCompaction() {
    long startTime = System.currentTimeMillis();
    // get resource of target file
    recoverMemoryStatus = true;
    LOGGER.info(
        "{}-{} [Compaction] {} InnerSpaceCompaction task starts with {} files, "
            + "total file size is {} MB, memory cost is {} MB",
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
        targetTsFileList = new ArrayList<>(Collections.singletonList(targetTsFileResource));
        compactionLogger.logSourceFiles(selectedTsFileResourceList);
        compactionLogger.logTargetFile(targetTsFileResource);
        compactionLogger.force();
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

        validateCompactionResult(
            sequence ? selectedTsFileResourceList : Collections.emptyList(),
            sequence ? Collections.emptyList() : selectedTsFileResourceList,
            targetTsFileList);

        // replace the old files with new file, the new is in same position as the old
        tsFileManager.replace(
            sequence ? selectedTsFileResourceList : Collections.emptyList(),
            sequence ? Collections.emptyList() : selectedTsFileResourceList,
            targetTsFileList,
            timePartition,
            sequence);

        if (targetTsFileResource.isDeleted()) {
          compactionLogger.logEmptyTargetFile(targetTsFileResource);
          isTargetTsFileEmpty = true;
          compactionLogger.force();
        }

        LOGGER.info(
            "{}-{} [Compaction] Compacted target files, try to get the write lock of source files",
            storageGroupName,
            dataRegionId);
        // release the read lock of all source files, and get the write lock of them to delete them
        for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
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
        CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(
            selectedTsFileResourceList, sequence);
        CompactionUtils.deleteModificationForSourceFile(
            selectedTsFileResourceList, storageGroupName + "-" + dataRegionId);

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
          targetTsFileResource.remove();
        }
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
            String.format("%.2f", costTime),
            String.format("%.2f", selectedFileSize / 1024.0d / 1024.0d / costTime),
            summary);
      } finally {
        Files.deleteIfExists(logFile.toPath());
        // may failed to set status if the status of target resource is DELETED
        targetTsFileResource.setStatus(TsFileResourceStatus.NORMAL);
      }
    } catch (Exception e) {
      isSuccess = false;
      printLogWhenException(LOGGER, e);
      recover();
    } finally {
      releaseAllLocks();
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

  private void rollback() throws IOException {
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

  private void finishTask() throws IOException {
    if (targetTsFileResource.isDeleted() || isTargetTsFileEmpty) {
      // it means the target file is empty after compaction
      if (targetTsFileResource.remove()) {
        throw new CompactionRecoverException(
            String.format("failed to delete empty target file %s", targetTsFileResource));
      }
    } else {
      File targetFile = targetTsFileResource.getTsFile();
      if (targetFile == null || !TsFileUtils.isTsFileComplete(targetTsFileResource.getTsFile())) {
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
    deleteCompactionModsFile(selectedTsFileResourceList);
  }

  private boolean shouldRollback() {
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
    maxModsFileSize = 0;
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
        if (!Objects.isNull(resource.getModFile())) {
          long modsFileSize = resource.getModFile().getSize();
          maxModsFileSize = Math.max(maxModsFileSize, modsFileSize);
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

  public long getMaxModsFileSize() {
    return maxModsFileSize;
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
  private void releaseAllLocks() {
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      TsFileResource resource = selectedTsFileResourceList.get(i);
      if (isHoldingWriteLock[i]) {
        resource.writeUnlock();
      }
    }
  }

  @Override
  public long getEstimatedMemoryCost() {
    if (innerSpaceEstimator != null && memoryCost == 0L) {
      try {
        memoryCost = innerSpaceEstimator.estimateInnerCompactionMemory(selectedTsFileResourceList);
      } catch (IOException e) {
        innerSpaceEstimator.cleanup();
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
}
