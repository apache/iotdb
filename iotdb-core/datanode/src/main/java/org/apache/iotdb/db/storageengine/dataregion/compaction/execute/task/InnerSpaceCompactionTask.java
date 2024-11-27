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
import org.apache.iotdb.db.storageengine.dataregion.modification.ModFileManagement;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InnerSpaceCompactionTask extends AbstractCompactionTask {
  protected InnerCompactionTaskFilesView filesView;
  protected File logFile;
  protected boolean[] isHoldingWriteLock;
  protected AbstractInnerSpaceEstimator innerSpaceEstimator;

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
    filesView = new InnerCompactionTaskFilesView(selectedTsFileResourceList, sequence);
    this.performer = performer;
    this.hashCode = this.hashCode();
    createSummary();
  }

  public InnerSpaceCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedTsFileResourceList,
      List<TsFileResource> skippedTsFileResourceList,
      boolean sequence,
      ICompactionPerformer performer,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    this.filesView =
        new InnerCompactionTaskFilesView(
            selectedTsFileResourceList, skippedTsFileResourceList, sequence);
    // may be modified later
    this.performer = performer;
    this.hashCode = this.hashCode();
    createSummary();
  }

  public InnerSpaceCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, 0L, tsFileManager, 0L);
    this.logFile = logFile;
    this.needRecoverTaskInfoFromLogFile = true;
    this.filesView = new InnerCompactionTaskFilesView();
  }

  protected static class InnerCompactionTaskFilesView {
    protected List<TsFileResource> sortedAllSourceFilesInTask;
    protected List<TsFileResource> sourceFilesInCompactionPerformer;
    protected List<TsFileResource> skippedSourceFiles;
    protected boolean sequence;
    protected List<TsFileResource> sourceFilesInLog;
    protected List<TsFileResource> targetFilesInLog;
    protected List<TsFileResource> targetFilesInPerformer;
    protected List<TsFileResource> renamedTargetFiles;

    protected long selectedFileSize;
    protected int sumOfCompactionCount;
    protected long maxFileVersion;
    protected int maxCompactionCount;

    protected InnerCompactionTaskFilesView(
        List<TsFileResource> selectedFiles, List<TsFileResource> skippedFiles, boolean sequence) {
      this.sourceFilesInCompactionPerformer = selectedFiles;
      this.skippedSourceFiles = skippedFiles;
      this.sequence = sequence;
      this.sortedAllSourceFilesInTask =
          Stream.concat(sourceFilesInCompactionPerformer.stream(), skippedSourceFiles.stream())
              .sorted(TsFileResource::compareFileName)
              .collect(Collectors.toList());
      collectSelectedFilesInfo();
    }

    protected InnerCompactionTaskFilesView(List<TsFileResource> selectedFiles, boolean sequence) {
      this(selectedFiles, Collections.emptyList(), sequence);
      this.sourceFilesInLog = selectedFiles;
    }

    protected InnerCompactionTaskFilesView() {}

    private void collectSelectedFilesInfo() {
      selectedFileSize = 0L;
      sumOfCompactionCount = 0;
      maxFileVersion = -1L;
      maxCompactionCount = -1;
      if (sourceFilesInCompactionPerformer == null) {
        return;
      }
      for (TsFileResource resource : sourceFilesInCompactionPerformer) {
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

    protected void setSourceFilesForRecover(List<TsFileResource> sourceFiles) {
      sourceFilesInCompactionPerformer = sourceFiles;
      sourceFilesInLog = sourceFiles;
      sortedAllSourceFilesInTask = sourceFiles;
    }

    protected void setTargetFileForRecover(TsFileResource resource) {
      targetFilesInLog = Collections.singletonList(resource);
      targetFilesInPerformer = targetFilesInLog;
      renamedTargetFiles = Collections.emptyList();
    }

    protected void setTargetFileForRecover(List<TsFileResource> resources) {
      targetFilesInLog = resources;
      targetFilesInPerformer = resources;
      renamedTargetFiles = Collections.emptyList();
    }
  }

  protected void prepareCompactionModFiles() throws IOException {
    if (!TsFileResource.useSharedModFile) {
      return;
    }
    TsFileResource firstSource = filesView.sourceFilesInLog.get(0);
    TsFileResource firstTarget = filesView.targetFilesInPerformer.get(0);
    ModFileManagement modFileManagement = firstSource.getModFileManagement();
    ModificationFile modificationFile = modFileManagement.allocateFor(firstTarget);
    for (TsFileResource tsFileResource : filesView.targetFilesInPerformer) {
      tsFileResource.setModFileManagement(modFileManagement);
      modFileManagement.addReference(tsFileResource, modificationFile);
      tsFileResource.setSharedModFile(modificationFile, false);
    }
    for (TsFileResource tsFileResource : filesView.sourceFilesInLog) {
      tsFileResource.setCompactionModFile(modificationFile);
    }
  }

  protected void prepare() throws IOException, DiskSpaceInsufficientException {
    calculateSourceFilesAndTargetFiles();
    prepareCompactionModFiles();
    isHoldingWriteLock = new boolean[this.filesView.sourceFilesInLog.size()];
    Arrays.fill(isHoldingWriteLock, false);
    String dataDirectory =
        filesView.sourceFilesInCompactionPerformer.get(0).getTsFile().getParent();
    String logSuffix =
        CompactionLogger.getLogSuffix(
            isSequence() ? CompactionTaskType.INNER_SEQ : CompactionTaskType.INNER_UNSEQ);
    logFile =
        new File(
            dataDirectory
                + File.separator
                + filesView.targetFilesInLog.get(0).getTsFile().getName()
                + logSuffix);
  }

  @Override
  @SuppressWarnings({"squid:S6541", "squid:S3776", "squid:S2142"})
  protected boolean doCompaction() {
    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    if ((filesView.sequence
            && !IoTDBDescriptor.getInstance().getConfig().isEnableSeqSpaceCompaction())
        || (!filesView.sequence
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
        filesView.sequence ? "Sequence" : "Unsequence",
        filesView.sourceFilesInCompactionPerformer.size(),
        filesView.selectedFileSize / 1024 / 1024,
        memoryCost == 0 ? 0 : (double) memoryCost / 1024 / 1024);
    boolean isSuccess = true;

    try {
      prepare();
      try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
        // Here is tmpTargetFile, which is xxx.target
        compactionLogger.logSourceFiles(filesView.sourceFilesInLog);
        compactionLogger.logTargetFiles(filesView.targetFilesInLog);
        compactionLogger.force();
        LOGGER.info(
            "{}-{} [Compaction] compaction with selected files {}, skipped files {}",
            storageGroupName,
            dataRegionId,
            filesView.sourceFilesInCompactionPerformer,
            filesView.skippedSourceFiles);
        compact(compactionLogger);
        double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
        LOGGER.info(
            "{}-{} [Compaction] {} InnerSpaceCompaction task finishes successfully, "
                + "target files are {},"
                + "time cost is {} s, "
                + "compaction speed is {} MB/s, {}",
            storageGroupName,
            dataRegionId,
            filesView.sequence ? "Sequence" : "Unsequence",
            filesView.targetFilesInLog,
            String.format("%.2f", costTime),
            String.format("%.2f", filesView.selectedFileSize / 1024.0d / 1024.0d / costTime),
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
      for (TsFileResource targetTsFileResource : filesView.targetFilesInLog) {
        targetTsFileResource.setStatus(TsFileResourceStatus.NORMAL);
      }
    }
    return isSuccess;
  }

  protected void calculateSourceFilesAndTargetFiles()
      throws DiskSpaceInsufficientException, IOException {
    LinkedList<TsFileResource> availablePositionForTargetFiles = new LinkedList<>();
    for (int i = filesView.sourceFilesInCompactionPerformer.size() - 1; i >= 0; i--) {
      TsFileResource resource1 = filesView.sourceFilesInCompactionPerformer.get(i);
      TsFileResource resource2 =
          filesView.sortedAllSourceFilesInTask.get(
              filesView.sortedAllSourceFilesInTask.size()
                  - 1
                  - availablePositionForTargetFiles.size());
      if (resource1 != resource2) {
        break;
      }
      availablePositionForTargetFiles.addFirst(resource1);
    }

    int requiredPositionNum =
        Math.min(
            (int)
                    (filesView.selectedFileSize
                        / IoTDBDescriptor.getInstance().getConfig().getTargetCompactionFileSize())
                + 1,
            filesView.sortedAllSourceFilesInTask.size());
    boolean needToAdjustSourceFilesPosition =
        requiredPositionNum > availablePositionForTargetFiles.size();
    filesView.targetFilesInLog = new ArrayList<>();

    if (needToAdjustSourceFilesPosition) {
      filesView.sourceFilesInLog = filesView.sortedAllSourceFilesInTask;
    } else {
      filesView.sourceFilesInLog = filesView.sourceFilesInCompactionPerformer;
    }
    // To avoid increasing the inner compaction cnt for skipped files, we increase the cross
    // compaction cnt. These skipped files will not be actually compacted, and increasing the level
    // may affect subsequent compaction task selections. The number of cross space compaction has
    // no practical effect, so we choose to modify the number of cross space compaction to avoid the
    // problem of the same file name.
    calculateRenamedTargetFiles(needToAdjustSourceFilesPosition);

    if (needToAdjustSourceFilesPosition) {
      filesView.targetFilesInPerformer =
          TsFileNameGenerator.getNewInnerCompactionTargetFileResources(
              filesView.sortedAllSourceFilesInTask.subList(
                  filesView.renamedTargetFiles.size(),
                  Math.min(
                      filesView.renamedTargetFiles.size() + requiredPositionNum,
                      filesView.sortedAllSourceFilesInTask.size())),
              filesView.sequence);
    } else {
      filesView.targetFilesInPerformer =
          TsFileNameGenerator.getNewInnerCompactionTargetFileResources(
              availablePositionForTargetFiles.subList(0, requiredPositionNum), filesView.sequence);
    }
    filesView.targetFilesInLog =
        new ArrayList<>(
            filesView.targetFilesInPerformer.size() + filesView.renamedTargetFiles.size());
    filesView.targetFilesInLog.addAll(filesView.renamedTargetFiles);
    filesView.targetFilesInLog.addAll(filesView.targetFilesInPerformer);
  }

  private void calculateRenamedTargetFiles(boolean needAdjustSourceFilePosition)
      throws IOException {
    filesView.renamedTargetFiles = new ArrayList<>();
    if (!needAdjustSourceFilePosition) {
      return;
    }
    for (int i = 0; i < filesView.skippedSourceFiles.size(); i++) {
      TsFileResource resource = filesView.sortedAllSourceFilesInTask.get(i);
      File file = resource.getTsFile();
      File skippedSourceFile = filesView.skippedSourceFiles.get(i).getTsFile();
      TsFileNameGenerator.TsFileName skippedSourceFileName =
          TsFileNameGenerator.getTsFileName(skippedSourceFile.getName());
      TsFileNameGenerator.TsFileName tsFileName = TsFileNameGenerator.getTsFileName(file.getName());
      String newFileName =
          String.format(
              "%s-%s-%s-%s" + TsFileConstant.TSFILE_SUFFIX,
              tsFileName.getTime(),
              tsFileName.getVersion(),
              skippedSourceFileName.getInnerCompactionCnt(),
              tsFileName.getCrossCompactionCnt() + 1);
      TsFileResource renamedTargetFile =
          new TsFileResource(
              new File(skippedSourceFile.getParentFile().getPath() + File.separator + newFileName),
              TsFileResourceStatus.COMPACTING);
      filesView.renamedTargetFiles.add(renamedTargetFile);
    }
  }

  protected void compact(SimpleCompactionLogger compactionLogger) throws Exception {
    // carry out the compaction
    performer.setSourceFiles(filesView.sourceFilesInCompactionPerformer);
    // As elements in targetFiles may be removed in performer, we should use a mutable list
    // instead of Collections.singletonList()
    performer.setTargetFiles(filesView.targetFilesInPerformer);
    performer.setSummary(summary);
    performer.perform();

    prepareTargetFiles();

    if (Thread.currentThread().isInterrupted() || summary.isCancel()) {
      throw new InterruptedException(
          String.format("%s-%s [Compaction] abort", storageGroupName, dataRegionId));
    }

    validateCompactionResult(
        filesView.sequence ? filesView.sourceFilesInLog : Collections.emptyList(),
        filesView.sequence ? Collections.emptyList() : filesView.sourceFilesInLog,
        filesView.targetFilesInLog);

    // replace the old files with new file, the new is in same position as the old
    tsFileManager.replace(
        filesView.sequence ? filesView.sourceFilesInLog : Collections.emptyList(),
        filesView.sequence ? Collections.emptyList() : filesView.sourceFilesInLog,
        filesView.targetFilesInLog,
        timePartition);

    // Some target files are not used in the performer, so these files are not generated during
    // compaction.
    // If we delete source files before recording the deleted target files and encounter a crash
    // after this operation, the recovery task cannot find complete source files or target files.
    // Therefore, we should log the deleted target files before removing the source files.
    for (TsFileResource targetTsFileResource : filesView.targetFilesInLog) {
      if (!targetTsFileResource.isDeleted()) {
        CompactionUtils.addFilesToFileMetrics(targetTsFileResource);
      } else {
        // target resource is empty after compaction, then delete it
        compactionLogger.logEmptyTargetFile(targetTsFileResource);
        compactionLogger.force();
        targetTsFileResource.remove();
      }
    }
    // get the write lock of them to delete them
    for (int i = 0; i < filesView.sourceFilesInLog.size(); ++i) {
      filesView.sourceFilesInLog.get(i).writeLock();
      isHoldingWriteLock[i] = true;
    }

    CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(
        filesView.sourceFilesInLog, filesView.sequence);

    CompactionMetrics.getInstance().recordSummaryInfo(summary);
  }

  protected void prepareTargetFiles() throws IOException {
    CompactionUtils.updateProgressIndex(
        filesView.targetFilesInPerformer,
        filesView.sequence ? filesView.sourceFilesInCompactionPerformer : Collections.emptyList(),
        filesView.sequence ? Collections.emptyList() : filesView.sourceFilesInCompactionPerformer);
    for (int i = 0; i < filesView.renamedTargetFiles.size(); i++) {
      TsFileResource oldFile = filesView.skippedSourceFiles.get(i);
      TsFileResource newFile = filesView.renamedTargetFiles.get(i);

      oldFile.link(newFile);

      newFile.deserialize();
    }
    CompactionUtils.moveTargetFile(
        filesView.targetFilesInPerformer,
        getCompactionTaskType(),
        storageGroupName + "-" + dataRegionId);

    CompactionUtils.combineModsInInnerCompaction(
        filesView.sourceFilesInCompactionPerformer, filesView.targetFilesInPerformer);
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

  private void recoverTaskInfoFromLogFile() throws IOException {
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    List<TsFileIdentifier> deletedTargetFileIdentifiers = logAnalyzer.getDeletedTargetFileInfos();

    // recover source files
    List<TsFileResource> selectedTsFileResourceList = new ArrayList<>();
    sourceFileIdentifiers.forEach(
        f -> selectedTsFileResourceList.add(new TsFileResource(f.getFileFromDataDirs())));

    filesView.setSourceFilesForRecover(selectedTsFileResourceList);
    // recover target files
    recoverTargetResource(targetFileIdentifiers, deletedTargetFileIdentifiers);
    this.taskStage = logAnalyzer.getTaskStage();
  }

  protected void recoverTargetResource(
      List<TsFileIdentifier> targetFileIdentifiers,
      List<TsFileIdentifier> deletedTargetFileIdentifiers) {
    List<TsFileResource> targetResources = new ArrayList<>(targetFileIdentifiers.size());
    for (TsFileIdentifier targetIdentifier : targetFileIdentifiers) {
      File tmpTargetFile = targetIdentifier.getFileFromDataDirsIfAnyAdjuvantFileExists();
      targetIdentifier.setFilename(
          targetIdentifier
              .getFilename()
              .replace(
                  CompactionUtils.getTmpFileSuffix(getCompactionTaskType()),
                  TsFileConstant.TSFILE_SUFFIX));
      File targetFile = targetIdentifier.getFileFromDataDirsIfAnyAdjuvantFileExists();
      TsFileResource resource;
      if (tmpTargetFile != null) {
        resource = new TsFileResource(tmpTargetFile);
      } else if (targetFile != null) {
        resource = new TsFileResource(targetFile);
      } else {
        // target file does not exist, then create empty resource
        resource = new TsFileResource(new File(targetIdentifier.getFilePath()));
      }
      // check if target file is deleted after compaction or not
      targetResources.add(resource);
      if (deletedTargetFileIdentifiers.contains(targetIdentifier)) {
        // target file is deleted after compaction
        resource.forceMarkDeleted();
      }
    }
    filesView.setTargetFileForRecover(targetResources);
  }

  protected void rollback() throws IOException {
    List<TsFileResource> targetFiles =
        filesView.targetFilesInLog == null ? Collections.emptyList() : filesView.targetFilesInLog;
    // if the task has started,
    if (recoverMemoryStatus) {
      replaceTsFileInMemory(targetFiles, filesView.sourceFilesInLog);
    }
    deleteCompactionModsFile(filesView.sortedAllSourceFilesInTask);
    // delete target file
    for (TsFileResource targetTsFileResource : targetFiles) {
      if (targetTsFileResource != null && !deleteTsFileOnDisk(targetTsFileResource)) {
        throw new CompactionRecoverException(
            String.format("failed to delete target file %s", targetTsFileResource));
      }
    }
  }

  protected void finishTask() throws IOException {
    for (TsFileResource targetTsFileResource : filesView.targetFilesInLog) {
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
    }
    if (!deleteTsFilesOnDisk(filesView.sourceFilesInLog)) {
      throw new CompactionRecoverException("source files cannot be deleted successfully");
    }
    if (recoverMemoryStatus) {
      FileMetrics.getInstance().deleteTsFile(filesView.sequence, filesView.sourceFilesInLog);
    }
  }

  protected boolean shouldRollback() {
    return checkAllSourceFileExists(filesView.sourceFilesInLog);
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof InnerSpaceCompactionTask)) {
      return false;
    }
    InnerSpaceCompactionTask task = (InnerSpaceCompactionTask) otherTask;
    return getAllSourceTsFiles().equals(task.getAllSourceTsFiles())
        && this.performer.getClass().isInstance(task.performer);
  }

  @Override
  public List<TsFileResource> getAllSourceTsFiles() {
    return filesView.sortedAllSourceFilesInTask;
  }

  public List<TsFileResource> getSelectedTsFileResourceList() {
    return filesView.sourceFilesInCompactionPerformer;
  }

  public double getAvgCompactionCount() {
    return (double) filesView.sumOfCompactionCount
        / filesView.sourceFilesInCompactionPerformer.size();
  }

  @TestOnly
  public void setTargetTsFileResource(TsFileResource targetTsFileResource) {
    this.filesView.setTargetFileForRecover(targetTsFileResource);
  }

  public boolean isSequence() {
    return filesView.sequence;
  }

  public long getSelectedFileSize() {
    return filesView.selectedFileSize;
  }

  public int getSumOfCompactionCount() {
    return filesView.sumOfCompactionCount;
  }

  public long getMaxFileVersion() {
    return filesView.maxFileVersion;
  }

  @Override
  public String toString() {
    return storageGroupName
        + "-"
        + dataRegionId
        + "-"
        + timePartition
        + " task file num is "
        + filesView.sourceFilesInCompactionPerformer.size()
        + ", files is "
        + filesView.sourceFilesInCompactionPerformer
        + ", total compaction count is "
        + filesView.sumOfCompactionCount;
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
    for (int i = 0; i < filesView.sourceFilesInLog.size(); ++i) {
      TsFileResource resource = filesView.sourceFilesInLog.get(i);
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
            innerSpaceEstimator.roughEstimateInnerCompactionMemory(
                filesView.sourceFilesInCompactionPerformer);
        memoryCost =
            CompactionEstimateUtils.shouldAccurateEstimate(roughEstimatedMemoryCost)
                ? roughEstimatedMemoryCost
                : innerSpaceEstimator.estimateInnerCompactionMemory(
                    filesView.sourceFilesInCompactionPerformer);
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
    return filesView.sourceFilesInCompactionPerformer.size();
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
    if (filesView.sequence) {
      return CompactionTaskType.INNER_SEQ;
    } else {
      return CompactionTaskType.INNER_UNSEQ;
    }
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
