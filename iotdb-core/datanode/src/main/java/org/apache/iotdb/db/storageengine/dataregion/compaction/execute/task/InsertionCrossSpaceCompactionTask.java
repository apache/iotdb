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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionValidationFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InsertionCrossSpaceCompactionTask extends AbstractCompactionTask {

  private Phaser phaser;
  private boolean failToPassValidation = false;

  public InsertionCrossSpaceCompactionTask(
      Phaser phaser,
      long timePartition,
      TsFileManager tsFileManager,
      InsertionCrossCompactionTaskResource taskResource,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    this.phaser = phaser;
    this.selectedSeqFiles = new ArrayList<>();
    this.selectedUnseqFiles = new ArrayList<>();
    if (taskResource.prevSeqFile != null) {
      selectedSeqFiles.add(taskResource.prevSeqFile);
    }
    if (taskResource.nextSeqFile != null) {
      selectedSeqFiles.add(taskResource.nextSeqFile);
    }
    if (taskResource.firstUnSeqFileInParitition != null) {
      selectedUnseqFiles.add(taskResource.firstUnSeqFileInParitition);
    }
    if (!taskResource.toInsertUnSeqFile.equals(taskResource.firstUnSeqFileInParitition)) {
      selectedUnseqFiles.add(taskResource.toInsertUnSeqFile);
    }
    this.unseqFileToInsert = taskResource.toInsertUnSeqFile;
    this.timestamp = taskResource.targetFileTimestamp;
    createSummary();
  }

  public InsertionCrossSpaceCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, 0L, tsFileManager, 0L);
    this.logFile = logFile;
    this.needRecoverTaskInfoFromLogFile = true;
    this.selectedSeqFiles = Collections.emptyList();
    this.selectedUnseqFiles = new ArrayList<>(1);
  }

  private TsFileResource unseqFileToInsert;
  private TsFileResource targetFile;
  private long timestamp;

  private List<TsFileResource> selectedSeqFiles;
  private List<TsFileResource> selectedUnseqFiles;
  private File logFile;
  protected List<TsFileResource> holdWriteLockList = new ArrayList<>();
  protected boolean needRecoverTaskInfoFromLogFile;

  @Override
  public List<TsFileResource> getAllSourceTsFiles() {
    return Stream.concat(selectedSeqFiles.stream(), selectedUnseqFiles.stream())
        .collect(Collectors.toList());
  }

  @Override
  public void handleTaskCleanup() {
    if (phaser != null) {
      phaser.arrive();
    }
  }

  @Override
  protected boolean doCompaction() {
    long startTime = System.currentTimeMillis();
    recoverMemoryStatus = true;
    LOGGER.info(
        "{}-{} [Compaction] InsertionCrossSpaceCompaction task starts with unseq file {}, "
            + "nearest seq files are {}, "
            + "target file name timestamp is {}, "
            + "file size is {} MB.",
        storageGroupName,
        dataRegionId,
        unseqFileToInsert,
        selectedSeqFiles,
        timestamp,
        unseqFileToInsert.getTsFileSize() / 1024 / 1024);
    boolean isSuccess = true;
    if (!tsFileManager.isAllowCompaction()
        || !IoTDBDescriptor.getInstance().getConfig().isEnableCrossSpaceCompaction()) {
      return true;
    }
    try {
      targetFile = new TsFileResource(generateTargetFile(), TsFileResourceStatus.COMPACTING);
    } catch (IOException e) {
      LOGGER.error(
          "{}-{} [InsertionCrossSpaceCompactionTask] failed to generate target file name, source unseq file is {}",
          storageGroupName,
          dataRegionId,
          unseqFileToInsert);
      return false;
    }
    logFile =
        new File(
            targetFile.getTsFilePath() + CompactionLogger.INSERTION_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger logger = new SimpleCompactionLogger(logFile)) {

      logger.logSourceFile(unseqFileToInsert);
      logger.logTargetFile(targetFile);
      logger.force();

      prepareTargetFiles();

      validateCompactionResult(
          Collections.emptyList(),
          Collections.singletonList(unseqFileToInsert),
          Collections.singletonList(targetFile));

      replaceTsFileInMemory(
          Collections.singletonList(unseqFileToInsert), Collections.singletonList(targetFile));

      lockWrite(Collections.singletonList(unseqFileToInsert));
      CompactionUtils.deleteTsFileResourceWithoutLock(unseqFileToInsert);

      double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
      LOGGER.info(
          "{}-{} [Compaction] InsertionCrossSpaceCompaction task finishes successfully, "
              + "target file is {},"
              + "time cost is {} s.",
          storageGroupName,
          dataRegionId,
          targetFile,
          String.format("%.2f", costTime));
    } catch (Exception e) {
      if (e instanceof CompactionValidationFailedException) {
        failToPassValidation = true;
      }
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
      if (targetFile != null && targetFile.tsFileExists()) {
        updateFileMetrics();
      }
      targetFile.setStatus(TsFileResourceStatus.NORMAL);
    }
    return isSuccess;
  }

  public File generateTargetFile() throws IOException {
    String path = unseqFileToInsert.getTsFile().getParentFile().getPath();
    int pos = path.lastIndexOf("unsequence");
    path = path.substring(0, pos) + "sequence" + path.substring(pos + "unsequence".length());

    TsFileNameGenerator.TsFileName tsFileName =
        TsFileNameGenerator.getTsFileName(unseqFileToInsert.getTsFile().getName());
    tsFileName.setTime(timestamp);
    String fileNameStr =
        String.format(
            "%d-%d-%d-%d.tsfile",
            tsFileName.getTime(), tsFileName.getVersion(), tsFileName.getInnerCompactionCnt(), 0);
    File targetTsFile = new File(path + File.separator + fileNameStr);
    if (!targetTsFile.getParentFile().exists()) {
      targetTsFile.getParentFile().mkdirs();
    }
    return targetTsFile;
  }

  private void prepareTargetFiles() throws IOException {
    File sourceTsFile = unseqFileToInsert.getTsFile();
    File targetTsFile = targetFile.getTsFile();
    Files.createLink(targetTsFile.toPath(), sourceTsFile.toPath());
    Files.createLink(
        new File(targetTsFile.getPath() + TsFileResource.RESOURCE_SUFFIX).toPath(),
        new File(sourceTsFile.getPath() + TsFileResource.RESOURCE_SUFFIX).toPath());

    unseqFileToInsert.linkModFile(targetFile);

    targetFile.setProgressIndex(unseqFileToInsert.getMaxProgressIndexAfterClose());
    targetFile.deserialize();
    targetFile.setProgressIndex(unseqFileToInsert.getMaxProgressIndexAfterClose());
  }

  private boolean recoverTaskInfoFromLogFile() throws IOException {
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    if (sourceFileIdentifiers.isEmpty() || targetFileIdentifiers.isEmpty()) {
      return false;
    }
    File sourceTsFile = sourceFileIdentifiers.get(0).getFileFromDataDirsIfAnyAdjuvantFileExists();
    if (sourceTsFile != null) {
      unseqFileToInsert = new TsFileResource(sourceTsFile);
      selectedUnseqFiles.add(unseqFileToInsert);
    }
    File targetTsFile = targetFileIdentifiers.get(0).getFileFromDataDirsIfAnyAdjuvantFileExists();
    if (targetTsFile != null) {
      targetFile = new TsFileResource(targetTsFile);
    }
    return true;
  }

  @Override
  public void recover() {
    try {
      if (needRecoverTaskInfoFromLogFile) {
        boolean isValidLog = recoverTaskInfoFromLogFile();
        if (!isValidLog) {
          return;
        }
      }
      if (!canRecover()) {
        throw new CompactionRecoverException("Can not recover InsertionCrossSpaceCompactionTask");
      }
      if (shouldRollback()) {
        rollback();
      } else {
        // That finishTask() is revoked means
        finishTask();
      }
    } catch (Exception e) {
      handleRecoverException(e);
    } finally {
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        handleException(LOGGER, e);
      }
    }
  }

  private boolean canRecover() {
    return unseqFileToInsert != null || targetFile != null;
  }

  private boolean shouldRollback() {
    // if target file or its responding file does not exist, then return true
    return targetFile == null
        || !targetFile.tsFileExists()
        || !targetFile.resourceFileExists()
        || (unseqFileToInsert != null
            && unseqFileToInsert.anyModFileExists()
            && !targetFile.anyModFileExists())
        || failToPassValidation;
  }

  private void rollback() throws IOException {
    // if the task has started,
    if (recoverMemoryStatus) {
      replaceTsFileInMemory(
          Collections.singletonList(targetFile), Collections.singletonList(unseqFileToInsert));
    }
    deleteCompactionModsFile(Collections.singletonList(unseqFileToInsert));
    if (targetFile == null) {
      return;
    }
    // delete target file
    if (!deleteTsFileOnDisk(targetFile)) {
      throw new CompactionRecoverException(
          String.format("failed to delete target file %s", targetFile));
    }
  }

  private void finishTask() throws IOException {
    if (unseqFileToInsert == null) {
      return;
    }
    if (!deleteTsFileOnDisk(unseqFileToInsert)) {
      throw new CompactionRecoverException("source files cannot be deleted successfully");
    }

    deleteCompactionModsFile(Collections.singletonList(unseqFileToInsert));
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    return false;
  }

  @Override
  public boolean isDiskSpaceCheckPassed() {
    return true;
  }

  @Override
  public long getEstimatedMemoryCost() {
    return 0;
  }

  @Override
  public int getProcessedFileNum() {
    return 0;
  }

  @Override
  protected void createSummary() {
    this.summary = new CompactionTaskSummary();
  }

  @Override
  public CompactionTaskType getCompactionTaskType() {
    return CompactionTaskType.INSERTION;
  }

  private void releaseAllLocks() {
    for (TsFileResource tsFileResource : holdWriteLockList) {
      tsFileResource.writeUnlock();
    }
    holdWriteLockList.clear();
  }

  private void lockWrite(List<TsFileResource> tsFileResourceList) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      tsFileResource.writeLock();
      holdWriteLockList.add(tsFileResource);
    }
  }

  private void updateFileMetrics() {
    // Here the target file is used for updating metrics because the source file
    // has been deleted here.
    // The statistics of the mods file can be left unchanged, as it does not
    // differentiate between sequence or unsequence.
    FileMetrics.getInstance().deleteTsFile(false, Collections.singletonList(targetFile));
    FileMetrics.getInstance()
        .addTsFile(
            targetFile.getDatabaseName(),
            targetFile.getDataRegionId(),
            targetFile.getTsFileSize(),
            true,
            targetFile.getTsFile().getName());
  }

  @Override
  public long getSelectedFileSize() {
    return unseqFileToInsert.getTsFileSize();
  }
}
