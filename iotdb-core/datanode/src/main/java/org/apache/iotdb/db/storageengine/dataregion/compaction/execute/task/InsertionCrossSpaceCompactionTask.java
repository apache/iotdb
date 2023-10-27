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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionValidationFailedException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.validator.CompactionValidator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.InsertionCrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
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
    super(databaseName, dataRegionId, 0L, tsFileManager, 0L, CompactionTaskType.NORMAL);
    this.logFile = logFile;
    this.needRecoverTaskInfoFromLogFile = true;
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
  protected List<TsFileResource> getAllSourceTsFiles() {
    return Stream.concat(selectedSeqFiles.stream(), selectedUnseqFiles.stream())
        .collect(Collectors.toList());
  }

  @Override
  public void handleTaskCleanup() {
    phaser.arrive();
  }

  @Override
  protected boolean doCompaction() {
    long startTime = System.currentTimeMillis();
    recoverMemoryStatus = true;
    LOGGER.info(
        "{}-{} [Compaction] InsertionCrossSpaceCompaction task starts with unseq file {}, "
            + "target file name timestamp is {}, "
            + "file size is {} MB.",
        storageGroupName,
        dataRegionId,
        unseqFileToInsert,
        timestamp,
        unseqFileToInsert.getTsFileSize() / 1024 / 1024);
    boolean isSuccess = true;
    if (!tsFileManager.isAllowCompaction()
        || !IoTDBDescriptor.getInstance().getConfig().isEnableInsertionCrossSpaceCompaction()) {
      return true;
    }
    try {
      targetFile = new TsFileResource(generateTargetFile());
      targetFile.setStatus(TsFileResourceStatus.NORMAL);
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

      // todo: overlap validation

      replaceTsFileInMemory(
          Collections.singletonList(unseqFileToInsert), Collections.singletonList(targetFile));

      CompactionValidator validator = CompactionValidator.getInstance();
      if (!validator.validateCompaction(
          tsFileManager,
          Collections.singletonList(targetFile),
          storageGroupName,
          timePartition,
          false)) {
        LOGGER.error(
            "Failed to pass compaction validation, source un seq files is: {}, target files is {}",
            unseqFileToInsert,
            targetFile);
        IoTDBDescriptor.getInstance().getConfig().setEnableInsertionCrossSpaceCompaction(false);
        throw new CompactionValidationFailedException("Failed to pass compaction validation");
      }

      lockWrite(Collections.singletonList(unseqFileToInsert));
      CompactionUtils.deleteCompactionModsFile(selectedSeqFiles, selectedUnseqFiles);
      CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(
          Collections.singletonList(unseqFileToInsert), false);

      FileMetrics.getInstance()
          .addTsFile(
              targetFile.getDatabaseName(),
              targetFile.getDataRegionId(),
              targetFile.getTsFileSize(),
              true,
              targetFile.getTsFile().getName());

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
      isSuccess = false;
      printLogWhenException(LOGGER, e);
      recover();
    } finally {
      releaseAllLocks();
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        printLogWhenException(LOGGER, e);
      }
    }
    return isSuccess;
  }

  public File generateTargetFile() throws IOException {
    String path = unseqFileToInsert.getTsFile().getParentFile().getPath();
    path = path.replace("unsequence", "sequence");
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
    if (unseqFileToInsert.getModFile().exists()) {
      Files.createLink(
          new File(targetTsFile.getPath() + ModificationFile.FILE_SUFFIX).toPath(),
          new File(sourceTsFile.getPath() + ModificationFile.FILE_SUFFIX).toPath());
    }
    targetFile.deserialize();
  }

  private void recoverTaskInfoFromLogFile() throws IOException {
    CompactionLogAnalyzer logAnalyzer = new CompactionLogAnalyzer(this.logFile);
    logAnalyzer.analyze();
    List<TsFileIdentifier> sourceFileIdentifiers = logAnalyzer.getSourceFileInfos();
    File sourceTsFile = sourceFileIdentifiers.get(0).getFileFromDataDirsIfAnyAdjuvantFileExists();
    if (sourceTsFile != null) {
      unseqFileToInsert =
          new TsFileResource(
              sourceFileIdentifiers.get(0).getFileFromDataDirsIfAnyAdjuvantFileExists());
    }
    List<TsFileIdentifier> targetFileIdentifiers = logAnalyzer.getTargetFileInfos();
    File targetTsFile = targetFileIdentifiers.get(0).getFileFromDataDirsIfAnyAdjuvantFileExists();
    if (targetTsFile != null) {
      targetFile =
          new TsFileResource(
              targetFileIdentifiers.get(0).getFileFromDataDirsIfAnyAdjuvantFileExists());
    }
  }

  @Override
  public void recover() {
    try {
      if (needRecoverTaskInfoFromLogFile) {
        recoverTaskInfoFromLogFile();
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
        printLogWhenException(LOGGER, e);
      }
    }
  }

  private boolean canRecover() {
    return unseqFileToInsert != null || targetFile != null;
  }

  private boolean shouldRollback() {
    // if target file or its responding file does not exist, then return true
    if (targetFile == null
        || !targetFile.tsFileExists()
        || !targetFile.resourceFileExists()
        || (unseqFileToInsert.modFileExists() && !targetFile.modFileExists())) {
      return true;
    }
    return false;
  }

  private void rollback() throws IOException {
    // if the task has started,
    if (recoverMemoryStatus) {
      replaceTsFileInMemory(
          Collections.singletonList(targetFile), Collections.singletonList(unseqFileToInsert));
    }
    deleteCompactionModsFile(Collections.singletonList(unseqFileToInsert));
    if (targetFile.tsFileExists()) {
      FileMetrics.getInstance().deleteTsFile(true, Collections.singletonList(targetFile));
    }
    // delete target file
    if (targetFile != null && !deleteTsFileOnDisk(targetFile)) {
      throw new CompactionRecoverException(
          String.format("failed to delete target file %s", targetFile));
    }
  }

  private void finishTask() throws IOException {
    // 检查目标文件是否存在

    if (recoverMemoryStatus && unseqFileToInsert.tsFileExists()) {
      FileMetrics.getInstance().deleteTsFile(false, Collections.singletonList(unseqFileToInsert));
    }
    if (!deleteTsFileOnDisk(unseqFileToInsert)) {
      throw new CompactionRecoverException("source files cannot be deleted successfully");
    }

    deleteCompactionModsFile(Collections.singletonList(unseqFileToInsert));
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof InsertionCrossSpaceCompactionTask)) {
      return false;
    }
    return false;
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
}
