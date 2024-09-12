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
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.RepairUnsortedFileCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.RepairUnsortedFileCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Repair the internal unsorted file by compaction and move it to unSequence space after compaction
 * whether the source TsFileResource is sequence or not.
 */
public class RepairUnsortedFileCompactionTask extends InnerSpaceCompactionTask {

  private static Lock generateTargetFileLock = new ReentrantLock();

  private final TsFileResource sourceFile;
  private final boolean rewriteFile;
  private CountDownLatch latch;

  public RepairUnsortedFileCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResource sourceFile,
      boolean sequence,
      long serialId) {
    super(
        timePartition,
        tsFileManager,
        Collections.singletonList(sourceFile),
        sequence,
        new RepairUnsortedFileCompactionPerformer(true),
        serialId);
    this.sourceFile = sourceFile;
    this.innerSpaceEstimator = new RepairUnsortedFileCompactionEstimator();
    this.rewriteFile = false;
  }

  public RepairUnsortedFileCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResource sourceFile,
      boolean sequence,
      boolean rewriteFile,
      long serialId) {
    super(
        timePartition,
        tsFileManager,
        Collections.singletonList(sourceFile),
        sequence,
        new RepairUnsortedFileCompactionPerformer(rewriteFile),
        serialId);
    this.sourceFile = sourceFile;
    if (rewriteFile) {
      this.innerSpaceEstimator = new RepairUnsortedFileCompactionEstimator();
    }
    this.rewriteFile = rewriteFile;
  }

  public RepairUnsortedFileCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResource sourceFile,
      CountDownLatch latch,
      boolean sequence,
      long serialId) {
    super(
        timePartition,
        tsFileManager,
        Collections.singletonList(sourceFile),
        sequence,
        new RepairUnsortedFileCompactionPerformer(true),
        serialId);
    this.sourceFile = sourceFile;
    this.innerSpaceEstimator = new RepairUnsortedFileCompactionEstimator();
    this.latch = latch;
    this.rewriteFile = false;
  }

  public RepairUnsortedFileCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      TsFileResource sourceFile,
      CountDownLatch latch,
      boolean sequence,
      boolean rewriteFile,
      long serialId) {
    super(
        timePartition,
        tsFileManager,
        Collections.singletonList(sourceFile),
        sequence,
        new RepairUnsortedFileCompactionPerformer(rewriteFile),
        serialId);
    this.sourceFile = sourceFile;
    if (rewriteFile) {
      this.innerSpaceEstimator = new RepairUnsortedFileCompactionEstimator();
    }
    this.rewriteFile = rewriteFile;
    this.latch = latch;
  }

  @Override
  protected void prepare() throws IOException {
    calculateSourceFilesAndTargetFiles();
    isHoldingWriteLock = new boolean[this.filesView.sourceFilesInLog.size()];
    Arrays.fill(isHoldingWriteLock, false);
  }

  @Override
  protected void calculateSourceFilesAndTargetFiles() throws IOException {
    filesView.sourceFilesInLog = filesView.sourceFilesInCompactionPerformer;
    filesView.targetFilesInLog =
        Collections.singletonList(
            new TsFileResource(generateTargetFile(), TsFileResourceStatus.COMPACTING));
    filesView.targetFilesInPerformer = filesView.targetFilesInLog;
  }

  private File generateTargetFile() throws IOException {
    String targetFileParentPath = sourceFile.getTsFile().getParentFile().getPath();
    // if source file is sequence, the sequence data targetFileParentPath should be replaced to
    // unsequence
    if (sourceFile.isSeq()) {
      int pos = targetFileParentPath.lastIndexOf("sequence");
      targetFileParentPath =
          targetFileParentPath.substring(0, pos)
              + "unsequence"
              + targetFileParentPath.substring(pos + "sequence".length());
    }

    TsFileNameGenerator.TsFileName tsFileName =
        TsFileNameGenerator.getTsFileName(sourceFile.getTsFile().getName());

    // Use the log file to determine whether there are other concurrent
    // repair tasks using the file with the same name.
    String dataDirForLogFile = TierManager.getInstance().getAllFoldersOfTier(0, false).get(0);
    String pathToTimePartition =
        sourceFile.getDatabaseName()
            + File.separator
            + sourceFile.getDataRegionId()
            + File.separator
            + sourceFile.getTimePartition();

    File targetTsFile = null;
    generateTargetFileLock.lock();
    List<String> allUnSequenceFolders = TierManager.getInstance().getAllUnSequenceFileFolders();
    try {
      while (logFile == null) {
        boolean canAllocateCurrentFileName = true;
        // set version = 0 to keep unsequence data cover sequence data
        String fileNamePrefixStr =
            String.format(
                "%d-%d-%d-%d", tsFileName.getTime(), 0, tsFileName.getInnerCompactionCnt() + 1, 0);
        // timestamp +1 for next times of the loop
        tsFileName.setTime(tsFileName.getTime() + 1);
        // 1. check files with same filename in all unsequence data dirs
        for (String folder : allUnSequenceFolders) {
          File file =
              new File(
                  folder
                      + File.separator
                      + pathToTimePartition
                      + File.separator
                      + fileNamePrefixStr
                      + ".tsfile");
          if (file.exists()) {
            canAllocateCurrentFileName = false;
            break;
          }
        }
        if (!canAllocateCurrentFileName) {
          continue;
        }
        // 2. check log file with same filename in first unsequence data dir
        File logFileForCurrentFileName =
            new File(
                dataDirForLogFile
                    + File.separator
                    + pathToTimePartition
                    + File.separator
                    + fileNamePrefixStr
                    + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX
                    + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
        logFileForCurrentFileName.getParentFile().mkdirs();
        if (!logFileForCurrentFileName.createNewFile()) {
          // There is a compaction task which will generate a tsfile with the same file name.
          continue;
        }
        logFile = logFileForCurrentFileName;
        targetTsFile =
            new File(
                targetFileParentPath
                    + File.separator
                    + fileNamePrefixStr
                    + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX);
        targetTsFile.getParentFile().mkdirs();
        break;
      }
    } finally {
      generateTargetFileLock.unlock();
    }
    return targetTsFile;
  }

  @Override
  protected void prepareTargetFiles() throws IOException {
    CompactionUtils.updateProgressIndex(
        filesView.targetFilesInPerformer,
        filesView.sourceFilesInCompactionPerformer,
        Collections.emptyList());
    CompactionUtils.moveTargetFile(
        filesView.targetFilesInPerformer,
        CompactionTaskType.REPAIR,
        storageGroupName + "-" + dataRegionId);

    LOGGER.info(
        "{}-{} [InnerSpaceCompactionTask] start to rename mods file",
        storageGroupName,
        dataRegionId);

    if (rewriteFile) {
      CompactionUtils.combineModsInInnerCompaction(
          filesView.sourceFilesInCompactionPerformer, filesView.targetFilesInPerformer);
    } else {
      if (sourceFile.modFileExists()) {
        Files.createLink(
            new File(filesView.targetFilesInPerformer.get(0).getModFile().getFilePath()).toPath(),
            new File(sourceFile.getModFile().getFilePath()).toPath());
      }
    }
  }

  @Override
  protected boolean doCompaction() {
    boolean isSuccess = super.doCompaction();
    if (!isSuccess) {
      LOGGER.info("Failed to repair file {}", sourceFile.getTsFile().getAbsolutePath());
      sourceFile.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
    }
    return isSuccess;
  }

  @Override
  public long getEstimatedMemoryCost() {
    if (innerSpaceEstimator != null && memoryCost == 0L) {
      try {
        memoryCost =
            innerSpaceEstimator.estimateInnerCompactionMemory(
                filesView.sourceFilesInCompactionPerformer);
      } catch (IOException e) {
        innerSpaceEstimator.cleanup();
      }
    }
    if (memoryCost > SystemInfo.getInstance().getMemorySizeForCompaction()) {
      sourceFile.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
      LOGGER.warn(
          "[RepairUnsortedFileCompactionTask]"
              + " Can not repair unsorted file {} "
              + "because the required memory to repair"
              + " is greater than the total compaction memory budget",
          sourceFile.getTsFile().getAbsolutePath());
    }
    return memoryCost;
  }

  @Override
  public boolean isDiskSpaceCheckPassed() {
    if (!rewriteFile) {
      return true;
    }
    return super.isDiskSpaceCheckPassed();
  }

  @Override
  public void handleTaskCleanup() {
    super.handleTaskCleanup();
    if (latch != null) {
      latch.countDown();
    }
  }

  @Override
  public CompactionTaskType getCompactionTaskType() {
    return CompactionTaskType.REPAIR;
  }
}
