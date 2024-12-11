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
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.RepairDataFileScanUtil;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.RepairUnsortedFileCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileRepairStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Repair the internal unsorted file by compaction and move it to unSequence space after compaction
 * whether the source TsFileResource is sequence or not.
 */
public class RepairUnsortedFileCompactionTask extends InnerSpaceCompactionTask {

  private static final AtomicLong lastAllocatedFileTimestamp = new AtomicLong(Long.MAX_VALUE / 2);

  public static void recoverAllocatedFileTimestamp(long timestamp) {
    if (timestamp > lastAllocatedFileTimestamp.get()) {
      lastAllocatedFileTimestamp.set(timestamp);
    }
  }

  public static long getInitialAllocatedFileTimestamp() {
    return Long.MAX_VALUE / 2;
  }

  private final TsFileResource sourceFile;
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
        new RepairUnsortedFileCompactionPerformer(),
        serialId);
    this.sourceFile = sourceFile;
    if (this.sourceFile.getTsFileRepairStatus() != TsFileRepairStatus.NEED_TO_REPAIR_BY_MOVE) {
      this.innerSpaceEstimator = new RepairUnsortedFileCompactionEstimator();
    }
  }

  // used for 'start repair data'
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
        new RepairUnsortedFileCompactionPerformer(),
        serialId);
    this.sourceFile = sourceFile;
    if (this.sourceFile.getTsFileRepairStatus() != TsFileRepairStatus.NEED_TO_REPAIR_BY_MOVE) {
      this.innerSpaceEstimator = new RepairUnsortedFileCompactionEstimator();
    }
    this.latch = latch;
  }

  @Override
  protected void prepare() throws IOException {
    calculateSourceFilesAndTargetFiles();
    isHoldingWriteLock = new boolean[this.filesView.sourceFilesInLog.size()];
    Arrays.fill(isHoldingWriteLock, false);
    logFile =
        new File(
            filesView.targetFilesInLog.get(0).getTsFilePath()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
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
    String targetFileDir = sourceFile.getTsFile().getParentFile().getPath();
    TsFileNameGenerator.TsFileName sourceFileName =
        TsFileNameGenerator.getTsFileName(sourceFile.getTsFile().getName());
    String fileNameStr =
        String.format(
            "%d-%d-%d-%d" + IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX,
            sourceFile.isSeq()
                ? lastAllocatedFileTimestamp.incrementAndGet()
                : sourceFileName.getTime(),
            sourceFile.isSeq() ? 0 : sourceFileName.getVersion(),
            sourceFileName.getInnerCompactionCnt() + 1,
            sourceFileName.getCrossCompactionCnt());
    // if source file is sequence, the sequence data targetFileDir should be replaced to unsequence
    if (sourceFile.isSeq()) {
      int pos = targetFileDir.lastIndexOf("sequence");
      targetFileDir =
          targetFileDir.substring(0, pos)
              + "unsequence"
              + targetFileDir.substring(pos + "sequence".length());
    }
    File targetFile = new File(targetFileDir + File.separator + fileNameStr);
    return targetFile;
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

    if (sourceFile.getTsFileRepairStatus() == TsFileRepairStatus.NEED_TO_REPAIR_BY_REWRITE) {
      CompactionUtils.combineModsInInnerCompaction(
          filesView.sourceFilesInCompactionPerformer, filesView.targetFilesInPerformer);
    } else {
      if (sourceFile.anyModFileExists()) {
        sourceFile.linkModFile(filesView.targetFilesInPerformer.get(0));
      }
      if (TsFileResource.useSharedModFile) {
        filesView
            .targetFilesInPerformer
            .get(0)
            .setSharedModFile(sourceFile.getSharedModFile(), false);
      }
    }
  }

  @Override
  protected boolean doCompaction() {
    calculateRepairMethod();
    if (!sourceFile.getTsFileRepairStatus().isRepairCompactionCandidate()) {
      return true;
    }
    boolean isSuccess = super.doCompaction();
    if (!isSuccess) {
      LOGGER.info("Failed to repair file {}", sourceFile.getTsFile().getAbsolutePath());
      sourceFile.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
    }
    return isSuccess;
  }

  private void calculateRepairMethod() {
    if (this.sourceFile.getTsFileRepairStatus() != TsFileRepairStatus.NEED_TO_CHECK) {
      return;
    }
    RepairDataFileScanUtil repairDataFileScanUtil = new RepairDataFileScanUtil(sourceFile, true);
    repairDataFileScanUtil.scanTsFile(true);
    if (repairDataFileScanUtil.isBrokenFile()) {
      sourceFile.setTsFileRepairStatus(TsFileRepairStatus.CAN_NOT_REPAIR);
      return;
    }
    if (repairDataFileScanUtil.hasUnsortedDataOrWrongStatistics()) {
      sourceFile.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_REPAIR_BY_REWRITE);
      return;
    }
    if (sourceFile.isSeq()) {
      sourceFile.setTsFileRepairStatus(TsFileRepairStatus.NEED_TO_REPAIR_BY_MOVE);
      return;
    }
    sourceFile.setTsFileRepairStatus(TsFileRepairStatus.NORMAL);
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
    if (sourceFile.getTsFileRepairStatus() == TsFileRepairStatus.NEED_TO_REPAIR_BY_MOVE) {
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
