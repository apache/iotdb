package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractInnerSpaceEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCompactionInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This settle task contains all_deleted files and partial_deleted files. The partial_deleted files
 * are divided into several groups, each group may contain one or several files. This task will do
 * the following two things respectively: 1. Settle all all_deleted files by deleting them directly.
 * 2. Settle partial_deleted files: put the files of each partial_deleted group into an invisible
 * innerCompactionTask, and then perform the cleanup work. The source files in a file group will be
 * compacted into a target file.
 */
public class SettleCompactionTask extends InnerSpaceCompactionTask {
  private List<TsFileResource> allDeletedFiles;
  private List<List<TsFileResource>> partialDeletedFileGroups;
  private List<Long> memoryCostList;

  private double allDeletedFileSize = 0;
  private double partialDeletedFileSize = 0;

  private int totalPartialDeletedFilesNum = 0;

  private AbstractInnerSpaceEstimator spaceEstimator;

  private boolean hasFinishedSettledAllDeletedFile = false;

  private int allDeletedSuccessNum = 0;
  private int partialGroupSuccessNum = 0;

  public SettleCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> allDeletedFiles,
      List<List<TsFileResource>> partialDeletedFileGroups,
      ICompactionPerformer performer,
      long serialId) {
    super(timePartition, tsFileManager, performer, serialId);
    this.allDeletedFiles = allDeletedFiles;
    this.partialDeletedFileGroups = partialDeletedFileGroups;
    allDeletedFiles.forEach(x -> allDeletedFileSize += x.getTsFileSize());
    partialDeletedFileGroups.forEach(
        x -> {
          totalPartialDeletedFilesNum += x.size();
          x.forEach(y -> partialDeletedFileSize += y.getTsFileSize());
        });
    if (IoTDBDescriptor.getInstance().getConfig().isEnableCompactionMemControl()) {
      spaceEstimator = new FastCompactionInnerCompactionEstimator();
    }
  }

  public SettleCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, tsFileManager, logFile);
    setSettleFlag(true);
  }

  @Override
  public List<TsFileResource> getAllSourceTsFiles() {
    List<TsFileResource> allSourceFiles = new ArrayList<>(allDeletedFiles);
    partialDeletedFileGroups.forEach(allSourceFiles::addAll);
    return allSourceFiles;
  }

  @Override
  protected boolean doCompaction() {
    recoverMemoryStatus = true;
    boolean isSuccess;

    if (!tsFileManager.isAllowCompaction()) {
      return true;
    }
    if (allDeletedFiles.isEmpty() && partialDeletedFileGroups.isEmpty()) {
      LOGGER.info(
          "{}-{} [Compaction] Settle compaction file list is empty, end it",
          storageGroupName,
          dataRegionId);
    }
    long startTime = System.currentTimeMillis();

    LOGGER.info(
        "{}-{} [Compaction] SettleCompaction task starts with {} all_deleted files "
            + "and {} partial_deleted groups with {} files. "
            + "All_deleted files : {}, partial_deleted files : {} . "
            + "All_deleted files size is {} MB, "
            + "partial_deleted file size is {} MB. ",
        storageGroupName,
        dataRegionId,
        allDeletedFiles.size(),
        partialDeletedFileGroups.size(),
        totalPartialDeletedFilesNum,
        allDeletedFiles,
        partialDeletedFileGroups,
        allDeletedFileSize / 1024 / 1024,
        partialDeletedFileSize / 1024 / 1024);

    isSuccess = settleWithAllDeletedFiles();
    // In order to prevent overlap of sequential files after settle task, partial_deleted files can
    // only be settled after all_deleted files are settled successfully, because multiple
    // partial_deleted files may be settled into one file.
    if (isSuccess) {
      hasFinishedSettledAllDeletedFile = true;
      isSuccess = settleWithPartialDeletedFiles();
    }

    double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
    LOGGER.info(
        "{}-{} [Compaction] SettleCompaction task finishes, time cost is {} s, compaction speed is {} MB/s."
            + "All_Deleted files num is {} and success num is {}, Partial_Deleted group num is {} and success num is {}.",
        storageGroupName,
        dataRegionId,
        String.format("%.2f", costTime),
        String.format(
            "%.2f", (allDeletedFileSize + partialDeletedFileSize) / 1024.0d / 1024.0d / costTime),
        allDeletedFiles.size(),
        allDeletedSuccessNum,
        partialDeletedFileGroups.size(),
        partialGroupSuccessNum);

    return isSuccess;
  }

  private boolean settleWithAllDeletedFiles() {
    if (allDeletedFiles.isEmpty()) {
      return true;
    }
    boolean isSuccess;
    logFile =
        new File(
            allDeletedFiles.get(0).getTsFile().getAbsolutePath()
                + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      compactionLogger.logSourceFiles(allDeletedFiles);
      compactionLogger.force();

      isSuccess = settleDeletedFiles();
    } catch (IOException e) {
      isSuccess = false;
      printLogWhenException(LOGGER, e);
      recover();
    } finally {
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        printLogWhenException(LOGGER, e);
      }
    }
    return isSuccess;
  }

  private boolean settleDeletedFiles() {
    boolean isSuccess = true;
    for (TsFileResource resource : allDeletedFiles) {
      if (recoverMemoryStatus) {
        tsFileManager.remove(resource, resource.isSeq());
        if (resource.getModFile().exists()) {
          FileMetrics.getInstance().decreaseModFileNum(1);
          FileMetrics.getInstance().decreaseModFileSize(resource.getModFile().getSize());
        }
      }
      boolean res = deleteTsFileOnDisk(resource);
      if (res) {
        allDeletedSuccessNum++;
        LOGGER.debug(
            "Settle task deletes all dirty tsfile {} successfully.",
            resource.getTsFile().getAbsolutePath());
      } else {
        LOGGER.error(
            "Settle task fail to delete all dirty tsfile {}.",
            resource.getTsFile().getAbsolutePath());
      }
      isSuccess = isSuccess && res;
      if (recoverMemoryStatus) {
        FileMetrics.getInstance()
            .deleteTsFile(resource.isSeq(), Collections.singletonList(resource));
      }
    }
    return isSuccess;
  }

  /** Use inner compaction task to compact the partial_deleted files. */
  private boolean settleWithPartialDeletedFiles() {
    boolean isSuccess = true;
    for (int i = 0; i < partialDeletedFileGroups.size(); i++) {
      List<TsFileResource> sourceFiles = partialDeletedFileGroups.get(i);
      if (sourceFiles.isEmpty()) {
        continue;
      }
      setSourceInfo(sourceFiles.get(0).isSeq(), sourceFiles, memoryCostList.get(i));
      LOGGER.info(
          "{}-{} [Compaction] Start to settle {} {} files, "
              + "total file size is {} MB, memory cost is {} MB",
          storageGroupName,
          dataRegionId,
          selectedTsFileResourceList.size(),
          sequence ? "Sequence" : "Unsequence",
          selectedFileSize / 1024 / 1024,
          memoryCost == 0 ? 0 : (double) memoryCost / 1024 / 1024);
      try {
        long startTime = System.currentTimeMillis();
        compact();
        double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
        LOGGER.info(
            "{}-{} [Compaction] Finish to settle {} {} file successfully , "
                + "target file is {},"
                + "time cost is {} s, "
                + "compaction speed is {} MB/s, {}",
            storageGroupName,
            dataRegionId,
            selectedTsFileResourceList.size(),
            sequence ? "Sequence" : "Unsequence",
            targetTsFileResource.getTsFile().getName(),
            String.format("%.2f", costTime),
            String.format("%.2f", selectedFileSize / 1024.0d / 1024.0d / costTime),
            summary);
        partialGroupSuccessNum++;
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
        // may fail to set status if the status of target resource is DELETED
        targetTsFileResource.setStatus(TsFileResourceStatus.NORMAL);
      }
    }
    return isSuccess;
  }

  @Override
  public void recover() {
    LOGGER.info(
        "{}-{} [Compaction][Recover] Start to recover settle compaction.",
        storageGroupName,
        dataRegionId);
    try {
      if (needRecoverTaskInfoFromLogFile) {
        recoverTaskInfoFromLogFile();
      }
      if (!hasFinishedSettledAllDeletedFile) {
        recoverAllDeletedFiles();
      } else {
        recoverPartialDeletedFiles();
      }
      LOGGER.info(
          "{}-{} [Compaction][Recover] Finish to recover settle compaction successfully.",
          storageGroupName,
          dataRegionId);
    } catch (Exception e) {
      handleRecoverException(e);
    }
  }

  private void recoverAllDeletedFiles() {
    if (!settleDeletedFiles()) {
      throw new CompactionRecoverException("Failed to delete all_deleted source file.");
    }
  }

  private void recoverPartialDeletedFiles() throws IOException {
    if (shouldRollback()) {
      rollback();
    } else {
      finishTask();
    }
  }

  private void recoverTaskInfoFromLogFile() throws IOException {
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

    if (targetFileIdentifiers.isEmpty()) {
      // in the process of settling all_deleted files
      sourceFileIdentifiers.forEach(
          x -> allDeletedFiles.add(new TsFileResource(new File(x.getFilePath()))));
      hasFinishedSettledAllDeletedFile = false;
    } else {
      // in the process of settling partial_deleted files
      recoverTaskInfo(sourceFileIdentifiers, targetFileIdentifiers, deletedTargetFileIdentifiers);
      hasFinishedSettledAllDeletedFile = true;
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof SettleCompactionTask)) {
      return false;
    }
    SettleCompactionTask otherSettleCompactionTask = (SettleCompactionTask) otherTask;
    return this.allDeletedFiles.equals(otherSettleCompactionTask.allDeletedFiles)
        && this.partialDeletedFileGroups.equals(otherSettleCompactionTask.partialDeletedFileGroups)
        && this.performer.getClass().isInstance(otherSettleCompactionTask.performer);
  }

  @Override
  public long getEstimatedMemoryCost() {
    if (partialDeletedFileGroups.isEmpty()) {
      return 0;
    } else {
      memoryCostList = new ArrayList<>();
      partialDeletedFileGroups.forEach(
          x -> {
            try {
              long cost = spaceEstimator.estimateInnerCompactionMemory(x);
              memoryCostList.add(cost);
              memoryCost = Math.max(memoryCost, cost);
            } catch (IOException e) {
              spaceEstimator.cleanup();
              LOGGER.error("Meet error when estimate settle compaction memory", e);
              memoryCost = -1;
            }
          });
      return memoryCost;
    }
  }

  @Override
  public int getProcessedFileNum() {
    int num = 0;
    for (List<TsFileResource> partialDeletedFileList : partialDeletedFileGroups) {
      num = Math.max(num, partialDeletedFileList.size());
    }
    return num;
  }

  @Override
  public CompactionTaskType getCompactionTaskType() {
    return CompactionTaskType.SETTLE;
  }

  public List<TsFileResource> getAllDeletedFiles() {
    return allDeletedFiles;
  }

  public List<List<TsFileResource>> getPartialDeletedFileGroups() {
    return partialDeletedFileGroups;
  }

  public double getAllDeletedFileSize() {
    return allDeletedFileSize;
  }

  public double getPartialDeletedFileSize() {
    return partialDeletedFileSize;
  }

  public int getTotalPartialDeletedFilesNum() {
    return totalPartialDeletedFilesNum;
  }
}
