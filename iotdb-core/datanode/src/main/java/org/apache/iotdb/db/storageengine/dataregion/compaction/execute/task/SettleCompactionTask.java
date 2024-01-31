package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.metrics.CompactionMetrics;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskPriorityType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception.CompactionRecoverException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.ICompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogAnalyzer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.TsFileIdentifier;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.AbstractInnerSpaceEstimator;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator.FastCompactionInnerCompactionEstimator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.utils.TsFileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SettleCompactionTask extends AbstractCompactionTask {
  private List<TsFileResource> allDeletedFiles;
  private List<List<TsFileResource>> partialDeletedFiles;

  /** Only partial deleted files have corresponding target file. */
  private TsFileResource targetFile;

  private int partialDeletedFilesIndex = 0;

  private File logFile;

  private double allDeletedFileSize = 0;
  private double partialDeletedFileSize = 0;

  private AbstractInnerSpaceEstimator spaceEstimator;

  public SettleCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> allDeletedFiles,
      List<List<TsFileResource>> partialDeletedFiles,
      ICompactionPerformer performer,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId,
        CompactionTaskPriorityType.SETTLE);
    this.allDeletedFiles = allDeletedFiles;
    this.partialDeletedFiles = partialDeletedFiles;
    allDeletedFiles.forEach(x -> allDeletedFileSize += x.getTsFileSize());
    partialDeletedFiles.forEach(x -> x.forEach(y -> partialDeletedFileSize += y.getTsFileSize()));
    this.performer = performer;
    if (IoTDBDescriptor.getInstance().getConfig().isEnableCompactionMemControl()) {
      spaceEstimator = new FastCompactionInnerCompactionEstimator();
    }
    this.hashCode = this.toString().hashCode();
    createSummary();
  }

  public SettleCompactionTask(
      String databaseName, String dataRegionId, TsFileManager tsFileManager, File logFile) {
    super(databaseName, dataRegionId, 0L, tsFileManager, 0L, CompactionTaskPriorityType.SETTLE);
    this.logFile = logFile;
    this.needRecoverTaskInfoFromLogFile = true;
  }

  @Override
  public List<TsFileResource> getAllSourceTsFiles() {
    List<TsFileResource> allSourceFiles = new ArrayList<>(allDeletedFiles);
    partialDeletedFiles.forEach(allSourceFiles::addAll);
    return allSourceFiles;
  }

  @Override
  protected boolean doCompaction() {
    recoverMemoryStatus = true;
    boolean isSuccess ;
    try {
      if (!tsFileManager.isAllowCompaction()) {
        return true;
      }
      if (allDeletedFiles.isEmpty() && partialDeletedFiles.isEmpty()) {
        LOGGER.info(
            "{}-{} [Compaction] Settle compaction file list is empty, end it",
            storageGroupName,
            dataRegionId);
      }
      long startTime = System.currentTimeMillis();

      LOGGER.info(
          "{}-{} [Compaction] SettleCompaction task starts with {} all_deleted files "
              + "and {} partial_deleted files. "
              + "All_deleted files : {}, partial_deleted files : {} . "
              + "All_deleted files size is {} MB, "
              + "partial_deleted file size is {} MB. ",
          storageGroupName,
          dataRegionId,
          allDeletedFiles.size(),
          partialDeletedFiles.size(),
          allDeletedFiles,
          partialDeletedFiles,
          allDeletedFileSize / 1024 / 1024,
          partialDeletedFileSize / 1024 / 1024);


      isSuccess = settleWithAllDeletedFile();
      if(isSuccess) {
        settleWithPartialDeletedFile();
      }

      CompactionMetrics.getInstance().recordSummaryInfo(summary);
      double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;

      LOGGER.info(
          "{}-{} [Compaction] SettleCompaction task finishes successfully, "
              + "time cost is {} s, "
              + "compaction speed is {} MB/s, {}",
          storageGroupName,
          dataRegionId,
          String.format("%.2f", costTime),
          String.format(
              "%.2f",
              (allDeletedFileSize + partialDeletedFileSize) / 1024.0d / 1024.0d / costTime),
          summary);

    } catch (Exception e) {
      isSuccess = false;
      printLogWhenException(LOGGER, e);
      recover();
    }
    return isSuccess;
  }

  private boolean settleWithAllDeletedFile() throws IOException {
    if(allDeletedFiles.isEmpty()){
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
    }finally {
      Files.deleteIfExists(logFile.toPath());
    }
    return isSuccess;
  }

  private boolean settleDeletedFiles(){
    boolean isSuccess = true;
    for (TsFileResource resource : allDeletedFiles) {
      if (recoverMemoryStatus) {
        tsFileManager.remove(resource, resource.isSeq());
        if (resource.getModFile().exists()) {
          FileMetrics.getInstance().decreaseModFileNum(1);
          FileMetrics.getInstance().decreaseModFileSize(resource.getModFile().getSize());
        }
      }
      isSuccess = isSuccess && deleteTsFileOnDisk(resource);
      if (recoverMemoryStatus) {
        FileMetrics.getInstance()
                .deleteTsFile(resource.isSeq(), Collections.singletonList(resource));
      }
    }
    return isSuccess;
  }

  private void settleWithPartialDeletedFile()
      throws Exception {
    for(List<TsFileResource> sourceFiles : partialDeletedFiles){
      if(sourceFiles.isEmpty()){
        continue;
      }
      boolean isSequence = sourceFiles.get(0).isSeq();
      boolean[] isHoldingWriteLock = new boolean[sourceFiles.size()];
      targetFile = TsFileNameGenerator.getSettleCompactionTargetFileResources(sourceFiles,isSequence);
      logFile =
              new File(
                      targetFile.getTsFile().getAbsolutePath()
                              + CompactionLogger.SETTLE_COMPACTION_LOG_NAME_SUFFIX);
      try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
        compactionLogger.logSourceFiles(sourceFiles);
        compactionLogger.logTargetFiles(Collections.singletonList(targetFile));
        compactionLogger.force();

        performer.setSourceFiles(sourceFiles);
        performer.setTargetFiles(Collections.singletonList(targetFile));
        performer.setSummary(summary);
        performer.perform();

        CompactionUtils.updateProgressIndex(
                Collections.singletonList(targetFile), sourceFiles, Collections.emptyList());
        CompactionUtils.moveTargetFile(
                Collections.singletonList(targetFile), CompactionTaskType.SETTLE, storageGroupName + "-" + dataRegionId);
        CompactionUtils.combineModsInInnerCompaction(sourceFiles, targetFile);

        validateCompactionResult(
                isSequence ? sourceFiles : Collections.emptyList(),
                isSequence ? Collections.emptyList() : sourceFiles,
                Collections.singletonList(targetFile));

        // replace tsfile resource in memory
        tsFileManager.replace(
                isSequence ? sourceFiles : Collections.emptyList(),
                isSequence ? Collections.emptyList() : sourceFiles,
                Collections.singletonList(targetFile),
                timePartition,
                isSequence);


        // add write lock
        for(int i=0;i<sourceFiles.size();i++){
          sourceFiles.get(i).writeLock();
          isHoldingWriteLock[i] = true;
        }

        CompactionUtils.deleteSourceTsFileAndUpdateFileMetrics(sourceFiles, isSequence);

        if (!targetFile.isDeleted()) {
          FileMetrics.getInstance()
                  .addTsFile(
                          targetFile.getDatabaseName(),
                          targetFile.getDataRegionId(),
                          targetFile.getTsFileSize(),
                          isSequence,
                          targetFile.getTsFile().getName());
        } else {
          // target resource is empty after compaction, then add log and delete it
          compactionLogger.logEmptyTargetFile(targetFile);
          compactionLogger.force();
          targetFile.remove();
        }
        partialDeletedFilesIndex++;
      }finally {
        // release write lock
        for(int i=0;i<sourceFiles.size();i++){
          if(isHoldingWriteLock[i]){
            sourceFiles.get(i).writeUnlock();
          }
        }
        Files.deleteIfExists(logFile.toPath());
        // may failed to set status if the status of target resource is DELETED
        targetFile.setStatus(TsFileResourceStatus.NORMAL);
      }
    }
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
      recoverAllDeletedFiles();
      recoverPartialDeletedFiles();
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
    // get the partial_deleted source files to be recovered
    List<TsFileResource> sourceFiles = partialDeletedFiles.get(partialDeletedFilesIndex);
    for (int i = 0; i < partialDeletedFiles.size(); i++) {
      TsFileResource resource = partialDeletedFiles.get(i);
      TsFileResource targetResource = targetFiles.get(i);
      if (resource.tsFileExists()) {
        // source file exists, then roll back
        rollback(resource, targetResource);
      } else {
        // source file lost, then finish task
        finishTask(resource, targetResource);
      }
    }
  }

  private void rollback(TsFileResource sourceResource, TsFileResource targetResource)
      throws IOException {
    deleteCompactionModsFile(Collections.singletonList(sourceResource));
    if (targetResource == null || !targetResource.tsFileExists()) {
      return;
    }
    if (recoverMemoryStatus) {
      replaceTsFileInMemory(
          Collections.singletonList(targetResource), Collections.singletonList(sourceResource));
    }
    // delete target file
    if (!deleteTsFileOnDisk(targetResource)) {
      throw new CompactionRecoverException(
          String.format("failed to delete target file %s", targetResource));
    }
  }

  private void finishTask(TsFileResource sourceResource, TsFileResource targetResource)
      throws IOException {
    if (targetResource.isDeleted()) {
      // it means the target file is empty after compaction
      if (targetResource.remove()) {
        throw new CompactionRecoverException(
            String.format("failed to delete empty target file %s", targetResource));
      }
    } else {
      File targetFile = targetResource.getTsFile();
      if (targetFile == null || !TsFileUtils.isTsFileComplete(targetResource.getTsFile())) {
        throw new CompactionRecoverException(
            String.format("Target file is not completed. %s", targetFile));
      }
      if (recoverMemoryStatus) {
        targetResource.setStatus(TsFileResourceStatus.NORMAL);
      }
    }
    if (!deleteTsFileOnDisk(sourceResource)) {
      throw new CompactionRecoverException("source files cannot be deleted successfully");
    }
    if (recoverMemoryStatus) {
      FileMetrics.getInstance().deleteTsFile(true, Collections.singletonList(sourceResource));
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

    if(targetFileIdentifiers.isEmpty()){
      // in the process of settling all_deleted files
      sourceFileIdentifiers.forEach(x-> allDeletedFiles.add(new TsFileResource(new File(x.getFilePath()))));


    }else {
      // in the process of settling partial_deleted files
      List<TsFileResource> sourceFiles = new ArrayList<>();
      // recover source files
      sourceFileIdentifiers.forEach(x-> {
        File sourceFile = new File(x.getFilePath());
        TsFileResource resource = new TsFileResource(sourceFile);
        if (!sourceFile.exists()) {
          // source file has been deleted
          resource.forceMarkDeleted();
        }
        sourceFiles.add(resource);
      });
      partialDeletedFiles.add(sourceFiles);

      // recover target file
      TsFileIdentifier targetIdentifier = targetFileIdentifiers.get(0);
      File tmpTargetFile = new File(targetIdentifier.getFilePath());
      File targetFile =
              new File(
                      targetIdentifier.getFilePath()
                              .replace(IoTDBConstant.SETTLE_SUFFIX, TsFileConstant.TSFILE_SUFFIX));
      if (tmpTargetFile.exists()) {
        this.targetFile = new TsFileResource(tmpTargetFile);
      } else if (targetFile.exists()) {
        this.targetFile = new TsFileResource(targetFile);
      } else {
        // target file does not exist, then create empty resource
        this.targetFile = new TsFileResource();
        // check if target file is deleted after compaction or not
        targetIdentifier.setFilename(
                targetIdentifier.getFilename().replace(IoTDBConstant.SETTLE_SUFFIX, TsFileConstant.TSFILE_SUFFIX));
        if (deletedTargetFileIdentifiers.contains(targetIdentifier)) {
          // target file is deleted after compaction
          this.targetFile.forceMarkDeleted();
        }
      }
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof SettleCompactionTask)) {
      return false;
    }
    SettleCompactionTask otherSettleCompactionTask = (SettleCompactionTask) otherTask;
    return this.allDeletedFiles.equals(otherSettleCompactionTask.allDeletedFiles)
        && this.partialDeletedFiles.equals(otherSettleCompactionTask.partialDeletedFiles)
        && this.performer.getClass().isInstance(otherSettleCompactionTask.performer);
  }

  @Override
  public long getEstimatedMemoryCost() {
    if (partialDeletedFiles.isEmpty()) {
      return 0;
    } else {
      partialDeletedFiles.forEach(
          x -> {
            try {
              memoryCost =
                  Math.max(
                      memoryCost,
                      spaceEstimator.estimateInnerCompactionMemory(Collections.singletonList(x)));
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
    return allDeletedFiles.size() + partialDeletedFiles.size();
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
    return CompactionTaskType.SETTLE;
  }

  public List<TsFileResource> getAllDeletedFiles() {
    return allDeletedFiles;
  }

  public List<TsFileResource> getPartialDeletedFiles() {
    return partialDeletedFiles;
  }

  public double getAllDeletedFileSize() {
    return allDeletedFileSize;
  }

  public double getPartialDeletedFileSize() {
    return partialDeletedFileSize;
  }
}
