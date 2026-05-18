package com.timecho.iotdb.dataregion.compaction.execute.task;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.service.metrics.FileMetrics;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.constant.CompactionTaskType;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.SimpleCompactionLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.selector.utils.CrossCompactionTaskResource;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import com.timecho.iotdb.dataregion.compaction.selector.impl.SharedStorageCompactionSelector;
import com.timecho.iotdb.dataregion.compaction.selector.utils.SharedStorageCompactionTaskResource;
import com.timecho.iotdb.dataregion.compaction.tool.SharedStorageCompactionUtils;
import com.timecho.iotdb.i18n.TimechoServerMessages;
import com.timecho.iotdb.os.HybridFileInputFactoryDecorator;
import com.timecho.iotdb.os.utils.RemoteStorageBlock;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.log.CompactionLogger.STR_DELETED_TARGET_FILES;

public class SharedStorageCompactionTask extends AbstractCompactionTask {
  public static String LOG_FILE_NAME =
      "shared-storage" + CompactionLogger.CROSS_COMPACTION_LOG_NAME_SUFFIX;
  public static String REMOTE_TMP_FILE_SUFFIX = ".remote";
  private static final Logger LOGGER = LoggerFactory.getLogger(SharedStorageCompactionTask.class);
  private static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  private static final Map<String, Map<Long, SharedStorageCompactionTask>>
      dataRegion2TimePartitionCompactionTask = new ConcurrentHashMap<>();

  private final DataRegion dataRegion;
  private final SharedStorageCompactionTaskResource taskResource;
  private List<TsFileResource> sourceFiles;
  private List<TsFileResource> targetFiles;
  private List<TsFileResource> overlappedSourceFiles = new ArrayList<>();
  private final long selectedFileSize;
  private File workDir;
  private File logFile;

  public SharedStorageCompactionTask(
      long timePartition,
      TsFileManager tsFileManager,
      CrossCompactionTaskResource taskResource,
      long serialId) {
    this(
        timePartition,
        StorageEngine.getInstance()
            .getDataRegion(new DataRegionId(Integer.parseInt(tsFileManager.getDataRegionId()))),
        tsFileManager,
        (SharedStorageCompactionTaskResource) taskResource,
        serialId);
  }

  public SharedStorageCompactionTask(
      long timePartition,
      DataRegion dataRegion,
      TsFileManager tsFileManager,
      SharedStorageCompactionTaskResource taskResource,
      long serialId) {
    super(
        tsFileManager.getStorageGroupName(),
        tsFileManager.getDataRegionId(),
        timePartition,
        tsFileManager,
        serialId);
    this.dataRegion = dataRegion;
    this.taskResource = taskResource;
    this.sourceFiles = new ArrayList<>();
    this.sourceFiles.addAll(taskResource.getSeqFiles());
    this.sourceFiles.addAll(taskResource.getUnseqFiles());
    this.selectedFileSize =
        this.sourceFiles.stream().mapToLong(TsFileResource::getTsFileSize).sum();
    createSummary();
  }

  @Override
  protected boolean doCompaction() {
    if (sourceFiles.isEmpty()) {
      return true;
    }
    // use last tier to do replica sharing
    int lastTierLevel = sourceFiles.get(0).getTierLevel();
    File lastTierDir = fsFactory.getFile(sourceFiles.get(0).getTsFile().getParent());
    for (TsFileResource resource : sourceFiles) {
      if (resource.getTierLevel() > lastTierLevel) {
        lastTierLevel = resource.getTierLevel();
        lastTierDir = fsFactory.getFile(resource.getTsFile().getParent());
      }
    }
    workDir = lastTierDir;
    targetFiles =
        SharedStorageCompactionUtils.pullRemoteReplica(dataRegion, timePartition, workDir);
    if (targetFiles == null || targetFiles.isEmpty()) {
      return false;
    }
    // load remote replica
    Map<String, TsFileResource> remotePath2SourceFiles = getRemotePaths(sourceFiles);
    if (remotePath2SourceFiles == null || remotePath2SourceFiles.isEmpty()) {
      return false;
    }
    logFile = fsFactory.getFile(workDir, LOG_FILE_NAME);
    try (SimpleCompactionLogger compactionLogger = new SimpleCompactionLogger(logFile)) {
      dataRegion2TimePartitionCompactionTask
          .compute(dataRegionId, (k, v) -> v == null ? new ConcurrentHashMap<>() : v)
          .put(timePartition, this);

      compactionLogger.logSourceFiles(sourceFiles);
      compactionLogger.logTargetFiles(targetFiles);

      compactionLogger.force();

      for (TsFileResource remoteResource : targetFiles) {
        synchronized (this) {
          // remove duplicate files
          removeRemoteTmpSuffix(remoteResource);
          remoteResource.deserialize();
          if (remoteResource.getRemoteStorageBlock() == null
              || remotePath2SourceFiles.containsKey(
                  remoteResource.getRemoteStorageBlock().getPath())) {
            overlappedSourceFiles.add(
                remotePath2SourceFiles.get(remoteResource.getRemoteStorageBlock().getPath()));
            SharedStorageCompactionUtils.removeLocalReplica(remoteResource);
            LOGGER.debug(
                "SharedStorageCompactionTask of DataRegion-{} removed remote replica tsfile {}.",
                dataRegionId,
                remoteResource);
            continue;
          }
          // load TsFile and update related info
          remoteResource.setStatusForShareStorageCompaction(
              TsFileResourceStatus.COMPACTION_CANDIDATE);
          remoteResource.setStatus(TsFileResourceStatus.COMPACTING);
          File oldFile = remoteResource.getTsFile();
          remoteResource.setLastValues(Collections.emptyMap());
          dataRegion.loadNewTsFile(remoteResource, true, true, true, Optional.empty());
          HybridFileInputFactoryDecorator.putRemotePathInfo(
              remoteResource.getTsFile(),
              HybridFileInputFactoryDecorator.removeRemotePathInfo(oldFile));
          LOGGER.debug(
              "SharedStorageCompactionTask of DataRegion-{} load remote replica tsfile {}.",
              dataRegionId,
              remoteResource);
        }

        compactionLogger.logTargetFile(remoteResource);
        compactionLogger.force();
      }
      // use empty target files to denote overlapped source files
      compactionLogger.logFiles(overlappedSourceFiles, STR_DELETED_TARGET_FILES);
      compactionLogger.force();
      // remove remote tmp files and old files
      deleteRemoteTmpFiles();
      unloadAndDeleteSourceFiles();
      SharedStorageCompactionSelector.updateLastVersion(
          dataRegionId, timePartition, taskResource.getMaxVersion());
    } catch (Exception e) {
      handleException(LOGGER, e);
      recover();
      return false;
    } finally {
      dataRegion2TimePartitionCompactionTask.get(dataRegionId).remove(timePartition, this);
      setNormalOnRemoteStatusForAllTargetFiles();
      try {
        Files.deleteIfExists(logFile.toPath());
      } catch (IOException e) {
        handleException(LOGGER, e);
      }
    }
    return true;
  }

  private Map<String, TsFileResource> getRemotePaths(List<TsFileResource> resources) {
    Map<String, TsFileResource> remotePath2Files = new HashMap<>();
    for (TsFileResource resource : resources) {
      RemoteStorageBlock remoteStorageBlock = resource.getRemoteStorageBlock();
      if (remoteStorageBlock == null) {
        LOGGER.info(TimechoServerMessages.CANNOT_GET_REMOTE_STORAGE_BLOCK_OF_TSFILE, resource);
        return null;
      }
      remotePath2Files.put(remoteStorageBlock.getPath(), resource);
    }
    return remotePath2Files;
  }

  private void removeRemoteTmpSuffix(TsFileResource resource) throws IOException {
    File localFile = resource.getTsFile();
    resource.writeLock();
    try {
      String resourceFilePath = localFile.getPath() + TsFileResource.RESOURCE_SUFFIX;
      fsFactory.moveFile(
          fsFactory.getFile(resourceFilePath + REMOTE_TMP_FILE_SUFFIX),
          fsFactory.getFile(resourceFilePath));
      String modsFilePath = localFile.getPath() + ModificationFile.FILE_SUFFIX;
      if (fsFactory.getFile(modsFilePath + REMOTE_TMP_FILE_SUFFIX).exists()) {
        fsFactory.moveFile(
            fsFactory.getFile(modsFilePath + REMOTE_TMP_FILE_SUFFIX),
            fsFactory.getFile(modsFilePath));
      }
    } finally {
      resource.writeUnlock();
    }
  }

  @Override
  protected void recover() {
    rollback();
  }

  private void rollback() {
    deleteRemoteTmpFiles();
    unloadAndDeleteTargetFiles();
  }

  private void deleteRemoteTmpFiles() {
    try {
      SharedStorageCompactionUtils.deleteRemoteTmpFiles(workDir);
    } catch (IOException e) {
      LOGGER.warn(TimechoServerMessages.FAIL_TO_DELETE_REMOTE_TMP_FILES_IN_DIR, workDir, e);
    }
  }

  private void updateUnloadedFilesMetrics(List<TsFileResource> resources) {
    FileMetrics.getInstance()
        .decreaseModFileNum(
            (int) resources.stream().filter(TsFileResource::exclusiveModFileExists).count());
    FileMetrics.getInstance()
        .decreaseModFileSize(
            resources.stream()
                .mapToLong(
                    f ->
                        f.exclusiveModFileExists()
                            ? f.getExclusiveModFile().getFile().length()
                            : 0L)
                .sum());
    FileMetrics.getInstance()
        .deleteTsFile(
            true, resources.stream().filter(TsFileResource::isSeq).collect(Collectors.toList()));
    FileMetrics.getInstance()
        .deleteTsFile(
            false, resources.stream().filter(f -> !f.isSeq()).collect(Collectors.toList()));
  }

  private void unloadAndDeleteSourceFiles() {
    if (sourceFiles == null || sourceFiles.isEmpty()) {
      return;
    }
    Set<TsFileResource> overlappedSourceFiles = new HashSet<>(this.overlappedSourceFiles);
    List<TsFileResource> files2Delete =
        sourceFiles.stream()
            .filter(f -> !overlappedSourceFiles.contains(f))
            .collect(Collectors.toList());
    updateUnloadedFilesMetrics(files2Delete);
    for (TsFileResource resource : files2Delete) {
      tsFileManager.remove(resource, resource.isSeq());
      resource.remove();
    }
  }

  private void unloadAndDeleteTargetFiles() {
    if (targetFiles == null || targetFiles.isEmpty()) {
      return;
    }
    updateUnloadedFilesMetrics(
        targetFiles.stream().filter(TsFileResource::isCompacting).collect(Collectors.toList()));
    for (TsFileResource resource : targetFiles) {
      tsFileManager.remove(resource, false);
      try {
        SharedStorageCompactionUtils.removeLocalReplica(resource);
      } catch (IOException e) {
        LOGGER.error(TimechoServerMessages.TSFILE_RESOURCE_CANNOT_BE_DELETED, resource, e);
      }
    }
  }

  @Override
  public void resetCompactionCandidateStatusForAllSourceFiles() {
    List<TsFileResource> resources = getAllSourceTsFiles();
    // only reset status of the resources whose status is COMPACTING and COMPACTING_CANDIDATE
    resources.forEach(f -> f.setStatus(TsFileResourceStatus.NORMAL_ON_REMOTE));
  }

  public void setNormalOnRemoteStatusForAllTargetFiles() {
    if (targetFiles == null) {
      return;
    }
    targetFiles.forEach(f -> f.setStatus(TsFileResourceStatus.NORMAL_ON_REMOTE));
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask otherTask) {
    if (!(otherTask instanceof SharedStorageCompactionTask)) {
      return false;
    }
    SharedStorageCompactionTask otherCrossCompactionTask = (SharedStorageCompactionTask) otherTask;
    return this.sourceFiles.equals(otherCrossCompactionTask.sourceFiles);
  }

  @Override
  public List<TsFileResource> getAllSourceTsFiles() {
    return sourceFiles;
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
    return CompactionTaskType.SHARED_STORAGE;
  }

  public synchronized void deleteData(ModEntry deletion) throws IOException {
    for (TsFileResource resource : targetFiles) {
      if (resource.getStatus() == TsFileResourceStatus.COMPACTING
          || DataRegion.canSkipDelete(resource, deletion)) {
        continue;
      }
      ModificationFile modFile =
          new ModificationFile(
              resource.getTsFilePath() + ModificationFile.FILE_SUFFIX + REMOTE_TMP_FILE_SUFFIX,
              true);
      long originSize = modFile.getFile().length();
      try {
        // write deletion into modification file
        modFile.write(deletion);
        // remember to close mod file
        modFile.close();
        // if file length greater than 1M,execute compact.
        modFile.compact();
      } catch (Throwable t) {
        if (originSize != -1) {
          modFile.truncate(originSize);
        }
        throw t;
      }
      LOGGER.info(
          "[Deletion] Deletion with deletion {} written into mods file:{}.",
          deletion,
          modFile.getFile());
    }
  }

  public static void deleteDataInRemoteFiles(DataRegion dataRegion, ModEntry deletion)
      throws IOException {
    Map<Long, SharedStorageCompactionTask> timePartitionTasks =
        dataRegion2TimePartitionCompactionTask.getOrDefault(
            dataRegion.getDataRegionIdString(), Collections.emptyMap());
    List<SharedStorageCompactionTask> selectedTasks = new ArrayList<>();
    timePartitionTasks.forEach(
        (k, v) -> {
          if (TimePartitionUtils.satisfyPartitionId(
              deletion.getStartTime(), deletion.getEndTime(), k)) {
            selectedTasks.add(v);
          }
        });

    for (SharedStorageCompactionTask task : selectedTasks) {
      task.deleteData(deletion);
    }
  }

  @TestOnly
  public void setTargetFiles(List<TsFileResource> targetFiles) {
    this.targetFiles = targetFiles;
  }

  @Override
  public long getSelectedFileSize() {
    return this.selectedFileSize;
  }
}
