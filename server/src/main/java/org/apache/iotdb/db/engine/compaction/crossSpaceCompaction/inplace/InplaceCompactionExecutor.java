package org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace;

import static org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.task.MergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.MERGING_MODIFICATION_FILE_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.CrossSpaceCompactionExecutor;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.manage.MergeResource;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.selector.IMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.selector.MaxFileMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.selector.MaxSeriesMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.selector.MergeFileStrategy;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.task.MergeTask;
import org.apache.iotdb.db.engine.compaction.crossSpaceCompaction.inplace.task.RecoverMergeTask;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InplaceCompactionExecutor extends CrossSpaceCompactionExecutor {
  private static final Logger logger = LoggerFactory.getLogger(InplaceCompactionExecutor.class);

  private final int unseqLevelNum =
      Math.max(IoTDBDescriptor.getInstance().getConfig().getUnseqLevelNum(), 1);
  private final int maxOpenFileNumInEachUnseqCompaction =
      IoTDBDescriptor.getInstance().getConfig().getMaxOpenFileNumInEachUnseqCompaction();

  public InplaceCompactionExecutor(TsFileManagement tsFileManagement) {
    super(tsFileManagement);
  }

  @Override
  public void recover() {
    // MERGE TODO: unify mods solution of inplace compaction and level compaction
    try {
      // recover mods
      File mergingMods =
          SystemFileFactory.INSTANCE.getFile(
              tsFileManagement.storageGroupSysDir, MERGING_MODIFICATION_FILE_NAME);
      if (mergingMods.exists()) {
        this.tsFileManagement.mergingModification = new ModificationFile(mergingMods.getPath());
      }
      RecoverMergeTask recoverMergeTask =
          new RecoverMergeTask(
              new ArrayList<>(tsFileManagement.getTsFileList(true)),
              tsFileManagement.getTsFileList(false),
              tsFileManagement.storageGroupSysDir,
              this::mergeEndAction,
              IoTDBDescriptor.getInstance().getConfig().isFullMerge(),
              tsFileManagement.storageGroupName);
      logger.info("{} a RecoverMergeTask starts...", tsFileManagement.storageGroupName);
      recoverMergeTask.recoverMerge(
          IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot());
      if (!IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot()) {
        tsFileManagement.mergingModification.remove();
      }
    } catch (MetadataException | IOException e) {
      logger.error("{} inplace compaction recover failed", tsFileManagement.storageGroupName, e);
    }
  }

  @Override
  public void doCrossSpaceCompaction(boolean fullMerge, long timePartition) {
    List<TsFileResource> seqMergeList =
        tsFileManagement.getTsFileListByTimePartition(true, timePartition);
    List<TsFileResource> unSeqMergeList =
        tsFileManagement.getTsFileListByTimePartitionAndLevel(
            false, timePartition, unseqLevelNum - 1);
    if (seqMergeList.isEmpty() || unSeqMergeList.isEmpty()) {
      logger.info(
          "{} no files to be merged, seqFiles={}, unseqFiles={}",
          tsFileManagement.storageGroupName,
          seqMergeList.size(),
          unSeqMergeList.size());
      return;
    }

    // the number of unseq files in one merge should not exceed maxOpenFileNumInEachUnseqCompaction
    if (unSeqMergeList.size() > maxOpenFileNumInEachUnseqCompaction) {
      unSeqMergeList = unSeqMergeList.subList(0, maxOpenFileNumInEachUnseqCompaction);
    }

    long timeLowerBound =
        System.currentTimeMillis() - IoTDBDescriptor.getInstance().getConfig().getDefaultTTL();
    MergeResource mergeResource = new MergeResource(seqMergeList, unSeqMergeList, timeLowerBound);

    IMergeFileSelector fileSelector = getMergeFileSelector(mergeResource);
    try {
      List[] mergeFiles = fileSelector.select();
      if (mergeFiles.length == 0) {
        logger.info(
            "{} cannot select merge candidates under the budget {}",
            tsFileManagement.storageGroupName,
            IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget());
        return;
      }

      mergeResource.startMerging();

      MergeTask mergeTask =
          new MergeTask(
              mergeResource,
              tsFileManagement.storageGroupSysDir,
              this::mergeEndAction,
              fullMerge,
              fileSelector.getConcurrentMergeNum(),
              tsFileManagement.storageGroupName);

      tsFileManagement.mergingModification =
          new ModificationFile(
              tsFileManagement.storageGroupSysDir
                  + File.separator
                  + MERGING_MODIFICATION_FILE_NAME);

      logger.info(
          "{} start merge {} seqFiles, {} unseqFiles",
          tsFileManagement.storageGroupName,
          mergeFiles[0].size(),
          mergeFiles[1].size());

      mergeTask.doMerge();

    } catch (Exception e) {
      logger.error("{} cannot select file for merge", tsFileManagement.storageGroupName, e);
    }
  }

  private IMergeFileSelector getMergeFileSelector(MergeResource resource) {
    long budget = IoTDBDescriptor.getInstance().getConfig().getMergeMemoryBudget();
    MergeFileStrategy strategy = IoTDBDescriptor.getInstance().getConfig().getMergeFileStrategy();
    switch (strategy) {
      case MAX_FILE_NUM:
        return new MaxFileMergeFileSelector(resource, budget);
      case MAX_SERIES_NUM:
        return new MaxSeriesMergeFileSelector(resource, budget);
      default:
        throw new UnsupportedOperationException("Unknown MergeFileStrategy " + strategy);
    }
  }

  /** acquire the write locks of the resource , the merge lock and the compaction lock */
  private void doubleWriteLock(TsFileResource seqFile) {
    boolean fileLockGot;
    boolean compactionLockGot;
    while (true) {
      fileLockGot = seqFile.tryWriteLock();
      compactionLockGot = tsFileManagement.tryWriteLock();

      if (fileLockGot && compactionLockGot) {
        break;
      } else {
        // did not get all of them, release the gotten one and retry
        if (compactionLockGot) {
          tsFileManagement.writeUnlock();
        }
        if (fileLockGot) {
          seqFile.writeUnlock();
        }
      }
    }
  }

  /** release the write locks of the resource , the merge lock and the compaction lock */
  private void doubleWriteUnlock(TsFileResource seqFile) {
    tsFileManagement.writeUnlock();
    seqFile.writeUnlock();
  }

  private void removeUnseqFiles(List<TsFileResource> unseqFiles) {
    tsFileManagement.writeLock();
    try {
      tsFileManagement.removeAll(unseqFiles, false);
      // clean cache
      if (IoTDBDescriptor.getInstance().getConfig().isMetaDataCacheEnable()) {
        ChunkCache.getInstance().clear();
        TimeSeriesMetadataCache.getInstance().clear();
      }
    } finally {
      tsFileManagement.writeUnlock();
    }

    for (TsFileResource unseqFile : unseqFiles) {
      unseqFile.writeLock();
      try {
        unseqFile.remove();
      } finally {
        unseqFile.writeUnlock();
      }
    }
  }

  @SuppressWarnings("squid:S1141")
  private void updateMergeModification(TsFileResource seqFile) {
    try {
      // remove old modifications and write modifications generated during merge
      seqFile.removeModFile();
      if (tsFileManagement.mergingModification != null) {
        for (Modification modification : tsFileManagement.mergingModification.getModifications()) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          seqFile.getModFile().write(modification);
        }
        try {
          seqFile.getModFile().close();
        } catch (IOException e) {
          logger.error(
              "Cannot close the ModificationFile {}", seqFile.getModFile().getFilePath(), e);
        }
      }
    } catch (IOException e) {
      logger.error(
          "{} cannot clean the ModificationFile of {} after merge",
          tsFileManagement.storageGroupName,
          seqFile.getTsFile(),
          e);
    }
  }

  private void removeMergingModification() {
    try {
      if (tsFileManagement.mergingModification != null) {
        tsFileManagement.mergingModification.remove();
        tsFileManagement.mergingModification = null;
      }
    } catch (IOException e) {
      logger.error("{} cannot remove merging modification ", tsFileManagement.storageGroupName, e);
    }
  }

  public void mergeEndAction(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, File mergeLog) {
    logger.info("{} a merge task is ending...", tsFileManagement.storageGroupName);

    if (Thread.currentThread().isInterrupted() || unseqFiles.isEmpty()) {
      // merge task abort, or merge runtime exception arose, just end this merge
      logger.info("{} a merge task abnormally ends", tsFileManagement.storageGroupName);
      return;
    }
    removeUnseqFiles(unseqFiles);

    for (TsFileResource seqFile : seqFiles) {
      // get both seqFile lock and merge lock
      doubleWriteLock(seqFile);

      try {
        // if meet error(like file not found) in merge task, the .merge file may not be deleted
        File mergedFile =
            FSFactoryProducer.getFSFactory().getFile(seqFile.getTsFilePath() + MERGE_SUFFIX);
        if (mergedFile.exists()) {
          if (!mergedFile.delete()) {
            logger.warn("Delete file {} failed", mergedFile);
          }
        }
        updateMergeModification(seqFile);
      } finally {
        doubleWriteUnlock(seqFile);
      }
    }

    try {
      removeMergingModification();
      Files.delete(mergeLog.toPath());
    } catch (IOException e) {
      logger.error(
          "{} a merge task ends but cannot delete log {}",
          tsFileManagement.storageGroupName,
          mergeLog.toPath());
    }

    logger.info("{} a merge task ends", tsFileManagement.storageGroupName);
  }
}
