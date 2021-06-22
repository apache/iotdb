package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.CompactionContext;
import org.apache.iotdb.db.engine.compaction.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class InplaceCompactionTask extends CrossSpaceCompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(CrossSpaceCompactionTask.class);
  protected CompactionContext context;
  protected CrossSpaceMergeResource mergeResource;
  protected String storageGroupDir;
  protected List<TsFileResource> selectedSeqTsFileResourceList;
  protected List<TsFileResource> selectedUnSeqTsFileResourceList;
  protected TsFileResourceList seqTsFileResourceList;
  protected TsFileResourceList unSeqTsFileResourceList;
  protected boolean sequence;

  public InplaceCompactionTask(CompactionContext context) {
    super(context.getStorageGroupName(), context.getTimePartitionId());
    this.context = context;
    this.mergeResource = context.getMergeResource();
    this.storageGroupDir = context.getStorageGroupDir();
    this.seqTsFileResourceList = context.getSequenceFileResourceList();
    this.unSeqTsFileResourceList = context.getUnsequenceFileResourceList();
    this.selectedSeqTsFileResourceList = context.getSelectedSequenceFiles();
    this.selectedUnSeqTsFileResourceList = context.getSelectedUnsequenceFiles();
    this.sequence = context.isSequence();
  }

  @Override
  protected void doCompaction() throws Exception {
    String taskName = storageGroupName + "-" + System.currentTimeMillis();
    CrossSpaceMergeTask mergeTask =
        new CrossSpaceMergeTask(
            mergeResource,
            storageGroupDir,
            this::mergeEndAction,
            taskName,
            IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(),
            context.getConcurrentMergeCount(),
            storageGroupName);
    mergeTask.call();
  }

  public void mergeEndAction(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles, File mergeLog) {
    // todo: add
    LOGGER.info("{} a merge task is ending...", storageGroupName);

    if (Thread.currentThread().isInterrupted() || unseqFiles.isEmpty()) {
      // merge task abort, or merge runtime exception arose, just end this merge
      LOGGER.info("{} a merge task abnormally ends", storageGroupName);
      return;
    }
    removeUnseqFiles(unseqFiles);

    for (int i = 0; i < seqFiles.size(); i++) {
      TsFileResource seqFile = seqFiles.get(i);
      // get both seqFile lock and merge lock
      doubleWriteLock(seqFile);

      try {
        // if meet error(like file not found) in merge task, the .merge file may not be deleted
        File mergedFile =
            FSFactoryProducer.getFSFactory().getFile(seqFile.getTsFilePath() + MERGE_SUFFIX);
        if (mergedFile.exists()) {
          if (!mergedFile.delete()) {
            LOGGER.warn("Delete file {} failed", mergedFile);
          }
        }
        updateMergeModification(seqFile);
      } finally {
        doubleWriteUnlock(seqFile);
      }
    }

    try {
      removeMergingModification(seqFiles, unseqFiles);
      Files.delete(mergeLog.toPath());
    } catch (IOException e) {
      LOGGER.error(
          "{} a merge task ends but cannot delete log {}", storageGroupName, mergeLog.toPath());
    }

    LOGGER.info("{} a merge task ends", storageGroupName);
  }

  private void removeUnseqFiles(List<TsFileResource> unseqFiles) {
    unSeqTsFileResourceList.writeLock();
    try {
      for (TsFileResource unSeqFileMerged : selectedUnSeqTsFileResourceList) {
        unSeqTsFileResourceList.remove(unSeqFileMerged);
      }
      // clean cache
      if (IoTDBDescriptor.getInstance().getConfig().isMetaDataCacheEnable()) {
        ChunkCache.getInstance().clear();
        TimeSeriesMetadataCache.getInstance().clear();
      }
    } finally {
      unSeqTsFileResourceList.writeUnlock();
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

  /** acquire the write locks of the resource , the merge lock and the compaction lock */
  private void doubleWriteLock(TsFileResource seqFile) {
    boolean fileLockGot;
    boolean compactionLockGot;
    while (true) {
      fileLockGot = seqFile.tryWriteLock();
      compactionLockGot = seqTsFileResourceList.tryWriteLock();

      if (fileLockGot && compactionLockGot) {
        break;
      } else {
        // did not get all of them, release the gotten one and retry
        if (compactionLockGot) {
          seqTsFileResourceList.writeUnlock();
        }
        if (fileLockGot) {
          seqFile.writeUnlock();
        }
      }
    }
  }

  private void doubleWriteUnlock(TsFileResource seqFile) {
    seqTsFileResourceList.writeUnlock();
    seqFile.writeUnlock();
  }

  private void updateMergeModification(TsFileResource seqFile) {
    try {
      // remove old modifications and write modifications generated during merge
      seqFile.removeModFile();
      ModificationFile compactionModificationFile = ModificationFile.getCompactionMods(seqFile);
      for (Modification modification : compactionModificationFile.getModifications()) {
        seqFile.getModFile().write(modification);
      }
      try {
        seqFile.getModFile().close();
      } catch (IOException e) {
        LOGGER.error("Cannot close the ModificationFile {}", seqFile.getModFile().getFilePath(), e);
      }
    } catch (IOException e) {
      LOGGER.error(
          "{} cannot clean the ModificationFile of {} after cross space merge",
          storageGroupName,
          seqFile.getTsFile(),
          e);
    }
  }

  private void removeMergingModification(
      List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles) {
    try {
      for (TsFileResource seqFile : seqFiles) {
        ModificationFile.getCompactionMods(seqFile).remove();
      }
      for (TsFileResource unseqFile : unseqFiles) {
        ModificationFile.getCompactionMods(unseqFile).remove();
      }
    } catch (IOException e) {
      LOGGER.error("{} cannot remove merging modification ", storageGroupName, e);
    }
  }
}
