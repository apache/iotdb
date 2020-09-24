package org.apache.iotdb.db.engine.merge.strategy.overlapped.inplace.task;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.strategy.overlapped.inplace.recover.InplaceMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.utils.MergeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeTask merges given seqFiles and unseqFiles into new ones, which basically consists of three
 * steps: 1. rewrite overflowed, modified or small-sized chunks into temp merge files 2. move the
 * merged chunks in the temp files back to the seqFiles or move the unmerged chunks in the seqFiles
 * into temp files and replace the seqFiles with the temp files. 3. remove unseqFiles
 */
public abstract class InplaceBaseMergeTask extends MergeTask {

  public static final String MERGE_SUFFIX = ".merge.inplace";
  private static final Logger logger = LoggerFactory.getLogger(InplaceFullMergeTask.class);

  MergeMultiChunkTask chunkTask;
  MergeFileTask fileTask;

  public InplaceBaseMergeTask(MergeResource mergeResource, String storageGroupSysDir,
      MergeCallback callback, boolean isFullMerge, String taskName, String storageGroupName) {
    super(mergeResource, storageGroupSysDir, callback, taskName, isFullMerge, storageGroupName);
  }

  @Override
  public Void call() throws Exception {
    try {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      abort();
    }
    return null;
  }

  private void abort() throws IOException {
    states = States.ABORTED;
    cleanUp(false);
    // call the callback to make sure the StorageGroup exit merging status, but passing 2
    // empty file lists to avoid files being deleted.
    callback.call(Collections.emptyList(), Collections.emptyList(),
        new File(storageGroupSysDir, InplaceMergeLogger.MERGE_LOG_NAME), null);
  }

  private void doMerge() throws IOException, MetadataException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} seqFiles", taskName, resource.getSeqFiles().size());
    }
    startTime = System.currentTimeMillis();
    mergeLogger = new InplaceMergeLogger(storageGroupSysDir);

    mergeLogger.logFiles(resource);

    resource.setChunkWriterCache(MergeUtils.constructChunkWriterCache(storageGroupName));

    unmergedSeries = resource.getUnmergedSeries();
    mergeLogger.logMergeStart();

    chunkTask = new MergeMultiChunkTask(mergeContext, taskName, (InplaceMergeLogger) mergeLogger,
        resource,
        fullMerge, unmergedSeries, storageGroupName);
    states = States.MERGE_CHUNKS;
    chunkTask.mergeSeries();
    if (Thread.interrupted()) {
      logger.info("Merge task {} aborted", taskName);
      abort();
      return;
    }

    fileTask = new MergeFileTask(taskName, mergeContext, (InplaceMergeLogger) mergeLogger, resource,
        (List<TsFileResource>) resource.getSeqFiles());
    states = States.MERGE_FILES;
    chunkTask = null;
    fileTask.mergeFiles();
    if (Thread.interrupted()) {
      logger.info("Merge task {} aborted", taskName);
      abort();
      return;
    }

    states = States.CLEAN_UP;
    fileTask = null;
    cleanUpAndLog();
  }

  @Override
  protected void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

    resource.clear();
    mergeContext.clear();

    if (mergeLogger != null) {
      mergeLogger.close();
    }

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      File mergeFile = new File(seqFile.getTsFilePath() + MERGE_SUFFIX);
      mergeFile.delete();
      seqFile.setMerging(false);
    }
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      unseqFile.setMerging(false);
    }

    File logFile = new File(storageGroupSysDir, InplaceMergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // make sure merge.log is not deleted until unseqFiles are cleared so that when system
      // reboots, the undeleted files can be deleted again
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile, null);
    } else {
      logFile.delete();
    }
  }
}
