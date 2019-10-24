package org.apache.iotdb.db.engine.merge.squeeze.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.squeeze.recover.MergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqueezeMergeTask implements Callable<Void> {

  static final String MERGE_SUFFIX = ".merge";
  private static final Logger logger = LoggerFactory.getLogger(SqueezeMergeTask.class);

  private MergeResource resource;
  private String storageGroupSysDir;
  private String storageGroupName;
  private MergeLogger mergeLogger;
  private MergeContext mergeContext = new MergeContext();

  private MergeCallback callback;
  private int concurrentMergeSeriesNum;
  private String taskName;

  private TsFileResource newResource;

  public SqueezeMergeTask(MergeResource mergeResource, String storageGroupSysDir, MergeCallback callback,
      String taskName, int concurrentMergeSeriesNum, String storageGroupName) {
    this.resource = mergeResource;
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  @Override
  public Void call() throws Exception {
    try  {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      cleanUp(false);
      // call the callback to make sure the StorageGroup exit merging status, but passing 2
      // empty file lists to avoid files being deleted.
      callback.call(
          Collections.emptyList(), Collections.emptyList(), new File(storageGroupSysDir,
              MergeLogger.MERGE_LOG_NAME), null);
      throw e;
    }
    return null;
  }

  private void doMerge() throws IOException, MetadataErrorException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} seqFiles, {} unseqFiles", taskName,
          resource.getSeqFiles().size(), resource.getUnseqFiles().size());
    }
    long startTime = System.currentTimeMillis();
    long totalFileSize = MergeUtils.collectFileSizes(resource.getSeqFiles(),
        resource.getUnseqFiles());
    mergeLogger = new MergeLogger(storageGroupSysDir);

    mergeLogger.logFiles(resource);

    List<MeasurementSchema> measurementSchemas = MManager.getInstance()
        .getSchemaForStorageGroup(storageGroupName);
    resource.addMeasurements(measurementSchemas);

    List<String> storageGroupPaths = MManager.getInstance().getPaths(storageGroupName + ".*");
    List<Path> unmergedSeries = new ArrayList<>();
    for (String path : storageGroupPaths) {
      unmergedSeries.add(new Path(path));
    }

    mergeLogger.logMergeStart();

    MergeSeriesTask mergeChunkTask = new MergeSeriesTask(mergeContext, taskName, mergeLogger, resource
        ,unmergedSeries, concurrentMergeSeriesNum);
    newResource = mergeChunkTask.mergeSeries();

    cleanUp(true);
    if (logger.isInfoEnabled()) {
      double elapsedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;
      double byteRate = totalFileSize / elapsedTime / 1024 / 1024;
      double seriesRate = unmergedSeries.size() / elapsedTime;
      double chunkRate = mergeContext.getTotalChunkWritten() / elapsedTime;
      double fileRate =
          (resource.getSeqFiles().size() + resource.getUnseqFiles().size()) / elapsedTime;
      double ptRate = mergeContext.getTotalPointWritten() / elapsedTime;
      logger.info("{} ends after {}s, byteRate: {}MB/s, seriesRate {}/s, chunkRate: {}/s, "
              + "fileRate: {}/s, ptRate: {}/s",
          taskName, elapsedTime, byteRate, seriesRate, chunkRate, fileRate, ptRate);
    }
  }

  private void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

    resource.clear();
    mergeContext.clear();

    if (mergeLogger != null) {
      mergeLogger.close();
    }

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      File mergeFile = new File(seqFile.getFile().getPath() + MERGE_SUFFIX);
      mergeFile.delete();
    }

    File logFile = new File(storageGroupSysDir, MergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // make sure merge.log is not deleted until unseqFiles are cleared so that when system
      // reboots, the undeleted files can be deleted again
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile, newResource);
    } else {
      logFile.delete();
    }
  }
}