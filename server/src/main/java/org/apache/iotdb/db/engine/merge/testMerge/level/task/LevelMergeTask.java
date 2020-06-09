package org.apache.iotdb.db.engine.merge.testMerge.level.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.independence.recover.IndependenceMergeLogger;
import org.apache.iotdb.db.engine.merge.sizeMerge.independence.task.IndependenceMergeSeriesTask;
import org.apache.iotdb.db.engine.merge.testMerge.level.recover.LevelMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LevelMergeTask implements Callable<Void> {

  public static final String MERGE_SUFFIX = ".merge.level";
  private static final Logger logger = LoggerFactory.getLogger(
      LevelMergeTask.class);

  MergeResource resource;
  String storageGroupSysDir;
  String storageGroupName;
  private LevelMergeLogger mergeLogger;
  private MergeContext mergeContext = new MergeContext();

  MergeCallback callback;
  String taskName;

  List<TsFileResource> newResources;

  public LevelMergeTask(
      MergeResource mergeResource, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    this.resource = mergeResource;
    this.storageGroupSysDir = storageGroupSysDir;
    this.callback = callback;
    this.taskName = taskName;
    this.storageGroupName = storageGroupName;
  }

  @Override
  public Void call() throws Exception {
    try {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      cleanUp(false);
      // call the callback to make sure the StorageGroup exit merging status, but passing 2
      // empty file lists to avoid files being deleted.
      callback.call(Collections.emptyList(), Collections.emptyList(),
          SystemFileFactory.INSTANCE
              .getFile(storageGroupSysDir, IndependenceMergeLogger.MERGE_LOG_NAME),
          null);
      throw e;
    }
    return null;
  }

  private void doMerge() throws IOException, MetadataException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} seqFiles, {} unseqFiles", taskName,
          resource.getSeqFiles().size(), resource.getUnseqFiles().size());
    }
    long startTime = System.currentTimeMillis();
    long totalFileSize = MergeUtils.collectFileSizes(resource.getSeqFiles(),
        resource.getUnseqFiles());
    mergeLogger = new LevelMergeLogger(storageGroupSysDir);

    Set<String> devices = MManager.getInstance().getDevices(storageGroupName);
    Map<Path, IChunkWriter> chunkWriterCacheMap = new HashMap<>();
    for (String device : devices) {
      InternalMNode deviceNode = (InternalMNode) MManager.getInstance().getNodeByPath(device);
      for (Entry<String, MNode> entry : deviceNode.getChildren().entrySet()) {
        MeasurementSchema measurementSchema = ((LeafMNode) entry.getValue()).getSchema();
        chunkWriterCacheMap
            .put(new Path(device, entry.getKey()), new ChunkWriterImpl(measurementSchema));
      }
    }
    resource.setChunkWriterCache(chunkWriterCacheMap);

    List<String> storageGroupPaths = MManager.getInstance()
        .getAllTimeseriesName(storageGroupName + ".*");
    List<Path> unmergedSeries = new ArrayList<>();
    for (String path : storageGroupPaths) {
      unmergedSeries.add(new Path(path));
    }

    mergeLogger.logMergeStart();

    LevelMergeSeriesTask mergeChunkTask = new LevelMergeSeriesTask(mergeContext,
        taskName, mergeLogger,
        resource, unmergedSeries);
    newResources = mergeChunkTask.mergeSeries();

    cleanUp(true);
    if (logger.isInfoEnabled()) {
      double elapsedTime = (double) (System.currentTimeMillis() - startTime) / 1000.0;
      double byteRate = totalFileSize / elapsedTime / 1024 / 1024;
      double chunkRate = mergeContext.getTotalChunkWritten() / elapsedTime;
      double fileRate =
          (resource.getSeqFiles().size() + resource.getUnseqFiles().size()) / elapsedTime;
      double ptRate = mergeContext.getTotalPointWritten() / elapsedTime;
      logger.info("{} ends after {}s, byteRate: {}MB/s, chunkRate: {}/s, "
              + "fileRate: {}/s, ptRate: {}/s",
          taskName, elapsedTime, byteRate, chunkRate, fileRate, ptRate);
    }
  }

  void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      seqFile.setDeleted(true);
    }

    resource.clear();
    mergeContext.clear();

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      deleteFile(seqFile);
    }

    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      deleteFile(unseqFile);
    }

    if (mergeLogger != null) {
      mergeLogger.close();
    }

    File logFile = FSFactoryProducer.getFSFactory().getFile(storageGroupSysDir,
        IndependenceMergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // make sure merge.log is not deleted until unseqFiles are cleared so that when system
      // reboots, the undeleted files can be deleted again
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile, newResources);
    } else {
      logFile.delete();
    }
  }

  private void deleteFile(TsFileResource seqFile) {
    seqFile.getWriteQueryLock().writeLock().lock();
    try {
      resource.removeFileReader(seqFile);
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getPath());
      seqFile.getFile().delete();
      seqFile.setDeleted(true);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.getWriteQueryLock().writeLock().unlock();
    }
  }

}
