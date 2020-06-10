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

package org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.regularization.recover.RegularizationMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RegularizationMergeTask merges given seqFiles into new ones, which basically consists of two
 * steps: 1. write whole data to new files by device 2. remove old files
 */
public class RegularizationMergeTask implements Callable<Void> {

  public static final String MERGE_SUFFIX = ".merge.regularization";
  private static final Logger logger = LoggerFactory.getLogger(
      RegularizationMergeTask.class);

  MergeResource resource;
  String storageGroupSysDir;
  String storageGroupName;
  private RegularizationMergeLogger mergeLogger;
  private MergeContext mergeContext = new MergeContext();

  MergeCallback callback;
  String taskName;

  List<TsFileResource> newResources = new ArrayList<>();

  public RegularizationMergeTask(
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
              .getFile(storageGroupSysDir, RegularizationMergeLogger.MERGE_LOG_NAME),
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
    mergeLogger = new RegularizationMergeLogger(storageGroupSysDir);

    resource.setChunkWriterCache(MergeUtils.constructChunkWriterCache(storageGroupName));

    List<String> storageGroupPaths = MManager.getInstance()
        .getAllTimeseriesName(storageGroupName + ".*");
    List<Path> unmergedSeries = new ArrayList<>();
    for (String path : storageGroupPaths) {
      unmergedSeries.add(new Path(path));
    }

    mergeLogger.logMergeStart();

    RegularizationMergeSeriesTask mergeChunkTask = new RegularizationMergeSeriesTask(mergeContext,
        taskName, mergeLogger,
        resource, unmergedSeries);
    newResources = mergeChunkTask.mergeSeries();

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

    if (mergeLogger != null) {
      mergeLogger.close();
    }

    File logFile = FSFactoryProducer.getFSFactory().getFile(storageGroupSysDir,
        RegularizationMergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      // make sure merge.log is not deleted until unseqFiles are cleared so that when system
      // reboots, the undeleted files can be deleted again
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile, newResources);
    } else {
      logFile.delete();
    }
  }

  private void deleteFile(TsFileResource seqFile) {
    seqFile.writeLock();
    try {
      resource.removeFileReader(seqFile);
      ChunkMetadataCache.getInstance().remove(seqFile);
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getPath());
      seqFile.setMerging(false);
      seqFile.setDeleted(true);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }
}
