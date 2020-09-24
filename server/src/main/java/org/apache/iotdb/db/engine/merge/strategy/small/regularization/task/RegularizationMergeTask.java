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

package org.apache.iotdb.db.engine.merge.strategy.small.regularization.task;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.cache.ChunkMetadataCache;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.merge.MergeCallback;
import org.apache.iotdb.db.engine.merge.MergeTask;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.strategy.small.regularization.recover.RegularizationMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RegularizationMergeTask merges given seqFiles into new ones, which basically consists of two
 * steps: 1. write whole data to new files by device 2. remove old files
 */
public class RegularizationMergeTask extends MergeTask {

  public static final String MERGE_SUFFIX = ".merge.regularization";
  private static final Logger logger = LoggerFactory.getLogger(
      RegularizationMergeTask.class);

  private RegularizationMergeLogger mergeLogger;
  private MergeContext mergeContext = new MergeContext();

  protected List<TsFileResource> newResources = new ArrayList<>();

  public RegularizationMergeTask(
      MergeResource mergeResource, String storageGroupSysDir, MergeCallback callback,
      String taskName, String storageGroupName) {
    super(mergeResource, storageGroupSysDir, callback, taskName, false, storageGroupName);
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
    startTime = System.currentTimeMillis();
    mergeLogger = new RegularizationMergeLogger(storageGroupSysDir);

    mergeLogger.logFiles(resource);

    resource.setChunkWriterCache(MergeUtils.constructChunkWriterCache(storageGroupName));
    unmergedSeries = resource.getUnmergedSeries();
    mergeLogger.logMergeStart();

    RegularizationMergeSchedulerTask mergeChunkTask = new RegularizationMergeSchedulerTask(mergeContext,
        taskName, mergeLogger, resource, unmergedSeries, storageGroupName);
    newResources = mergeChunkTask.mergeSeries();

    cleanUpAndLog();
  }

  @Override
  protected void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

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
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setMerging(false);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
    } finally {
      seqFile.writeUnlock();
    }
  }
}
