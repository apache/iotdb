/**
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

package org.apache.iotdb.db.engine.merge.task;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeTask merges given SeqFiles and UnseqFiles into a new one.
 */
public class MergeTask implements Callable<Void> {

  public static final String MERGE_SUFFIX = ".merge";
  private static final Logger logger = LoggerFactory.getLogger(MergeTask.class);

  MergeResource resource;
  String storageGroupDir;
  MergeLogger mergeLogger;

  Map<TsFileResource, Integer> mergedChunkCnt = new HashMap<>();
  Map<TsFileResource, Integer> unmergedChunkCnt = new HashMap<>();
  Map<TsFileResource, Map<Path, List<Long>>> unmergedChunkStartTimes = new HashMap<>();

  private MergeCallback callback;

  String taskName;

  boolean fullMerge;

  public MergeTask(List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles, String storageGroupDir, MergeCallback callback,
      String taskName, boolean fullMerge) throws IOException {
    this.resource = new MergeResource(seqFiles, unseqFiles);
    this.storageGroupDir = storageGroupDir;
    this.callback = callback;
    this.taskName = taskName;
    this.fullMerge = fullMerge;
  }

  @Override
  public Void call() throws Exception {
    try  {
      doMerge();
    } catch (Exception e) {
      logger.error("Runtime exception in merge {}", taskName, e);
      resource.setSeqFiles(Collections.emptyList());
      resource.setUnseqFiles(Collections.emptyList());
      cleanUp(true);
      throw e;
    }
    return null;
  }

  private void doMerge() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} seqFiles, {} unseqFiles", taskName,
          resource.getSeqFiles().size(), resource.getUnseqFiles().size());
    }
    long startTime = System.currentTimeMillis();
    long totalFileSize = MergeUtils.collectFileSizes(resource.getSeqFiles(),
        resource.getUnseqFiles());
    mergeLogger = new MergeLogger(storageGroupDir);
    mergeLogger.logFiles(resource);

    List<Path> unmergedSeries = MergeUtils.collectPaths(resource);
    MergeSeriesTask mergeSeriesTask = new MergeSeriesTask(mergedChunkCnt, unmergedChunkCnt,
        unmergedChunkStartTimes, taskName, mergeLogger, resource, fullMerge, unmergedSeries);
    mergeSeriesTask.mergeSeries();

    MergeFileTask mergeFileTask = new MergeFileTask(taskName,mergedChunkCnt, unmergedChunkCnt,
        unmergedChunkStartTimes, mergeLogger, resource, resource.getSeqFiles());
    mergeFileTask.mergeFiles();

    cleanUp(true);
    if (logger.isInfoEnabled()) {
      double elapsedTime = (double) (System.currentTimeMillis() - startTime);
      double byteRate = totalFileSize / elapsedTime / 1024 / 1024 * 1000;
      double seriesRate = unmergedSeries.size() / elapsedTime * 1000;
      double chunkRate = mergeSeriesTask.totalChunkWritten / elapsedTime * 1000;
      double fileRate =
          (resource.getSeqFiles().size() + resource.getUnseqFiles().size()) / elapsedTime * 1000;
      logger.info("{} ends after {}ms, byteRate: {}MB/s, seriesRate {}/s, chunkRate: {}/s, "
              + "fileRate: {}/s",
          taskName, elapsedTime, byteRate, seriesRate, chunkRate, fileRate);
    }
  }



  void cleanUp(boolean executeCallback) throws IOException {
    logger.info("{} is cleaning up", taskName);

    resource.clear();

    mergedChunkCnt.clear();
    unmergedChunkCnt.clear();
    unmergedChunkStartTimes.clear();

    mergeLogger.close();

    for (TsFileResource seqFile : resource.getSeqFiles()) {
      File mergeFile = new File(seqFile.getFile().getPath() + MERGE_SUFFIX);
      mergeFile.delete();
    }

    File logFile = new File(storageGroupDir, MergeLogger.MERGE_LOG_NAME);
    if (executeCallback) {
      callback.call(resource.getSeqFiles(), resource.getUnseqFiles(), logFile);
    } else {
      logFile.delete();
    }
  }


}
