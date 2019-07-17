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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.merge.recover.LogAnalyzer;
import org.apache.iotdb.db.engine.merge.recover.LogAnalyzer.Status;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoverMergeTask extends MergeTask {

  private static final Logger logger = LoggerFactory.getLogger(RecoverMergeTask.class);

  private LogAnalyzer analyzer;


  public RecoverMergeTask(String storageGroupDir, MergeCallback callback, String taskName,
      boolean fullMerge) throws IOException {
    super(null, null, storageGroupDir, callback, taskName, fullMerge);
  }

  public void recoverMerge(boolean continueMerge) throws IOException {
    File logFile = new File(storageGroupDir, MergeLogger.MERGE_LOG_NAME);
    if (!logFile.exists()) {
      logger.info("{} no merge.log, merge recovery ends", taskName);
      return;
    }
    long startTime = System.currentTimeMillis();

    analyzer = new LogAnalyzer(resource, taskName, logFile);
    Status status = analyzer.analyze();
    if (logger.isInfoEnabled()) {
      logger.info("{} merge recovery status determined: {} after {}ms", taskName, status,
          (System.currentTimeMillis() - startTime));
    }
    switch (status) {
      case NONE:
        logFile.delete();
        break;
      case FILES_LOGGED:
        resumeAfterFilesLogged(continueMerge);
        break;
      case ALL_TS_MERGED:
        resumeAfterAllTsMerged(continueMerge);
        break;
      case MERGE_END:
        cleanUp(continueMerge);
        break;
      default:
        throw new UnsupportedOperationException(taskName + " found unrecognized status " + status);
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} merge recovery ends after {}ms", taskName,
          (System.currentTimeMillis() - startTime));
    }
  }

  private void resumeAfterFilesLogged(boolean continueMerge) throws IOException {
    if (continueMerge) {
      resumeMerge();
      MergeSeriesTask mergeSeriesTask = new MergeSeriesTask(mergedChunkCnt, unmergedChunkCnt,
          unmergedChunkStartTimes, taskName, mergeLogger, resource, fullMerge, analyzer.getUnmergedPaths());
      mergeSeriesTask.mergeSeries();

      MergeFileTask mergeFileTask = new MergeFileTask(taskName,mergedChunkCnt, unmergedChunkCnt,
          unmergedChunkStartTimes, mergeLogger, resource, resource.getSeqFiles());
      mergeFileTask.mergeFiles();
    }
    cleanUp(continueMerge);
  }

  private void resumeAfterAllTsMerged(boolean continueMerge) throws IOException {
    if (continueMerge) {
      resumeMerge();
      MergeFileTask mergeFileTask = new MergeFileTask(taskName,mergedChunkCnt, unmergedChunkCnt,
          unmergedChunkStartTimes, mergeLogger, resource, analyzer.getUnmergedFiles());
      mergeFileTask.mergeFiles();
    } else {
      // NOTICE: although some of the seqFiles may have been truncated, later TsFile recovery
      // will recover them, so they are not a concern here
      truncateFiles();
    }
    cleanUp(continueMerge);
  }

  private void resumeMerge() throws IOException {
    mergeLogger = new MergeLogger(storageGroupDir);
    truncateFiles();
    recoverChunkCounts();
  }


  // scan metadata to compute how many chunks are merged/unmerged so at last we can decide to
  // move the merged chunks or the unmerged chunks
  private void recoverChunkCounts() throws IOException {
    logger.debug("{} recovering chunk counts", taskName);
    for (TsFileResource tsFileResource : resource.getSeqFiles()) {
      RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(tsFileResource);
      mergeFileWriter.makeMetadataVisible();
      unmergedChunkStartTimes.put(tsFileResource, new HashMap<>());
      for(Path path : analyzer.getMergedPaths()) {
        recoverChunkCounts(path, tsFileResource, mergeFileWriter);
      }
    }
  }

  private void recoverChunkCounts(Path path, TsFileResource tsFileResource,
      RestorableTsFileIOWriter mergeFileWriter) throws IOException {
    unmergedChunkStartTimes.get(tsFileResource).put(path, new ArrayList<>());

    List<ChunkMetaData> seqFileChunks = resource.queryChunkMetadata(path, tsFileResource);
    List<ChunkMetaData> mergeFileChunks =
        mergeFileWriter.getVisibleMetadataList(path.getDevice(), path.getMeasurement(), null);
    mergedChunkCnt.compute(tsFileResource, (k, v) -> v == null ? mergeFileChunks.size() :
        v + mergeFileChunks.size());
    int seqIndex = 0;
    int mergeIndex = 0;
    int unmergedCnt = 0;
    while (seqIndex < seqFileChunks.size() && mergeIndex < mergeFileChunks.size()) {
      ChunkMetaData seqChunk = seqFileChunks.get(seqIndex);
      ChunkMetaData mergedChunk = mergeFileChunks.get(mergeIndex);
      if (seqChunk.getStartTime() < mergedChunk.getStartTime()) {
        // this seqChunk is unmerged
        unmergedCnt ++;
        seqIndex ++;
        unmergedChunkStartTimes.get(tsFileResource).get(path).add(seqChunk.getStartTime());
      } else if (mergedChunk.getStartTime() <= seqChunk.getStartTime() &&
          seqChunk.getStartTime() <= mergedChunk.getEndTime()) {
        // this seqChunk is merged
        seqIndex ++;
      } else {
        // seqChunk.startTime > mergeChunk.endTime, find next mergedChunk that may cover the
        // seqChunk
        mergeIndex ++;
      }
    }
    int finalUnmergedCnt = unmergedCnt;
    unmergedChunkCnt.compute(tsFileResource, (k, v) -> v == null ? finalUnmergedCnt :
        v + finalUnmergedCnt);
  }

  private void truncateFiles() throws IOException {
    logger.debug("{} truncating {} files", taskName, analyzer.getFileLastPositions().size());
    for (Entry<File, Long> entry : analyzer.getFileLastPositions().entrySet()) {
      File file = entry.getKey();
      Long lastPosition = entry.getValue();
      if (file.exists() && file.length() != lastPosition) {
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
          FileChannel channel = fileInputStream.getChannel();
          channel.truncate(lastPosition);
          channel.close();
        }
      }
    }
  }




}
