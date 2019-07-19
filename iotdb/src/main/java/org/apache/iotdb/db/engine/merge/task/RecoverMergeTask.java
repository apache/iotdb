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

/**
 * RecoverMergeTask is an extension of MergeTask, which resumes the last merge progress by
 * scanning merge.log using LogAnalyzer and continue the unfinished merge.
 */
public class RecoverMergeTask extends MergeTask {

  private static final Logger logger = LoggerFactory.getLogger(RecoverMergeTask.class);

  private LogAnalyzer analyzer;

  public RecoverMergeTask(List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles, String storageGroupDir,
      MergeCallback callback, String taskName,
      boolean fullMerge) throws IOException {
    super(seqFiles, unseqFiles, storageGroupDir, callback, taskName, fullMerge);
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
      resumeMergeProgress();
      MergeChunkTask mergeChunkTask = new MergeChunkTask(mergedChunkCnt, unmergedChunkCnt,
          unmergedChunkStartTimes, taskName, mergeLogger, resource, fullMerge, analyzer.getUnmergedPaths());
      mergeChunkTask.mergeSeries();

      MergeFileTask mergeFileTask = new MergeFileTask(taskName,mergedChunkCnt, unmergedChunkCnt,
          unmergedChunkStartTimes, mergeLogger, resource, resource.getSeqFiles());
      mergeFileTask.mergeFiles();
    }
    cleanUp(continueMerge);
  }

  private void resumeAfterAllTsMerged(boolean continueMerge) throws IOException {
    if (continueMerge) {
      resumeMergeProgress();
      MergeFileTask mergeFileTask = new MergeFileTask(taskName,mergedChunkCnt, unmergedChunkCnt,
          unmergedChunkStartTimes, mergeLogger, resource, analyzer.getUnmergedFiles());
      mergeFileTask.mergeFiles();
    } else {
      // NOTICE: although some of the seqFiles may have been truncated in last merge, we do not
      // recover them here because later TsFile recovery will recover them
      truncateFiles();
    }
    cleanUp(continueMerge);
  }

  private void resumeMergeProgress() throws IOException {
    mergeLogger = new MergeLogger(storageGroupDir);
    truncateFiles();
    recoverChunkCounts();
  }

  // scan the metadata to compute how many chunks are merged/unmerged so at last we can decide to
  // move the merged chunks or the unmerged chunks
  private void recoverChunkCounts() throws IOException {
    logger.info("{} recovering chunk counts", taskName);
    for (TsFileResource tsFileResource : resource.getSeqFiles()) {
      logger.info("{} recovering {}", taskName, tsFileResource.getFile().getName());
      RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(tsFileResource);
      mergeFileWriter.makeMetadataVisible();
      unmergedChunkStartTimes.put(tsFileResource, new HashMap<>());
      List<Path> pathsToRecover = analyzer.getMergedPaths();
      int cnt = 0;
      double progress = 0.0;
      for(Path path : pathsToRecover) {
        recoverChunkCounts(path, tsFileResource, mergeFileWriter);
        if (logger.isInfoEnabled()) {
          cnt += 1.0;
          double newProgress = 100.0 * cnt / pathsToRecover.size();
          if (newProgress - progress >= 1.0) {
            progress = newProgress;
            logger.info("{} {}% series count of {} are recovered", taskName, progress,
                tsFileResource.getFile().getName());
          }
        }
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
    int seqChunkIndex = 0;
    int mergeChunkIndex = 0;
    int unmergedCnt = 0;
    while (seqChunkIndex < seqFileChunks.size() && mergeChunkIndex < mergeFileChunks.size()) {
      ChunkMetaData seqChunk = seqFileChunks.get(seqChunkIndex);
      ChunkMetaData mergedChunk = mergeFileChunks.get(mergeChunkIndex);
      if (seqChunk.getStartTime() < mergedChunk.getStartTime()) {
        // this seqChunk is unmerged
        unmergedCnt ++;
        seqChunkIndex ++;
        unmergedChunkStartTimes.get(tsFileResource).get(path).add(seqChunk.getStartTime());
      } else if (mergedChunk.getStartTime() <= seqChunk.getStartTime() &&
          seqChunk.getStartTime() <= mergedChunk.getEndTime()) {
        // this seqChunk is merged
        seqChunkIndex ++;
      } else {
        // seqChunk.startTime > mergeChunk.endTime, find next mergedChunk that may cover the
        // seqChunk
        mergeChunkIndex ++;
      }
    }
    int finalUnmergedCnt = unmergedCnt;
    unmergedChunkCnt.compute(tsFileResource, (k, v) -> v == null ? finalUnmergedCnt :
        v + finalUnmergedCnt);
  }

  private void truncateFiles() throws IOException {
    logger.info("{} truncating {} files", taskName, analyzer.getFileLastPositions().size());
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
