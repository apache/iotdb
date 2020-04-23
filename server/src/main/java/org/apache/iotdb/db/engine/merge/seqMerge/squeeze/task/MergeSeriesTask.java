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

package org.apache.iotdb.db.engine.merge.seqMerge.squeeze.task;

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.BaseMergeSeriesTask;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.recover.SqueezeMergeLogger;
import org.apache.iotdb.db.engine.merge.sizeMerge.SizeMergeFileStrategy;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeSeriesTask extends BaseMergeSeriesTask {

  private static final Logger logger = LoggerFactory.getLogger(
      MergeSeriesTask.class);

  private int mergedSeriesCnt;
  private double progress;

  private List<Path> currMergingPaths = new ArrayList<>();

  private SizeMergeFileStrategy sizeMergeFileStrategy;
  private List<Pair<RestorableTsFileIOWriter, TsFileResource>> newTsFilePairs;
  private RestorableTsFileIOWriter currFileWriter;
  private TsFileResource currTsFile;
  private List<TsFileResource> currUnseqFiles;
  private long currMinTime = Long.MAX_VALUE;
  private int writerIdx = 0;

  MergeSeriesTask(MergeContext context, String taskName, SqueezeMergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries) {
    super(context, taskName, mergeLogger, mergeResource, unmergedSeries);
    this.sizeMergeFileStrategy = IoTDBDescriptor.getInstance().getConfig()
        .getSizeMergeFileStrategy();
    this.newTsFilePairs = new ArrayList<>();
  }

  List<TsFileResource> mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();

    // merge each series and write data into each seqFile's corresponding temp merge file
    List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<Path> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      currMergingPaths = pathList;
      mergePaths();
      mergedSeriesCnt += currMergingPaths.size();
      logMergeProgress();
    }

    List<TsFileResource> newResources = new ArrayList<>();
    for (Pair<RestorableTsFileIOWriter, TsFileResource> tsFilePair : newTsFilePairs) {
      newResources.add(tsFilePair.right);
      tsFilePair.left.endFile();
    }
    // the new file is ready to replace the old ones, write logs so we will not need to start from
    // the beginning after system failure
    mergeLogger.logAllTsEnd();

    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }

    return newResources;
  }

  private void logMergeProgress() {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * mergedSeriesCnt / (double) (unmergedSeries.size());
      if (newProgress - progress >= 1.0) {
        progress = newProgress;
        logger.info("{} has merged {}% series", taskName, progress);
      }
    }
  }

  private void mergePaths() throws IOException {
    Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = getNewFileWriter(
        sizeMergeFileStrategy, newTsFilePairs,
        writerIdx);
    currFileWriter = newTsFilePair.left;
    currTsFile = newTsFilePair.right;
    currMinTime = Long.MAX_VALUE;
    String deviceId = currMergingPaths.get(0).getDevice();
    currUnseqFiles = new ArrayList<>();
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      if (unseqFile.getStartTimeMap().containsKey(deviceId)) {
        currUnseqFiles.add(unseqFile);
      }
    }
    currFileWriter.startChunkGroup(deviceId);
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      if (!seqFile.getStartTimeMap().containsKey(deviceId)) {
        continue;
      }
      // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
      long maxTime = mergeChunks(seqFile);
      if (maxTime - currMinTime > timeBlock) {
        currTsFile.getStartTimeMap().put(deviceId, currMinTime);
        currTsFile.getEndTimeMap().put(deviceId, maxTime);
        resource.flushChunks(currFileWriter);
        currFileWriter.endChunkGroup();

        writerIdx++;
        newTsFilePair = getNewFileWriter(
            sizeMergeFileStrategy, newTsFilePairs,
            writerIdx);
        currFileWriter = newTsFilePair.left;
        currTsFile = newTsFilePair.right;
        currMinTime = Long.MAX_VALUE;
      }
    }
    resource.flushChunks(currFileWriter);
    currFileWriter.endChunkGroup();
  }

  private long mergeChunks(TsFileResource seqFile) throws IOException {
    int mergeChunkSubTaskNum = IoTDBDescriptor.getInstance().getConfig()
        .getMergeChunkSubThreadNum();
    PriorityQueue<Path>[] seriesHeaps = new PriorityQueue[mergeChunkSubTaskNum];
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      seriesHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    for (Path currMergingPath : currMergingPaths) {
      seriesHeaps[idx % mergeChunkSubTaskNum].add(currMergingPath);
      idx++;
    }

    List<Future<Long>> futures = new ArrayList<>();
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      int finalI = i;
      futures.add(MergeManager.getINSTANCE()
          .submitChunkSubTask(() -> mergeSubChunks(seriesHeaps[finalI], seqFile)));
    }
    long maxTime = Long.MIN_VALUE;
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      try {
        Long heapMaxTimeStamp = futures.get(i).get();
        maxTime = Math.max(maxTime, heapMaxTimeStamp);
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
    return maxTime;
  }

  private long mergeSubChunks(PriorityQueue<Path> seriesHeaps, TsFileResource seqFile)
      throws IOException {
    long maxTime = Long.MIN_VALUE;
    while (!seriesHeaps.isEmpty()) {
      Path series = seriesHeaps.poll();
      IChunkWriter chunkWriter = resource.getChunkWriter(series);
      QueryContext context = new QueryContext();
      MeasurementSchema schema = chunkWriter.getMeasurementSchema();
      currFileWriter.addSchema(series, schema);
      // start merging a device
      List<TsFileResource> seqFileList = new ArrayList<>();
      seqFileList.add(seqFile);
      IBatchReader tsFilesReader = new SeriesRawDataBatchReader(series, schema.getType(),
          context, seqFileList, currUnseqFiles, null, null);
      while (tsFilesReader.hasNextBatch()) {
        BatchData batchData = tsFilesReader.nextBatch();
        currMinTime = Math.min(currMinTime, batchData.getTimeByIndex(0));
        for (int i = 0; i < batchData.length(); i++) {
          writeBatchPoint(batchData, i, chunkWriter);
        }
        if (!tsFilesReader.hasNextBatch()) {
          maxTime = Math.max(batchData.getTimeByIndex(batchData.length() - 1), maxTime);
        }
      }
      synchronized (currFileWriter) {
        mergeContext.incTotalPointWritten(chunkWriter.getPtNum());
      }
    }
    return maxTime;
  }
}
