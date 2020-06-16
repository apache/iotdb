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
package org.apache.iotdb.db.engine.merge;

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.MergeSizeSelectorStrategy;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMergeSeriesTask {

  private static final Logger logger = LoggerFactory.getLogger(
      BaseMergeSeriesTask.class);
  protected MergeLogger mergeLogger;

  protected String taskName;
  protected MergeResource resource;
  protected MergeContext mergeContext;
  private long timeBlock;
  protected List<Path> unmergedSeries;
  private MergeSizeSelectorStrategy mergeSizeSelectorStrategy;
  private int minChunkPointNum;
  protected int mergedSeriesCnt;
  private double progress;

  protected RestorableTsFileIOWriter currentFileWriter;
  protected TsFileResource currentMergeResource;

  protected BaseMergeSeriesTask(MergeContext context, String taskName, MergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.timeBlock = IoTDBDescriptor.getInstance().getConfig().getMergeFileTimeBlock();
    this.unmergedSeries = unmergedSeries;
    this.mergeSizeSelectorStrategy = IoTDBDescriptor.getInstance().getConfig()
        .getMergeSizeSelectorStrategy();
    this.minChunkPointNum = IoTDBDescriptor.getInstance().getConfig()
        .getChunkMergePointThreshold();
  }

  protected Pair<RestorableTsFileIOWriter, TsFileResource> createNewFileWriter(
      PriorityQueue<File> mergeFileNameHeap, String mergeSuffix) throws IOException {
    // use the minimum version as the version of the new file
    File newFile = createNewFile(mergeFileNameHeap, mergeSuffix);
    Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = new Pair<>(
        new RestorableTsFileIOWriter(newFile), new TsFileResource(newFile));
    mergeLogger.logNewFile(newTsFilePair.right);
    return newTsFilePair;
  }

  private File createNewFile(PriorityQueue<File> mergeFileNameHeap, String mergeSuffix) {
    File originFile = mergeFileNameHeap.poll();
    String[] splits = originFile.getName().replace(TSFILE_SUFFIX, "")
        .split(IoTDBConstant.TSFILE_NAME_SEPARATOR);
    int mergeVersion = Integer.parseInt(splits[2]) + 1;
    return FSFactoryProducer.getFSFactory().getFile(originFile.getParentFile(),
        splits[0] + IoTDBConstant.TSFILE_NAME_SEPARATOR + splits[1] +
            IoTDBConstant.TSFILE_NAME_SEPARATOR + mergeVersion + TSFILE_SUFFIX + mergeSuffix);
  }

  protected void logMergeProgress() {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * mergedSeriesCnt / (double) (unmergedSeries.size());
      if (newProgress - progress >= 1.0) {
        progress = newProgress;
        logger.info("{} has merged {}% series", taskName, progress);
      }
    }
  }

  protected void mergePaths(List<Path> pathList) throws IOException {
    List<TsFileResource> currSeqFiles = new ArrayList<>();
    List<TsFileResource> currUnseqFiles = new ArrayList<>();
    String deviceId = pathList.get(0).getDevice();
    for (TsFileResource unseqFile : resource.getUnseqFiles()) {
      if (unseqFile.getDeviceToIndexMap().containsKey(deviceId)) {
        currUnseqFiles.add(unseqFile);
      }
    }
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      if (seqFile.getDeviceToIndexMap().containsKey(deviceId)) {
        currSeqFiles.add(seqFile);
      }
    }
    currentFileWriter.startChunkGroup(deviceId);
    int mergeChunkSubTaskNum = IoTDBDescriptor.getInstance().getConfig()
        .getMergeChunkSubThreadNum();
    PriorityQueue<Path>[] seriesHeaps = new PriorityQueue[mergeChunkSubTaskNum];
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      seriesHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    for (Path currMergingPath : pathList) {
      seriesHeaps[idx % mergeChunkSubTaskNum].add(currMergingPath);
      idx++;
    }

    List<Future<Long>> futures = new ArrayList<>();
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      int finalI = i;
      futures.add(MergeManager.getINSTANCE()
          .submitChunkSubTask(
              () -> mergeSubChunks(seriesHeaps[finalI], currSeqFiles, currUnseqFiles)));
    }
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      try {
        futures.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }
    currentFileWriter.writeVersion(0L);
    currentFileWriter.endChunkGroup();
  }

  private long mergeSubChunks(PriorityQueue<Path> seriesHeaps, List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles)
      throws IOException {
    while (!seriesHeaps.isEmpty()) {
      Path path = seriesHeaps.poll();
      long currMinTime = Long.MAX_VALUE;
      long currMaxTime = Long.MIN_VALUE;
      IChunkWriter chunkWriter = resource.getChunkWriter(path);
      currentFileWriter.addSchema(path, chunkWriter.getMeasurementSchema());
      QueryContext context = new QueryContext();
      IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path,
          chunkWriter.getMeasurementSchema().getType(),
          context, seqFiles, unseqFiles, null, null);
      while (tsFilesReader.hasNextBatch()) {
        BatchData batchData = tsFilesReader.nextBatch();
        currMinTime = Math.min(currMinTime, batchData.getTimeByIndex(0));
        for (int i = 0; i < batchData.length(); i++) {
          writeBatchPoint(batchData, i, chunkWriter);
        }
        if (!tsFilesReader.hasNextBatch()) {
          currMaxTime = Math.max(batchData.getTimeByIndex(batchData.length() - 1), currMaxTime);
        }
        if (mergeSizeSelectorStrategy
            .isChunkEnoughLarge(chunkWriter, minChunkPointNum, currMinTime, currMaxTime,
                timeBlock)) {
          mergeContext.incTotalPointWritten(chunkWriter.getPtNum());
          synchronized (currentFileWriter) {
            chunkWriter.writeToFileWriter(currentFileWriter);
          }
        }
      }
      synchronized (currentFileWriter) {
        chunkWriter.writeToFileWriter(currentFileWriter);
      }
      currentMergeResource.updateStartTime(path.getDevice(), currMinTime);
      currentMergeResource.updateEndTime(path.getDevice(), currMaxTime);
      mergeContext.incTotalPointWritten(chunkWriter.getPtNum());
      tsFilesReader.close();
    }
    return 0;
  }
}
