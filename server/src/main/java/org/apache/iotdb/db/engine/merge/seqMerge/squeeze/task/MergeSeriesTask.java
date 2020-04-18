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

import static org.apache.iotdb.db.engine.merge.seqMerge.squeeze.task.SqueezeMergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.seqMerge.squeeze.recover.SqueezeMergeLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeSeriesTask {

  private static final Logger logger = LoggerFactory.getLogger(
      MergeSeriesTask.class);
  private static int minChunkPointNum = IoTDBDescriptor.getInstance().getConfig()
      .getChunkMergePointThreshold();

  private SqueezeMergeLogger mergeLogger;
  private List<Path> unmergedSeries;

  private String taskName;
  private MergeResource resource;

  private MergeContext mergeContext;

  private int mergedSeriesCnt;
  private double progress;

  private List<Path> currMergingPaths = new ArrayList<>();

  private RestorableTsFileIOWriter newFileWriter;
  private TsFileResource newResource;

  MergeSeriesTask(MergeContext context, String taskName, SqueezeMergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.unmergedSeries = unmergedSeries;
  }

  TsFileResource mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();

    createNewFileWriter();
    // merge each series and write data into each seqFile's corresponding temp merge file
    List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<Path> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      currMergingPaths = pathList;
      mergePaths();
      mergedSeriesCnt += currMergingPaths.size();
      logMergeProgress();
    }

//    newFileWriter.addSchema(new Schema(newFileWriter.getKnownSchema())));
    newFileWriter.endFile();
    // the new file is ready to replace the old ones, write logs so we will not need to start from
    // the beginning after system failure
    mergeLogger.logAllTsEnd();
    mergeLogger.logNewFile(newResource);

    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }

    return newResource;
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
    String deviceId = currMergingPaths.get(0).getDevice();
    newFileWriter.startChunkGroup(deviceId);
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    long maxTime = mergeChunks();
    newResource.updateEndTime(deviceId, maxTime);
    resource.flushChunks(newFileWriter);
    newFileWriter.endChunkGroup();
  }

  private void createNewFileWriter() throws IOException {
    // use the minimum version as the version of the new file
    long currFileVersion =
        Long.parseLong(
            resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "")
                .split(TSFILE_SEPARATOR)[1]);
    long prevMergeNum =
        Long.parseLong(
            resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "")
                .split(TSFILE_SEPARATOR)[2]);
    File parent = resource.getSeqFiles().get(0).getFile().getParentFile();
    File newFile = FSFactoryProducer.getFSFactory().getFile(parent,
        System.currentTimeMillis() + TSFILE_SEPARATOR + currFileVersion + TSFILE_SEPARATOR + (
            prevMergeNum + 1) + TSFILE_SUFFIX + MERGE_SUFFIX);
    newFileWriter = new RestorableTsFileIOWriter(newFile);
    newResource = new TsFileResource(newFile);
  }

  private long mergeChunks() throws IOException {
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
          .submitChunkSubTask(() -> mergeSubChunks(seriesHeaps[finalI])));
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

  private long mergeSubChunks(PriorityQueue<Path> seriesHeaps)
      throws IOException {
    long maxTime = Long.MIN_VALUE;
    while (!seriesHeaps.isEmpty()) {
      Path series = seriesHeaps.poll();
      IChunkWriter chunkWriter = resource.getChunkWriter(series);
      QueryContext context = new QueryContext();
      MeasurementSchema schema = chunkWriter.getMeasurementSchema();
      newFileWriter.addSchema(series, schema);
      // start merging a device
      IBatchReader tsFilesReader = new SeriesRawDataBatchReader(series, schema.getType(),
          context, resource.getSeqFiles(), resource.getUnseqFiles(), null, null);
      while (tsFilesReader.hasNextBatch()) {
        BatchData batchData = tsFilesReader.nextBatch();
        for (int i = 0; i < batchData.length(); i++) {
          writeBatchPoint(batchData, i, chunkWriter);
          batchData.next();
          System.out.println(batchData.currentTime());
        }
        if (!tsFilesReader.hasNextBatch()) {
          maxTime = Math.max(batchData.currentTime(), maxTime);
        }
      }
      synchronized (newFileWriter) {
        mergeContext.incTotalPointWritten(chunkWriter.getPtNum());
        chunkWriter.writeToFileWriter(newFileWriter);
      }
    }
    return maxTime;
  }
}
