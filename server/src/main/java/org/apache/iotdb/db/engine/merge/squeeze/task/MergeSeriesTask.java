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

package org.apache.iotdb.db.engine.merge.squeeze.task;

import static org.apache.iotdb.db.engine.merge.squeeze.task.SqueezeMergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;
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
import org.apache.iotdb.db.engine.merge.IMergePathSelector;
import org.apache.iotdb.db.engine.merge.NaivePathSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.squeeze.recover.SqueezeMergeLogger;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
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
  private TimeValuePair[] currTimeValuePairs;

  private MergeContext mergeContext;

  private int mergedSeriesCnt;
  private double progress;

  private int concurrentMergeSeriesNum;
  private List<Path> currMergingPaths = new ArrayList<>();

  private RestorableTsFileIOWriter newFileWriter;
  private TsFileResource newResource;
  private String currDevice = null;

  MergeSeriesTask(MergeContext context, String taskName, SqueezeMergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries,
      int concurrentMergeSeriesNum) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
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
      IMergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        currMergingPaths = pathSelector.next();
        mergePaths();
        mergedSeriesCnt += currMergingPaths.size();
        logMergeProgress();
      }
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
    IPointReader[] unseqReaders;
    unseqReaders = resource.getUnseqReaders(currMergingPaths);
    currTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (unseqReaders[i].hasNextTimeValuePair()) {
        currTimeValuePairs[i] = unseqReaders[i].currentTimeValuePair();
      }
    }

    // merge data of the current paths in each seq file
    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      pathsMergeOneFile(i, unseqReaders);
    }

    // merge the remaining data in unseq files
    // this should not change the device max time, because time range of unseq files is always
    // covered by that of seq files
    for (int i = 0; i < currMergingPaths.size(); i++) {
      Path path = currMergingPaths.get(i);
      IChunkWriter chunkWriter = resource.getChunkWriter(path);
      writeRemainingUnseq(chunkWriter, unseqReaders[i], i);
      chunkWriter.writeToFileWriter(newFileWriter);
    }
    newFileWriter.addVersionPair(new Pair<>(newFileWriter.getPos(), 0L));
    newFileWriter.endChunkGroup();
    currDevice = null;
  }

  private void writeRemainingUnseq(IChunkWriter chunkWriter,
      IPointReader unseqReader, int pathIdx) throws IOException {
    while (currTimeValuePairs[pathIdx] != null) {
      writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
      unseqReader.nextTimeValuePair();
      currTimeValuePairs[pathIdx] =
          unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
    }
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

  private void pathsMergeOneFile(int seqFileIdx, IPointReader[] unseqReaders)
      throws IOException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    String deviceId = currMergingPaths.get(0).getDevice();
    Long currDeviceMinTime = currTsFile.getStartTimeMap().get(deviceId);
    if (currDeviceMinTime == null) {
      return;
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    for (TimeValuePair timeValuePair : currTimeValuePairs) {
      if (timeValuePair != null && timeValuePair.getTimestamp() < currDeviceMinTime) {
        currDeviceMinTime = timeValuePair.getTimestamp();
      }
    }

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification> modifications;
    List<ChunkMetadata>[] seqChunkMeta = new List[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      modifications = resource.getModifications(currTsFile, currMergingPaths.get(i));
      seqChunkMeta[i] = resource.queryChunkMetadata(currMergingPaths.get(i), currTsFile);
      modifyChunkMetaData(seqChunkMeta[i], modifications);
    }

    if (allPathEmpty(seqChunkMeta)) {
      return;
    }

    for (Path path : currMergingPaths) {
      MeasurementSchema schema = resource.getChunkWriter(path).getMeasurementSchema();
      newFileWriter.addSchema(path, schema);
    }
    // start merging a device
    if (!deviceId.equals(currDevice)) {
      if (currDevice != null) {
        // flush previous device
        resource.flushChunks(newFileWriter);
        newFileWriter.addVersionPair(new Pair<>(newFileWriter.getPos(), 0L));
        newFileWriter.endChunkGroup();
      }
      newFileWriter.startChunkGroup(deviceId);
      currDevice = deviceId;
      newResource.getStartTimeMap().put(deviceId, currDeviceMinTime);
    }
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    long maxTime = mergeChunks(seqChunkMeta, fileSequenceReader, unseqReaders);
    newResource.updateEndTime(deviceId, maxTime);
  }

  private boolean allPathEmpty(List[] seqChunkMeta) {
    for (int i = 0; i < seqChunkMeta.length; i++) {
      if (!seqChunkMeta[i].isEmpty() || currTimeValuePairs[i] != null) {
        return false;
      }
    }
    return true;
  }

  private long mergeChunks(List<ChunkMetadata>[] seqChunkMeta,
      TsFileSequenceReader reader, IPointReader[] unseqReaders)
      throws IOException {
    int mergeChunkSubTaskNum = IoTDBDescriptor.getInstance().getConfig()
        .getMergeChunkSubThreadNum();
    PriorityQueue<MetaListEntry>[] chunkMetaHeaps = new PriorityQueue[mergeChunkSubTaskNum];
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      chunkMetaHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (seqChunkMeta[i].isEmpty()) {
        continue;
      }
      MetaListEntry entry = new MetaListEntry(i, seqChunkMeta[i]);
      entry.next();

      chunkMetaHeaps[idx % mergeChunkSubTaskNum].add(entry);
      idx++;
    }

    List<Future<Long>> futures = new ArrayList<>();
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      int finalI = i;
      futures.add(MergeManager.getINSTANCE()
          .submitChunkSubTask(() -> mergeChunkHeap(chunkMetaHeaps[finalI], reader, unseqReaders)));
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

  private long mergeChunkHeap(PriorityQueue<MetaListEntry> chunkMetaHeap,
      TsFileSequenceReader reader, IPointReader[] unseqReaders)
      throws IOException {
    long maxTime = Long.MIN_VALUE;
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      ChunkMetadata currMeta = metaListEntry.current();
      int pathIdx = metaListEntry.getPathId();
      boolean isLastChunk = !metaListEntry.hasNext();
      Path path = currMergingPaths.get(pathIdx);
      IChunkWriter chunkWriter = resource.getChunkWriter(path);

      boolean chunkOverflowed = MergeUtils
          .isChunkOverflowed(currTimeValuePairs[pathIdx], currMeta);
      boolean chunkTooSmall =
          chunkWriter.getPtNum() > 0 || currMeta.getNumOfPoints() < minChunkPointNum;

      Chunk chunk;
      synchronized (reader) {
        chunk = reader.readMemChunk(currMeta);
      }

      long maxMergedTime = mergeChunkV2(currMeta, chunkOverflowed, chunkTooSmall, chunk, pathIdx,
          unseqReaders[pathIdx], chunkWriter);
      maxTime = Math.max(maxMergedTime, maxTime);

      if (!isLastChunk) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      }
    }
    return maxTime;
  }

  private long mergeChunkV2(ChunkMetadata currMeta, boolean chunkOverflowed,
      boolean chunkTooSmall, Chunk chunk, int pathIdx,
      IPointReader unseqReader, IChunkWriter chunkWriter) throws IOException {

    // write the chunk to .merge.file without compressing
    if (chunkWriter.getPtNum() == 0 && !chunkTooSmall && !chunkOverflowed) {
      synchronized (newFileWriter) {
        newFileWriter.writeChunk(chunk, currMeta);
      }
      return currMeta.getEndTime();
    }

    // uncompress and write the chunk
    writeChunkWithUnseq(chunk, chunkWriter, unseqReader, pathIdx);

    // check chunk size for flush and update points written statistics
    if (minChunkPointNum > 0 && chunkWriter.getPtNum() >= minChunkPointNum
        || chunkWriter.getPtNum() > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      synchronized (newFileWriter) {
        mergeContext.incTotalPointWritten(chunkWriter.getPtNum());
        chunkWriter.writeToFileWriter(newFileWriter);
      }
    }
    return currMeta.getEndTime();
  }

  private void writeChunkWithUnseq(Chunk chunk, IChunkWriter chunkWriter, IPointReader unseqReader,
      int pathIdx) throws IOException {
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
  }

  private void mergeWriteBatch(BatchData batchData, IChunkWriter chunkWriter,
      IPointReader unseqReader, int pathIdx) throws IOException {
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader

      boolean overwriteSeqPoint = false;
      // unseq point.time <= sequence point.time, write unseq point
      while (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].getTimestamp() <= time) {
        writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
        if (currTimeValuePairs[pathIdx].getTimestamp() == time) {
          overwriteSeqPoint = true;
        }
        unseqReader.nextTimeValuePair();
        currTimeValuePairs[pathIdx] =
            unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
      }
      // unseq point.time > sequence point.time, write seq point
      if (!overwriteSeqPoint) {
        writeBatchPoint(batchData, i, chunkWriter);
      }
    }
  }
}
