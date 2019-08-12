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

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.merge.selector.MergePathSelector;
import org.apache.iotdb.db.engine.merge.selector.NaivePathSelector;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MergeMultiChunkTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeMultiChunkTask.class);
  private static int minChunkPointNum = IoTDBDescriptor.getInstance().getConfig()
      .getChunkMergePointThreshold();

  private MergeLogger mergeLogger;
  private List<Path> unmergedSeries;

  private String taskName;
  private MergeResource resource;
  private TimeValuePair[] currTimeValuePairs;
  private boolean fullMerge;

  private MergeContext mergeContext;

  private AtomicInteger mergedChunkNum = new AtomicInteger();
  private AtomicInteger unmergedChunkNum = new AtomicInteger();
  private int mergedSeriesCnt;
  private double progress;

  private int concurrentMergeSeriesNum;
  private List<Path> currMergingPaths = new ArrayList<>();

  MergeMultiChunkTask(MergeContext context, String taskName, MergeLogger mergeLogger,
      MergeResource mergeResource, boolean fullMerge, List<Path> unmergedSeries,
      int concurrentMergeSeriesNum) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.fullMerge = fullMerge;
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
  }

  void mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      mergeContext.getUnmergedChunkStartTimes().put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's corresponding temp merge file
    List<List<Path>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<Path> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      MergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        currMergingPaths = pathSelector.next();
        mergePaths();
        mergedSeriesCnt += currMergingPaths.size();
        logMergeProgress();
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }
    mergeLogger.logAllTsEnd();
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
    mergeLogger.logTSStart(currMergingPaths);
    IPointReader[] unseqReaders;
    unseqReaders = resource.getUnseqReaders(currMergingPaths);
    currTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (unseqReaders[i].hasNext()) {
        currTimeValuePairs[i] = unseqReaders[i].current();
      }
    }

    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      pathsMergeOneFile(i, unseqReaders);
    }
    mergeLogger.logTSEnd();
  }

  private void pathsMergeOneFile(int seqFileIdx, IPointReader[] unseqReaders)
      throws IOException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    for (Path path : currMergingPaths) {
      mergeContext.getUnmergedChunkStartTimes().get(currTsFile).put(path, new ArrayList<>());
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    String deviceId = currMergingPaths.get(0).getDevice();
    long currDeviceMinTime = currTsFile.getStartTimeMap().get(deviceId);
    for (TimeValuePair timeValuePair : currTimeValuePairs) {
      if (timeValuePair != null && timeValuePair.getTimestamp() < currDeviceMinTime) {
        currDeviceMinTime = timeValuePair.getTimestamp();
      }
    }
    boolean isLastFile = seqFileIdx + 1 == resource.getSeqFiles().size();

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification>[] modifications = new List[currMergingPaths.size()];
    List<ChunkMetaData>[] seqChunkMeta = new List[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      modifications[i] = resource.getModifications(currTsFile, currMergingPaths.get(i));
      seqChunkMeta[i] = resource.queryChunkMetadata(currMergingPaths.get(i), currTsFile);
      modifyChunkMetaData(seqChunkMeta[i], modifications[i]);
    }

    List<Integer> unskippedPathIndices = filterNoDataPaths(seqChunkMeta, seqFileIdx);
    if (unskippedPathIndices.isEmpty()) {
      return;
    }

    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile);
    for (Path path : currMergingPaths) {
      MeasurementSchema schema = resource.getSchema(path.getMeasurement());
      mergeFileWriter.addSchema(schema);
    }
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(deviceId);
    boolean dataWritten = mergeChunks(seqChunkMeta, isLastFile, fileSequenceReader, unseqReaders,
        mergeFileWriter, currTsFile);
    if (dataWritten) {
      mergeFileWriter.endChunkGroup(0);
      mergeLogger.logFilePositionUpdate(mergeFileWriter.getFile());
      currTsFile.getStartTimeMap().put(deviceId, currDeviceMinTime);
    }
  }

  private List<Integer> filterNoDataPaths(List[] seqChunkMeta, int seqFileIdx) {
    // if the last seqFile does not contains this series but the unseqFiles do, data of this
    // series should also be written into a new chunk
    List<Integer> ret = new ArrayList<>();
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (seqChunkMeta[i].isEmpty()
          && !(seqFileIdx + 1 == resource.getSeqFiles().size() && currTimeValuePairs[i] != null)) {
        continue;
      }
      ret.add(i);
    }
    return ret;
  }

  private boolean mergeChunks(List<ChunkMetaData>[] seqChunkMeta, boolean isLastFile,
      TsFileSequenceReader reader, IPointReader[] unseqReaders,
      RestorableTsFileIOWriter mergeFileWriter, TsFileResource currFile)
      throws IOException {
    int[] ptWrittens = new int[seqChunkMeta.length];
    int mergeChunkSubTaskNum = IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
    PriorityQueue<MetaListEntry>[] chunkMetaHeaps = new PriorityQueue[mergeChunkSubTaskNum];
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      chunkMetaHeaps[i] = new PriorityQueue<>();
    }
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (seqChunkMeta[i].isEmpty()) {
        continue;
      }
      MetaListEntry entry = new MetaListEntry(i, seqChunkMeta[i]);
      entry.next();

      int index = entry.current().getMeasurementUid().hashCode() % mergeChunkSubTaskNum;
      index = index < 0 ? -index : index;
      chunkMetaHeaps[index].add(entry);
      ptWrittens[i] = 0;
    }

    mergedChunkNum.set(0);
    unmergedChunkNum.set(0);

    List<Future> futures = new ArrayList<>();
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      int finalI = i;
      futures.add(MergeManager.getINSTANCE().submitChunkSubTask(() -> {
        mergeChunkHeap(chunkMetaHeaps[finalI], ptWrittens, reader, mergeFileWriter, unseqReaders, currFile,
            isLastFile);
        return null;
      }));
    }
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      try {
        futures.get(i).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IOException(e);
      }
    }

    updateChunkCounts(currFile, mergedChunkNum, unmergedChunkNum);

    return mergedChunkNum.get() > 0;
  }

  private void mergeChunkHeap(PriorityQueue<MetaListEntry> chunkMetaHeap, int[] ptWrittens, TsFileSequenceReader reader,
                              RestorableTsFileIOWriter mergeFileWriter, IPointReader[] unseqReaders, TsFileResource currFile,
                              boolean isLastFile) throws IOException {
    while (!chunkMetaHeap.isEmpty()) {
      MetaListEntry metaListEntry = chunkMetaHeap.poll();
      ChunkMetaData currMeta = metaListEntry.current();
      int pathIdx = metaListEntry.getPathId();
      boolean isLastChunk = !metaListEntry.hasNext();
      Path path = currMergingPaths.get(pathIdx);
      MeasurementSchema measurementSchema = resource.getSchema(path.getMeasurement());
      IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);

      boolean chunkOverflowed = MergeUtils.isChunkOverflowed(currTimeValuePairs[pathIdx], currMeta);
      boolean chunkTooSmall = MergeUtils
              .isChunkTooSmall(ptWrittens[pathIdx], currMeta, isLastChunk, minChunkPointNum);

      Chunk chunk;
      synchronized (reader) {
        chunk = reader.readMemChunk(currMeta);
      }
      ptWrittens[pathIdx] = mergeChunk(currMeta, chunkOverflowed, chunkTooSmall, chunk,
              ptWrittens[pathIdx], pathIdx, mergeFileWriter, unseqReaders[pathIdx], chunkWriter,
              currFile);

      if (!isLastChunk) {
        metaListEntry.next();
        chunkMetaHeap.add(metaListEntry);
      } else {
        // this only happens when the seqFiles do not contain this series, otherwise the remaining
        // data will be merged with the last chunk in the seqFiles
        if (isLastFile && currTimeValuePairs[pathIdx] != null) {
          ptWrittens[pathIdx] += writeRemainingUnseq(chunkWriter, unseqReaders[pathIdx], Long.MAX_VALUE,
                  pathIdx);
          mergedChunkNum.incrementAndGet();
        }
        // the last merged chunk may still be smaller than the threshold, flush it anyway
        if (ptWrittens[pathIdx] > 0) {
          synchronized (mergeFileWriter) {
            chunkWriter.writeToFileWriter(mergeFileWriter);
          }
        }
      }
    }
  }

  private int mergeChunk(ChunkMetaData currMeta, boolean chunkOverflowed,
      boolean chunkTooSmall,Chunk chunk, int ptWritten, int pathIdx,
      TsFileIOWriter mergeFileWriter, IPointReader unseqReader,
      IChunkWriter chunkWriter, TsFileResource currFile) throws IOException {

    int newPtWritten = ptWritten;
    if (ptWritten == 0 && fullMerge && !chunkOverflowed && !chunkTooSmall) {
      // write without unpacking the chunk
      synchronized (mergeFileWriter) {
        mergeFileWriter.writeChunk(chunk, currMeta);
      }
      mergedChunkNum.incrementAndGet();
      mergeContext.incTotalPointWritten(currMeta.getNumOfPoints());
    } else {
      // the chunk should be unpacked to merge with other chunks
      int chunkPtNum = writeChunk(chunk, currMeta, chunkWriter,
          unseqReader, chunkOverflowed, chunkTooSmall, pathIdx);
      if (chunkPtNum > 0) {
        mergedChunkNum.incrementAndGet();
        newPtWritten += chunkPtNum;
      } else {
        unmergedChunkNum.incrementAndGet();
        mergeContext.getUnmergedChunkStartTimes().get(currFile).get(currMergingPaths.get(pathIdx))
            .add(currMeta.getStartTime());
      }
    }

    mergeContext.incTotalPointWritten(newPtWritten - ptWritten);
    if (minChunkPointNum > 0 && newPtWritten >= minChunkPointNum
        || newPtWritten > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      synchronized (mergeFileWriter) {
        chunkWriter.writeToFileWriter(mergeFileWriter);
      }
      newPtWritten = 0;
    }
    return newPtWritten;
  }

  private int writeRemainingUnseq(IChunkWriter chunkWriter,
      IPointReader unseqReader, long timeLimit, int pathIdx) throws IOException {
    int ptWritten = 0;
    while (currTimeValuePairs[pathIdx] != null
        && currTimeValuePairs[pathIdx].getTimestamp() < timeLimit) {
      writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
      ptWritten++;
      unseqReader.next();
      currTimeValuePairs[pathIdx] = unseqReader.hasNext() ? unseqReader.current() : null;
    }
    return ptWritten;
  }

  private void updateChunkCounts(TsFileResource currFile, AtomicInteger newMergedChunkNum,
      AtomicInteger newUnmergedChunkNum) {
    mergeContext.getMergedChunkCnt().compute(currFile, (tsFileResource, anInt) -> anInt == null ?
        newMergedChunkNum.get() : anInt + newMergedChunkNum.get());
    mergeContext.getUnmergedChunkCnt().compute(currFile, (tsFileResource, anInt) -> anInt == null ?
        newUnmergedChunkNum.get() : anInt + newUnmergedChunkNum.get());
  }

  private int writeChunk(Chunk chunk,
      ChunkMetaData currMeta, IChunkWriter chunkWriter, IPointReader unseqReader,
      boolean chunkOverflowed,
      boolean chunkTooSmall, int pathIdx) throws IOException {

    boolean chunkModified = currMeta.getDeletedAt() > Long.MIN_VALUE;
    int newPtWritten = 0;

    if (!chunkOverflowed && (chunkTooSmall || chunkModified || fullMerge)) {
      // just rewrite the (modified) chunk
      newPtWritten += MergeUtils.writeChunkWithoutUnseq(chunk, chunkWriter);
      mergeContext.incTotalChunkWritten();
    } else if (chunkOverflowed) {
      // this chunk may merge with the current point
      newPtWritten += writeChunkWithUnseq(chunk, chunkWriter, unseqReader, currMeta.getEndTime(), pathIdx);
      mergeContext.incTotalChunkWritten();
    }
    return newPtWritten;
  }

  private int writeChunkWithUnseq(Chunk chunk, IChunkWriter chunkWriter, IPointReader unseqReader,
      long chunkLimitTime, int pathIdx) throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);
    while (chunkReader.hasNextBatch()) {
      BatchData batchData = chunkReader.nextBatch();
      cnt += mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
    cnt += writeRemainingUnseq(chunkWriter, unseqReader, chunkLimitTime, pathIdx);
    return cnt;
  }

  private int mergeWriteBatch(BatchData batchData, IChunkWriter chunkWriter,
      IPointReader unseqReader, int pathIdx) throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader
      while (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].getTimestamp() < time) {
        writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
        unseqReader.next();
        currTimeValuePairs[pathIdx] = unseqReader.hasNext() ? unseqReader.current() : null;
        cnt++;
      }
      if (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].getTimestamp() == time) {
        writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
        unseqReader.next();
        currTimeValuePairs[pathIdx] = unseqReader.hasNext() ? unseqReader.current() : null;
        cnt++;
      } else {
        writeBatchPoint(batchData, i, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }
}
