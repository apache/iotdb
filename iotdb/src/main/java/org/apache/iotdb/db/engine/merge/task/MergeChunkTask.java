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
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.MergeUtils;
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

/**
 * MergeChunkTask rewrites chunks that satisfy that following conditions into the merge temp file:
 *  1. the number of points in this chunk (size of chunk) < minChunkPointNum
 *  2. the previous chunks are rewritten but their total size is still less than minChunkPointNum
 *  3. the chunk is overflowed (contains the duplicated data with some unseq chunks)
 *  4, the chunk is updated/deleted
 */
class MergeChunkTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeChunkTask.class);
  private static int minChunkPointNum = IoTDBDescriptor.getInstance().getConfig().getChunkMergePointThreshold();

  private Map<TsFileResource, Integer> mergedChunkCnt;
  private Map<TsFileResource, Integer> unmergedChunkCnt;
  private Map<TsFileResource, Map<Path, List<Long>>> unmergedChunkStartTimes;
  private MergeLogger mergeLogger;
  private List<Path> unmergedSeries;

  private String taskName;
  private MergeResource resource;
  private TimeValuePair currTimeValuePair;
  private long currDeviceMaxTime;
  private boolean fullMerge;

  int totalChunkWritten;
  private int mergedChunkNum = 0;
  private int unmergedChunkNum = 0;

  MergeChunkTask(
      Map<TsFileResource, Integer> mergedChunkCnt,
      Map<TsFileResource, Integer> unmergedChunkCnt,
      Map<TsFileResource, Map<Path, List<Long>>> unmergedChunkStartTimes,
      String taskName, MergeLogger mergeLogger, MergeResource mergeResource, boolean fullMerge,
      List<Path> unmergedSeries) {
    this.mergedChunkCnt = mergedChunkCnt;
    this.unmergedChunkCnt = unmergedChunkCnt;
    this.unmergedChunkStartTimes = unmergedChunkStartTimes;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.fullMerge = fullMerge;
    this.unmergedSeries = unmergedSeries;
  }

  void mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      unmergedChunkStartTimes.put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's corresponding temp merge file
    int mergedCnt = 0;
    double progress = 0.0;
    for (Path path : unmergedSeries) {
      mergeLogger.logTSStart(path);
      mergeOnePath(path);
      mergeLogger.logTSEnd(path);
      mergedCnt ++;
      if (logger.isInfoEnabled()) {
        double newProgress = 100 * mergedCnt / (double) (unmergedSeries.size());
        if (newProgress - progress >= 1.0) {
          progress = newProgress;
          logger.info("{} has merged {}% series", taskName, progress);
        }
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName, System.currentTimeMillis() - startTime);
    }
    mergeLogger.logAllTsEnd();
  }

  private void mergeOnePath(Path path) throws IOException {
    IPointReader unseqReader = resource.getUnseqReader(path);
    try {
      if (unseqReader.hasNext()) {
        currTimeValuePair = unseqReader.next();
      }
      for (int i = 0; i < resource.getSeqFiles().size(); i++) {
        pathMergeOneFile(path, i, unseqReader);
      }
    } catch (IOException e) {
      logger.error("Cannot read unseq data of {} during merge", path, e);
    } finally {
      try {
        unseqReader.close();
      } catch (IOException e) {
        logger.error("Cannot close unseqReader when merging path {}", path, e);
      }
    }
  }

  private void pathMergeOneFile(Path path, int seqFileIdx, IPointReader unseqReader)
      throws IOException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    unmergedChunkStartTimes.get(currTsFile).put(path, new ArrayList<>());

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    String deviceId = path.getDevice();
    long fileLimitTime = Long.MAX_VALUE;
    for (int i = seqFileIdx + 1; i < resource.getSeqFiles().size(); i++) {
      Long nextStartTime = resource.getSeqFiles().get(i).getStartTimeMap().get(deviceId);
      if (nextStartTime != null) {
        fileLimitTime = nextStartTime;
        break;
      }
    }

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification> modifications = resource.getModifications(currTsFile, path);
    List<ChunkMetaData> seqChunkMeta = resource.queryChunkMetadata(path, currTsFile);
    // if the last seqFile does not contains this series but the unseqFiles do, data of this
    // series should also be written into a new chunk
    if (seqChunkMeta.isEmpty()
        && !(seqFileIdx + 1 == resource.getSeqFiles().size() && currTimeValuePair != null)) {
      return;
    }
    modifyChunkMetaData(seqChunkMeta, modifications);
    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile);

    currDeviceMaxTime = currTsFile.getEndTimeMap().get(path.getDevice());
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(deviceId);
    if (mergeChunks(seqChunkMeta, fileLimitTime, fileSequenceReader, unseqReader, mergeFileWriter,
        currTsFile, path)) {
      long version = !seqChunkMeta.isEmpty() ?
          seqChunkMeta.get(seqChunkMeta.size() - 1).getVersion() + 1 : 0;
      mergeFileWriter.endChunkGroup(version);
      mergeLogger.logFilePositionUpdate(mergeFileWriter.getFile());
    }
    currTsFile.updateTime(path.getDevice(), currDeviceMaxTime);
  }

  private boolean mergeChunks(List<ChunkMetaData> seqChunkMeta, long fileLimitTime,
      TsFileSequenceReader reader, IPointReader unseqReader, RestorableTsFileIOWriter mergeFileWriter,
      TsFileResource currFile, Path path)
      throws IOException {
    int ptWritten = 0;
    MeasurementSchema measurementSchema = resource.getSchema(path.getMeasurement());
    IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);
    mergedChunkNum = 0;
    unmergedChunkNum = 0;
    for (int i = 0; i < seqChunkMeta.size(); i++) {
      ChunkMetaData currMeta = seqChunkMeta.get(i);
      boolean isLastChunk = i + 1 == seqChunkMeta.size();
      long chunkLimitTime = isLastChunk ? fileLimitTime : seqChunkMeta.get(i + 1).getStartTime();
      // the unseq data is not over and this chunk's time range covers the current overflow point
      boolean chunkOverflowed =
          currTimeValuePair != null && currTimeValuePair.getTimestamp() < chunkLimitTime;
      // a small chunk has been written, this chunk should be merged with it to create a larger chunk
      // or this chunk is too small and it is not the last chunk, merge it with the next chunks
      boolean chunkTooSmall =
          ptWritten > 0 || (minChunkPointNum >= 0 && currMeta.getNumOfPoints() < minChunkPointNum && !isLastChunk);
      Chunk chunk = reader.readMemChunk(currMeta);
      ptWritten = mergeChunk(currMeta, chunkOverflowed, chunkTooSmall, chunkLimitTime, chunk,
          ptWritten, path, mergeFileWriter, unseqReader, chunkWriter, currFile);
    }
    // this only happens when the last seqFile does not contain this series, otherwise the remaining
    // parts will be merged with the last chunk in the seqFile
    if (fileLimitTime == Long.MAX_VALUE && currTimeValuePair != null) {
      ptWritten += writeRemainingUnseq(chunkWriter,
          unseqReader, fileLimitTime);
      mergedChunkNum ++;
    }

    // the last merged chunk may still be smaller than the threshold, flush it anyway
    if (ptWritten > 0) {
      chunkWriter.writeToFileWriter(mergeFileWriter);
    }
    updateChunkCounts(currFile, mergedChunkNum, unmergedChunkNum);

    return mergedChunkNum > 0;
  }

  private int mergeChunk(ChunkMetaData currMeta, boolean chunkOverflowed,
      boolean chunkTooSmall, long chunkLimitTime, Chunk chunk, int ptWritten, Path path,
      TsFileIOWriter mergeFileWriter, IPointReader unseqReader,
      IChunkWriter chunkWriter, TsFileResource currFile) throws IOException {

    int newPtWritten = ptWritten;
    if (ptWritten == 0 && fullMerge && !chunkOverflowed && !chunkTooSmall) {
      // write without unpacking the chunk
      mergeFileWriter.writeChunk(chunk, currMeta);
      mergedChunkNum ++;
    } else {
      // the chunk should be unpacked to merge with other chunks
      int chunkPtNum = writeChunk(chunkLimitTime, chunk, currMeta, chunkWriter,
          unseqReader, chunkOverflowed, chunkTooSmall);
      if (chunkPtNum > 0) {
        mergedChunkNum ++;
        newPtWritten += chunkPtNum;
      } else {
        unmergedChunkNum ++;
        unmergedChunkStartTimes.get(currFile).get(path).add(currMeta.getStartTime());
      }
    }

    if (minChunkPointNum > 0 && newPtWritten >= minChunkPointNum || newPtWritten > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      chunkWriter.writeToFileWriter(mergeFileWriter);
      newPtWritten = 0;
    }
    return newPtWritten;
  }

  private int writeRemainingUnseq(IChunkWriter chunkWriter,
      IPointReader unseqReader, long timeLimit) throws IOException {
    int ptWritten = 0;
    while (currTimeValuePair != null && currTimeValuePair.getTimestamp() < timeLimit) {
      writeTVPair(currTimeValuePair, chunkWriter);
      if (currTimeValuePair.getTimestamp() > currDeviceMaxTime) {
        currDeviceMaxTime = currTimeValuePair.getTimestamp();
      }
      ptWritten ++;
      currTimeValuePair = unseqReader.hasNext() ? unseqReader.next() : null;
    }
    return ptWritten;
  }

  private void updateChunkCounts(TsFileResource currFile, int newMergedChunkNum,
      int newUnmergedChunkNum) {
    mergedChunkCnt.compute(currFile, (tsFileResource, anInt) -> anInt == null ? newMergedChunkNum
        : anInt + newMergedChunkNum);
    unmergedChunkCnt.compute(currFile, (tsFileResource, anInt) -> anInt == null ? newUnmergedChunkNum
        : anInt + newUnmergedChunkNum);
  }

  private int writeChunk(long chunkLimitTime, Chunk chunk,
      ChunkMetaData currMeta, IChunkWriter chunkWriter, IPointReader unseqReader, boolean chunkOverflowed,
      boolean chunkTooSmall) throws IOException {

    boolean chunkModified = currMeta.getDeletedAt() > Long.MIN_VALUE;
    int newPtWritten = 0;

    if (!chunkOverflowed && (chunkTooSmall || chunkModified || fullMerge)) {
      // just rewrite the (modified) chunk
      newPtWritten += MergeUtils.writeChunkWithoutUnseq(chunk, chunkWriter);
      totalChunkWritten ++;
    } else if (chunkOverflowed) {
      // this chunk may merge with the current point
      newPtWritten += writeChunkWithUnseq(chunk, chunkWriter, unseqReader, chunkLimitTime);
      totalChunkWritten ++;
    }
    return newPtWritten;
  }

  private int writeChunkWithUnseq(Chunk chunk, IChunkWriter chunkWriter, IPointReader unseqReader,
      long chunkLimitTime) throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);
    while (chunkReader.hasNextBatch()) {
      BatchData batchData = chunkReader.nextBatch();
      cnt += mergeWriteBatch(batchData, chunkWriter,unseqReader);
    }
    cnt += writeRemainingUnseq(chunkWriter, unseqReader, chunkLimitTime);
    return cnt;
  }

  private int mergeWriteBatch(BatchData batchData, IChunkWriter chunkWriter,
      IPointReader unseqReader) throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader
      while (currTimeValuePair != null && currTimeValuePair.getTimestamp() < time) {
        writeTVPair(currTimeValuePair, chunkWriter);
        currTimeValuePair = unseqReader.hasNext() ? unseqReader.next() : null;
        cnt++;
      }
      if (currTimeValuePair != null && currTimeValuePair.getTimestamp() == time) {
        writeTVPair(currTimeValuePair, chunkWriter);
        currTimeValuePair = unseqReader.hasNext() ? unseqReader.next() : null;
        cnt++;
      } else {
        writeBatchPoint(batchData, i, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }

}
