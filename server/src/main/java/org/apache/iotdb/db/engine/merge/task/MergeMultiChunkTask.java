package org.apache.iotdb.db.engine.merge.task;

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
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

  private int mergedChunkNum = 0;
  private int unmergedChunkNum = 0;
  private int mergedSeriesCnt;
  private double progress;

  private int concurrentMergeSeriesNum;
  private List<Path> currMergingPaths = new ArrayList<>();
  private long currDeviceMinTime;

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

    String lastDevice = null;
    for (Path path : unmergedSeries) {
      // mergeLogger.logTSStart(path);
      boolean canMerge = false;
      if (lastDevice == null) {
        currMergingPaths.add(path);
        lastDevice = path.getDevice();
      } else if (lastDevice.equals(path.getDevice())) {
        currMergingPaths.add(path);
      } else {
        canMerge = true;
      }
      canMerge = canMerge || currMergingPaths.size() >= concurrentMergeSeriesNum;
      if (canMerge) {
        mergePaths();
        mergedSeriesCnt += currMergingPaths.size();
        currMergingPaths.clear();
        lastDevice = null;
        logMergeProgress();
      }
      // mergeLogger.logTSEnd(path);
    }
    if (!currMergingPaths.isEmpty()) {
      mergePaths();
      mergedSeriesCnt += currMergingPaths.size();
      logMergeProgress();
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
    List<IPointReader> unseqReaders = new ArrayList<>();
    for (Path path : currMergingPaths) {
      unseqReaders.add(resource.getUnseqReader(path));
    }
    currTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (unseqReaders.get(i).hasNext()) {
        currTimeValuePairs[i] = unseqReaders.get(i).next();
      }
    }

    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      pathsMergeOneFile(i, unseqReaders);
    }
    mergeLogger.logTSEnd();
  }

  private void pathsMergeOneFile(int seqFileIdx, List<IPointReader> unseqReader)
      throws IOException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    for (Path path : currMergingPaths) {
      mergeContext.getUnmergedChunkStartTimes().get(currTsFile).put(path, new ArrayList<>());
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    String deviceId = currMergingPaths.get(0).getDevice();
    currDeviceMinTime = currTsFile.getStartTimeMap().get(deviceId);
    boolean isLastFile = seqFileIdx + 1 == resource.getSeqFiles().size();

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification>[] modifications = new List[currMergingPaths.size()];
    List<ChunkMetaData>[] seqChunkMeta = new List[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      modifications[i] = resource.getModifications(currTsFile, currMergingPaths.get(i));
      seqChunkMeta[i] = resource.queryChunkMetadata(currMergingPaths.get(i), currTsFile);
      modifyChunkMetaData(seqChunkMeta[i], modifications[i]);
    }

    List<Integer> unskippedPathIndices = new ArrayList<>();
    // if the last seqFile does not contains this series but the unseqFiles do, data of this
    // series should also be written into a new chunk
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (seqChunkMeta[i].isEmpty()
          && !(seqFileIdx + 1 == resource.getSeqFiles().size() && currTimeValuePairs[i] != null)) {
        continue;
      }
      unskippedPathIndices.add(i);
    }
    if (unskippedPathIndices.isEmpty()) {
      return;
    }

    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile);
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(deviceId);
    boolean dataWritten = false;
    for (int pathIdx : unskippedPathIndices) {
      if (currTimeValuePairs[pathIdx] != null) {
        currDeviceMinTime = currDeviceMinTime > currTimeValuePairs[pathIdx].getTimestamp() ?
            currTimeValuePairs[pathIdx].getTimestamp() : currDeviceMinTime;
      }
      dataWritten = mergeChunks(seqChunkMeta[pathIdx], isLastFile,
          fileSequenceReader, unseqReader.get(pathIdx), mergeFileWriter, currTsFile, pathIdx)
          || dataWritten;

    }
    if (dataWritten) {
      mergeFileWriter.endChunkGroup(0);
      mergeLogger.logFilePositionUpdate(mergeFileWriter.getFile());
      currTsFile.getStartTimeMap().put(deviceId, currDeviceMinTime);
    }
  }

  private boolean mergeChunks(List<ChunkMetaData> seqChunkMeta, boolean isLastFile,
      TsFileSequenceReader reader, IPointReader unseqReader,
      RestorableTsFileIOWriter mergeFileWriter, TsFileResource currFile, int pathIdx)
      throws IOException {
    int ptWritten = 0;
    Path path = currMergingPaths.get(pathIdx);

    MeasurementSchema measurementSchema = resource.getSchema(path.getMeasurement());
    IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);
    mergedChunkNum = 0;
    unmergedChunkNum = 0;
    for (int i = 0; i < seqChunkMeta.size(); i++) {
      ChunkMetaData currMeta = seqChunkMeta.get(i);
      boolean isLastChunk = i + 1 == seqChunkMeta.size();
      // the unseq data is not over and this chunk's time range covers the current overflow point
      boolean chunkOverflowed =
          currTimeValuePairs[pathIdx] != null
              && currTimeValuePairs[pathIdx].getTimestamp() < currMeta.getEndTime();
      // a small chunk has been written, this chunk should be merged with it to create a larger chunk
      // or this chunk is too small and it is not the last chunk, merge it with the next chunks
      boolean chunkTooSmall =
          ptWritten > 0 || (minChunkPointNum >= 0 && currMeta.getNumOfPoints() < minChunkPointNum
              && !isLastChunk);
      Chunk chunk = reader.readMemChunk(currMeta);
      ptWritten = mergeChunk(currMeta, chunkOverflowed, chunkTooSmall, chunk,
          ptWritten, pathIdx, mergeFileWriter, unseqReader, chunkWriter, currFile);
    }
    // this only happens when the seqFiles do not contain this series, otherwise the remaining
    // data will be merged with the last chunk in the seqFiles
    if (isLastFile && currTimeValuePairs[pathIdx] != null) {
      ptWritten += writeRemainingUnseq(chunkWriter, unseqReader, Long.MAX_VALUE, pathIdx);
      mergedChunkNum++;
    }

    // the last merged chunk may still be smaller than the threshold, flush it anyway
    if (ptWritten > 0) {
      chunkWriter.writeToFileWriter(mergeFileWriter);
    }
    updateChunkCounts(currFile, mergedChunkNum, unmergedChunkNum);

    return mergedChunkNum > 0;
  }

  private int mergeChunk(ChunkMetaData currMeta, boolean chunkOverflowed,
      boolean chunkTooSmall,Chunk chunk, int ptWritten, int pathIdx,
      TsFileIOWriter mergeFileWriter, IPointReader unseqReader,
      IChunkWriter chunkWriter, TsFileResource currFile) throws IOException {

    int newPtWritten = ptWritten;
    if (ptWritten == 0 && fullMerge && !chunkOverflowed && !chunkTooSmall) {
      // write without unpacking the chunk
      mergeFileWriter.writeChunk(chunk, currMeta);
      mergedChunkNum++;
    } else {
      // the chunk should be unpacked to merge with other chunks
      int chunkPtNum = writeChunk(chunk, currMeta, chunkWriter,
          unseqReader, chunkOverflowed, chunkTooSmall, pathIdx);
      if (chunkPtNum > 0) {
        mergedChunkNum++;
        newPtWritten += chunkPtNum;
      } else {
        unmergedChunkNum++;
        mergeContext.getUnmergedChunkStartTimes().get(currFile).get(currMergingPaths.get(pathIdx))
            .add(currMeta.getStartTime());
      }
    }

    if (minChunkPointNum > 0 && newPtWritten >= minChunkPointNum
        || newPtWritten > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      chunkWriter.writeToFileWriter(mergeFileWriter);
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
      currTimeValuePairs[pathIdx] = unseqReader.hasNext() ? unseqReader.next() : null;
    }
    return ptWritten;
  }

  private void updateChunkCounts(TsFileResource currFile, int newMergedChunkNum,
      int newUnmergedChunkNum) {
    mergeContext.getMergedChunkCnt().compute(currFile, (tsFileResource, anInt) -> anInt == null ?
        newMergedChunkNum
        : anInt + newMergedChunkNum);
    mergeContext.getMergedChunkCnt().compute(currFile, (tsFileResource, anInt) -> anInt == null ?
        newUnmergedChunkNum
        : anInt + newUnmergedChunkNum);
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
      mergeContext.setTotalChunkWritten(mergeContext.getTotalChunkWritten() + 1);
    } else if (chunkOverflowed) {
      // this chunk may merge with the current point
      newPtWritten += writeChunkWithUnseq(chunk, chunkWriter, unseqReader, currMeta.getEndTime(), pathIdx);
      mergeContext.setTotalChunkWritten(mergeContext.getTotalChunkWritten() + 1);
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
        currTimeValuePairs[pathIdx] = unseqReader.hasNext() ? unseqReader.next() : null;
        cnt++;
      }
      if (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].getTimestamp() == time) {
        writeTVPair(currTimeValuePairs[pathIdx], chunkWriter);
        currTimeValuePairs[pathIdx] = unseqReader.hasNext() ? unseqReader.next() : null;
        cnt++;
      } else {
        writeBatchPoint(batchData, i, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }
}
