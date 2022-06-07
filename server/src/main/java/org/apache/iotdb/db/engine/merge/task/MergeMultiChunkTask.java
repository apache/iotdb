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

package org.apache.iotdb.db.engine.merge.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeManager;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.recover.MergeLogger;
import org.apache.iotdb.db.engine.merge.selector.IMergePathSelector;
import org.apache.iotdb.db.engine.merge.selector.NaivePathSelector;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.PreviewIterator;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

@SuppressWarnings("java:S107") // suppress too many args
public class MergeMultiChunkTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeMultiChunkTask.class);
  private static int minChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();

  private MergeLogger mergeLogger;
  // this task will be merging the series
  private List<PartialPath> unmergedSeries;

  private String storageGroupName;
  private String taskName;
  private MergeResource resource;
  private MergeContext mergeContext;

  private int concurrentMergeSeriesNum;
  private boolean fullMerge;

  private List<PartialPath> currMergingPaths = new ArrayList<>();
  // the earliest point of each "currMergingPaths" from unseq files
  private TimeValuePair[] currUnseqTimeValuePairs;

  private AtomicInteger mergedChunkNum = new AtomicInteger();
  private AtomicInteger unmergedChunkNum = new AtomicInteger();
  private int mergedSeriesCnt;
  private double progress;

  // the key of the map is a seq file, and the value is an iterator that traverse chunk lists of
  // the current merging device. The chunk lists are ordered by measurements.
  private final Map<TsFileSequenceReader, PreviewIterator<Pair<String, List<ChunkMetadata>>>>
      seqFileChunkMetadataListIteratorCache =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManagement.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));

  public MergeMultiChunkTask(
      MergeContext context,
      String taskName,
      MergeLogger mergeLogger,
      MergeResource mergeResource,
      boolean fullMerge,
      List<PartialPath> unmergedSeries,
      int concurrentMergeSeriesNum,
      String storageGroupName) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.fullMerge = fullMerge;
    this.unmergedSeries = unmergedSeries;
    this.concurrentMergeSeriesNum = concurrentMergeSeriesNum;
    this.storageGroupName = storageGroupName;
  }

  void mergeSeries() throws IOException, MetadataException {
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} series", taskName, unmergedSeries.size());
    }
    long startTime = System.currentTimeMillis();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      mergeContext.getUnmergedChunkStartTimes().put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's corresponding temp merge file
    List<List<PartialPath>> devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    for (List<PartialPath> pathList : devicePaths) {
      // TODO: use statistics of queries to better rearrange series
      IMergePathSelector pathSelector = new NaivePathSelector(pathList, concurrentMergeSeriesNum);
      while (pathSelector.hasNext()) {
        currMergingPaths = pathSelector.next();
        mergePaths();
        resource.clearChunkWriterCache();
        if (Thread.interrupted()) {
          logger.info("MergeMultiChunkTask {} aborted", taskName);
          Thread.currentThread().interrupt();
          return;
        }
        mergedSeriesCnt += currMergingPaths.size();
        logMergeProgress();
      }
      seqFileChunkMetadataListIteratorCache.clear();
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} all series are merged after {}ms", taskName, System.currentTimeMillis() - startTime);
    }
  }

  private void logMergeProgress() {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * mergedSeriesCnt / (double) (unmergedSeries.size());
      if (newProgress - progress >= 10.0) {
        progress = newProgress;
        logger.info("{} has merged {}% series", taskName, progress);
      }
    }
  }

  public String getProgress() {
    return String.format("Processed %d/%d series", mergedSeriesCnt, unmergedSeries.size());
  }

  private void mergePaths() throws IOException, MetadataException {
    // read the first point of each series from unseq files
    IPointReader[] unseqReaders = resource.getUnseqReaders(currMergingPaths);
    currUnseqTimeValuePairs = new TimeValuePair[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (unseqReaders[i].hasNextTimeValuePair()) {
        currUnseqTimeValuePairs[i] = unseqReaders[i].currentTimeValuePair();
      }
    }

    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      // merge unseq data into seq files one by one
      pathsMergeOneFile(i, unseqReaders);

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private long calculateDeviceMinTime(TsFileResource currTsFile, String deviceId) {
    long currDeviceMinTime = currTsFile.getStartTime(deviceId);
    for (TimeValuePair timeValuePair : currUnseqTimeValuePairs) {
      if (timeValuePair != null && timeValuePair.getTimestamp() < currDeviceMinTime) {
        currDeviceMinTime = timeValuePair.getTimestamp();
      }
    }
    return currDeviceMinTime;
  }

  private void pathsMergeOneFile(int seqFileIdx, IPointReader[] unseqReaders)
      throws IOException, MetadataException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    // all paths in one call are from the same device
    String deviceId = currMergingPaths.get(0).getDevice();
    long currDeviceMinTime = calculateDeviceMinTime(currTsFile, deviceId);

    for (PartialPath path : currMergingPaths) {
      mergeContext.getUnmergedChunkStartTimes().get(currTsFile).put(path, new ArrayList<>());
    }

    boolean isLastFile = seqFileIdx + 1 == resource.getSeqFiles().size();

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification>[] modifications = new List[currMergingPaths.size()];
    List<ChunkMetadata>[] seqChunkMeta = new List[currMergingPaths.size()];

    PreviewIterator<Pair<String, List<ChunkMetadata>>> measurementChunkMetadataListIterator =
        seqFileChunkMetadataListIteratorCache.computeIfAbsent(
            fileSequenceReader,
            (tsFileSequenceReader -> {
              try {
                return tsFileSequenceReader.getMeasurementChunkMetadataListIterator(deviceId);
              } catch (IOException e) {
                logger.error(
                    "unseq compaction task {}, getMeasurementChunkMetadataListMapIterator meets error. iterator create failed.",
                    taskName,
                    e);
                return null;
              }
            }));
    if (measurementChunkMetadataListIterator == null) {
      return;
    }

    readChunkMetaAndModifications(
        measurementChunkMetadataListIterator, modifications, seqChunkMeta, currTsFile, deviceId);

    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile, false);
    for (PartialPath path : currMergingPaths) {
      MeasurementSchema schema = IoTDB.metaManager.getSeriesSchema(path);
      mergeFileWriter.addSchema(path, schema);
    }
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(deviceId);
    boolean dataWritten =
        mergeChunks(
            deviceId,
            seqChunkMeta,
            isLastFile,
            fileSequenceReader,
            unseqReaders,
            mergeFileWriter,
            currTsFile);
    if (dataWritten) {
      mergeFileWriter.endChunkGroup();
      currTsFile.updateStartTime(deviceId, currDeviceMinTime);
    }
  }

  private void readChunkMetaAndModifications(
      PreviewIterator<Pair<String, List<ChunkMetadata>>> measurementChunkMetadataListIterator,
      List<Modification>[] modifications,
      List<ChunkMetadata>[] seqChunkMeta,
      TsFileResource currTsFile,
      String deviceId) {
    if (!measurementChunkMetadataListIterator.hasNext()) {
      return;
    }
    Pair<String, List<ChunkMetadata>> nextFileMeasurement =
        measurementChunkMetadataListIterator.previewNext();
    boolean noMoreMeasurement = false;
    int i = 0;
    while (!noMoreMeasurement) {
      String currMergingMeasurement = currMergingPaths.get(i).getMeasurement();
      if (currMergingMeasurement.equals(nextFileMeasurement.left)) {
        // current series is found in the file
        modifications[i] = resource.getModifications(currTsFile, currMergingPaths.get(i));
        seqChunkMeta[i] = nextFileMeasurement.right;
        modifyChunkMetaData(seqChunkMeta[i], modifications[i]);
        for (ChunkMetadata chunkMetadata : seqChunkMeta[i]) {
          resource.updateStartTime(currTsFile, deviceId, chunkMetadata.getStartTime());
          resource.updateEndTime(currTsFile, deviceId, chunkMetadata.getEndTime());
        }

        // move both cursors to the next
        i++;
        measurementChunkMetadataListIterator.next();
      } else if (currMergingPaths.get(i).getMeasurement().compareTo(nextFileMeasurement.left) < 0) {
        // current series cannot be found in the file, move to the next merging series
        i++;
      } else {
        // check the next series in the file
        measurementChunkMetadataListIterator.next();
      }

      if (measurementChunkMetadataListIterator.hasNext()) {
        nextFileMeasurement = measurementChunkMetadataListIterator.previewNext();
      } else {
        noMoreMeasurement = true;
      }

      if (i >= currMergingPaths.size()) {
        noMoreMeasurement = true;
      }
    }

    if (Thread.interrupted()) {
      Thread.currentThread().interrupt();
      return;
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean mergeChunks(
      String deviceId,
      List<ChunkMetadata>[] seqChunkMeta,
      boolean isLastFile,
      TsFileSequenceReader reader,
      IPointReader[] unseqReaders,
      RestorableTsFileIOWriter mergeFileWriter,
      TsFileResource currFile)
      throws IOException {
    int[] ptWrittens = new int[seqChunkMeta.length];
    int mergeChunkSubTaskNum =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
    MetaListEntry[] seqMetaListEntries = new MetaListEntry[currMergingPaths.size()];
    // split merge task into sub-tasks
    PriorityQueue<Integer>[] pathIdxHeaps = new PriorityQueue[mergeChunkSubTaskNum];

    // if merge path is smaller than mergeChunkSubTaskNum, will use merge path number.
    // so thread are not wasted.
    if (currMergingPaths.size() < mergeChunkSubTaskNum) {
      mergeChunkSubTaskNum = currMergingPaths.size();
    }

    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      pathIdxHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    for (int i = 0; i < currMergingPaths.size(); i++) {
      // assign paths to sub-tasks in a round-robin way
      pathIdxHeaps[idx % mergeChunkSubTaskNum].add(i);
      if (seqChunkMeta[i] == null || seqChunkMeta[i].isEmpty()) {
        continue;
      }

      MetaListEntry entry = new MetaListEntry(i, seqChunkMeta[i]);
      // move to the first chunk metadata
      entry.next();
      seqMetaListEntries[i] = entry;
      idx++;
      ptWrittens[i] = 0;
    }

    mergedChunkNum.set(0);
    unmergedChunkNum.set(0);

    List<Future<Void>> futures = new ArrayList<>();
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      futures.add(
          MergeManager.getINSTANCE()
              .submitChunkSubTask(
                  new MergeChunkHeapTask(
                      pathIdxHeaps[i],
                      seqMetaListEntries,
                      ptWrittens,
                      reader,
                      mergeFileWriter,
                      unseqReaders,
                      currFile,
                      isLastFile,
                      currFile.getEndTime(deviceId),
                      i)));

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      try {
        futures.get(i).get();
      } catch (InterruptedException e) {
        logger.error("MergeChunkHeapTask interrupted", e);
        Thread.currentThread().interrupt();
        return false;
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }

    // add merge and unmerged chunk statistic
    mergeContext
        .getMergedChunkCnt()
        .compute(
            currFile,
            (tsFileResource, anInt) ->
                anInt == null ? mergedChunkNum.get() : anInt + mergedChunkNum.get());
    mergeContext
        .getUnmergedChunkCnt()
        .compute(
            currFile,
            (tsFileResource, anInt) ->
                anInt == null ? unmergedChunkNum.get() : anInt + unmergedChunkNum.get());

    return mergedChunkNum.get() > 0;
  }

  /**
   * merge a sequence chunk SK
   *
   * <p>1. no need to write the chunk to .merge file when: isn't full merge & there isn't unclosed
   * chunk before & SK is big enough & SK isn't overflowed & SK isn't modified
   *
   * <p>
   *
   * <p>2. write SK to .merge.file without compressing when: is full merge & there isn't unclosed
   * chunk before & SK is big enough & SK isn't overflowed & SK isn't modified
   *
   * <p>3. other cases: need to unCompress the chunk and write 3.1 SK isn't overflowed 3.2 SK is
   * overflowed
   */
  @SuppressWarnings("java:S2445") // avoid writing the same writer concurrently
  private int mergeChunkV2(
      ChunkMetadata currMeta,
      boolean chunkOverflowed,
      boolean chunkTooSmall,
      boolean isLastChunk,
      long resourceEndTime,
      Chunk chunk,
      int lastUnclosedChunkPoint,
      int pathIdx,
      TsFileIOWriter mergeFileWriter,
      IPointReader unseqReader,
      IChunkWriter chunkWriter,
      TsFileResource currFile)
      throws IOException {
    int unclosedChunkPoint = lastUnclosedChunkPoint;
    boolean chunkModified =
        (currMeta.getDeleteIntervalList() != null && !currMeta.getDeleteIntervalList().isEmpty());

    // no need to write the chunk to .merge file
    if (!fullMerge
        && lastUnclosedChunkPoint == 0
        && !chunkTooSmall
        && !chunkOverflowed
        && !chunkModified) {
      unmergedChunkNum.incrementAndGet();
      mergeContext
          .getUnmergedChunkStartTimes()
          .get(currFile)
          .get(currMergingPaths.get(pathIdx))
          .add(currMeta.getStartTime());
      return 0;
    }

    // write SK to .merge.file without compressing
    if (fullMerge
        && lastUnclosedChunkPoint == 0
        && !chunkTooSmall
        && !chunkOverflowed
        && !chunkModified) {
      synchronized (mergeFileWriter) {
        mergeFileWriter.writeChunk(chunk, currMeta);
      }
      mergeContext.incTotalPointWritten(currMeta.getNumOfPoints());
      mergeContext.incTotalChunkWritten();
      mergedChunkNum.incrementAndGet();
      return 0;
    }

    // 3.1 SK isn't overflowed, just uncompress and write sequence chunk
    if (!chunkOverflowed) {
      unclosedChunkPoint += MergeUtils.writeChunkWithoutUnseq(chunk, chunkWriter);
      mergedChunkNum.incrementAndGet();
    } else {
      // 3.2 SK is overflowed, uncompress sequence chunk and merge with unseq chunk, then write
      unclosedChunkPoint +=
          writeChunkWithUnseq(
              chunk,
              chunkWriter,
              unseqReader,
              isLastChunk ? resourceEndTime + 1 : currMeta.getEndTime(),
              pathIdx);
      mergedChunkNum.incrementAndGet();
    }

    // update points written statistics
    mergeContext.incTotalPointWritten((long) unclosedChunkPoint - lastUnclosedChunkPoint);
    if (minChunkPointNum > 0 && unclosedChunkPoint >= minChunkPointNum
        || unclosedChunkPoint > 0 && minChunkPointNum < 0) {
      // the new chunk's size is large enough and it should be flushed
      synchronized (mergeFileWriter) {
        chunkWriter.writeToFileWriter(mergeFileWriter);
      }
      unclosedChunkPoint = 0;
    }
    return unclosedChunkPoint;
  }

  private int writeRemainingUnseq(
      IChunkWriter chunkWriter, IPointReader unseqReader, long timeLimit, int pathIdx)
      throws IOException {
    int ptWritten = 0;
    while (currUnseqTimeValuePairs[pathIdx] != null
        && currUnseqTimeValuePairs[pathIdx].getTimestamp() < timeLimit) {
      writeTVPair(currUnseqTimeValuePairs[pathIdx], chunkWriter);
      ptWritten++;
      unseqReader.nextTimeValuePair();
      currUnseqTimeValuePairs[pathIdx] =
          unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
    }
    return ptWritten;
  }

  private int writeChunkWithUnseq(
      Chunk chunk,
      IChunkWriter chunkWriter,
      IPointReader unseqReader,
      long chunkLimitTime,
      int pathIdx)
      throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReader(chunk, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      cnt += mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
    cnt += writeRemainingUnseq(chunkWriter, unseqReader, chunkLimitTime, pathIdx);
    return cnt;
  }

  private int mergeWriteBatch(
      BatchData batchData, IChunkWriter chunkWriter, IPointReader unseqReader, int pathIdx)
      throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader
      boolean overwriteSeqPoint = false;
      // unseq point.time <= sequence point.time, write unseq point
      while (currUnseqTimeValuePairs[pathIdx] != null
          && currUnseqTimeValuePairs[pathIdx].getTimestamp() <= time) {
        writeTVPair(currUnseqTimeValuePairs[pathIdx], chunkWriter);
        if (currUnseqTimeValuePairs[pathIdx].getTimestamp() == time) {
          overwriteSeqPoint = true;
        }
        unseqReader.nextTimeValuePair();
        currUnseqTimeValuePairs[pathIdx] =
            unseqReader.hasNextTimeValuePair() ? unseqReader.currentTimeValuePair() : null;
        cnt++;
      }
      // unseq point.time > sequence point.time, write seq point
      if (!overwriteSeqPoint) {
        writeBatchPoint(batchData, i, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }

  public class MergeChunkHeapTask implements Callable<Void> {

    private PriorityQueue<Integer> pathIdxHeap;
    private MetaListEntry[] metaListEntries;
    private int[] ptWrittens;
    private TsFileSequenceReader reader;
    private RestorableTsFileIOWriter mergeFileWriter;
    private IPointReader[] unseqReaders;
    private TsFileResource currFile;
    private int taskNum;
    private long endTimeOfCurrentResource;
    private boolean isLastFile;

    private int totalSeriesNum;

    public MergeChunkHeapTask(
        PriorityQueue<Integer> pathIdxHeap,
        MetaListEntry[] metaListEntries,
        int[] ptWrittens,
        TsFileSequenceReader reader,
        RestorableTsFileIOWriter mergeFileWriter,
        IPointReader[] unseqReaders,
        TsFileResource currFile,
        boolean isLastFile,
        long endTimeOfCurrentResource,
        int taskNum) {
      this.pathIdxHeap = pathIdxHeap;
      this.metaListEntries = metaListEntries;
      this.ptWrittens = ptWrittens;
      this.reader = reader;
      this.mergeFileWriter = mergeFileWriter;
      this.unseqReaders = unseqReaders;
      this.currFile = currFile;
      this.taskNum = taskNum;
      this.totalSeriesNum = pathIdxHeap.size();
      this.endTimeOfCurrentResource = endTimeOfCurrentResource;
    }

    @Override
    public Void call() throws Exception {
      mergeChunkHeap();
      return null;
    }

    @SuppressWarnings("java:S2445") // avoid reading the same reader concurrently
    private void mergeChunkHeap() throws IOException, MetadataException {
      while (!pathIdxHeap.isEmpty()) {
        int pathIdx = pathIdxHeap.poll();
        PartialPath path = currMergingPaths.get(pathIdx);
        MeasurementSchema measurementSchema = IoTDB.metaManager.getSeriesSchema(path);
        IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        if (metaListEntries[pathIdx] != null) {
          // the seq file has this series, read a chunk and merge it with unseq data
          MetaListEntry metaListEntry = metaListEntries[pathIdx];
          ChunkMetadata currMeta = metaListEntry.current();
          boolean isLastChunk = !metaListEntry.hasNext();
          boolean chunkOverflowed =
              MergeUtils.isChunkOverflowed(
                  currUnseqTimeValuePairs[pathIdx],
                  currMeta,
                  isLastChunk,
                  endTimeOfCurrentResource);
          boolean chunkTooSmall =
              MergeUtils.isChunkTooSmall(
                  ptWrittens[pathIdx], currMeta, isLastChunk, minChunkPointNum);

          Chunk chunk;
          synchronized (reader) {
            chunk = reader.readMemChunk(currMeta);
          }
          ptWrittens[pathIdx] =
              mergeChunkV2(
                  currMeta,
                  chunkOverflowed,
                  chunkTooSmall,
                  isLastChunk,
                  endTimeOfCurrentResource,
                  chunk,
                  ptWrittens[pathIdx],
                  pathIdx,
                  mergeFileWriter,
                  unseqReaders[pathIdx],
                  chunkWriter,
                  currFile);

          if (!isLastChunk) {
            // put the series back so its remaining chunks can be merged
            metaListEntry.next();
            pathIdxHeap.add(pathIdx);
            continue;
          }
        }
        // the seq file does not have the series, otherwise the remaining
        // data will be merged with the last chunk in the seqFiles
        // writing device-level overlapped data into this file
        if (currUnseqTimeValuePairs[pathIdx] != null) {
          ptWrittens[pathIdx] +=
              writeRemainingUnseq(
                  chunkWriter,
                  unseqReaders[pathIdx],
                  isLastFile ? Long.MAX_VALUE : endTimeOfCurrentResource + 1,
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

    public String getStorageGroupName() {
      return storageGroupName;
    }

    public String getTaskName() {
      return taskName + "_" + taskNum;
    }

    public String getProgress() {
      return String.format(
          "Processed %d/%d series", totalSeriesNum - pathIdxHeap.size(), totalSeriesNum);
    }
  }
}
