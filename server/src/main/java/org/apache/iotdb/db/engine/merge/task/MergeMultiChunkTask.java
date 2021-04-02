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
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.db.utils.MergeUtils.MetaListEntry;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.VectorChunkMetadata;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.utils.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

public class MergeMultiChunkTask {

  private static final Logger logger = LoggerFactory.getLogger(MergeMultiChunkTask.class);
  private static int minChunkPointNum =
      IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();

  private MergeLogger mergeLogger;

  private String taskName;
  private MergeResource resource;
  private List<TimeValuePair>[] currTimeValuePairs;
  private boolean fullMerge;

  private MergeContext mergeContext;

  private AtomicInteger mergedChunkNum = new AtomicInteger();
  private AtomicInteger unmergedChunkNum = new AtomicInteger();
  private int mergedSeriesCnt;
  private int totalSeriesCnt;
  private double progress;

  private String currMeringDevice;
  private List<IMeasurementSchema> currMergingPaths = new ArrayList<>();
  // need to be cleared every device
  private final Map<
          TsFileSequenceReader, Iterator<LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>>>
      measurementChunkMetadataListMapIteratorCache =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManagement.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));
  // need to be cleared every device
  private final Map<TsFileSequenceReader, LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>>
      chunkMetadataListCacheForMerge =
          new TreeMap<>(
              (o1, o2) ->
                  TsFileManagement.compareFileName(
                      new File(o1.getFileName()), new File(o2.getFileName())));

  private String storageGroupName;

  public MergeMultiChunkTask(
      MergeContext context,
      String taskName,
      MergeLogger mergeLogger,
      MergeResource mergeResource,
      boolean fullMerge,
      String storageGroupName) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.fullMerge = fullMerge;
    this.storageGroupName = storageGroupName;
  }

  void mergeSeries() throws IOException, IllegalPathException {
    // calculate total series count for progress and log
    totalSeriesCnt = 0;
    for (List<IMeasurementSchema> measurementSchemas :
        resource.getDeviceMeasurementSchemaMap().values()) {
      totalSeriesCnt += measurementSchemas.size();
    }

    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge {} devices", taskName, totalSeriesCnt);
    }
    long startTime = System.currentTimeMillis();
    for (TsFileResource seqFile : resource.getSeqFiles()) {
      mergeContext.getUnmergedChunkStartTimes().put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's corresponding temp merge file
    for (Entry<PartialPath, List<IMeasurementSchema>> deviceMeasurementSchemaEntry :
        resource.getDeviceMeasurementSchemaMap().entrySet()) {
      currMeringDevice = deviceMeasurementSchemaEntry.getKey().getDevice();
      currMergingPaths = deviceMeasurementSchemaEntry.getValue();
      mergePaths();
      resource.clearChunkWriterCache();
      if (Thread.interrupted()) {
        logger.info("MergeMultiChunkTask {} aborted", taskName);
        Thread.currentThread().interrupt();
        return;
      }
      mergedSeriesCnt += currMergingPaths.size();
      logMergeProgress();
      measurementChunkMetadataListMapIteratorCache.clear();
      chunkMetadataListCacheForMerge.clear();
    }
    if (logger.isInfoEnabled()) {
      logger.info(
          "{} all series are merged after {}ms", taskName, System.currentTimeMillis() - startTime);
    }
    mergeLogger.logAllTsEnd();
  }

  private void logMergeProgress() {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * mergedSeriesCnt / (double) totalSeriesCnt;
      if (newProgress - progress >= 10.0) {
        progress = newProgress;
        logger.info("{} has merged {}% series", taskName, progress);
      }
    }
  }

  public String getProgress() {
    return String.format("Processed %d/%d series", mergedSeriesCnt, totalSeriesCnt);
  }

  private void mergePaths() throws IOException, IllegalPathException {
    mergeLogger.logTSStart(currMergingPaths);
    List<IPointReader>[] unseqReaders =
        resource.getUnseqReaders(currMeringDevice, currMergingPaths);
    currTimeValuePairs = new List[currMergingPaths.size()];
    for (int i = 0; i < currMergingPaths.size(); i++) {
      // whether it is a vector or not, we just have to rank time by the first column -- time column
      if (unseqReaders[i].get(0).hasNextTimeValuePair()) {
        List<TimeValuePair> timeValuePairList = new ArrayList<>();
        timeValuePairList.add(unseqReaders[i].get(0).currentTimeValuePair());
        long time = timeValuePairList.get(0).getTimestamp();
        for (int j = 1; j < unseqReaders[i].size(); j++) {
          TimeValuePair timeValuePair = unseqReaders[i].get(j).currentTimeValuePair();
          if (timeValuePair.getTimestamp() == time) {
            timeValuePairList.add(unseqReaders[i].get(j).currentTimeValuePair());
          }
        }
        currTimeValuePairs[i] = timeValuePairList;
      }
    }

    for (int i = 0; i < resource.getSeqFiles().size(); i++) {
      pathsMergeOneFile(i, unseqReaders);

      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        return;
      }
    }
    mergeLogger.logTSEnd();
  }

  private void pathsMergeOneFile(int seqFileIdx, List<IPointReader>[] unseqReaders)
      throws IOException, IllegalPathException {
    TsFileResource currTsFile = resource.getSeqFiles().get(seqFileIdx);
    long currDeviceMinTime = currTsFile.getStartTime(currMeringDevice);
    // all paths in one call are from the same device
    if (currDeviceMinTime == Long.MAX_VALUE) {
      return;
    }

    for (IMeasurementSchema path : currMergingPaths) {
      mergeContext
          .getUnmergedChunkStartTimes()
          .get(currTsFile)
          .put(new PartialPath(currMeringDevice, path.getMeasurementId()), new ArrayList<>());
    }

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    for (List<TimeValuePair> timeValuePair : currTimeValuePairs) {
      if (timeValuePair != null && timeValuePair.get(0).getTimestamp() < currDeviceMinTime) {
        currDeviceMinTime = timeValuePair.get(0).getTimestamp();
      }
    }
    boolean isLastFile = seqFileIdx + 1 == resource.getSeqFiles().size();

    TsFileSequenceReader fileSequenceReader = resource.getFileReader(currTsFile);
    List<Modification>[] modifications = new List[currMergingPaths.size()];
    List<IChunkMetadata>[] seqChunkMeta = new List[currMergingPaths.size()];
    Iterator<LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>>>
        measurementChunkMetadataListMapIterator =
            measurementChunkMetadataListMapIteratorCache.computeIfAbsent(
                fileSequenceReader,
                (tsFileSequenceReader -> {
                  try {
                    return tsFileSequenceReader.getMeasurementChunkMetadataListMapIterator(
                        currMeringDevice);
                  } catch (IOException e) {
                    logger.error(
                        "unseq compaction task {}, getMeasurementChunkMetadataListMapIterator meets error. iterator create failed.",
                        taskName,
                        e);
                    return null;
                  }
                }));
    if (measurementChunkMetadataListMapIterator == null) {
      return;
    }

    String lastSensor = currMergingPaths.get(currMergingPaths.size() - 1).getMeasurementId();
    IMeasurementSchema currSensor = null;
    LinkedHashMap<IMeasurementSchema, List<IChunkMetadata>> measurementChunkMetadataListMap =
        new LinkedHashMap<>();
    // find all sensor to merge in order, if exceed, then break
    while (currSensor == null || currSensor.getMeasurementId().compareTo(lastSensor) < 0) {
      measurementChunkMetadataListMap =
          chunkMetadataListCacheForMerge.computeIfAbsent(
              fileSequenceReader, tsFileSequenceReader -> new LinkedHashMap<>());
      // if empty, get measurementChunkMetadataList block to use later
      if (measurementChunkMetadataListMap.isEmpty()) {
        // if do not have more sensor, just break
        if (measurementChunkMetadataListMapIterator.hasNext()) {
          measurementChunkMetadataListMap.putAll(measurementChunkMetadataListMapIterator.next());
        } else {
          break;
        }
      }

      Iterator<Entry<IMeasurementSchema, List<IChunkMetadata>>>
          measurementChunkMetadataListEntryIterator =
              measurementChunkMetadataListMap.entrySet().iterator();
      while (measurementChunkMetadataListEntryIterator.hasNext()) {
        Entry<IMeasurementSchema, List<IChunkMetadata>> measurementChunkMetadataListEntry =
            measurementChunkMetadataListEntryIterator.next();
        currSensor = measurementChunkMetadataListEntry.getKey();

        // fill modifications and seqChunkMetas to be used later
        for (int i = 0; i < currMergingPaths.size(); i++) {
          if (currMergingPaths.get(i).getMeasurementId().equals(currSensor.getMeasurementId())) {
            modifications[i] =
                resource.getModifications(
                    currTsFile,
                    new PartialPath(currMeringDevice, currMergingPaths.get(i).getMeasurementId()));
            seqChunkMeta[i] = measurementChunkMetadataListEntry.getValue();
            modifyChunkMetaData(seqChunkMeta[i], modifications[i]);
            for (IChunkMetadata iChunkMetadata : seqChunkMeta[i]) {
              resource.updateStartTime(currTsFile, currMeringDevice, iChunkMetadata.getStartTime());
              resource.updateEndTime(currTsFile, currMeringDevice, iChunkMetadata.getEndTime());
            }

            if (Thread.interrupted()) {
              Thread.currentThread().interrupt();
              return;
            }
            break;
          }
        }

        // current sensor larger than last needed sensor, just break out to outer loop
        if (currSensor.getMeasurementId().compareTo(lastSensor) > 0) {
          break;
        } else {
          measurementChunkMetadataListEntryIterator.remove();
        }
      }
    }
    // update measurementChunkMetadataListMap
    chunkMetadataListCacheForMerge.put(fileSequenceReader, measurementChunkMetadataListMap);

    List<Integer> unskippedPathIndices = filterNoDataPaths(seqChunkMeta, seqFileIdx);

    if (unskippedPathIndices.isEmpty()) {
      return;
    }

    RestorableTsFileIOWriter mergeFileWriter = resource.getMergeFileWriter(currTsFile);
    for (IMeasurementSchema path : currMergingPaths) {
      mergeFileWriter.addSchema(new PartialPath(currMeringDevice, path.getMeasurementId()), path);
    }
    // merge unseq data with seq data in this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(currMeringDevice);
    boolean dataWritten =
        mergeChunks(
            seqChunkMeta,
            isLastFile,
            fileSequenceReader,
            unseqReaders,
            mergeFileWriter,
            currTsFile);
    if (dataWritten) {
      mergeFileWriter.endChunkGroup();
      mergeLogger.logFilePosition(mergeFileWriter.getFile());
      currTsFile.updateStartTime(currMeringDevice, currDeviceMinTime);
    }
  }

  private List<Integer> filterNoDataPaths(List[] seqChunkMeta, int seqFileIdx) {
    // if the last seqFile does not contains this series but the unseqFiles do, data of this
    // series should also be written into a new chunk
    List<Integer> ret = new ArrayList<>();
    for (int i = 0; i < currMergingPaths.size(); i++) {
      if (seqChunkMeta[i] == null
          || seqChunkMeta[i].isEmpty()
              && !(seqFileIdx + 1 == resource.getSeqFiles().size()
                  && currTimeValuePairs[i] != null)) {
        continue;
      }
      ret.add(i);
    }
    return ret;
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private boolean mergeChunks(
      List<IChunkMetadata>[] seqChunkMeta,
      boolean isLastFile,
      TsFileSequenceReader reader,
      List<IPointReader>[] unseqReaders,
      RestorableTsFileIOWriter mergeFileWriter,
      TsFileResource currFile)
      throws IOException {
    int[] ptWrittens = new int[seqChunkMeta.length];
    int mergeChunkSubTaskNum =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkSubThreadNum();
    MetaListEntry[] metaListEntries = new MetaListEntry[currMergingPaths.size()];
    PriorityQueue<Integer>[] chunkIdxHeaps = new PriorityQueue[mergeChunkSubTaskNum];

    // if merge path is smaller than mergeChunkSubTaskNum, will use merge path number.
    // so thread are not wasted.
    if (currMergingPaths.size() < mergeChunkSubTaskNum) {
      mergeChunkSubTaskNum = currMergingPaths.size();
    }

    for (int i = 0; i < mergeChunkSubTaskNum; i++) {
      chunkIdxHeaps[i] = new PriorityQueue<>();
    }
    int idx = 0;
    for (int i = 0; i < currMergingPaths.size(); i++) {
      chunkIdxHeaps[idx % mergeChunkSubTaskNum].add(i);
      if (seqChunkMeta[i] == null || seqChunkMeta[i].isEmpty()) {
        continue;
      }

      MetaListEntry entry = new MetaListEntry(i, seqChunkMeta[i]);
      entry.next();
      metaListEntries[i] = entry;
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
                      chunkIdxHeaps[i],
                      metaListEntries,
                      ptWrittens,
                      reader,
                      mergeFileWriter,
                      unseqReaders,
                      currFile,
                      isLastFile,
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
      IChunkMetadata currMeta,
      boolean chunkOverflowed,
      boolean chunkTooSmall,
      List<Chunk> chunk,
      int lastUnclosedChunkPoint,
      int pathIdx,
      TsFileIOWriter mergeFileWriter,
      List<IPointReader> unseqReader,
      IChunkWriter chunkWriter,
      TsFileResource currFile)
      throws IOException, IllegalPathException {

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
          .get(new PartialPath(currMeringDevice, currMergingPaths.get(pathIdx).getMeasurementId()))
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
        if (currMeta instanceof ChunkMetadata) {
          mergeFileWriter.writeChunk(chunk.get(0), (ChunkMetadata) currMeta);
        } else {
          VectorChunkMetadata vectorChunkMetadata = (VectorChunkMetadata) currMeta;
          IChunkMetadata timeChunkMetadata = vectorChunkMetadata.getTimeChunkMetadata();
          mergeFileWriter.writeChunk(chunk.get(0), (ChunkMetadata) timeChunkMetadata);
          List<IChunkMetadata> valueChunkMetadataList =
              vectorChunkMetadata.getValueChunkMetadataList();
          for (int i = 0; i < valueChunkMetadataList.size(); i++) {
            mergeFileWriter.writeChunk(
                chunk.get(i + 1), (ChunkMetadata) valueChunkMetadataList.get(i));
          }
        }
      }
      mergeContext.incTotalPointWritten(currMeta.getStatistics().getCount());
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
          writeChunkWithUnseq(chunk, chunkWriter, unseqReader, currMeta.getEndTime(), pathIdx);
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
      IChunkWriter chunkWriter, List<IPointReader> unseqReader, long timeLimit, int pathIdx)
      throws IOException {
    int ptWritten = 0;
    while (currTimeValuePairs[pathIdx] != null
        && currTimeValuePairs[pathIdx].get(0).getTimestamp() < timeLimit) {
      long time = currTimeValuePairs[pathIdx].get(0).getTimestamp();
      if (chunkWriter instanceof ChunkWriterImpl) {
        writeTVPair(
            currTimeValuePairs[pathIdx].get(0).getTimestamp(),
            currTimeValuePairs[pathIdx].get(0),
            chunkWriter);
        ptWritten++;
        unseqReader.get(0).nextTimeValuePair();
        List<TimeValuePair> newTimeValuePairs = new ArrayList<>();
        if (unseqReader.get(0).hasNextTimeValuePair()) {
          newTimeValuePairs.add(unseqReader.get(0).currentTimeValuePair());
        } else {
          newTimeValuePairs = null;
        }
        currTimeValuePairs[pathIdx] = newTimeValuePairs;
      } else {
        for (int j = 1; j < currTimeValuePairs[pathIdx].size(); j++) {
          if (currTimeValuePairs[pathIdx].get(j).getTimestamp() == time) {
            writeTVPair(time, currTimeValuePairs[pathIdx].get(j), chunkWriter);
            unseqReader.get(j).nextTimeValuePair();
            List<TimeValuePair> newTimeValuePairs = currTimeValuePairs[pathIdx];
            if (unseqReader.get(j).hasNextTimeValuePair()) {
              newTimeValuePairs.set(j, unseqReader.get(j).currentTimeValuePair());
            } else {
              newTimeValuePairs = null;
            }
            currTimeValuePairs[pathIdx] = newTimeValuePairs;
          } else {
            writeTVPair(time, null, chunkWriter);
          }
        }
        chunkWriter.write(time);
        ptWritten++;
        unseqReader.get(0).nextTimeValuePair();
        List<TimeValuePair> newTimeValuePairs = currTimeValuePairs[pathIdx];
        if (unseqReader.get(0).hasNextTimeValuePair()) {
          newTimeValuePairs.set(0, unseqReader.get(0).currentTimeValuePair());
        } else {
          newTimeValuePairs = null;
        }
        currTimeValuePairs[pathIdx] = newTimeValuePairs;
      }
    }
    return ptWritten;
  }

  private int writeChunkWithUnseq(
      List<Chunk> chunk,
      IChunkWriter chunkWriter,
      List<IPointReader> unseqReader,
      long chunkLimitTime,
      int pathIdx)
      throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReader(chunk.get(0), null);
    while (chunkReader.hasNextSatisfiedPage()) {
      BatchData batchData = chunkReader.nextPageData();
      cnt += mergeWriteBatch(batchData, chunkWriter, unseqReader, pathIdx);
    }
    cnt += writeRemainingUnseq(chunkWriter, unseqReader, chunkLimitTime, pathIdx);
    return cnt;
  }

  private int mergeWriteBatch(
      BatchData batchData, IChunkWriter chunkWriter, List<IPointReader> unseqReader, int pathIdx)
      throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader

      boolean overwriteSeqPoint = false;
      // unseq point.time <= sequence point.time, write unseq point
      while (currTimeValuePairs[pathIdx] != null
          && currTimeValuePairs[pathIdx].get(0).getTimestamp() <= time) {
        if (chunkWriter instanceof ChunkWriterImpl) {
          writeTVPair(
              currTimeValuePairs[pathIdx].get(0).getTimestamp(),
              currTimeValuePairs[pathIdx].get(0),
              chunkWriter);
          if (currTimeValuePairs[pathIdx].get(0).getTimestamp() == time) {
            overwriteSeqPoint = true;
          }
          unseqReader.get(0).nextTimeValuePair();
          List<TimeValuePair> newTimeValuePairs = new ArrayList<>();
          if (unseqReader.get(0).hasNextTimeValuePair()) {
            newTimeValuePairs.add(unseqReader.get(0).currentTimeValuePair());
          } else {
            newTimeValuePairs = null;
          }
          currTimeValuePairs[pathIdx] = newTimeValuePairs;
        } else {
          long unseqTime = currTimeValuePairs[pathIdx].get(0).getTimestamp();
          for (int j = 1; j < currTimeValuePairs[pathIdx].size(); j++) {
            if (currTimeValuePairs[pathIdx].get(j).getTimestamp() == unseqTime) {
              writeTVPair(unseqTime, currTimeValuePairs[pathIdx].get(j), chunkWriter);
              unseqReader.get(j).nextTimeValuePair();
              List<TimeValuePair> newTimeValuePairs = currTimeValuePairs[pathIdx];
              if (unseqReader.get(j).hasNextTimeValuePair()) {
                newTimeValuePairs.set(j, unseqReader.get(j).currentTimeValuePair());
              } else {
                newTimeValuePairs = null;
              }
              currTimeValuePairs[pathIdx] = newTimeValuePairs;
            } else {
              writeTVPair(unseqTime, null, chunkWriter);
            }
          }
          chunkWriter.write(unseqTime);
          if (unseqTime == time) {
            overwriteSeqPoint = true;
          }
          unseqReader.get(0).nextTimeValuePair();
          List<TimeValuePair> newTimeValuePairs = currTimeValuePairs[pathIdx];
          if (unseqReader.get(0).hasNextTimeValuePair()) {
            newTimeValuePairs.set(0, unseqReader.get(0).currentTimeValuePair());
          } else {
            newTimeValuePairs = null;
          }
          currTimeValuePairs[pathIdx] = newTimeValuePairs;
        }
        cnt++;
      }
      // unseq point.time > sequence point.time, write seq point
      if (!overwriteSeqPoint) {
        if (chunkWriter instanceof ChunkWriterImpl) {
          writeBatchPoint(batchData, i, (ChunkWriterImpl) chunkWriter);
        } else {

        }
        cnt++;
      }
    }
    return cnt;
  }

  public class MergeChunkHeapTask implements Callable<Void> {

    private PriorityQueue<Integer> chunkIdxHeap;
    private MetaListEntry[] metaListEntries;
    private int[] ptWrittens;
    private TsFileSequenceReader reader;
    private RestorableTsFileIOWriter mergeFileWriter;
    private List<IPointReader>[] unseqReaders;
    private TsFileResource currFile;
    private boolean isLastFile;
    private int taskNum;

    private int totalSeriesNum;

    public MergeChunkHeapTask(
        PriorityQueue<Integer> chunkIdxHeap,
        MetaListEntry[] metaListEntries,
        int[] ptWrittens,
        TsFileSequenceReader reader,
        RestorableTsFileIOWriter mergeFileWriter,
        List<IPointReader>[] unseqReaders,
        TsFileResource currFile,
        boolean isLastFile,
        int taskNum) {
      this.chunkIdxHeap = chunkIdxHeap;
      this.metaListEntries = metaListEntries;
      this.ptWrittens = ptWrittens;
      this.reader = reader;
      this.mergeFileWriter = mergeFileWriter;
      this.unseqReaders = unseqReaders;
      this.currFile = currFile;
      this.isLastFile = isLastFile;
      this.taskNum = taskNum;
      this.totalSeriesNum = chunkIdxHeap.size();
    }

    @Override
    public Void call() throws Exception {
      mergeChunkHeap();
      return null;
    }

    @SuppressWarnings("java:S2445") // avoid reading the same reader concurrently
    private void mergeChunkHeap() throws IOException, IllegalPathException {
      while (!chunkIdxHeap.isEmpty()) {
        int pathIdx = chunkIdxHeap.poll();
        IMeasurementSchema measurementSchema = currMergingPaths.get(pathIdx);
        IChunkWriter chunkWriter = resource.getChunkWriter(measurementSchema);
        if (Thread.interrupted()) {
          Thread.currentThread().interrupt();
          return;
        }

        if (metaListEntries[pathIdx] != null) {
          MetaListEntry metaListEntry = metaListEntries[pathIdx];
          IChunkMetadata currMeta = metaListEntry.current();
          boolean isLastChunk = !metaListEntry.hasNext();
          boolean chunkOverflowed =
              MergeUtils.isChunkOverflowed(currTimeValuePairs[pathIdx], currMeta);
          boolean chunkTooSmall =
              MergeUtils.isChunkTooSmall(
                  ptWrittens[pathIdx], currMeta, isLastChunk, minChunkPointNum);

          List<Chunk> chunks = new ArrayList<>();
          synchronized (reader) {
            if (currMeta instanceof ChunkMetadata) {
              chunks.add(reader.readMemChunk((ChunkMetadata) currMeta));
            } else {
              VectorChunkMetadata vectorChunkMetadata = (VectorChunkMetadata) currMeta;
              chunks.add(vectorChunkMetadata.getTimeChunk());
              chunks.addAll(vectorChunkMetadata.getValueChunkList());
            }
          }
          ptWrittens[pathIdx] =
              mergeChunkV2(
                  currMeta,
                  chunkOverflowed,
                  chunkTooSmall,
                  chunks,
                  ptWrittens[pathIdx],
                  pathIdx,
                  mergeFileWriter,
                  unseqReaders[pathIdx],
                  chunkWriter,
                  currFile);

          if (!isLastChunk) {
            metaListEntry.next();
            chunkIdxHeap.add(pathIdx);
            continue;
          }
        }
        // this only happens when the seqFiles do not contain this series, otherwise the remaining
        // data will be merged with the last chunk in the seqFiles
        if (isLastFile && currTimeValuePairs[pathIdx] != null) {
          ptWrittens[pathIdx] +=
              writeRemainingUnseq(chunkWriter, unseqReaders[pathIdx], Long.MAX_VALUE, pathIdx);
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
          "Processed %d/%d series", totalSeriesNum - chunkIdxHeap.size(), totalSeriesNum);
    }
  }
}
