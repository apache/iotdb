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

package org.apache.iotdb.db.engine.merge;

import static org.apache.iotdb.db.engine.merge.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.db.engine.merge.MergeUtils.writeTVPair;
import static org.apache.iotdb.db.utils.QueryUtils.modifyChunkMetaData;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MetadataErrorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.SeriesReaderFactoryImpl;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.ChunkLoaderImpl;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderWithoutFilter;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.ForceAppendTsFileWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MergeTask merges given SeqFiles and UnseqFiles into a new one.
 */
public class MergeTask implements Callable<Void> {

  private static final int MIN_CHUNK_POINT_NUM = 4096;
  public static final String MERGE_SUFFIX = ".merge";
  private static final Logger logger = LoggerFactory.getLogger(MergeTask.class);

  protected List<TsFileResource> seqFiles;
  protected List<TsFileResource> unseqFiles;
  protected String storageGroupDir;
  protected MergeLogger mergeLogger;

  private TimeValuePair currTimeValuePair;

  protected Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();
  protected Map<TsFileResource, RestorableTsFileIOWriter> fileWriterCache = new HashMap<>();
  protected Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();
  protected Map<TsFileResource, MetadataQuerier> metadataQuerierCache = new HashMap<>();

  protected Map<TsFileResource, Integer> mergedChunkCnt = new HashMap<>();
  protected Map<TsFileResource, Integer> unmergedChunkCnt = new HashMap<>();
  protected Map<TsFileResource, Map<Path, List<Long>>> unmergedChunkStartTimes = new HashMap<>();

  private QueryContext mergeContext = new QueryContext();

  private boolean keepChunkMetadata = false;

  private long currDeviceMaxTime;

  protected MergeTask() {

  }

  public MergeTask(List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles, String storageGroupDir) throws IOException {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
    this.storageGroupDir = storageGroupDir;
    this.mergeLogger = new MergeLogger(storageGroupDir);
  }

  @Override
  public Void call() throws Exception {
    doMerge();
    return null;
  }

  private void doMerge() throws MetadataErrorException, IOException {

    logFiles();

    List<Path> unmergedSeries = collectPathsInUnseqFiles();
    mergeSeries(unmergedSeries);

    List<TsFileResource> unmergedFiles = seqFiles;
    mergeFiles(unmergedFiles);

    cleanUp(true);
  }

  protected void mergeFiles(List<TsFileResource> unmergedFiles) throws IOException {
    // decide whether to write the unmerged chunks to the merge files or to move the merged data
    // back to the origin seqFile's
    for (TsFileResource seqFile : unmergedFiles) {
      int mergedChunkNum = mergedChunkCnt.getOrDefault(seqFile, 0);
      int unmergedChunkNum = unmergedChunkCnt.getOrDefault(seqFile, 0);
      if (mergedChunkNum >= unmergedChunkNum) {
        // move the unmerged data to the new file
        moveUnmergedToNew(seqFile);
      } else {
        // move the merged data to the old file
        moveMergedToOld(seqFile);
      }
    }
    mergeLogger.logMergeEnd();
  }

  protected void logFiles() throws IOException {
    mergeLogger.logSeqFiles(seqFiles);
    mergeLogger.logUnseqFiles(unseqFiles);
    mergeLogger.logMergeStart();
  }

  protected void mergeSeries(List<Path> unmergedSeries) throws IOException {
    for (TsFileResource seqFile : seqFiles) {
      unmergedChunkStartTimes.put(seqFile, new HashMap<>());
    }
    // merge each series and write data into each seqFile's temp merge file
    for (Path path : unmergedSeries) {
      mergeLogger.logTSStart(path);
      mergeOnePath(path);
      mergeLogger.logTSEnd(path);
    }
    mergeLogger.logAllTsEnd();
  }


  protected void cleanUp(boolean executeCallback) throws IOException {
    for (TsFileSequenceReader sequenceReader : fileReaderCache.values()) {
      sequenceReader.close();
    }

    fileReaderCache.clear();
    fileWriterCache.clear();
    modificationCache.clear();
    metadataQuerierCache.clear();
    mergedChunkCnt.clear();
    unmergedChunkCnt.clear();
    unmergedChunkStartTimes.clear();

    mergeLogger.close();
    File logFile = new File(storageGroupDir, MergeLogger.MERGE_LOG_NAME);
    logFile.delete();
    for (TsFileResource seqFile : seqFiles) {
      File mergeFile = new File(seqFile.getFile().getPath() + MERGE_SUFFIX);
      mergeFile.delete();
    }
  }

  private void moveMergedToOld(TsFileResource seqFile) throws IOException {
    int mergedChunkNum = mergedChunkCnt.getOrDefault(seqFile, 0);
    if (mergedChunkNum == 0) {
      RestorableTsFileIOWriter newFileWriter = getMergeFileWriter(seqFile);
      newFileWriter.close();
      newFileWriter.getFile().delete();
      fileWriterCache.remove(seqFile);
      return;
    }

    seqFile.getMergeQueryLock().writeLock().lock();
    try {
      TsFileIOWriter oldFileWriter;
      try {
        oldFileWriter = new ForceAppendTsFileWriter(seqFile.getFile());
        mergeLogger.logFileMergeStart(seqFile.getFile(), ((ForceAppendTsFileWriter) oldFileWriter).getTruncatePosition());
        ((ForceAppendTsFileWriter) oldFileWriter).doTruncate();
      } catch (TsFileNotCompleteException e) {
        // this file may already be truncated if this merge is a system reboot merge
        oldFileWriter = new RestorableTsFileIOWriter(seqFile.getFile());
      }

      oldFileWriter.filterChunks(unmergedChunkStartTimes.get(seqFile));

      RestorableTsFileIOWriter newFileWriter = getMergeFileWriter(seqFile);
      newFileWriter.close();
      try ( TsFileSequenceReader newFileReader =
          new TsFileSequenceReader(newFileWriter.getFile().getPath())) {
        ChunkLoader chunkLoader = new ChunkLoaderImpl(newFileReader);
        List<ChunkGroupMetaData> chunkGroupMetaDatas = newFileWriter.getChunkGroupMetaDatas();
        for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDatas) {
          writeMergedChunkGroup(chunkGroupMetaData, chunkLoader, oldFileWriter);
        }
      }
      oldFileWriter.endFile(new FileSchema(oldFileWriter.getKnownSchema()));

      seqFile.serialize();
      mergeLogger.logFileMergeEnd(seqFile.getFile());

      newFileWriter.getFile().delete();
    } finally {
      seqFile.getMergeQueryLock().writeLock().unlock();
    }
  }

  private void writeMergedChunkGroup(ChunkGroupMetaData chunkGroupMetaData,
      ChunkLoader chunkLoader, TsFileIOWriter fileWriter)
      throws IOException {
    fileWriter.startChunkGroup(chunkGroupMetaData.getDeviceID());
    long maxVersion = Long.MIN_VALUE;
    for (ChunkMetaData chunkMetaData : chunkGroupMetaData.getChunkMetaDataList()) {
      Path path = new Path(chunkGroupMetaData.getDeviceID(), chunkMetaData.getMeasurementUid());
      MeasurementSchema measurementSchema = getSchema(path);
      IChunkWriter chunkWriter = getChunkWriter(measurementSchema);
      Chunk chunk = chunkLoader.getChunk(chunkMetaData);
      writeChunkWithoutUnseq(chunk, chunkWriter, measurementSchema);
      chunkWriter.writeToFileWriter(fileWriter);
      maxVersion = maxVersion < chunkMetaData.getVersion() ? chunkMetaData.getVersion() :
          maxVersion;
    }
    fileWriter.endChunkGroup(maxVersion);
  }

  private void moveUnmergedToNew(TsFileResource seqFile) throws IOException {
    Map<Path, List<Long>> fileUnmergedChunkStartTimes = this.unmergedChunkStartTimes.get(seqFile);
    RestorableTsFileIOWriter fileWriter = getMergeFileWriter(seqFile);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(getFileReader(seqFile));

    mergeLogger.logFileMergeStart(fileWriter.getFile(), fileWriter.getFile().length());

    int unmergedChunkNum = unmergedChunkCnt.getOrDefault(seqFile, 0);

    if (unmergedChunkNum > 0) {
      for (Entry<Path, List<Long>> entry : fileUnmergedChunkStartTimes.entrySet()) {
        Path path = entry.getKey();
        List<Long> chunkStartTimes = entry.getValue();
        if (chunkStartTimes.isEmpty()) {
          continue;
        }

        List<ChunkMetaData> chunkMetaDataList = queryChunkMetadata(path, seqFile);
        MeasurementSchema measurementSchema = getSchema(path);
        IChunkWriter chunkWriter = getChunkWriter(measurementSchema);

        fileWriter.startChunkGroup(path.getDevice());
        long maxVersion = writeUnmergedChunks(chunkStartTimes, chunkMetaDataList, chunkLoader,
            chunkWriter, measurementSchema, fileWriter);
        fileWriter.endChunkGroup(maxVersion);
      }
    }

    fileWriter.endFile(new FileSchema(fileWriter.getKnownSchema()));

    seqFile.serialize();
    mergeLogger.logFileMergeEnd(fileWriter.getFile());

    seqFile.getMergeQueryLock().writeLock().lock();
    try {
      FileUtils.moveFile(fileWriter.getFile(), seqFile.getFile());
    } finally {
      seqFile.getMergeQueryLock().writeLock().unlock();
    }
  }

  private long writeUnmergedChunks(List<Long> chunkStartTimes,
      List<ChunkMetaData> chunkMetaDataList, ChunkLoader chunkLoader, IChunkWriter chunkWriter,
      MeasurementSchema measurementSchema, RestorableTsFileIOWriter fileWriter) throws IOException {
    long maxVersion = 0;
    int chunkIdx = 0;
    for (Long startTime : chunkStartTimes) {
      for (; chunkIdx < chunkMetaDataList.size(); chunkIdx ++) {
        ChunkMetaData metaData = chunkMetaDataList.get(chunkIdx);
        if (metaData.getStartTime() == startTime) {
          Chunk chunk = chunkLoader.getChunk(metaData);
          writeChunkWithoutUnseq(chunk, chunkWriter, measurementSchema);
          chunkWriter.writeToFileWriter(fileWriter);
          maxVersion = metaData.getVersion() > maxVersion ? metaData.getVersion() : maxVersion;
          break;
        }
      }
    }
    return maxVersion;
  }

  private void mergeOnePath(Path path) throws IOException {
    IPointReader unseqReader = getUnseqReader(path);
    MeasurementSchema schema = getSchema(path);
    try {
      currTimeValuePair = unseqReader.next();
      for (int i = 0; i < seqFiles.size(); i++) {
        mergeOneFile(path, i, unseqReader, schema);
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

  private void mergeOneFile(Path path, int seqFileIdx, IPointReader unseqReader,
      MeasurementSchema measurementSchema)
      throws IOException {
    TsFileResource currTsFile = seqFiles.get(seqFileIdx);
    unmergedChunkStartTimes.get(currTsFile).put(path, new ArrayList<>());

    // if this TsFile receives data later than fileLimitTime, it will overlap the next TsFile,
    // which is forbidden
    String deviceId = path.getDevice();
    long fileLimitTime = Long.MAX_VALUE;
    for (int i = seqFileIdx + 1; i < seqFiles.size(); i++) {
      Long nextStartTime = seqFiles.get(i).getStartTimeMap().get(deviceId);
      if (nextStartTime != null) {
        fileLimitTime = nextStartTime;
        break;
      }
    }

    TsFileSequenceReader fileSequenceReader = getFileReader(currTsFile);
    ChunkLoader chunkLoader = new ChunkLoaderImpl(fileSequenceReader);
    List<Modification> modifications = getModifications(currTsFile, path);
    List<ChunkMetaData> seqChunkMeta = queryChunkMetadata(path, currTsFile);
    if (seqChunkMeta.isEmpty()) {
      return;
    }
    modifyChunkMetaData(seqChunkMeta, modifications);
    RestorableTsFileIOWriter mergeFileWriter = getMergeFileWriter(currTsFile);

    currDeviceMaxTime = currTsFile.getEndTimeMap().get(path.getDevice());
    // merge unseq data with this file or small chunks in this file into a larger chunk
    mergeFileWriter.startChunkGroup(deviceId);
    if (mergeChunks(seqChunkMeta, fileLimitTime, chunkLoader, measurementSchema,
        unseqReader, mergeFileWriter, currTsFile, path)) {
      mergeFileWriter.endChunkGroup(seqChunkMeta.get(seqChunkMeta.size() - 1).getVersion());
      mergeLogger.logFilePositionUpdate(mergeFileWriter.getFile());
    }
    currTsFile.updateTime(path.getDevice(), currDeviceMaxTime);
  }

  private boolean mergeChunks(List<ChunkMetaData> seqChunkMeta, long fileLimitTime,
      ChunkLoader chunkLoader, MeasurementSchema measurementSchema,
      IPointReader unseqReader, RestorableTsFileIOWriter mergeFileWriter, TsFileResource currFile
      , Path path)
      throws IOException {
    int ptWritten = 0;
    IChunkWriter chunkWriter = getChunkWriter(measurementSchema);
    int mergedChunkNum = 0;
    int unmergedChunkNum = 0;
    for (int i = 0; i < seqChunkMeta.size(); i++) {
      ChunkMetaData currMeta = seqChunkMeta.get(i);
      boolean isLastChunk = i + 1 == seqChunkMeta.size();
      long chunkLimitTime = i + 1 < seqChunkMeta.size() ? seqChunkMeta.get(i + 1).getStartTime()
          : fileLimitTime;

      int newPtWritten = writeChunk(chunkLimitTime, ptWritten, chunkLoader, currMeta, chunkWriter,
          isLastChunk,
          measurementSchema, unseqReader);

      if (newPtWritten > ptWritten) {
        mergedChunkNum ++;
        ptWritten = newPtWritten;
      } else {
        unmergedChunkNum ++;
        unmergedChunkStartTimes.get(currFile).get(path).add(currMeta.getStartTime());
      }

      if (ptWritten >= MIN_CHUNK_POINT_NUM) {
        // the new chunk's size is large enough and it should be flushed
        chunkWriter.writeToFileWriter(mergeFileWriter);
        ptWritten = 0;
      }
    }
    if (ptWritten >= 0) {
      // the last merged chunk may still be smaller than the threshold, flush it anyway
      chunkWriter.writeToFileWriter(mergeFileWriter);
    }
    int finalMergedChunkNum = mergedChunkNum;
    int finalUnmergedChunkNum = unmergedChunkNum;
    mergedChunkCnt.compute(currFile, (tsFileResource, anInt) -> anInt == null ? finalMergedChunkNum
        : anInt + finalMergedChunkNum);
    unmergedChunkCnt.compute(currFile, (tsFileResource, anInt) -> anInt == null ? finalUnmergedChunkNum
        : anInt + finalUnmergedChunkNum);
    return mergedChunkNum > 0;
  }

  private int writeChunk(long chunkLimitTime, int ptWritten, ChunkLoader chunkLoader,
      ChunkMetaData currMeta, IChunkWriter chunkWriter, boolean isLastChunk,
      MeasurementSchema measurementSchema, IPointReader unseqReader) throws IOException {

    // unseq data is not over and this chunk's time range cover the overflow point
    boolean chunkOverlap =
        currTimeValuePair != null && currTimeValuePair.getTimestamp() < chunkLimitTime;
    // a small chunk has been written, this chunk merge with it to create a larger chunk
    // or this chunk is too small and it is not the last chunk, merge it with the next chunk
    boolean chunkTooSmall =
        ptWritten > 0 || (currMeta.getNumOfPoints() < MIN_CHUNK_POINT_NUM && !isLastChunk);
    boolean chunkModified = currMeta.getDeletedAt() > Long.MIN_VALUE;
    int newPtWritten = ptWritten;

    if (!chunkOverlap && (chunkTooSmall || chunkModified)) {
      // just rewrite the (modified) chunk
      Chunk chunk = chunkLoader.getChunk(currMeta);
      newPtWritten += writeChunkWithoutUnseq(chunk, chunkWriter, measurementSchema);
    } else if (chunkOverlap) {
      // this chunk may merge with the current point
      Chunk chunk = chunkLoader.getChunk(currMeta);
      newPtWritten += writeChunkWithUnseq(chunk, chunkWriter, measurementSchema.getType(),
          unseqReader, chunkLimitTime);
    }
    return newPtWritten;
  }

  private int writeChunkWithUnseq(Chunk chunk, IChunkWriter chunkWriter,
     TSDataType dataType, IPointReader unseqReader, long chunkLimitTime) throws IOException {
    int cnt = 0;
    ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);
    while (chunkReader.hasNextBatch()) {
      BatchData batchData = chunkReader.nextBatch();
      cnt += mergeWriteBatch(batchData, chunkWriter, dataType, unseqReader);
    }
    while (currTimeValuePair != null && currTimeValuePair.getTimestamp() < chunkLimitTime) {
      writeTVPair(currTimeValuePair, chunkWriter, dataType);
      if (currTimeValuePair.getTimestamp() > currDeviceMaxTime) {
        currDeviceMaxTime = currTimeValuePair.getTimestamp();
      }
      currTimeValuePair = unseqReader.hasNext() ? unseqReader.next() : null;
      cnt ++;
    }
    return cnt;
  }

  private int mergeWriteBatch(BatchData batchData, IChunkWriter chunkWriter,
      TSDataType dataType, IPointReader unseqReader) throws IOException {
    int cnt = 0;
    for (int i = 0; i < batchData.length(); i++) {
      long time = batchData.getTimeByIndex(i);
      // merge data in batch and data in unseqReader
      while (currTimeValuePair != null && currTimeValuePair.getTimestamp() < time) {
        writeTVPair(currTimeValuePair, chunkWriter, dataType);
        currTimeValuePair = unseqReader.hasNext() ? unseqReader.next() : null;
        cnt++;
      }
      if (currTimeValuePair != null && currTimeValuePair.getTimestamp() == time) {
        writeTVPair(currTimeValuePair, chunkWriter, dataType);
        currTimeValuePair = unseqReader.hasNext() ? unseqReader.next() : null;
        cnt++;
      } else {
        writeBatchPoint(batchData, i, dataType, chunkWriter);
        cnt++;
      }
    }
    return cnt;
  }

  private int writeChunkWithoutUnseq(Chunk chunk, IChunkWriter chunkWriter,
      MeasurementSchema measurementSchema) throws IOException {
    ChunkReader chunkReader = new ChunkReaderWithoutFilter(chunk);
    TSDataType dataType = measurementSchema.getType();
    while (chunkReader.hasNextBatch()) {
      BatchData batchData = chunkReader.nextBatch();
      for (int i = 0; i < batchData.length(); i++) {
        writeBatchPoint(batchData, i, dataType, chunkWriter);
      }
    }
    return chunk.getHeader().getNumOfPages();
  }

  private TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getFile().getPath());
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  private List<Modification> getModifications(TsFileResource tsFileResource, Path path) {
    List<Modification> modifications = modificationCache.computeIfAbsent(tsFileResource,
        tsFileResource1 -> (List<Modification>) tsFileResource.getModFile().getModifications());
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    // each path is visited only once in a merge, so the modifications can be removed after visiting
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().equals(path)) {
        pathModifications.add(modification);
        modificationIterator.remove();
      }
    }
    return pathModifications;
  }

  protected List<Path> collectPathsInUnseqFiles() throws MetadataErrorException {
    Set<String> devices = new HashSet<>();
    for (TsFileResource tsFileResource : unseqFiles) {
      devices.addAll(tsFileResource.getEndTimeMap().keySet());
    }
    List<Path> paths = new ArrayList<>();
    for (String deviceId : devices) {
      List<String> strPaths = MManager.getInstance().getPaths(deviceId + ".*");
      for (String strPath : strPaths) {
        paths.add(new Path(strPath));
      }
    }
    return paths;
  }

  private MeasurementSchema getSchema(Path path) {
    List<MeasurementSchema> measurementSchemas =
        MManager.getInstance().getSchemaForStorageGroup(path.getDevice());
    for (MeasurementSchema measurementSchema : measurementSchemas) {
      if (measurementSchema.getMeasurementId().equals(path.getMeasurement())) {
        return measurementSchema;
      }
    }
    return null;
  }

  protected RestorableTsFileIOWriter getMergeFileWriter(TsFileResource resource) throws IOException {
    RestorableTsFileIOWriter writer = fileWriterCache.get(resource);
    if (writer == null) {
      writer = new RestorableTsFileIOWriter(new File(resource.getFile().getPath() + MERGE_SUFFIX));
      fileWriterCache.put(resource, writer);
    }
    return writer;
  }

  protected MetadataQuerier getMetadataQuerier(TsFileResource seqFile) throws IOException {
    MetadataQuerier metadataQuerier = metadataQuerierCache.get(seqFile);
    if (metadataQuerier == null) {
      metadataQuerier = new MetadataQuerierByFileImpl(getFileReader(seqFile));
      metadataQuerierCache.put(seqFile, metadataQuerier);
    }
    return metadataQuerier;
  }

  protected List<ChunkMetaData> queryChunkMetadata(Path path, TsFileResource seqFile)
      throws IOException {
    MetadataQuerier metadataQuerier = getMetadataQuerier(seqFile);
    List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
    if (!keepChunkMetadata) {
      metadataQuerier.clear();
    }
    return chunkMetaDataList;
  }

  private IPointReader getUnseqReader(Path path) throws IOException {
    return SeriesReaderFactoryImpl.getInstance().createUnseqSeriesReader(path, unseqFiles,
        mergeContext, null);
  }

  private IChunkWriter getChunkWriter(MeasurementSchema measurementSchema) {
    return  new ChunkWriterImpl(measurementSchema,
        new ChunkBuffer(measurementSchema), MIN_CHUNK_POINT_NUM);
  }
}
