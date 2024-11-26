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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionPathUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.AlignedSeriesBatchCompactionUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.AlignedPageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.FileElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.PageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader.CompactionAlignedChunkReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader.CompactionChunkReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FastAlignedSeriesCompactionExecutor extends SeriesCompactionExecutor {

  // measurementID -> tsfile resource -> timeseries metadata <startOffset, endOffset>
  // linked hash map, which has the same measurement lexicographical order as measurementSchemas.
  // used to get the chunk metadatas from tsfile directly according to timeseries metadata offset.
  protected final Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap;

  protected final List<IMeasurementSchema> measurementSchemas;
  protected final IMeasurementSchema timeColumnMeasurementSchema;
  protected final Map<String, IMeasurementSchema> measurementSchemaMap;
  protected final boolean ignoreAllNullRows;

  @SuppressWarnings("squid:S107")
  public FastAlignedSeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<String, PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer>>
          modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      IDeviceID deviceId,
      int subTaskId,
      List<IMeasurementSchema> measurementSchemas,
      FastCompactionTaskSummary summary,
      boolean ignoreAllNullRows) {
    super(
        compactionWriter, readerCacheMap, modificationCacheMap, deviceId, true, subTaskId, summary);
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.measurementSchemas = measurementSchemas;
    this.timeColumnMeasurementSchema = measurementSchemas.get(0);
    this.measurementSchemaMap = new HashMap<>();
    this.measurementSchemas.forEach(
        schema -> measurementSchemaMap.put(schema.getMeasurementName(), schema));
    this.ignoreAllNullRows = ignoreAllNullRows;
    // get source files which are sorted by the startTime of current device from old to new,
    // files that do not contain the current device have been filtered out as well.
    sortedSourceFiles.forEach(x -> fileList.add(new FileElement(x)));
  }

  @Override
  public void execute()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    compactionWriter.startMeasurement(
        TsFileConstant.TIME_COLUMN_ID,
        new AlignedChunkWriterImpl(measurementSchemas.remove(0), measurementSchemas),
        subTaskId);
    compactFiles();
    compactionWriter.endMeasurement(subTaskId);
  }

  @Override
  protected void compactFiles()
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    markStartOfAlignedSeries();

    while (!fileList.isEmpty()) {
      List<FileElement> overlappedFiles = findOverlapFiles(fileList.get(0));

      // read chunk metadatas from files and put them into chunk metadata queue
      deserializeFileIntoChunkMetadataQueue(overlappedFiles);

      compactChunks();
    }

    markEndOfAlignedSeries();
  }

  private void markStartOfAlignedSeries() {
    for (TsFileSequenceReader reader : readerCacheMap.values()) {
      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markStartOfAlignedSeries();
      }
    }
  }

  private void markEndOfAlignedSeries() {
    for (TsFileSequenceReader reader : readerCacheMap.values()) {
      if (reader instanceof CompactionTsFileReader) {
        ((CompactionTsFileReader) reader).markEndOfAlignedSeries();
      }
    }
  }

  /**
   * Deserialize files into chunk metadatas and put them into the chunk metadata queue.
   *
   * @throws IOException if io errors occurred
   * @throws IllegalPathException if the file path is illegal
   */
  @SuppressWarnings("squid:S3776")
  void deserializeFileIntoChunkMetadataQueue(List<FileElement> fileElements)
      throws IOException, IllegalPathException {
    for (FileElement fileElement : fileElements) {
      TsFileResource resource = fileElement.resource;
      List<AlignedChunkMetadata> alignedChunkMetadataList = getAlignedChunkMetadataList(resource);

      if (alignedChunkMetadataList.isEmpty()) {
        // all chunks have been deleted in this file or current file does not contain this aligned
        // device, just remove it
        removeFile(fileElement);
      }

      // put aligned chunk metadatas into queue
      for (int i = 0; i < alignedChunkMetadataList.size(); i++) {
        chunkMetadataQueue.add(
            new ChunkMetadataElement(
                alignedChunkMetadataList.get(i),
                i == alignedChunkMetadataList.size() - 1,
                fileElement));
      }
    }
  }

  protected List<AlignedChunkMetadata> getAlignedChunkMetadataList(TsFileResource resource)
      throws IOException, IllegalPathException {
    // read time chunk metadatas and value chunk metadatas in the current file
    List<IChunkMetadata> timeChunkMetadatas = null;
    List<List<IChunkMetadata>> valueChunkMetadatas = new ArrayList<>();
    for (Map.Entry<String, Map<TsFileResource, Pair<Long, Long>>> entry :
        timeseriesMetadataOffsetMap.entrySet()) {
      String measurementID = entry.getKey();
      Pair<Long, Long> timeseriesOffsetInCurrentFile = entry.getValue().get(resource);
      if (measurementID.equals("")) {
        // read time chunk metadatas
        if (timeseriesOffsetInCurrentFile == null) {
          // current file does not contain this aligned device
          timeChunkMetadatas = null;
          break;
        }
        timeChunkMetadatas =
            readerCacheMap
                .get(resource)
                .getChunkMetadataListByTimeseriesMetadataOffset(
                    timeseriesOffsetInCurrentFile.left, timeseriesOffsetInCurrentFile.right);
      } else {
        // read value chunk metadatas
        if (timeseriesOffsetInCurrentFile == null) {
          // current file does not contain this aligned timeseries
          valueChunkMetadatas.add(null);
        } else {
          // current file contains this aligned timeseries
          List<IChunkMetadata> valueColumnChunkMetadataList =
              readerCacheMap
                  .get(resource)
                  .getChunkMetadataListByTimeseriesMetadataOffset(
                      timeseriesOffsetInCurrentFile.left, timeseriesOffsetInCurrentFile.right);
          if (isValueChunkDataTypeMatchSchema(valueColumnChunkMetadataList)) {
            valueChunkMetadatas.add(valueColumnChunkMetadataList);
          } else {
            valueChunkMetadatas.add(null);
          }
        }
      }
    }

    List<AlignedChunkMetadata> alignedChunkMetadataList = new ArrayList<>();
    // if current file contains this aligned device,then construct aligned chunk metadatas
    if (timeChunkMetadatas != null) {
      for (int i = 0; i < timeChunkMetadatas.size(); i++) {
        List<IChunkMetadata> valueChunkMetadataList = new ArrayList<>();
        for (List<IChunkMetadata> chunkMetadata : valueChunkMetadatas) {
          if (chunkMetadata == null) {
            valueChunkMetadataList.add(null);
          } else {
            valueChunkMetadataList.add(chunkMetadata.get(i));
          }
        }
        AlignedChunkMetadata alignedChunkMetadata =
            new AlignedChunkMetadata(timeChunkMetadatas.get(i), valueChunkMetadataList);

        alignedChunkMetadataList.add(alignedChunkMetadata);
      }

      // get time modifications of this file
      List<ModEntry> timeModifications =
          getModificationsFromCache(
              resource, CompactionPathUtils.getPath(deviceId, AlignedPath.VECTOR_PLACEHOLDER));
      // get value modifications of this file
      List<List<ModEntry>> valueModifications = new ArrayList<>();
      alignedChunkMetadataList
          .get(0)
          .getValueChunkMetadataList()
          .forEach(
              x -> {
                try {
                  if (x == null) {
                    valueModifications.add(null);
                  } else {
                    valueModifications.add(
                        getModificationsFromCache(
                            resource,
                            CompactionPathUtils.getPath(deviceId, x.getMeasurementUid())));
                  }
                } catch (IllegalPathException e) {
                  throw new RuntimeException(e);
                }
              });

      // modify aligned chunk metadatas
      ModificationUtils.modifyAlignedChunkMetaData(
          alignedChunkMetadataList, timeModifications, valueModifications, ignoreAllNullRows);
    }
    return alignedChunkMetadataList;
  }

  private boolean isValueChunkDataTypeMatchSchema(
      List<IChunkMetadata> chunkMetadataListOfOneValueColumn) {
    for (IChunkMetadata chunkMetadata : chunkMetadataListOfOneValueColumn) {
      if (chunkMetadata == null) {
        continue;
      }
      String measurement = chunkMetadata.getMeasurementUid();
      IMeasurementSchema schema = measurementSchemaMap.get(measurement);
      return schema.getType() == chunkMetadata.getDataType();
    }
    return true;
  }

  /**
   * Deserialize chunk into pages without uncompressing and put them into the page queue.
   *
   * @throws IOException if io errors occurred
   */
  @SuppressWarnings("squid:S3776")
  protected void deserializeChunkIntoPageQueue(ChunkMetadataElement chunkMetadataElement)
      throws IOException {
    updateSummary(chunkMetadataElement, ChunkStatus.DESERIALIZE_CHUNK);

    // deserialize time chunk
    Chunk timeChunk = chunkMetadataElement.chunk;

    CompactionChunkReader chunkReader = new CompactionChunkReader(timeChunk);
    List<Pair<PageHeader, ByteBuffer>> timePages = chunkReader.readPageDataWithoutUncompressing();

    // deserialize value chunks
    List<List<Pair<PageHeader, ByteBuffer>>> valuePagesList = new ArrayList<>();
    List<Chunk> valueChunks = chunkMetadataElement.valueChunks;
    for (int i = 0; i < valueChunks.size(); i++) {
      Chunk valueChunk = valueChunks.get(i);
      if (valueChunk == null) {
        // value chunk has been deleted completely
        valuePagesList.add(null);
        continue;
      }

      chunkReader = new CompactionChunkReader(valueChunk);
      List<Pair<PageHeader, ByteBuffer>> valuesPages =
          chunkReader.readPageDataWithoutUncompressing();
      valuePagesList.add(valuesPages);
    }

    // add aligned pages into page queue
    for (int i = 0; i < timePages.size(); i++) {
      List<PageHeader> alignedPageHeaders = new ArrayList<>();
      List<ByteBuffer> alignedPageDatas = new ArrayList<>();
      for (int j = 0; j < valuePagesList.size(); j++) {
        if (valuePagesList.get(j) == null) {
          alignedPageHeaders.add(null);
          alignedPageDatas.add(null);
          continue;
        }
        Pair<PageHeader, ByteBuffer> valuePage = valuePagesList.get(j).get(i);
        alignedPageHeaders.add(valuePage == null ? null : valuePage.left);
        alignedPageDatas.add(valuePage == null ? null : valuePage.right);
      }

      AlignedPageElement alignedPageElement =
          new AlignedPageElement(
              timePages.get(i).left,
              alignedPageHeaders,
              timePages.get(i).right,
              alignedPageDatas,
              new CompactionAlignedChunkReader(timeChunk, valueChunks, ignoreAllNullRows),
              chunkMetadataElement,
              i == timePages.size() - 1,
              isBatchedCompaction);
      pageQueue.add(alignedPageElement);
    }
    chunkMetadataElement.clearChunks();
  }

  @Override
  void readChunk(ChunkMetadataElement chunkMetadataElement) throws IOException {
    updateSummary(chunkMetadataElement, ChunkStatus.READ_IN);
    AlignedChunkMetadata alignedChunkMetadata =
        (AlignedChunkMetadata) chunkMetadataElement.chunkMetadata;
    TsFileSequenceReader reader = readerCacheMap.get(chunkMetadataElement.fileElement.resource);
    chunkMetadataElement.chunk =
        readChunk(reader, (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
    List<Chunk> valueChunks = new ArrayList<>();
    for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (valueChunkMetadata == null || valueChunkMetadata.getStatistics().getCount() == 0) {
        // value chunk has been deleted completely or is empty value chunk
        valueChunks.add(null);
        continue;
      }
      valueChunks.add(readChunk(reader, (ChunkMetadata) valueChunkMetadata));
    }
    chunkMetadataElement.valueChunks = valueChunks;
    setForceDecoding(chunkMetadataElement);
  }

  protected Chunk readChunk(TsFileSequenceReader reader, ChunkMetadata chunkMetadata)
      throws IOException {
    return reader.readMemChunk(chunkMetadata);
  }

  @Override
  protected boolean flushChunkToCompactionWriter(ChunkMetadataElement chunkMetadataElement)
      throws IOException {
    return compactionWriter.flushAlignedChunk(chunkMetadataElement, subTaskId);
  }

  void setForceDecoding(ChunkMetadataElement chunkMetadataElement) {
    if (timeColumnMeasurementSchema.getCompressor()
            != chunkMetadataElement.chunk.getHeader().getCompressionType()
        || timeColumnMeasurementSchema.getEncodingType()
            != chunkMetadataElement.chunk.getHeader().getEncodingType()) {
      chunkMetadataElement.needForceDecodingPage = true;
      return;
    }
    for (Chunk chunk : chunkMetadataElement.valueChunks) {
      if (chunk == null) {
        continue;
      }
      ChunkHeader header = chunk.getHeader();
      String measurementId = header.getMeasurementID();
      IMeasurementSchema measurementSchema = measurementSchemaMap.get(measurementId);
      if (measurementSchema == null) {
        continue;
      }
      if (measurementSchema.getCompressor() != header.getCompressionType()
          || measurementSchema.getEncodingType() != header.getEncodingType()) {
        chunkMetadataElement.needForceDecodingPage = true;
        return;
      }
    }
  }

  @Override
  protected boolean flushPageToCompactionWriter(PageElement pageElement)
      throws PageException, IOException {
    AlignedPageElement alignedPageElement = (AlignedPageElement) pageElement;
    return compactionWriter.flushAlignedPage(alignedPageElement, subTaskId);
  }

  /**
   * NONE_DELETED means that no data on this page has been deleted. <br>
   * PARTIAL_DELETED means that there is data on this page been deleted. <br>
   * ALL_DELETED means that all data on this page has been deleted.
   *
   * <p>Notice: If is aligned page, return ALL_DELETED if and only if all value pages are deleted.
   * Return NONE_DELETED if and only if no data exists on all value pages is deleted
   */
  protected ModifiedStatus isPageModified(PageElement pageElement) {
    long startTime = pageElement.getStartTime();
    long endTime = pageElement.getEndTime();
    AlignedChunkMetadata alignedChunkMetadata =
        (AlignedChunkMetadata) pageElement.getChunkMetadataElement().chunkMetadata;
    return AlignedSeriesBatchCompactionUtils.calculateAlignedPageModifiedStatus(
        startTime, endTime, alignedChunkMetadata, ignoreAllNullRows);
  }
}
