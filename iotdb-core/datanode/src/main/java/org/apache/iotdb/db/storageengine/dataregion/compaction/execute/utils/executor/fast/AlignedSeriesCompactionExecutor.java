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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.FileElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.PageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.LazyChunkLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader.LazyPageLoader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlignedSeriesCompactionExecutor extends SeriesCompactionExecutor {

  // measurementID -> tsfile resource -> timeseries metadata <startOffset, endOffset>
  // linked hash map, which has the same measurement lexicographical order as measurementSchemas.
  // used to get the chunk metadatas from tsfile directly according to timeseries metadata offset.
  private final Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap;

  private final List<IMeasurementSchema> measurementSchemas;
  private final IMeasurementSchema timeColumnMeasurementSchema;
  private final Map<String, IMeasurementSchema> measurementSchemaMap;

  @SuppressWarnings("squid:S107")
  public AlignedSeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      String deviceId,
      int subTaskId,
      List<IMeasurementSchema> measurementSchemas,
      FastCompactionTaskSummary summary) {
    super(
        compactionWriter, readerCacheMap, modificationCacheMap, deviceId, true, subTaskId, summary);
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    this.measurementSchemas = measurementSchemas;
    this.timeColumnMeasurementSchema = measurementSchemas.get(0);
    this.measurementSchemaMap = new HashMap<>();
    this.measurementSchemas.forEach(
        schema -> measurementSchemaMap.put(schema.getMeasurementId(), schema));
    // get source files which are sorted by the startTime of current device from old to new,
    // files that do not contain the current device have been filtered out as well.
    sortedSourceFiles.forEach(x -> fileList.add(new FileElement(x)));
  }

  @Override
  public void execute()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    compactionWriter.startMeasurement(measurementSchemas, subTaskId);
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

      // read time chunk metadatas and value chunk metadatas in the current file
      List<IChunkMetadata> timeChunkMetadatas = new ArrayList<>();
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
            valueChunkMetadatas.add(
                readerCacheMap
                    .get(resource)
                    .getChunkMetadataListByTimeseriesMetadataOffset(
                        timeseriesOffsetInCurrentFile.left, timeseriesOffsetInCurrentFile.right));
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

        // get value modifications of this file
        List<List<Modification>> valueModifications = new ArrayList<>();
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
                              resource, new PartialPath(deviceId, x.getMeasurementUid())));
                    }
                  } catch (IllegalPathException e) {
                    throw new RuntimeException(e);
                  }
                });

        // modify aligned chunk metadatas
        ModificationUtils.modifyAlignedChunkMetaData(alignedChunkMetadataList, valueModifications);
      }

      if (alignedChunkMetadataList.isEmpty()) {
        // all chunks has been deleted in this file or current file does not contain this aligned
        // device, just remove it
        removeFile(fileElement);
      }

      // put aligned chunk metadatas into queue
      for (int i = 0; i < alignedChunkMetadataList.size(); i++) {
        chunkMetadataQueue.add(
            new ChunkMetadataElement(
                alignedChunkMetadataList.get(i),
                resource.getVersion(),
                i == alignedChunkMetadataList.size() - 1,
                fileElement));
      }
    }
  }

  /**
   * Deserialize chunk into pages without uncompressing and put them into the page queue.
   *
   * @throws IOException if io errors occurred
   */
  @SuppressWarnings("squid:S3776")
  void deserializeChunkIntoPageQueue(ChunkMetadataElement chunkMetadataElement) throws IOException {
    updateSummary(chunkMetadataElement, ChunkStatus.DESERIALIZE_CHUNK);
    // deserialize time chunk
    ChunkMetadata timeChunkMetadata = chunkMetadataElement.timeChunkLoader.getChunkMetadata();
    ChunkHeader chunkHeader = chunkMetadataElement.timeChunkLoader.loadChunkHeader();

    CompactionTsFileReader reader =
        (CompactionTsFileReader) readerCacheMap.get(chunkMetadataElement.fileElement.resource);
    List<LazyPageLoader> timePageLoaders =
        reader.getLazyPageLoadersOfChunk(timeChunkMetadata, chunkHeader);

    // deserialize value chunks
    List<LazyChunkLoader> valueChunkLoaders = chunkMetadataElement.valueChunkLoaders;
    List<List<LazyPageLoader>> valuePageLoaders = new ArrayList<>();

    for (int i = 0; i < valueChunkLoaders.size(); i++) {
      LazyChunkLoader valueChunkLoader = valueChunkLoaders.get(i);
      if (valueChunkLoader.isEmpty()) {
        // value chunk has been deleted completely
        valuePageLoaders.add(Collections.emptyList());
        continue;
      }
      ChunkMetadata valueChunkMetadata = valueChunkLoader.getChunkMetadata();
      ChunkHeader valueChunkHeader = valueChunkLoader.loadChunkHeader();
      valuePageLoaders.add(reader.getLazyPageLoadersOfChunk(valueChunkMetadata, valueChunkHeader));
    }

    // add aligned pages into page queue
    for (int i = 0; i < timePageLoaders.size(); i++) {
      List<LazyPageLoader> alignedPageLoaders = new ArrayList<>();
      for (int j = 0; j < valuePageLoaders.size(); j++) {
        if (valuePageLoaders.get(j).isEmpty()) {
          alignedPageLoaders.add(new LazyPageLoader());
          continue;
        }
        alignedPageLoaders.add(valuePageLoaders.get(j).get(i));
      }
      pageQueue.add(
          new PageElement(
              timePageLoaders.get(i).getPageHeader(),
              timePageLoaders.get(i),
              alignedPageLoaders,
              chunkMetadataElement,
              i == timePageLoaders.size() - 1,
              chunkMetadataElement.priority));
    }
    chunkMetadataElement.clearChunks();
  }

  @Override
  void readChunk(ChunkMetadataElement chunkMetadataElement) throws IOException {
    updateSummary(chunkMetadataElement, ChunkStatus.READ_IN);
    AlignedChunkMetadata alignedChunkMetadata =
        (AlignedChunkMetadata) chunkMetadataElement.chunkMetadata;
    LazyChunkLoader timeChunkLoader =
        new LazyChunkLoader(
            readerCacheMap.get(chunkMetadataElement.fileElement.resource),
            (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
    List<LazyChunkLoader> valueChunkLoaders =
        new ArrayList<>(alignedChunkMetadata.getValueChunkMetadataList().size());
    for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (valueChunkMetadata == null || valueChunkMetadata.getStatistics().getCount() == 0) {
        valueChunkLoaders.add(new LazyChunkLoader());
      } else {
        valueChunkLoaders.add(
            new LazyChunkLoader(
                readerCacheMap.get(chunkMetadataElement.fileElement.resource),
                (ChunkMetadata) valueChunkMetadata));
      }
    }
    chunkMetadataElement.timeChunkLoader = timeChunkLoader;
    chunkMetadataElement.valueChunkLoaders = valueChunkLoaders;
    setForceDecoding(chunkMetadataElement);
  }

  void setForceDecoding(ChunkMetadataElement chunkMetadataElement) throws IOException {
    ChunkHeader timeChunkHeader = chunkMetadataElement.timeChunkLoader.loadChunkHeader();
    if (timeColumnMeasurementSchema.getCompressor() != timeChunkHeader.getCompressionType()
        || timeColumnMeasurementSchema.getEncodingType() != timeChunkHeader.getEncodingType()) {
      chunkMetadataElement.needForceDecoding = true;
      return;
    }
    for (LazyChunkLoader valueChunkLoader : chunkMetadataElement.valueChunkLoaders) {
      if (valueChunkLoader.isEmpty()) {
        continue;
      }
      ChunkHeader header = valueChunkLoader.loadChunkHeader();
      String measurementId = header.getMeasurementID();
      IMeasurementSchema measurementSchema = measurementSchemaMap.get(measurementId);
      if (measurementSchema == null) {
        continue;
      }
      if (measurementSchema.getCompressor() != header.getCompressionType()
          || measurementSchema.getEncodingType() != header.getEncodingType()) {
        chunkMetadataElement.needForceDecoding = true;
        return;
      }
    }
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
    long startTime = pageElement.startTime;
    long endTime = pageElement.pageHeader.getEndTime();
    AlignedChunkMetadata alignedChunkMetadata =
        (AlignedChunkMetadata) pageElement.chunkMetadataElement.chunkMetadata;
    ModifiedStatus lastPageStatus = null;
    for (IChunkMetadata valueChunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      ModifiedStatus currentPageStatus =
          valueChunkMetadata == null
              ? ModifiedStatus.ALL_DELETED
              : checkIsModified(startTime, endTime, valueChunkMetadata.getDeleteIntervalList());
      if (currentPageStatus == ModifiedStatus.PARTIAL_DELETED) {
        // one of the value pages exist data been deleted partially
        return ModifiedStatus.PARTIAL_DELETED;
      }
      if (lastPageStatus == null) {
        // first page
        lastPageStatus = currentPageStatus;
        continue;
      }
      if (!lastPageStatus.equals(currentPageStatus)) {
        // there are at least two value pages, one is that all data is deleted, the other is that no
        // data is deleted
        lastPageStatus = ModifiedStatus.NONE_DELETED;
      }
    }
    return lastPageStatus;
  }
}
