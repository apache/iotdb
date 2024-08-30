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
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.CompactionPathUtils;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.FileElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.NonAlignedPageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element.PageElement;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader.CompactionChunkReader;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class FastNonAlignedSeriesCompactionExecutor extends SeriesCompactionExecutor {
  private boolean hasStartMeasurement = false;

  private CompressionType seriesCompressionType = null;

  private TSEncoding seriesTSEncoding = null;

  // tsfile resource -> timeseries metadata <startOffset, endOffset>
  // used to get the chunk metadatas from tsfile directly according to timeseries metadata offset.
  private Map<TsFileResource, Pair<Long, Long>> timeseriesMetadataOffsetMap;

  // it is used to initialize the fileList when compacting a new series
  private final List<TsFileResource> sortResources;

  public FastNonAlignedSeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<String, PatternTreeMap<Modification, PatternTreeMapFactory.ModsSerializer>>
          modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      IDeviceID deviceId,
      int subTaskId,
      FastCompactionTaskSummary summary) {
    super(
        compactionWriter,
        readerCacheMap,
        modificationCacheMap,
        deviceId,
        false,
        subTaskId,
        summary);
    this.sortResources = sortedSourceFiles;
  }

  @Override
  public void execute()
      throws PageException, IllegalPathException, IOException, WriteProcessException {
    compactFiles();
    if (hasStartMeasurement) {
      compactionWriter.endMeasurement(subTaskId);
    }
  }

  public void setNewMeasurement(Map<TsFileResource, Pair<Long, Long>> timeseriesMetadataOffsetMap) {
    this.timeseriesMetadataOffsetMap = timeseriesMetadataOffsetMap;
    // get source files which are sorted by the startTime of current device from old to new,
    // files that do not contain the current device have been filtered out as well.
    sortResources.forEach(x -> fileList.add(new FileElement(x)));
    hasStartMeasurement = false;
  }

  @Override
  protected void compactFiles()
      throws PageException, IOException, WriteProcessException, IllegalPathException {
    while (!fileList.isEmpty()) {
      List<FileElement> overlappedFiles = findOverlapFiles(fileList.get(0));

      // read chunk metadatas from files and put them into chunk metadata queue
      deserializeFileIntoChunkMetadataQueue(overlappedFiles);

      compactChunks();
    }
  }

  /**
   * Deserialize files into chunk metadatas and put them into the chunk metadata queue.
   *
   * @throws IOException if io errors occurred
   * @throws IllegalPathException if the file path is illegal
   */
  @SuppressWarnings("checkstyle:LocalVariableNameCheck")
  void deserializeFileIntoChunkMetadataQueue(List<FileElement> fileElements)
      throws IOException, IllegalPathException {
    for (FileElement fileElement : fileElements) {
      TsFileResource resource = fileElement.resource;
      Pair<Long, Long> timeseriesMetadataOffset = timeseriesMetadataOffsetMap.get(resource);
      if (timeseriesMetadataOffset == null) {
        // tsfile does not contain this timeseries
        removeFile(fileElement);
        continue;
      }
      List<IChunkMetadata> iChunkMetadataList =
          readerCacheMap
              .get(resource)
              .getChunkMetadataListByTimeseriesMetadataOffset(
                  timeseriesMetadataOffset.left, timeseriesMetadataOffset.right);

      if (!iChunkMetadataList.isEmpty()) {
        // modify chunk metadatas
        ModificationUtils.modifyChunkMetaData(
            iChunkMetadataList,
            getModificationsFromCache(
                resource,
                CompactionPathUtils.getPath(
                    deviceId, iChunkMetadataList.get(0).getMeasurementUid())));
        if (iChunkMetadataList.isEmpty()) {
          // all chunks has been deleted in this file, just remove it
          removeFile(fileElement);
        }
      }

      for (int i = 0; i < iChunkMetadataList.size(); i++) {
        IChunkMetadata chunkMetadata = iChunkMetadataList.get(i);

        // add into queue
        chunkMetadataQueue.add(
            new ChunkMetadataElement(
                chunkMetadata, i == iChunkMetadataList.size() - 1, fileElement));
      }
    }
  }

  /** Deserialize chunk into pages without uncompressing and put them into the page queue. */
  protected void deserializeChunkIntoPageQueue(ChunkMetadataElement chunkMetadataElement)
      throws IOException {
    updateSummary(chunkMetadataElement, ChunkStatus.DESERIALIZE_CHUNK);
    Chunk chunk = chunkMetadataElement.chunk;
    CompactionChunkReader chunkReader = new CompactionChunkReader(chunk);
    List<Pair<PageHeader, ByteBuffer>> pages = chunkReader.readPageDataWithoutUncompressing();
    for (int i = 0; i < pages.size(); i++) {
      boolean isLastPage = i == pages.size() - 1;
      pageQueue.add(
          new NonAlignedPageElement(
              pages.get(i).left,
              pages.get(i).right,
              chunkReader,
              chunkMetadataElement,
              isLastPage));
    }
    chunkMetadataElement.clearChunks();
  }

  @Override
  void readChunk(ChunkMetadataElement chunkMetadataElement) throws IOException {
    updateSummary(chunkMetadataElement, ChunkStatus.READ_IN);
    chunkMetadataElement.chunk =
        readerCacheMap
            .get(chunkMetadataElement.fileElement.resource)
            .readMemChunk((ChunkMetadata) chunkMetadataElement.chunkMetadata);

    if (!hasStartMeasurement) {
      // for nonAligned sensors, only after getting chunkMetadatas can we create metadata to
      // start
      // measurement; for aligned sensors, we get all schemas of value sensors and
      // startMeasurement() in the previous process, because we need to get all chunk metadatas of
      // sensors and their schemas under the current device, but since the compaction process is
      // to read a batch of overlapped files each time, which may not contain all the sensors.
      ChunkHeader header = chunkMetadataElement.chunk.getHeader();
      MeasurementSchema schema =
          new MeasurementSchema(
              header.getMeasurementID(),
              header.getDataType(),
              header.getEncodingType(),
              header.getCompressionType());
      compactionWriter.startMeasurement(
          schema.getMeasurementId(), new ChunkWriterImpl(schema, true), subTaskId);
      hasStartMeasurement = true;
      seriesCompressionType = header.getCompressionType();
      seriesTSEncoding = header.getEncodingType();
      chunkMetadataElement.needForceDecodingPage = false;
    } else {
      ChunkHeader header = chunkMetadataElement.chunk.getHeader();
      chunkMetadataElement.needForceDecodingPage =
          header.getCompressionType() != seriesCompressionType
              || header.getEncodingType() != seriesTSEncoding;
    }
  }

  @Override
  protected boolean flushChunkToCompactionWriter(ChunkMetadataElement chunkMetadataElement)
      throws IOException {
    return compactionWriter.flushNonAlignedChunk(
        chunkMetadataElement.chunk, (ChunkMetadata) chunkMetadataElement.chunkMetadata, subTaskId);
  }

  @Override
  protected boolean flushPageToCompactionWriter(PageElement pageElement)
      throws PageException, IOException {
    NonAlignedPageElement nonAlignedPageElement = (NonAlignedPageElement) pageElement;
    return compactionWriter.flushNonAlignedPage(
        nonAlignedPageElement.getPageData(), nonAlignedPageElement.getPageHeader(), subTaskId);
  }

  /**
   * NONE_DELETED means that no data on this page has been deleted. <br>
   * PARTIAL_DELETED means that there is data on this page been deleted. <br>
   * ALL_DELETED means that all data on this page has been deleted.
   */
  protected ModifiedStatus isPageModified(PageElement pageElement) {
    long startTime = pageElement.getStartTime();
    long endTime = pageElement.getEndTime();
    return checkIsModified(
        startTime,
        endTime,
        pageElement.getChunkMetadataElement().chunkMetadata.getDeleteIntervalList());
  }
}
