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
package org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.compaction.execute.task.subtask.FastCompactionTaskSummary;
import org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.element.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.element.FileElement;
import org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.element.PageElement;
import org.apache.iotdb.db.engine.compaction.execute.utils.writer.AbstractCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.ModificationUtils;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NonAlignedSeriesCompactionExecutor extends SeriesCompactionExecutor {
  private boolean hasStartMeasurement = false;

  // tsfile resource -> timeseries metadata <startOffset, endOffset>
  // used to get the chunk metadatas from tsfile directly according to timeseries metadata offset.
  private Map<TsFileResource, Pair<Long, Long>> timeseriesMetadataOffsetMap;

  // it is used to initialize the fileList when compacting a new series
  private final List<TsFileResource> sortResources;

  public NonAlignedSeriesCompactionExecutor(
      AbstractCompactionWriter compactionWriter,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      String deviceId,
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

  /** Deserialize files into chunk metadatas and put them into the chunk metadata queue. */
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

      if (iChunkMetadataList.size() > 0) {
        // modify chunk metadatas
        ModificationUtils.modifyChunkMetaData(
            iChunkMetadataList,
            getModificationsFromCache(
                resource,
                new PartialPath(deviceId, iChunkMetadataList.get(0).getMeasurementUid())));
        if (iChunkMetadataList.size() == 0) {
          // all chunks has been deleted in this file, just remove it
          removeFile(fileElement);
        }
      }

      for (int i = 0; i < iChunkMetadataList.size(); i++) {
        IChunkMetadata chunkMetadata = iChunkMetadataList.get(i);

        // add into queue
        chunkMetadataQueue.add(
            new ChunkMetadataElement(
                chunkMetadata,
                resource.getVersion(),
                i == iChunkMetadataList.size() - 1,
                fileElement));
      }
    }
  }

  /** Deserialize chunk into pages without uncompressing and put them into the page queue. */
  void deserializeChunkIntoPageQueue(ChunkMetadataElement chunkMetadataElement) throws IOException {
    updateSummary(chunkMetadataElement, ChunkStatus.DESERIALIZE_CHUNK);
    Chunk chunk = chunkMetadataElement.chunk;
    ChunkReader chunkReader = new ChunkReader(chunk);
    ByteBuffer chunkDataBuffer = chunk.getData();
    ChunkHeader chunkHeader = chunk.getHeader();
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      ByteBuffer compressedPageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);

      boolean isLastPage = chunkDataBuffer.remaining() <= 0;
      pageQueue.add(
          new PageElement(
              pageHeader,
              compressedPageData,
              chunkReader,
              chunkMetadataElement,
              isLastPage,
              chunkMetadataElement.priority));
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
      // for nonAligned sensors, only after getting chunkMetadatas can we create schema to start
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
      compactionWriter.startMeasurement(Collections.singletonList(schema), subTaskId);
      hasStartMeasurement = true;
    }
  }

  /**
   * NONE_DELETED means that no data on this page has been deleted. <br>
   * PARTIAL_DELETED means that there is data on this page been deleted. <br>
   * ALL_DELETED means that all data on this page has been deleted.
   */
  protected ModifiedStatus isPageModified(PageElement pageElement) {
    long startTime = pageElement.startTime;
    long endTime = pageElement.pageHeader.getEndTime();
    return checkIsModified(
        startTime, endTime, pageElement.chunkMetadataElement.chunkMetadata.getDeleteIntervalList());
  }
}
