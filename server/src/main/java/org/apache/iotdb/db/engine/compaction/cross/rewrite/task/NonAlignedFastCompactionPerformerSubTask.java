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
package org.apache.iotdb.db.engine.compaction.cross.rewrite.task;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.compaction.cross.utils.ChunkMetadataElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.FileElement;
import org.apache.iotdb.db.engine.compaction.cross.utils.PageElement;
import org.apache.iotdb.db.engine.compaction.writer.FastCrossCompactionWriter;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.QueryUtils;
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

public class NonAlignedFastCompactionPerformerSubTask extends FastCompactionPerformerSubTask {
  // measurements of the current device to be compacted, which is assigned to the current sub thread
  private final List<String> measurements;

  private String currentMeasurement;

  boolean hasStartMeasurement = false;

  public NonAlignedFastCompactionPerformerSubTask(
      FastCrossCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      List<String> measurements,
      Map<TsFileResource, TsFileSequenceReader> readerCacheMap,
      Map<TsFileResource, List<Modification>> modificationCacheMap,
      List<TsFileResource> sortedSourceFiles,
      String deviceId,
      int subTaskId) {
    super(
        compactionWriter,
        timeseriesMetadataOffsetMap,
        readerCacheMap,
        modificationCacheMap,
        sortedSourceFiles,
        deviceId,
        false,
        subTaskId);
    this.measurements = measurements;
  }

  @Override
  public Void call()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    for (String measurement : measurements) {
      // get source files which are sorted by the startTime of current device from old to new, files
      // that do not contain the current device have been filtered out as well.
      sortedSourceFiles.forEach(x -> fileList.add(new FileElement(x)));
      currentMeasurement = measurement;
      hasStartMeasurement = false;

      compactFiles();

      if (hasStartMeasurement) {
        compactionWriter.endMeasurement(subTaskId);
      }
    }

    return null;
  }

  protected void startMeasurement() throws IOException {
    if (!hasStartMeasurement && !chunkMetadataQueue.isEmpty()) {
      ChunkMetadataElement firstChunkMetadataElement = chunkMetadataQueue.peek();
      MeasurementSchema measurementSchema =
          readerCacheMap
              .get(firstChunkMetadataElement.fileElement.resource)
              .getMeasurementSchema(
                  Collections.singletonList(firstChunkMetadataElement.chunkMetadata));
      compactionWriter.startMeasurement(Collections.singletonList(measurementSchema), subTaskId);
      hasStartMeasurement = true;
    }
  }

  /** Deserialize files into chunk metadatas and put them into the chunk metadata queue. */
  void deserializeFileIntoQueue(List<FileElement> fileElements)
      throws IOException, IllegalPathException {
    for (FileElement fileElement : fileElements) {
      TsFileResource resource = fileElement.resource;
      Pair<Long, Long> timeseriesMetadataOffset =
          timeseriesMetadataOffsetMap.get(currentMeasurement).get(resource);
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
        QueryUtils.modifyChunkMetaData(
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
        // set file path
        chunkMetadata.setFilePath(resource.getTsFilePath());

        // add into queue
        chunkMetadataQueue.add(
            new ChunkMetadataElement(
                chunkMetadata,
                (int) resource.getVersion(),
                i == iChunkMetadataList.size() - 1,
                fileElement));
      }
    }
  }

  /** Deserialize chunk into pages without uncompressing and put them into the page queue. */
  void deserializeChunkIntoQueue(ChunkMetadataElement chunkMetadataElement) throws IOException {
    Chunk chunk = ChunkCache.getInstance().get((ChunkMetadata) chunkMetadataElement.chunkMetadata);
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
  }

  /**
   * -1 means that no data on this page has been deleted. <br>
   * 0 means that there is data on this page been deleted. <br>
   * 1 means that all data on this page has been deleted.
   */
  protected int isPageModified(PageElement pageElement) {
    long startTime = pageElement.startTime;
    long endTime = pageElement.pageHeader.getEndTime();
    return checkIsModified(
        startTime, endTime, pageElement.chunkMetadataElement.chunkMetadata.getDeleteIntervalList());
  }
}
