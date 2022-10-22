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
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlignedFastCompactionPerformerSubTask extends FastCompactionPerformerSubTask {
  private List<IMeasurementSchema> measurementSchemas;

  public AlignedFastCompactionPerformerSubTask(
      FastCrossCompactionWriter compactionWriter,
      Map<String, Map<TsFileResource, Pair<Long, Long>>> timeseriesMetadataOffsetMap,
      List<IMeasurementSchema> measurementSchemas,
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
        true,
        subTaskId);
    this.measurementSchemas = measurementSchemas;
  }

  @Override
  protected void startMeasurement() {
    throw new RuntimeException("Does not support startMeasruement methods");
  }

  @Override
  public Void call()
      throws IOException, PageException, WriteProcessException, IllegalPathException {
    // get source files which are sorted by the startTime of current device from old to new, files
    // that do not contain the current device have been filtered out as well.
    sortedSourceFiles.forEach(x -> fileList.add(new FileElement(x)));

    compactionWriter.startMeasurement(measurementSchemas, subTaskId);
    compactFiles();
    compactionWriter.endMeasurement(subTaskId);

    return null;
  }

  /** Deserialize files into chunk metadatas and put them into the chunk metadata queue. */
  void deserializeFileIntoQueue(List<FileElement> fileElements)
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

          // set file path
          alignedChunkMetadata.setFilePath(resource.getTsFilePath());
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
        QueryUtils.modifyAlignedChunkMetaData(alignedChunkMetadataList, valueModifications);
      }

      if (alignedChunkMetadataList.size() == 0) {
        // all chunks has been deleted in this file or current file does not contain this aligned
        // device, just remove it
        removeFile(fileElement);
      }

      // put aligned chunk metadatas into queue
      for (int i = 0; i < alignedChunkMetadataList.size(); i++) {
        chunkMetadataQueue.add(
            new ChunkMetadataElement(
                alignedChunkMetadataList.get(i),
                (int) resource.getVersion(),
                i == alignedChunkMetadataList.size() - 1,
                fileElement));
      }
    }
  }

  /** Deserialize chunk into pages without uncompressing and put them into the page queue. */
  void deserializeChunkIntoQueue(ChunkMetadataElement chunkMetadataElement) throws IOException {
    AlignedChunkMetadata alignedChunkMetadata =
        (AlignedChunkMetadata) chunkMetadataElement.chunkMetadata;

    List<PageHeader> timePageHeaders = new ArrayList<>();
    List<ByteBuffer> compressedTimePageDatas = new ArrayList<>();
    List<List<PageHeader>> valuePageHeaders = new ArrayList<>();
    List<List<ByteBuffer>> compressedValuePageDatas = new ArrayList<>();

    // deserialize time chunk
    ChunkMetadata chunkMetadata = (ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata();
    Chunk timeChunk = ChunkCache.getInstance().get(chunkMetadata);
    ChunkReader chunkReader = new ChunkReader(timeChunk);
    ByteBuffer chunkDataBuffer = timeChunk.getData();
    ChunkHeader chunkHeader = timeChunk.getHeader();
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, timeChunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      ByteBuffer compressedPageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);
      timePageHeaders.add(pageHeader);
      compressedTimePageDatas.add(compressedPageData);
    }

    // deserialize value chunks
    List<Chunk> valueChunks = new ArrayList<>();
    for (int i = 0; i < alignedChunkMetadata.getValueChunkMetadataList().size(); i++) {
      chunkMetadata = (ChunkMetadata) alignedChunkMetadata.getValueChunkMetadataList().get(i);
      if (chunkMetadata == null) {
        // value chunk has been deleted completely
        valuePageHeaders.add(null);
        compressedValuePageDatas.add(null);
        valueChunks.add(null);
        continue;
      }
      Chunk valueChunk = ChunkCache.getInstance().get(chunkMetadata);
      chunkReader = new ChunkReader(valueChunk);
      chunkDataBuffer = valueChunk.getData();
      chunkHeader = valueChunk.getHeader();
      valueChunks.add(valueChunk);

      valuePageHeaders.add(new ArrayList<>());
      compressedValuePageDatas.add(new ArrayList<>());
      while (chunkDataBuffer.remaining() > 0) {
        // deserialize a PageHeader from chunkDataBuffer
        PageHeader pageHeader;
        if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, valueChunk.getChunkStatistic());
        } else {
          pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        }
        ByteBuffer compressedPageData = chunkReader.readPageDataWithoutUncompressing(pageHeader);
        valuePageHeaders.get(i).add(pageHeader);
        compressedValuePageDatas.get(i).add(compressedPageData);
      }
    }

    // add aligned pages into page queue
    for (int i = 0; i < timePageHeaders.size(); i++) {
      List<PageHeader> alignedPageHeaders = new ArrayList<>();
      List<ByteBuffer> alignedPageDatas = new ArrayList<>();
      for (int j = 0; j < valuePageHeaders.size(); j++) {
        if (valuePageHeaders.get(j) == null) {
          alignedPageHeaders.add(null);
          alignedPageDatas.add(null);
          continue;
        }
        alignedPageHeaders.add(valuePageHeaders.get(j).get(i));
        alignedPageDatas.add(compressedValuePageDatas.get(j).get(i));
      }
      pageQueue.add(
          new PageElement(
              timePageHeaders.get(i),
              alignedPageHeaders,
              compressedTimePageDatas.get(i),
              alignedPageDatas,
              new AlignedChunkReader(timeChunk, valueChunks, null),
              chunkMetadataElement,
              i == timePageHeaders.size() - 1,
              chunkMetadataElement.priority));
    }
  }

  /**
   * -1 means that no data on this page has been deleted. <br>
   * 0 means that there is data on this page been deleted. <br>
   * 1 means that all data on this page has been deleted.
   *
   * <p>Notice: If is aligned page, return 1 if and only if all value pages are deleted. Return -1
   * if value page has no data or all data being deleted.
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
              ? ModifiedStatus.AllDeleted
              : checkIsModified(startTime, endTime, valueChunkMetadata.getDeleteIntervalList());
      if (currentPageStatus == ModifiedStatus.PartialDeleted) {
        // one of the value pages exist data been deleted partially
        return ModifiedStatus.PartialDeleted;
      }
      if (lastPageStatus == null) {
        // first page
        lastPageStatus = currentPageStatus;
        continue;
      }
      if (!currentPageStatus.equals(lastPageStatus)) {
        // there are at least two value pages, one is that all data is deleted, the other is that no
        // data is deleted
        lastPageStatus = ModifiedStatus.NoneDeleted;
      }
    }
    return lastPageStatus;
  }
}
