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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.Modification;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.TsFileRuntimeException;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkGroupHeader;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

public class TsFileSplitter {
  private static final Logger logger = LoggerFactory.getLogger(TsFileSplitter.class);

  private final File tsFile;
  private final Function<TsFileData, Boolean> consumer;
  private Map<Long, IChunkMetadata> offset2ChunkMetadata = new HashMap<>();
  private TreeMap<Long, List<Deletion>> offset2Deletions = new TreeMap<>();
  private Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData = new HashMap<>();
  private Map<Integer, long[]> pageIndex2Times = new HashMap<>();
  private boolean isTimeChunkNeedDecode = true;
  private IDeviceID curDevice = null;
  private boolean isAligned;
  private int timeChunkIndexOfCurrentValueColumn = 0;

  // Maintain the number of times the chunk of each measurement appears.
  private Map<String, Integer> valueColumn2TimeChunkIndex = new HashMap<>();
  // When encountering a value chunk, find the corresponding time chunk index through
  // valueColumn2TimeChunkIndex,
  // and then restore the corresponding context in the following List through time chunk index
  private List<Map<Integer, List<AlignedChunkData>>> pageIndex2ChunkDataList = new ArrayList<>();
  private List<Map<Integer, long[]>> pageIndex2TimesList = null;
  private List<Boolean> isTimeChunkNeedDecodeList = new ArrayList<>();

  public TsFileSplitter(File tsFile, Function<TsFileData, Boolean> consumer) {
    this.tsFile = tsFile;
    this.consumer = consumer;
  }

  @SuppressWarnings({"squid:S3776", "squid:S6541"})
  public void splitTsFileByDataPartition() throws IOException, IllegalStateException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      getAllModification(offset2Deletions);

      if (!checkMagic(reader)) {
        throw new TsFileRuntimeException(
            String.format("Magic String check error when parsing TsFile %s.", tsFile.getPath()));
      }

      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      getChunkMetadata(reader, offset2ChunkMetadata);
      byte marker;
      // It should be noted that time chunk and its corresponding value chunk are not necessarily
      // consecutive in the file.
      // Therefore, every time after consuming a set of AlignedChunkData, we still need to retain
      // some structural information
      // for the corresponding value chunk that may appear later.
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
            processTimeChunkOrNonAlignedChunk(reader, marker);
            if (isAligned) {
              storeTimeChunkContext();
            }
            break;
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            processValueChunk(reader, marker);
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            curDevice = chunkGroupHeader.getDeviceID();
            pageIndex2ChunkDataList = new ArrayList<>();
            pageIndex2TimesList = new ArrayList<>();
            isTimeChunkNeedDecodeList = new ArrayList<>();
            valueColumn2TimeChunkIndex = new HashMap<>();
            timeChunkIndexOfCurrentValueColumn = 0;
            break;
          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }

      consumeAllAlignedChunkData(reader.position(), pageIndex2ChunkData);
      handleModification(offset2Deletions, Long.MAX_VALUE);
    }
  }

  private void processTimeChunkOrNonAlignedChunk(TsFileSequenceReader reader, byte marker)
      throws IOException {
    long chunkOffset = reader.position();
    timeChunkIndexOfCurrentValueColumn = pageIndex2TimesList.size();
    consumeAllAlignedChunkData(chunkOffset, pageIndex2ChunkData);
    handleModification(offset2Deletions, chunkOffset);

    ChunkHeader header = reader.readChunkHeader(marker);
    String measurementId = header.getMeasurementID();
    if (header.getDataSize() == 0) {
      throw new TsFileRuntimeException(
          String.format(
              "Empty Nonaligned Chunk or Time Chunk with offset %d in TsFile %s.",
              chunkOffset, tsFile.getPath()));
    }

    isAligned =
        ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
            == TsFileConstant.TIME_COLUMN_MASK);
    IChunkMetadata chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
    // When loading TsFile with Chunk in data zone but no matched ChunkMetadata
    // at the end of file, this Chunk needs to be skipped.
    if (chunkMetadata == null) {
      reader.readChunk(-1, header.getDataSize());
      return;
    }
    TTimePartitionSlot timePartitionSlot =
        TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getStartTime());
    ChunkData chunkData =
        ChunkData.createChunkData(isAligned, curDevice.toString(), header, timePartitionSlot);

    if (!needDecodeChunk(chunkMetadata)) {
      chunkData.setNotDecode();
      chunkData.writeEntireChunk(reader.readChunk(-1, header.getDataSize()), chunkMetadata);
      if (isAligned) {
        isTimeChunkNeedDecode = false;
        pageIndex2ChunkData
            .computeIfAbsent(1, o -> new ArrayList<>())
            .add((AlignedChunkData) chunkData);
      } else {
        consumeChunkData(measurementId, chunkOffset, chunkData);
      }
      return;
    }

    decodeAndWriteTimeChunkOrNonAlignedChunk(reader, header, chunkMetadata, chunkOffset, chunkData);
  }

  private void decodeAndWriteTimeChunkOrNonAlignedChunk(
      TsFileSequenceReader reader,
      ChunkHeader header,
      IChunkMetadata chunkMetadata,
      long chunkOffset,
      ChunkData chunkData)
      throws IOException {
    String measurementId = header.getMeasurementID();
    TTimePartitionSlot timePartitionSlot = chunkData.getTimePartitionSlot();
    Decoder defaultTimeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
    Decoder valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
    int dataSize = header.getDataSize();
    int pageIndex = 0;
    if (isAligned) {
      isTimeChunkNeedDecode = true;
      pageIndex2Times = new HashMap<>();
    }

    while (dataSize > 0) {
      PageHeader pageHeader =
          reader.readPageHeader(
              header.getDataType(), (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      long pageDataSize = pageHeader.getSerializedPageSize();
      if (!needDecodePage(pageHeader, chunkMetadata)) { // an entire page
        long startTime =
            pageHeader.getStatistics() == null
                ? chunkMetadata.getStartTime()
                : pageHeader.getStartTime();
        TTimePartitionSlot pageTimePartitionSlot =
            TimePartitionUtils.getTimePartitionSlot(startTime);
        if (!timePartitionSlot.equals(pageTimePartitionSlot)) {
          if (!isAligned) {
            consumeChunkData(measurementId, chunkOffset, chunkData);
          }
          timePartitionSlot = pageTimePartitionSlot;
          chunkData =
              ChunkData.createChunkData(isAligned, curDevice.toString(), header, timePartitionSlot);
        }
        if (isAligned) {
          pageIndex2ChunkData
              .computeIfAbsent(pageIndex, o -> new ArrayList<>())
              .add((AlignedChunkData) chunkData);
        }
        chunkData.writeEntirePage(pageHeader, reader.readCompressedPage(pageHeader));
      } else { // split page
        ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
        Pair<long[], Object[]> tvArray =
            decodePage(isAligned, pageData, pageHeader, defaultTimeDecoder, valueDecoder, header);
        long[] times = tvArray.left;
        Object[] values = tvArray.right;
        if (isAligned) {
          pageIndex2Times.put(pageIndex, times);
        }

        int satisfiedLength = 0;
        long endTime =
            timePartitionSlot.getStartTime() + TimePartitionUtils.getTimePartitionInterval();
        for (int i = 0; i < times.length; i++) {
          if (times[i] >= endTime) {
            chunkData.writeDecodePage(times, values, satisfiedLength);
            if (isAligned) {
              pageIndex2ChunkData
                  .computeIfAbsent(pageIndex, o -> new ArrayList<>())
                  .add((AlignedChunkData) chunkData);
            } else {
              consumeChunkData(measurementId, chunkOffset, chunkData);
            }

            timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(times[i]);
            satisfiedLength = 0;
            endTime =
                timePartitionSlot.getStartTime() + TimePartitionUtils.getTimePartitionInterval();
            chunkData =
                ChunkData.createChunkData(
                    isAligned, curDevice.toString(), header, timePartitionSlot);
          }
          satisfiedLength += 1;
        }
        chunkData.writeDecodePage(times, values, satisfiedLength);
        if (isAligned) {
          pageIndex2ChunkData
              .computeIfAbsent(pageIndex, o -> new ArrayList<>())
              .add((AlignedChunkData) chunkData);
        }
      }

      pageIndex += 1;
      dataSize -= pageDataSize;
    }

    if (!isAligned) {
      consumeChunkData(measurementId, chunkOffset, chunkData);
    }
  }

  private void processValueChunk(TsFileSequenceReader reader, byte marker) throws IOException {
    long chunkOffset = reader.position();
    IChunkMetadata chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
    ChunkHeader header = reader.readChunkHeader(marker);
    // When loading TsFile with Chunk in data zone but no matched ChunkMetadata
    // at the end of file, this Chunk needs to be skipped.
    if (chunkMetadata == null) {
      reader.readChunk(-1, header.getDataSize());
      return;
    }
    switchToTimeChunkContextOfCurrentMeasurement(reader, header.getMeasurementID());
    if (header.getDataSize() == 0) {
      handleEmptyValueChunk(header, pageIndex2ChunkData, chunkMetadata, isTimeChunkNeedDecode);
      return;
    }

    if (!isTimeChunkNeedDecode) {
      AlignedChunkData alignedChunkData = pageIndex2ChunkData.get(1).get(0);
      alignedChunkData.addValueChunk(header);
      alignedChunkData.writeEntireChunk(reader.readChunk(-1, header.getDataSize()), chunkMetadata);
      return;
    }

    Set<ChunkData> allChunkData = new HashSet<>();
    int dataSize = header.getDataSize();
    int pageIndex = 0;
    Decoder valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());

    while (dataSize > 0) {
      PageHeader pageHeader =
          reader.readPageHeader(
              header.getDataType(), (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
      List<AlignedChunkData> alignedChunkDataList = pageIndex2ChunkData.get(pageIndex);
      for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
        if (!allChunkData.contains(alignedChunkData)) {
          alignedChunkData.addValueChunk(header);
          allChunkData.add(alignedChunkData);
        }
      }
      if (alignedChunkDataList.size() == 1) { // write entire page
        // write the entire page if it's not an empty page.
        alignedChunkDataList
            .get(0)
            .writeEntirePage(pageHeader, reader.readCompressedPage(pageHeader));
      } else { // decode page
        long[] times = pageIndex2Times.get(pageIndex);
        TsPrimitiveType[] values = decodeValuePage(reader, header, pageHeader, times, valueDecoder);
        for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
          alignedChunkData.writeDecodeValuePage(times, values, header.getDataType());
        }
      }
      long pageDataSize = pageHeader.getSerializedPageSize();
      pageIndex += 1;
      dataSize -= pageDataSize;
    }
  }

  private void storeTimeChunkContext() {
    pageIndex2TimesList.add(pageIndex2Times);
    pageIndex2ChunkDataList.add(pageIndex2ChunkData);
    isTimeChunkNeedDecodeList.add(isTimeChunkNeedDecode);
    pageIndex2Times = new HashMap<>();
    pageIndex2ChunkData = new HashMap<>();
    isTimeChunkNeedDecode = true;
  }

  private void switchToTimeChunkContextOfCurrentMeasurement(
      TsFileSequenceReader reader, String measurement) throws IOException {
    int index = valueColumn2TimeChunkIndex.getOrDefault(measurement, 0);
    if (index != timeChunkIndexOfCurrentValueColumn) {
      consumeAllAlignedChunkData(reader.position(), pageIndex2ChunkData);
    }
    timeChunkIndexOfCurrentValueColumn = index;
    valueColumn2TimeChunkIndex.put(measurement, index + 1);
    pageIndex2Times = pageIndex2TimesList.get(index);
    pageIndex2ChunkData = pageIndex2ChunkDataList.get(index);

    isTimeChunkNeedDecode = isTimeChunkNeedDecodeList.get(index);
  }

  private void getAllModification(Map<Long, List<Deletion>> offset2Deletions) throws IOException {
    try (ModificationFile modificationFile =
        new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)) {
      for (Modification modification : modificationFile.getModifications()) {
        offset2Deletions
            .computeIfAbsent(modification.getFileOffset(), o -> new ArrayList<>())
            .add((Deletion) modification);
      }
    }
  }

  private boolean checkMagic(TsFileSequenceReader reader) throws IOException {
    String magic = reader.readHeadMagic();
    if (!magic.equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file's MAGIC STRING is incorrect, file path: {}", reader.getFileName());
      return false;
    }

    byte versionNumber = reader.readVersionNumber();
    if (versionNumber < TSFileConfig.VERSION_NUMBER) {
      if (versionNumber == TSFileConfig.VERSION_NUMBER_V3 && TSFileConfig.VERSION_NUMBER == 4) {
        logger.info(
            "try to load TsFile V3 into current version (V4), file path: {}", reader.getFileName());
      } else {
        logger.error("the file's Version Number is too old, file path: {}", reader.getFileName());
        return false;
      }
    } else if (versionNumber > TSFileConfig.VERSION_NUMBER) {
      logger.error(
          "the file's Version Number is higher than current, file path: {}", reader.getFileName());
      return false;
    }

    if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
      logger.error("the file is not closed correctly, file path: {}", reader.getFileName());
      return false;
    }
    return true;
  }

  private void getChunkMetadata(
      TsFileSequenceReader reader, Map<Long, IChunkMetadata> offset2ChunkMetadata)
      throws IOException {
    Map<IDeviceID, List<TimeseriesMetadata>> device2Metadata =
        reader.getAllTimeseriesMetadata(true);
    for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
      for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
        for (IChunkMetadata chunkMetadata : timeseriesMetadata.getChunkMetadataList()) {
          offset2ChunkMetadata.put(chunkMetadata.getOffsetOfChunkHeader(), chunkMetadata);
        }
      }
    }
  }

  private void handleModification(
      TreeMap<Long, List<Deletion>> offset2Deletions, long chunkOffset) {
    while (!offset2Deletions.isEmpty() && offset2Deletions.firstEntry().getKey() <= chunkOffset) {
      offset2Deletions
          .pollFirstEntry()
          .getValue()
          .forEach(o -> consumer.apply(new DeletionData(o)));
    }
  }

  private void consumeAllAlignedChunkData(
      long offset, Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData) {
    if (pageIndex2ChunkData.isEmpty()) {
      return;
    }

    Map<AlignedChunkData, BatchedAlignedValueChunkData> chunkDataMap = new HashMap<>();
    for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
      List<AlignedChunkData> alignedChunkDataList = entry.getValue();
      for (int i = 0; i < alignedChunkDataList.size(); i++) {
        AlignedChunkData oldChunkData = alignedChunkDataList.get(i);
        BatchedAlignedValueChunkData chunkData =
            chunkDataMap.computeIfAbsent(oldChunkData, BatchedAlignedValueChunkData::new);
        alignedChunkDataList.set(i, chunkData);
      }
    }
    for (AlignedChunkData chunkData : chunkDataMap.keySet()) {
      if (Boolean.FALSE.equals(consumer.apply(chunkData))) {
        throw new IllegalStateException(
            String.format(
                "Consume aligned chunk data error, next chunk offset: %d, chunkData: %s",
                offset, chunkData));
      }
    }
    this.pageIndex2ChunkData = new HashMap<>();
  }

  private void consumeChunkData(String measurement, long offset, ChunkData chunkData) {
    if (Boolean.FALSE.equals(consumer.apply(chunkData))) {
      throw new IllegalStateException(
          String.format(
              "Consume chunkData error, chunk offset: %d, measurement: %s, chunkData: %s",
              offset, measurement, chunkData));
    }
  }

  private boolean needDecodeChunk(IChunkMetadata chunkMetadata) {
    return !TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getStartTime())
        .equals(TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getEndTime()));
  }

  private boolean needDecodePage(PageHeader pageHeader, IChunkMetadata chunkMetadata) {
    if (pageHeader.getStatistics() == null) {
      return !TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getStartTime())
          .equals(TimePartitionUtils.getTimePartitionSlot(chunkMetadata.getEndTime()));
    }
    return !TimePartitionUtils.getTimePartitionSlot(pageHeader.getStartTime())
        .equals(TimePartitionUtils.getTimePartitionSlot(pageHeader.getEndTime()));
  }

  private Pair<long[], Object[]> decodePage(
      boolean isAligned,
      ByteBuffer pageData,
      PageHeader pageHeader,
      Decoder timeDecoder,
      Decoder valueDecoder,
      ChunkHeader chunkHeader)
      throws IOException {
    if (isAligned) {
      TimePageReader timePageReader = new TimePageReader(pageHeader, pageData, timeDecoder);
      long[] times = timePageReader.getNextTimeBatch();
      return new Pair<>(times, new Object[times.length]);
    }

    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    long[] times = new long[batchData.length()];
    Object[] values = new Object[batchData.length()];
    int index = 0;
    while (batchData.hasCurrent()) {
      times[index] = batchData.currentTime();
      values[index++] = batchData.currentValue();
      batchData.next();
    }
    return new Pair<>(times, values);
  }

  private void handleEmptyValueChunk(
      ChunkHeader header,
      Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData,
      IChunkMetadata chunkMetadata,
      boolean isTimeChunkNeedDecode)
      throws IOException {
    Set<ChunkData> allChunkData = new HashSet<>();
    for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
      for (AlignedChunkData alignedChunkData : entry.getValue()) {
        if (!allChunkData.contains(alignedChunkData)) {
          alignedChunkData.addValueChunk(header);
          if (!isTimeChunkNeedDecode) {
            alignedChunkData.writeEntireChunk(ByteBuffer.allocate(0), chunkMetadata);
          }
          allChunkData.add(alignedChunkData);
        }
      }
    }
  }

  /**
   * handle empty page in aligned chunk, if uncompressedSize and compressedSize are both 0, and the
   * statistics is null, then the page is empty.
   *
   * @param pageHeader page header
   * @return true if the page is empty
   */
  private boolean isEmptyPage(PageHeader pageHeader) {
    return pageHeader.getUncompressedSize() == 0
        && pageHeader.getCompressedSize() == 0
        && pageHeader.getStatistics() == null;
  }

  private TsPrimitiveType[] decodeValuePage(
      TsFileSequenceReader reader,
      ChunkHeader chunkHeader,
      PageHeader pageHeader,
      long[] times,
      Decoder valueDecoder)
      throws IOException {
    if (pageHeader.getSerializedPageSize() == 0) {
      return new TsPrimitiveType[times.length];
    }

    valueDecoder.reset();
    ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
    ValuePageReader valuePageReader =
        new ValuePageReader(pageHeader, pageData, chunkHeader.getDataType(), valueDecoder);
    return valuePageReader.nextValueBatch(
        times); // should be origin time, so recording satisfied length is necessary
  }
}
