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

package org.apache.iotdb.db.engine.load;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

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

  public TsFileSplitter(File tsFile, Function<TsFileData, Boolean> consumer) {
    this.tsFile = tsFile;
    this.consumer = consumer;
  }

  public void splitTsFileByDataPartition() throws IOException, IllegalStateException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      TreeMap<Long, List<Deletion>> offset2Deletions = new TreeMap<>();
      getAllModification(offset2Deletions);

      if (!checkMagic(reader)) {
        throw new TsFileRuntimeException(
            String.format("Magic String check error when parsing TsFile %s.", tsFile.getPath()));
      }

      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      String curDevice = null;
      boolean isTimeChunkNeedDecode = true;
      Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData = new HashMap<>();
      Map<Integer, long[]> pageIndex2Times = null;
      Map<Long, IChunkMetadata> offset2ChunkMetadata = new HashMap<>();
      getChunkMetadata(reader, offset2ChunkMetadata);
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
            long chunkOffset = reader.position();
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

            boolean isAligned =
                ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                    == TsFileConstant.TIME_COLUMN_MASK);
            IChunkMetadata chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
            TTimePartitionSlot timePartitionSlot =
                TimePartitionUtils.getTimePartition(chunkMetadata.getStartTime());
            ChunkData chunkData =
                ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);

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
              break;
            }

            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            int pageIndex = 0;
            if (isAligned) {
              isTimeChunkNeedDecode = true;
              pageIndex2Times = new HashMap<>();
            }

            while (dataSize > 0) {
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              long pageDataSize = pageHeader.getSerializedPageSize();
              if (!needDecodePage(pageHeader, chunkMetadata)) { // an entire page
                long startTime =
                    pageHeader.getStatistics() == null
                        ? chunkMetadata.getStartTime()
                        : pageHeader.getStartTime();
                TTimePartitionSlot pageTimePartitionSlot =
                    TimePartitionUtils.getTimePartition(startTime);
                if (!timePartitionSlot.equals(pageTimePartitionSlot)) {
                  if (!isAligned) {
                    consumeChunkData(measurementId, chunkOffset, chunkData);
                  }
                  timePartitionSlot = pageTimePartitionSlot;
                  chunkData =
                      ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);
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
                    decodePage(
                        isAligned, pageData, pageHeader, defaultTimeDecoder, valueDecoder, header);
                long[] times = tvArray.left;
                Object[] values = tvArray.right;
                if (isAligned) {
                  pageIndex2Times.put(pageIndex, times);
                }

                int satisfiedLength = 0;
                long endTime =
                    timePartitionSlot.getStartTime()
                        + TimePartitionUtils.getTimePartitionInterval();
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

                    timePartitionSlot = TimePartitionUtils.getTimePartition(times[i]);
                    satisfiedLength = 0;
                    endTime =
                        timePartitionSlot.getStartTime()
                            + TimePartitionUtils.getTimePartitionInterval();
                    chunkData =
                        ChunkData.createChunkData(isAligned, curDevice, header, timePartitionSlot);
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
            break;
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            chunkOffset = reader.position();
            chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
            header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              handleEmptyValueChunk(header, pageIndex2ChunkData);
              break;
            }

            if (!isTimeChunkNeedDecode) {
              AlignedChunkData alignedChunkData = pageIndex2ChunkData.get(1).get(0);
              alignedChunkData.addValueChunk(header);
              alignedChunkData.writeEntireChunk(
                  reader.readChunk(-1, header.getDataSize()), chunkMetadata);
              break;
            }

            Set<ChunkData> allChunkData = new HashSet<>();
            dataSize = header.getDataSize();
            pageIndex = 0;
            valueDecoder = Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());

            while (dataSize > 0) {
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              long pageDataSize = pageHeader.getSerializedPageSize();
              List<AlignedChunkData> alignedChunkDataList = pageIndex2ChunkData.get(pageIndex);
              for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
                if (!allChunkData.contains(alignedChunkData)) {
                  alignedChunkData.addValueChunk(header);
                  allChunkData.add(alignedChunkData);
                }
              }
              if (alignedChunkDataList.size() == 1) { // write entire page
                alignedChunkDataList
                    .get(0)
                    .writeEntirePage(pageHeader, reader.readCompressedPage(pageHeader));
              } else { // decode page
                long[] times = pageIndex2Times.get(pageIndex);
                TsPrimitiveType[] values =
                    decodeValuePage(reader, header, pageHeader, times, valueDecoder);
                for (AlignedChunkData alignedChunkData : alignedChunkDataList) {
                  alignedChunkData.writeDecodeValuePage(times, values, header.getDataType());
                }
              }

              pageIndex += 1;
              dataSize -= pageDataSize;
            }
            break;
          case MetaMarker.CHUNK_GROUP_HEADER:
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            curDevice = chunkGroupHeader.getDeviceID();
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
    if (versionNumber != TSFileConfig.VERSION_NUMBER) {
      logger.error("the file's Version Number is incorrect, file path: {}", reader.getFileName());
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
    Map<String, List<TimeseriesMetadata>> device2Metadata = reader.getAllTimeseriesMetadata(true);
    for (Map.Entry<String, List<TimeseriesMetadata>> entry : device2Metadata.entrySet()) {
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

    Set<ChunkData> allChunkData = new HashSet<>();
    for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
      allChunkData.addAll(entry.getValue());
    }
    for (ChunkData chunkData : allChunkData) {
      if (!consumer.apply(chunkData)) {
        throw new IllegalStateException(
            String.format(
                "Consume aligned chunk data error, next chunk offset: %d, chunkData: %s",
                offset, chunkData));
      }
    }
    pageIndex2ChunkData.clear();
  }

  private void consumeChunkData(String measurement, long offset, ChunkData chunkData) {
    if (!consumer.apply(chunkData)) {
      throw new IllegalStateException(
          String.format(
              "Consume chunkData error, chunk offset: %d, measurement: %s, chunkData: %s",
              offset, measurement, chunkData));
    }
  }

  private boolean needDecodeChunk(IChunkMetadata chunkMetadata) {
    return !TimePartitionUtils.getTimePartition(chunkMetadata.getStartTime())
        .equals(TimePartitionUtils.getTimePartition(chunkMetadata.getEndTime()));
  }

  private boolean needDecodePage(PageHeader pageHeader, IChunkMetadata chunkMetadata) {
    if (pageHeader.getStatistics() == null) {
      return !TimePartitionUtils.getTimePartition(chunkMetadata.getStartTime())
          .equals(TimePartitionUtils.getTimePartition(chunkMetadata.getEndTime()));
    }
    return !TimePartitionUtils.getTimePartition(pageHeader.getStartTime())
        .equals(TimePartitionUtils.getTimePartition(pageHeader.getEndTime()));
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
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, null);
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
      ChunkHeader header, Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData) {
    Set<ChunkData> allChunkData = new HashSet<>();
    for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
      for (AlignedChunkData alignedChunkData : entry.getValue()) {
        if (!allChunkData.contains(alignedChunkData)) {
          alignedChunkData.addValueChunk(header);
          allChunkData.add(alignedChunkData);
        }
      }
    }
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
