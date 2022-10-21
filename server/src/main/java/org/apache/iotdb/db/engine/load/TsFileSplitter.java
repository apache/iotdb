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

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.exception.LoadFileException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.load.LoadTsFilePieceNode;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TsFileSplitter {
  private static final Logger logger = LoggerFactory.getLogger(TsFileSplitter.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final long MAX_MEMORY_SIZE =
      Math.min(config.getThriftMaxFrameSize() / 2, config.getAllocateMemoryForStorageEngine() / 8);

  private final File tsFile;
  private final DataPartition dataPartition;
  private final Function<LoadTsFilePieceNode, Boolean> dispatcher;

  private long dataSize;
  private Map<TRegionReplicaSet, LoadTsFilePieceNode> replicaSet2Piece;

  public TsFileSplitter(
      File tsFile, DataPartition dataPartition, Function<LoadTsFilePieceNode, Boolean> dispatcher) {
    this.tsFile = tsFile;
    this.dataPartition = dataPartition;
    this.dispatcher = dispatcher;
  }

  public void addOrSendTsFileData(TsFileData tsFileData, TRegionReplicaSet replicaSet)
      throws LoadFileException {
    long tsFileDataSize = tsFileData.getDataSize();
    if (tsFileDataSize + dataSize > MAX_MEMORY_SIZE) {
      List<TRegionReplicaSet> sortedReplicaSets =
          replicaSet2Piece.keySet().stream()
              .sorted(
                  Comparator.comparingLong(o -> replicaSet2Piece.get(o).getDataSize()).reversed())
              .collect(Collectors.toList());

      for (TRegionReplicaSet sortedReplicaSet : sortedReplicaSets) {
        LoadTsFilePieceNode pieceNode = replicaSet2Piece.get(sortedReplicaSet);
        if (pieceNode.getDataSize() != 0 && !dispatcher.apply(pieceNode)) {
          throw new LoadFileException(
              String.format("Dispatch error when handle TsFile %s.", tsFile.getPath()));
        }

        dataSize -= pieceNode.getDataSize();
        replicaSet2Piece.put(sortedReplicaSet, new LoadTsFilePieceNode(new PlanNodeId("")))
        if (dataSize + tsFileDataSize <= MAX_MEMORY_SIZE) {
          break;
        }
      }
    }
  }

  public void splitTsFileByDataPartition() throws IOException {
    dataSize = 0;
    replicaSet2Piece = new HashMap<>();
    List<TsFileData> tsFileDataList = new ArrayList<>();

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
      Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData = null;
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
            handleModification(offset2Deletions, tsFileDataList, chunkOffset);

            ChunkHeader header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              throw new TsFileRuntimeException(
                  String.format("Chunk data error when parsing TsFile %s.", tsFile.getPath()));
            }

            boolean isAligned =
                ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                    == TsFileConstant.TIME_COLUMN_MASK);
            IChunkMetadata chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
            TTimePartitionSlot timePartitionSlot =
                TimePartitionUtils.getTimePartitionForRouting(chunkMetadata.getStartTime());
            ChunkData chunkData =
                ChunkData.createChunkData(isAligned, reader.position(), curDevice, header);
            chunkData.setTimePartitionSlot(timePartitionSlot);
            if (!needDecodeChunk(chunkMetadata)) {
              if (isAligned) {
                isTimeChunkNeedDecode = false;
                pageIndex2ChunkData = new HashMap<>();
                pageIndex2ChunkData
                    .computeIfAbsent(1, o -> new ArrayList<>())
                    .add((AlignedChunkData) chunkData);
              }
              chunkData.setNotDecode(chunkMetadata);
              chunkData.addDataSize(header.getDataSize());
              tsFileDataList.add(chunkData);
              reader.position(reader.position() + header.getDataSize());
              break;
            }
            if (isAligned) {
              isTimeChunkNeedDecode = true;
              pageIndex2ChunkData = new HashMap<>();
            }

            Decoder defaultTimeDecoder =
                Decoder.getDecoderByType(
                    TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                    TSDataType.INT64);
            Decoder valueDecoder =
                Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            int pageIndex = 0;
            while (dataSize > 0) {
              long pageOffset = reader.position();
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
                    TimePartitionUtils.getTimePartitionForRouting(startTime);
                if (!timePartitionSlot.equals(pageTimePartitionSlot)) {
                  tsFileDataList.add(chunkData);
                  timePartitionSlot = pageTimePartitionSlot;
                  chunkData = ChunkData.createChunkData(isAligned, pageOffset, curDevice, header);
                  chunkData.setTimePartitionSlot(timePartitionSlot);
                }
                if (isAligned) {
                  pageIndex2ChunkData
                      .computeIfAbsent(pageIndex, o -> new ArrayList<>())
                      .add((AlignedChunkData) chunkData);
                }
                chunkData.addDataSize(pageDataSize);
                reader.position(pageOffset + pageDataSize);
              } else { // split page
                ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                long[] timeBatch =
                    decodePage(
                        isAligned, pageData, pageHeader, defaultTimeDecoder, valueDecoder, header);
                boolean isFirstData = true;
                for (long currentTime : timeBatch) {
                  TTimePartitionSlot currentTimePartitionSlot =
                      TimePartitionUtils.getTimePartitionForRouting(
                          currentTime); // TODO: can speed up
                  if (!timePartitionSlot.equals(currentTimePartitionSlot)) {
                    if (!isFirstData) {
                      chunkData.setTailPageNeedDecode(true); // close last chunk data
                      chunkData.addDataSize(pageDataSize);
                      if (isAligned) {
                        pageIndex2ChunkData
                            .computeIfAbsent(pageIndex, o -> new ArrayList<>())
                            .add((AlignedChunkData) chunkData);
                      }
                    }
                    tsFileDataList.add(chunkData);

                    chunkData =
                        ChunkData.createChunkData(
                            isAligned, pageOffset, curDevice, header); // open a new chunk data
                    chunkData.setTimePartitionSlot(currentTimePartitionSlot);
                    chunkData.setHeadPageNeedDecode(true);
                    timePartitionSlot = currentTimePartitionSlot;
                  }
                  isFirstData = false;
                }
                chunkData.addDataSize(pageDataSize);
                if (isAligned) {
                  pageIndex2ChunkData
                      .computeIfAbsent(pageIndex, o -> new ArrayList<>())
                      .add((AlignedChunkData) chunkData);
                }
              }

              pageIndex += 1;
              dataSize -= pageDataSize;
            }

            tsFileDataList.add(chunkData);
            break;
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            chunkOffset = reader.position();
            chunkMetadata = offset2ChunkMetadata.get(chunkOffset - Byte.BYTES);
            header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              handleEmptyValueChunk(chunkOffset, header, chunkMetadata, pageIndex2ChunkData);
              break;
            }

            Set<ChunkData> allChunkData = new HashSet<>();
            if (!isTimeChunkNeedDecode) {
              AlignedChunkData alignedChunkData = pageIndex2ChunkData.get(1).get(0);
              alignedChunkData.addValueChunk(chunkOffset, header, chunkMetadata);
              alignedChunkData.addValueChunkDataSize(header.getDataSize());
              reader.position(reader.position() + header.getDataSize());
              break;
            }

            dataSize = header.getDataSize();
            pageIndex = 0;

            while (dataSize > 0) {
              long pageOffset = reader.position();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              long pageDataSize = pageHeader.getSerializedPageSize();
              for (AlignedChunkData alignedChunkData : pageIndex2ChunkData.get(pageIndex)) {
                if (!allChunkData.contains(alignedChunkData)) {
                  alignedChunkData.addValueChunk(pageOffset, header, chunkMetadata);
                  allChunkData.add(alignedChunkData);
                }
                alignedChunkData.addValueChunkDataSize(pageDataSize);
              }
              reader.position(pageOffset + pageDataSize);

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

      handleModification(offset2Deletions, tsFileDataList, Long.MAX_VALUE);
    }

    for (TsFileData tsFileData : tsFileDataList) {
      if (!tsFileData.isModification()) {
        ChunkData chunkData = (ChunkData) tsFileData;
        getPieceNode(chunkData.getDevice(), chunkData.getTimePartitionSlot(), dataPartition)
            .addTsFileData(chunkData);
      } else {
        for (Map.Entry<TRegionReplicaSet, List<LoadTsFilePieceNode>> entry :
            replicaSet2Piece.entrySet()) {
          LoadTsFilePieceNode pieceNode = entry.getValue().get(entry.getValue().size() - 1);
          pieceNode.addTsFileData(tsFileData);
        }
      }
    }

    logger.info(
        String.format(
            "Finish Parsing TsFile %s, split to %d pieces, send to %d RegionReplicaSet.",
            tsFile.getPath(), tsFileDataList.size(), replicaSet2Piece.keySet().size()));
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
      TreeMap<Long, List<Deletion>> offset2Deletions,
      List<TsFileData> tsFileDataList,
      long chunkOffset) {
    while (!offset2Deletions.isEmpty() && offset2Deletions.firstEntry().getKey() <= chunkOffset) {
      tsFileDataList.addAll(
          offset2Deletions.pollFirstEntry().getValue().stream()
              .map(DeletionData::new)
              .collect(Collectors.toList()));
    }
  }

  private boolean needDecodeChunk(IChunkMetadata chunkMetadata) {
    return !TimePartitionUtils.getTimePartitionForRouting(chunkMetadata.getStartTime())
        .equals(TimePartitionUtils.getTimePartitionForRouting(chunkMetadata.getEndTime()));
  }

  private boolean needDecodePage(PageHeader pageHeader, IChunkMetadata chunkMetadata) {
    if (pageHeader.getStatistics() == null) {
      return !TimePartitionUtils.getTimePartitionForRouting(chunkMetadata.getStartTime())
          .equals(TimePartitionUtils.getTimePartitionForRouting(chunkMetadata.getEndTime()));
    }
    return !TimePartitionUtils.getTimePartitionForRouting(pageHeader.getStartTime())
        .equals(TimePartitionUtils.getTimePartitionForRouting(pageHeader.getEndTime()));
  }

  private long[] decodePage(
      boolean isAligned,
      ByteBuffer pageData,
      PageHeader pageHeader,
      Decoder timeDecoder,
      Decoder valueDecoder,
      ChunkHeader chunkHeader)
      throws IOException {
    if (isAligned) {
      TimePageReader timePageReader = new TimePageReader(pageHeader, pageData, timeDecoder);
      return timePageReader.getNextTimeBatch();
    }

    valueDecoder.reset();
    PageReader pageReader =
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    long[] timeBatch = new long[batchData.length()];
    int index = 0;
    while (batchData.hasCurrent()) {
      timeBatch[index++] = batchData.currentTime();
      batchData.next();
    }
    return timeBatch;
  }

  private void handleEmptyValueChunk(
      long chunkOffset,
      ChunkHeader header,
      IChunkMetadata chunkMetadata,
      Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData) {
    Set<ChunkData> allChunkData = new HashSet<>();
    for (Map.Entry<Integer, List<AlignedChunkData>> entry : pageIndex2ChunkData.entrySet()) {
      for (AlignedChunkData alignedChunkData : entry.getValue()) {
        if (!allChunkData.contains(alignedChunkData)) {
          alignedChunkData.addValueChunk(chunkOffset, header, chunkMetadata);
          allChunkData.add(alignedChunkData);
        }
      }
    }
  }

  private LoadTsFilePieceNode getPieceNode(
      String device, TTimePartitionSlot timePartitionSlot, DataPartition dataPartition) {
    TRegionReplicaSet replicaSet =
        dataPartition.getDataRegionReplicaSetForWriting(device, timePartitionSlot);
    List<LoadTsFilePieceNode> pieceNodes =
        replicaSet2Piece.computeIfAbsent(replicaSet, o -> new ArrayList<>());
    if (pieceNodes.isEmpty() || pieceNodes.get(pieceNodes.size() - 1).exceedSize()) {
      pieceNodes.add(new LoadTsFilePieceNode(getPlanNodeId(), tsFile));
    }
    return pieceNodes.get(pieceNodes.size() - 1);
  }

  public void clean() {
    try {
      if (deleteAfterLoad) {
        Files.deleteIfExists(tsFile.toPath());
      }
    } catch (IOException e) {
      logger.warn(String.format("Delete After Loading %s error.", tsFile), e);
    }
  }
}
