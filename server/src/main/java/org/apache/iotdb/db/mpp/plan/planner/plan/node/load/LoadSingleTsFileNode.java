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

package org.apache.iotdb.db.mpp.plan.planner.plan.node.load;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.load.AlignedChunkData;
import org.apache.iotdb.db.engine.load.ChunkData;
import org.apache.iotdb.db.mpp.plan.analyze.Analysis;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LoadSingleTsFileNode extends WritePlanNode {
  private static final Logger logger = LoggerFactory.getLogger(LoadSingleTsFileNode.class);

  private File tsFile;
  private Map<TRegionReplicaSet, List<LoadTsFilePieceNode>> replicaSet2Pieces;

  public LoadSingleTsFileNode(PlanNodeId id) {
    super(id);
  }

  public LoadSingleTsFileNode(PlanNodeId id, File tsFile) {
    super(id);
    this.tsFile = tsFile;
  }

  public Map<TRegionReplicaSet, List<LoadTsFilePieceNode>> getReplicaSet2Pieces() {
    return replicaSet2Pieces;
  }

  private LoadTsFilePieceNode getPieceNode(
      String device, TTimePartitionSlot timePartitionSlot, DataPartition dataPartition) {
    TRegionReplicaSet replicaSet =
        dataPartition.getDataRegionReplicaSetForWriting(device, timePartitionSlot);
    List<LoadTsFilePieceNode> pieceNodes =
        replicaSet2Pieces.computeIfAbsent(replicaSet, o -> new ArrayList<>());
    if (pieceNodes.isEmpty() || pieceNodes.get(pieceNodes.size() - 1).exceedSize()) {
      pieceNodes.add(new LoadTsFilePieceNode(getPlanNodeId(), tsFile));
    }
    return pieceNodes.get(pieceNodes.size() - 1);
  }

  @Override
  public TRegionReplicaSet getRegionReplicaSet() {
    return null;
  }

  @Override
  public List<PlanNode> getChildren() {
    return null;
  }

  @Override
  public void addChild(PlanNode child) {}

  @Override
  public PlanNode clone() {
    throw new NotImplementedException("clone of load single TsFile is not implemented");
  }

  @Override
  public int allowedChildCount() {
    return NO_CHILD_ALLOWED;
  }

  @Override
  public List<String> getOutputColumnNames() {
    return null;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {}

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {}

  @Override
  public List<WritePlanNode> splitByPartition(Analysis analysis) {
    throw new NotImplementedException("split load single TsFile is not implemented");
  }

  @Override
  public String toString() {
    return "LoadSingleTsFileNode{" +
            "tsFile=" + tsFile +
            ", replicaSets=" + replicaSet2Pieces.keySet() +
            '}';
  }

  public void splitTsFileByDataPartition(DataPartition dataPartition) throws IOException {
    replicaSet2Pieces = new HashMap<>();
    List<ChunkData> chunkDataList = new ArrayList<>();

    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      if (checkMagic(reader)) {
        throw new TsFileRuntimeException(
            String.format("Magic String check error when parsing TsFile %s.", tsFile.getPath()));
      }

      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      String curDevice = null;
      Map<Integer, List<AlignedChunkData>> pageIndex2ChunkData = null;
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
            ChunkHeader header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
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

            boolean isAligned =
                ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                    == TsFileConstant.TIME_COLUMN_MASK);
            TTimePartitionSlot timePartitionSlot = null;
            ChunkData chunkData =
                ChunkData.createChunkData(isAligned, reader.position(), curDevice, header);
            if (isAligned) {
              pageIndex2ChunkData = new HashMap<>();
            }

            while (dataSize > 0) {
              long pageOffset = reader.position();
              PageHeader pageHeader =
                  reader.readPageHeader(
                      header.getDataType(),
                      (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              long pageDataSize = pageHeader.getSerializedPageSize();
              if (timePartitionSlot == null) { // init time slot
                timePartitionSlot = StorageEngineV2.getTimePartitionSlot(pageHeader.getStartTime());
                chunkData.setTimePartitionSlot(timePartitionSlot);
              }
              if (!needDecodePage(pageHeader)) { // an entire page
                TTimePartitionSlot pageTimePartitionSlot =
                    StorageEngineV2.getTimePartitionSlot(pageHeader.getStartTime());
                if (!timePartitionSlot.equals(pageTimePartitionSlot)) {
                  chunkDataList.add(chunkData);
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
                reader.position(reader.position() + pageDataSize);
              } else { // split page
                ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
                long[] timeBatch =
                    decodePage(
                        isAligned, pageData, pageHeader, defaultTimeDecoder, valueDecoder, header);
                boolean isFirstData = true;
                for (long currentTime : timeBatch) {
                  TTimePartitionSlot currentTimePartitionSlot =
                      StorageEngineV2.getTimePartitionSlot(currentTime);
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
                    chunkDataList.add(chunkData);

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

            chunkDataList.add(chunkData);
            break;
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            header = reader.readChunkHeader(marker);
            if (header.getDataSize() == 0) {
              break;
            }

            Set<ChunkData> allChunkData = new HashSet<>();
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
                  alignedChunkData.addValueChunk(pageOffset, header);
                  allChunkData.add(alignedChunkData);
                }
                alignedChunkData.addValueChunkDataSize(pageDataSize);
              }
              reader.position(reader.position() + pageDataSize);

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
    }

    for (ChunkData chunkData : chunkDataList) {
      getPieceNode(chunkData.getDevice(), chunkData.getTimePartitionSlot(), dataPartition)
          .addChunkData(chunkData);
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

  private boolean needDecodePage(PageHeader pageHeader) {
    if (pageHeader.getStatistics() == null) {
      return true;
    }
    return !StorageEngineV2.getTimePartitionSlot(pageHeader.getStartTime())
        .equals(StorageEngineV2.getTimePartitionSlot(pageHeader.getEndTime()));
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
}
