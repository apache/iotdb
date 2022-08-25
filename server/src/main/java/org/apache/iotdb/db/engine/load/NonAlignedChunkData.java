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
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class NonAlignedChunkData implements ChunkData {
  private long offset;
  private long dataSize;
  private boolean isHeadPageNeedDecode;
  private boolean isTailPageNeedDecode;

  private TTimePartitionSlot timePartitionSlot;
  private String device;
  private ChunkHeader chunkHeader;
  private IChunkMetadata chunkMetadata;

  private ChunkWriterImpl chunkWriter;
  private Chunk chunk;

  public NonAlignedChunkData(long offset, String device, ChunkHeader chunkHeader) {
    this.offset = offset;
    this.dataSize = 0;
    this.isHeadPageNeedDecode = false;
    this.isTailPageNeedDecode = false;
    this.device = device;
    this.chunkHeader = chunkHeader;
  }

  @Override
  public String getDevice() {
    return device;
  }

  @Override
  public TTimePartitionSlot getTimePartitionSlot() {
    return timePartitionSlot;
  }

  @Override
  public long getDataSize() {
    return dataSize;
  }

  @Override
  public void addDataSize(long pageSize) {
    dataSize += pageSize;
  }

  @Override
  public void setNotDecode(IChunkMetadata chunkMetadata) {
    this.chunkMetadata = chunkMetadata;
  }

  @Override
  public boolean needDecodeChunk() {
    return chunkMetadata == null;
  }

  @Override
  public void setHeadPageNeedDecode(boolean headPageNeedDecode) {
    isHeadPageNeedDecode = headPageNeedDecode;
  }

  @Override
  public void setTailPageNeedDecode(boolean tailPageNeedDecode) {
    isTailPageNeedDecode = tailPageNeedDecode;
  }

  @Override
  public void setTimePartitionSlot(TTimePartitionSlot timePartitionSlot) {
    this.timePartitionSlot = timePartitionSlot;
  }

  @Override
  public boolean isAligned() {
    return false;
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter writer) throws IOException {
    if (chunk != null) {
      writer.writeChunk(chunk);
    } else {
      chunkWriter.writeToFileWriter(writer);
    }
  }

  @Override
  public void serialize(DataOutputStream stream, File tsFile) throws IOException {
    ReadWriteIOUtils.write(isAligned(), stream);
    serializeAttr(stream);
    if (needDecodeChunk()) {
      ReadWriteIOUtils.write(true, stream);
      serializeDecodeChunk(stream, tsFile);
    } else {
      ReadWriteIOUtils.write(false, stream);
      serializeEntireChunk(stream, tsFile);
    }
  }

  private void serializeAttr(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(timePartitionSlot.getStartTime(), stream);
    ReadWriteIOUtils.write(device, stream);
    chunkHeader.serializeTo(stream); // chunk header already serialize chunk type
  }

  private void serializeEntireChunk(DataOutputStream stream, File tsFile) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      ByteBuffer chunkData =
          reader.readChunk(
              chunkMetadata.getOffsetOfChunkHeader() + chunkHeader.getSerializedSize(),
              chunkHeader.getDataSize());
      ReadWriteIOUtils.write(chunkData, stream);
      chunkMetadata.getStatistics().serialize(stream);
    }
  }

  private void serializeDecodeChunk(DataOutputStream stream, File tsFile) throws IOException {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      Decoder defaultTimeDecoder =
          Decoder.getDecoderByType(
              TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
              TSDataType.INT64);
      Decoder valueDecoder =
          Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());

      reader.position(offset);
      long dataSize = this.dataSize;
      while (dataSize > 0) {
        boolean hasStatistic = (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER;
        PageHeader pageHeader = reader.readPageHeader(chunkHeader.getDataType(), hasStatistic);
        long pageDataSize = pageHeader.getSerializedPageSize();
        if ((dataSize == this.dataSize && isHeadPageNeedDecode) // decode head page
            || (dataSize == pageDataSize && isTailPageNeedDecode)) { // decode tail page
          ReadWriteIOUtils.write(true, stream); // decode
          decodePage(reader, pageHeader, defaultTimeDecoder, valueDecoder, stream);
        } else { // entire page
          ReadWriteIOUtils.write(false, stream); // don't decode
          pageHeader.serializeTo(stream);
          ByteBuffer pageData = reader.readCompressedPage(pageHeader);
          ReadWriteIOUtils.write(pageData, stream);
        }
        dataSize -= pageDataSize;
      }
    }

    ReadWriteIOUtils.write(true, stream); // means ending
    ReadWriteIOUtils.write(-1, stream);
  }

  private void decodePage(
      TsFileSequenceReader reader,
      PageHeader pageHeader,
      Decoder timeDecoder,
      Decoder valueDecoder,
      DataOutputStream stream)
      throws IOException {
    valueDecoder.reset();
    ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
    PageReader pageReader =
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();

    int length = 0;
    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      if (time < timePartitionSlot.getStartTime()) {
        batchData.next();
        continue;
      } else if (!timePartitionSlot.equals(StorageEngineV2.getTimePartitionSlot(time))) {
        break;
      }
      length += 1;
      batchData.next();
    }

    ReadWriteIOUtils.write(length, stream);
    batchData.resetBatchData();
    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      if (time < timePartitionSlot.getStartTime()) {
        batchData.next();
        continue;
      } else if (!timePartitionSlot.equals(StorageEngineV2.getTimePartitionSlot(time))) {
        break;
      }

      ReadWriteIOUtils.write(time, stream);
      Object value = batchData.currentValue();
      switch (chunkHeader.getDataType()) {
        case INT32:
          ReadWriteIOUtils.write((int) value, stream);
          break;
        case INT64:
          ReadWriteIOUtils.write((long) value, stream);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((float) value, stream);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((double) value, stream);
          break;
        case BOOLEAN:
          ReadWriteIOUtils.write((boolean) value, stream);
          break;
        case TEXT:
          ReadWriteIOUtils.write((Binary) value, stream);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", chunkHeader.getDataType()));
      }
      batchData.next();
    }
  }

  private void deserializeTsFileData(InputStream stream) throws IOException, PageException {
    boolean needDecodeChunk = ReadWriteIOUtils.readBool(stream);
    if (needDecodeChunk) {
      buildChunkWriter(stream);
    } else {
      deserializeEntireChunk(stream);
    }
  }

  private void buildChunkWriter(InputStream stream) throws IOException, PageException {
    chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(
                chunkHeader.getMeasurementID(),
                chunkHeader.getDataType(),
                chunkHeader.getEncodingType(),
                chunkHeader.getCompressionType()));
    boolean needDecode;
    while (true) {
      needDecode = ReadWriteIOUtils.readBool(stream);
      if (needDecode) {
        int length = ReadWriteIOUtils.readInt(stream);
        if (length == -1) {
          break;
        }

        for (int i = 0; i < length; i++) {
          long time = ReadWriteIOUtils.readLong(stream);
          switch (chunkHeader.getDataType()) {
            case INT32:
              chunkWriter.write(time, ReadWriteIOUtils.readInt(stream));
              break;
            case INT64:
              chunkWriter.write(time, ReadWriteIOUtils.readLong(stream));
              break;
            case FLOAT:
              chunkWriter.write(time, ReadWriteIOUtils.readFloat(stream));
              break;
            case DOUBLE:
              chunkWriter.write(time, ReadWriteIOUtils.readDouble(stream));
              break;
            case BOOLEAN:
              chunkWriter.write(time, ReadWriteIOUtils.readBool(stream));
              break;
            case TEXT:
              chunkWriter.write(time, ReadWriteIOUtils.readBinary(stream));
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", chunkHeader.getDataType()));
          }
        }

        chunkWriter.sealCurrentPage();
      } else {
        PageHeader pageHeader = PageHeader.deserializeFrom(stream, chunkHeader.getDataType(), true);
        chunkWriter.writePageHeaderAndDataIntoBuff(
            ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream)),
            pageHeader);
      }
    }
  }

  private void deserializeEntireChunk(InputStream stream) throws IOException {
    ByteBuffer chunkData =
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream));
    Statistics<? extends Serializable> statistics =
        Statistics.deserialize(stream, chunkHeader.getDataType());
    chunk = new Chunk(chunkHeader, chunkData, null, statistics);
  }

  public static NonAlignedChunkData deserialize(InputStream stream)
      throws IOException, PageException {
    long timePartition = ReadWriteIOUtils.readLong(stream);
    String device = ReadWriteIOUtils.readString(stream);
    byte chunkType = ReadWriteIOUtils.readByte(stream);
    ChunkHeader chunkHeader = ChunkHeader.deserializeFrom(stream, chunkType);
    NonAlignedChunkData chunkData = new NonAlignedChunkData(-1, device, chunkHeader);
    chunkData.setTimePartitionSlot(StorageEngineV2.getTimePartitionSlot(timePartition));
    chunkData.deserializeTsFileData(stream);
    return chunkData;
  }

  @Override
  public String toString() {
    return "NonAlignedChunkData{"
        + "offset="
        + offset
        + ", dataSize="
        + dataSize
        + ", isHeadPageNeedDecode="
        + isHeadPageNeedDecode
        + ", isTailPageNeedDecode="
        + isTailPageNeedDecode
        + ", timePartitionSlot="
        + timePartitionSlot
        + ", device='"
        + device
        + '\''
        + ", chunkHeader="
        + chunkHeader
        + '}';
  }
}
