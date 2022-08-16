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
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class NonAlignedChunkData implements ChunkData {
  private long offset;
  private long dataSize;
  private boolean isHeadPageNeedDecode;
  private boolean isTailPageNeedDecode;

  private TTimePartitionSlot timePartitionSlot;
  private String device;
  private ChunkHeader chunkHeader;

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
  public IChunkWriter getChunkWriter(File tsFile) throws IOException, PageException {
    ChunkWriterImpl chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(
                chunkHeader.getMeasurementID(),
                chunkHeader.getDataType(),
                chunkHeader.getEncodingType(),
                chunkHeader.getCompressionType()));
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
        PageHeader pageHeader =
            reader.readPageHeader(
                chunkHeader.getDataType(),
                (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
        long pageDataSize = pageHeader.getSerializedPageSize();
        if ((dataSize == this.dataSize && isHeadPageNeedDecode) // decode head page
            || (dataSize == pageDataSize && isTailPageNeedDecode)) { // decode tail page
          decodePage(reader, pageHeader, defaultTimeDecoder, valueDecoder, chunkWriter);
        } else { // entire page
          ByteBuffer pageData = reader.readCompressedPage(pageHeader);
          chunkWriter.writePageHeaderAndDataIntoBuff(pageData, pageHeader);
        }
        dataSize -= pageDataSize;
      }
      return chunkWriter;
    }
  }

  private void decodePage(
      TsFileSequenceReader reader,
      PageHeader pageHeader,
      Decoder timeDecoder,
      Decoder valueDecoder,
      ChunkWriterImpl chunkWriter)
      throws IOException {
    valueDecoder.reset();
    ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
    PageReader pageReader =
        new PageReader(pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, null);
    BatchData batchData = pageReader.getAllSatisfiedPageData();
    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      if (time < timePartitionSlot.getStartTime()) {
        continue;
      } else if (time > timePartitionSlot.getStartTime()) {
        break;
      }

      Object value = batchData.currentValue();
      switch (chunkHeader.getDataType()) {
        case INT32:
          chunkWriter.write(time, (int) value);
          break;
        case INT64:
          chunkWriter.write(time, (long) value);
          break;
        case FLOAT:
          chunkWriter.write(time, (float) value);
          break;
        case DOUBLE:
          chunkWriter.write(time, (double) value);
          break;
        case BOOLEAN:
          chunkWriter.write(time, (boolean) value);
          break;
        case TEXT:
          chunkWriter.write(time, (Binary) value);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", chunkHeader.getDataType()));
      }
      batchData.next();
    }
    chunkWriter.sealCurrentPage();
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
