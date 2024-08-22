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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class NonAlignedChunkData implements ChunkData {

  private final TTimePartitionSlot timePartitionSlot;
  private final String device;
  private final ChunkHeader chunkHeader;

  private final PublicBAOS byteStream;
  private final DataOutputStream stream;
  private long dataSize;
  private boolean needDecodeChunk;
  private int pageNumber;

  private ChunkWriterImpl chunkWriter;
  private Chunk chunk;

  public NonAlignedChunkData(
      final String device,
      final ChunkHeader chunkHeader,
      final TTimePartitionSlot timePartitionSlot) {
    this.dataSize = 0;
    this.device = device;
    this.chunkHeader = chunkHeader;
    this.timePartitionSlot = timePartitionSlot;
    this.needDecodeChunk = true;
    this.pageNumber = 0;
    this.byteStream = new PublicBAOS();
    this.stream = new DataOutputStream(byteStream);

    addAttrDataSize();
  }

  private void addAttrDataSize() { // should be init before serialize, corresponding serializeAttr
    dataSize += 2 * Byte.BYTES; // isModification and isAligned
    dataSize += Long.BYTES; // timePartitionSlot
    final int deviceLength = device.getBytes(TSFileConfig.STRING_CHARSET).length;
    dataSize += ReadWriteForEncodingUtils.varIntSize(deviceLength);
    dataSize += deviceLength; // device
    dataSize += chunkHeader.getSerializedSize(); // timeChunkHeader
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
  public void setNotDecode() {
    needDecodeChunk = false;
  }

  @Override
  public boolean isAligned() {
    return false;
  }

  @Override
  public void writeToFileWriter(final TsFileIOWriter writer) throws IOException {
    if (chunk != null) {
      writer.writeChunk(chunk);
    } else {
      chunkWriter.writeToFileWriter(writer);
    }
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(isModification(), stream);
    ReadWriteIOUtils.write(isAligned(), stream);
    serializeAttr(stream);
    byteStream.writeTo(stream);
    close();
  }

  private void serializeAttr(final DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(timePartitionSlot.getStartTime(), stream);
    ReadWriteIOUtils.write(device, stream);
    ReadWriteIOUtils.write(dataSize, stream);
    ReadWriteIOUtils.write(needDecodeChunk, stream);
    chunkHeader.serializeTo(stream); // chunk header already serialize chunk type
    if (needDecodeChunk) {
      ReadWriteIOUtils.write(pageNumber, stream);
    }
  }

  @Override
  public void writeEntireChunk(final ByteBuffer chunkData, final IChunkMetadata chunkMetadata)
      throws IOException {
    dataSize += ReadWriteIOUtils.write(chunkData, stream);
    dataSize += chunkMetadata.getStatistics().serialize(stream);
  }

  @Override
  public void writeEntirePage(final PageHeader pageHeader, final ByteBuffer pageData)
      throws IOException {
    pageNumber += 1;
    dataSize += ReadWriteIOUtils.write(false, stream);
    dataSize += pageHeader.serializeTo(stream);
    dataSize += ReadWriteIOUtils.write(pageData, stream);
  }

  @Override
  public void writeDecodePage(final long[] times, final Object[] values, final int satisfiedLength)
      throws IOException {
    pageNumber += 1;
    final long startTime = timePartitionSlot.getStartTime();
    final long endTime = startTime + TimePartitionUtils.getTimePartitionInterval();
    dataSize += ReadWriteIOUtils.write(true, stream);
    dataSize += ReadWriteIOUtils.write(satisfiedLength, stream);

    for (int i = 0; i < times.length; i++) {
      if (times[i] >= endTime) {
        break;
      }
      if (times[i] >= startTime) {
        dataSize += ReadWriteIOUtils.write(times[i], stream);
        switch (chunkHeader.getDataType()) {
          case INT32:
          case DATE:
            dataSize += ReadWriteIOUtils.write((int) values[i], stream);
            break;
          case INT64:
          case TIMESTAMP:
            dataSize += ReadWriteIOUtils.write((long) values[i], stream);
            break;
          case FLOAT:
            dataSize += ReadWriteIOUtils.write((float) values[i], stream);
            break;
          case DOUBLE:
            dataSize += ReadWriteIOUtils.write((double) values[i], stream);
            break;
          case BOOLEAN:
            dataSize += ReadWriteIOUtils.write((boolean) values[i], stream);
            break;
          case TEXT:
          case BLOB:
          case STRING:
            dataSize += ReadWriteIOUtils.write((Binary) values[i], stream);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", chunkHeader.getDataType()));
        }
      }
    }
  }

  private void deserializeTsFileData(final InputStream stream) throws IOException, PageException {
    if (needDecodeChunk) {
      buildChunkWriter(stream);
    } else {
      deserializeEntireChunk(stream);
    }
  }

  private void deserializeEntireChunk(final InputStream stream) throws IOException {
    final ByteBuffer chunkData =
        ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream));
    final Statistics<? extends Serializable> statistics =
        Statistics.deserialize(stream, chunkHeader.getDataType());
    chunk = new Chunk(chunkHeader, chunkData, null, statistics);
  }

  private void buildChunkWriter(final InputStream stream) throws IOException, PageException {
    chunkWriter =
        new ChunkWriterImpl(
            new MeasurementSchema(
                chunkHeader.getMeasurementID(),
                chunkHeader.getDataType(),
                chunkHeader.getEncodingType(),
                chunkHeader.getCompressionType()));
    boolean needDecode;
    for (int j = 0; j < pageNumber; j++) {
      needDecode = ReadWriteIOUtils.readBool(stream);
      if (needDecode) {
        final int length = ReadWriteIOUtils.readInt(stream);
        for (int i = 0; i < length; i++) {
          final long time = ReadWriteIOUtils.readLong(stream);
          switch (chunkHeader.getDataType()) {
            case INT32:
            case DATE:
              chunkWriter.write(time, ReadWriteIOUtils.readInt(stream));
              break;
            case INT64:
            case TIMESTAMP:
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
            case BLOB:
            case STRING:
              chunkWriter.write(time, ReadWriteIOUtils.readBinary(stream));
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", chunkHeader.getDataType()));
          }
        }

        chunkWriter.sealCurrentPage();
      } else {
        final PageHeader pageHeader =
            PageHeader.deserializeFrom(stream, chunkHeader.getDataType(), true);
        chunkWriter.writePageHeaderAndDataIntoBuff(
            ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream)),
            pageHeader);
      }
    }
  }

  public static NonAlignedChunkData deserialize(final InputStream stream)
      throws IOException, PageException {
    final TTimePartitionSlot timePartitionSlot =
        TimePartitionUtils.getTimePartitionSlot(ReadWriteIOUtils.readLong(stream));
    final String device = ReadWriteIOUtils.readString(stream);
    final long dataSize = ReadWriteIOUtils.readLong(stream);
    final boolean needDecodeChunk = ReadWriteIOUtils.readBool(stream);
    final byte chunkType = ReadWriteIOUtils.readByte(stream);
    final ChunkHeader chunkHeader = ChunkHeader.deserializeFrom(stream, chunkType);
    int pageNumber = 0;
    if (needDecodeChunk) {
      pageNumber = ReadWriteIOUtils.readInt(stream);
    }

    final NonAlignedChunkData chunkData =
        new NonAlignedChunkData(device, chunkHeader, timePartitionSlot);
    chunkData.needDecodeChunk = needDecodeChunk;
    chunkData.pageNumber = pageNumber;
    chunkData.deserializeTsFileData(stream);
    chunkData.dataSize = dataSize;
    chunkData.close();
    return chunkData;
  }

  private void close() throws IOException {
    byteStream.close();
    stream.close();
  }

  @Override
  public String toString() {
    return "NonAlignedChunkData{"
        + "dataSize="
        + dataSize
        + ", timePartitionSlot="
        + timePartitionSlot
        + ", device='"
        + device
        + '\''
        + ", chunkHeader="
        + chunkHeader
        + ", needDecodeChunk="
        + needDecodeChunk
        + '}';
  }
}
