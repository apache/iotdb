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
import org.apache.tsfile.enums.TSDataType;
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
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class AlignedChunkData implements ChunkData {
  protected static final int DEFAULT_INT32 = 0;
  protected static final long DEFAULT_INT64 = 0L;
  protected static final float DEFAULT_FLOAT = 0;
  protected static final double DEFAULT_DOUBLE = 0.0;
  protected static final boolean DEFAULT_BOOLEAN = false;
  protected static final Binary DEFAULT_BINARY = null;

  protected final TTimePartitionSlot timePartitionSlot;
  protected final String device;
  protected List<ChunkHeader> chunkHeaderList;

  protected final PublicBAOS byteStream;
  protected final DataOutputStream stream;
  protected List<long[]> timeBatch;
  protected long dataSize;
  protected boolean needDecodeChunk;
  protected List<Integer> pageNumbers;
  protected Queue<Integer> satisfiedLengthQueue;

  private AlignedChunkWriterImpl chunkWriter;
  protected List<Chunk> chunkList;

  public AlignedChunkData(
      final String device,
      final ChunkHeader chunkHeader,
      final TTimePartitionSlot timePartitionSlot) {
    this(device, timePartitionSlot);
    chunkHeaderList.add(chunkHeader);
    pageNumbers.add(0);
    addAttrDataSize();
  }

  protected AlignedChunkData(AlignedChunkData alignedChunkData) {
    this(alignedChunkData.device, alignedChunkData.timePartitionSlot);
    this.satisfiedLengthQueue = new LinkedList<>(alignedChunkData.satisfiedLengthQueue);
    this.needDecodeChunk = alignedChunkData.needDecodeChunk;
    addAttrDataSize();
  }

  protected AlignedChunkData(String device, TTimePartitionSlot timePartitionSlot) {
    this.dataSize = 0;
    this.device = device;
    this.chunkHeaderList = new ArrayList<>();
    this.timePartitionSlot = timePartitionSlot;
    this.needDecodeChunk = true;
    this.pageNumbers = new ArrayList<>();
    this.satisfiedLengthQueue = new LinkedList<>();
    this.byteStream = new PublicBAOS();
    this.stream = new DataOutputStream(byteStream);
  }

  private void addAttrDataSize() { // Should be init before serialize, corresponding serializeAttr
    dataSize += 2 * Byte.BYTES; // isModification and isAligned
    dataSize += Long.BYTES; // timePartitionSlot
    final int deviceLength = device.getBytes(TSFileConfig.STRING_CHARSET).length;
    dataSize += ReadWriteForEncodingUtils.varIntSize(deviceLength);
    dataSize += deviceLength; // device
    dataSize += Integer.BYTES; // chunkHeaderListSize
    if (!chunkHeaderList.isEmpty()) {
      dataSize += chunkHeaderList.get(0).getSerializedSize(); // timeChunkHeader
    }
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
    return true;
  }

  @Override
  public void writeToFileWriter(final TsFileIOWriter writer) throws IOException {
    if (chunkList != null) {
      for (final Chunk chunk : chunkList) {
        writer.writeChunk(chunk);
      }
    } else {
      chunkWriter.writeToFileWriter(writer);
    }
  }

  public void addValueChunk(final ChunkHeader chunkHeader) {
    this.chunkHeaderList.add(chunkHeader);
    this.pageNumbers.add(0);
    dataSize += chunkHeader.getSerializedSize();
    if (needDecodeChunk) {
      dataSize += Integer.BYTES; // pageNumber
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
    ReadWriteIOUtils.write(chunkHeaderList.size(), stream);
    for (final ChunkHeader chunkHeader : chunkHeaderList) {
      chunkHeader.serializeTo(stream); // Chunk header already serialize chunk type
    }
    if (needDecodeChunk) {
      for (final Integer pageNumber : pageNumbers) {
        ReadWriteIOUtils.write(pageNumber, stream);
      }
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
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    // serialize needDecode==false
    dataSize += ReadWriteIOUtils.write(false, stream);
    // serialize pageHeader
    dataSize += pageHeader.serializeTo(stream);
    // serialize pageData
    dataSize += ReadWriteIOUtils.write(pageData, stream);
  }

  @Override
  public void writeDecodePage(final long[] times, final Object[] values, final int satisfiedLength)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    satisfiedLengthQueue.offer(satisfiedLength);
    final long startTime = timePartitionSlot.getStartTime();
    final long endTime = startTime + TimePartitionUtils.getTimePartitionInterval();
    // serialize needDecode==true
    dataSize += ReadWriteIOUtils.write(true, stream);
    dataSize += ReadWriteIOUtils.write(satisfiedLength, stream);

    for (final long time : times) {
      if (time >= endTime) {
        break;
      }
      if (time >= startTime) {
        dataSize += ReadWriteIOUtils.write(time, stream);
      }
    }
  }

  public void writeDecodeValuePage(
      final long[] times, final TsPrimitiveType[] values, final TSDataType dataType)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    final long startTime = timePartitionSlot.getStartTime();
    final long endTime = startTime + TimePartitionUtils.getTimePartitionInterval();
    final int satisfiedLength = satisfiedLengthQueue.poll();
    // serialize needDecode==true
    dataSize += ReadWriteIOUtils.write(true, stream);
    dataSize += ReadWriteIOUtils.write(satisfiedLength, stream);
    satisfiedLengthQueue.offer(satisfiedLength);

    for (int i = 0; i < times.length; i++) {
      if (times[i] >= endTime) {
        break;
      }
      if (times[i] >= startTime) {
        if (values.length == 0 || values[i] == null) {
          dataSize += ReadWriteIOUtils.write(true, stream);
        } else {
          dataSize += ReadWriteIOUtils.write(false, stream);
          switch (dataType) {
            case INT32:
            case DATE:
              dataSize += ReadWriteIOUtils.write(values[i].getInt(), stream);
              break;
            case INT64:
            case TIMESTAMP:
              dataSize += ReadWriteIOUtils.write(values[i].getLong(), stream);
              break;
            case FLOAT:
              dataSize += ReadWriteIOUtils.write(values[i].getFloat(), stream);
              break;
            case DOUBLE:
              dataSize += ReadWriteIOUtils.write(values[i].getDouble(), stream);
              break;
            case BOOLEAN:
              dataSize += ReadWriteIOUtils.write(values[i].getBoolean(), stream);
              break;
            case TEXT:
            case BLOB:
            case STRING:
              dataSize += ReadWriteIOUtils.write(values[i].getBinary(), stream);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", dataType));
          }
        }
      }
    }
  }

  protected void deserializeTsFileData(final InputStream stream) throws IOException, PageException {
    if (needDecodeChunk) {
      buildChunkWriter(stream);
    } else {
      deserializeEntireChunk(stream);
    }
  }

  private void deserializeEntireChunk(final InputStream stream) throws IOException {
    chunkList = new ArrayList<>();
    for (final ChunkHeader chunkHeader : chunkHeaderList) {
      final ByteBuffer chunkData =
          ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream));
      final Statistics<? extends Serializable> statistics =
          Statistics.deserialize(stream, chunkHeader.getDataType());
      chunkList.add(new Chunk(chunkHeader, chunkData, null, statistics));
    }
  }

  protected void buildChunkWriter(final InputStream stream) throws IOException, PageException {
    final List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    IMeasurementSchema timeSchema = null;
    for (final ChunkHeader chunkHeader : chunkHeaderList) {
      if (TSDataType.VECTOR.equals(chunkHeader.getDataType())) {
        timeSchema =
            new MeasurementSchema(
                chunkHeader.getMeasurementID(),
                chunkHeader.getDataType(),
                chunkHeader.getEncodingType(),
                chunkHeader.getCompressionType());
        continue;
      }
      measurementSchemaList.add(
          new MeasurementSchema(
              chunkHeader.getMeasurementID(),
              chunkHeader.getDataType(),
              chunkHeader.getEncodingType(),
              chunkHeader.getCompressionType()));
    }
    chunkWriter = new AlignedChunkWriterImpl(timeSchema, measurementSchemaList);
    timeBatch = new ArrayList<>();
    final int chunkHeaderSize = chunkHeaderList.size();
    for (int i = 0; i < chunkHeaderSize; i++) {
      buildChunk(stream, chunkHeaderList.get(i), pageNumbers.get(i), i - 1, i == 0);
    }
    timeBatch = null;
  }

  @SuppressWarnings({"squid:S6541", "squid:S3776"})
  private void buildChunk(
      final InputStream stream,
      final ChunkHeader chunkHeader,
      final int pageNumber,
      final int valueChunkIndex,
      final boolean isTimeChunk)
      throws IOException, PageException {
    boolean needDecode;
    int decodePageIndex = 0;
    for (int j = 0; j < pageNumber; j++) {
      needDecode = ReadWriteIOUtils.readBool(stream);
      if (needDecode) {
        final int length = ReadWriteIOUtils.readInt(stream);
        long[] timePageBatch = new long[length];
        if (!isTimeChunk) {
          timePageBatch = timeBatch.get(decodePageIndex);
        }
        for (int i = 0; i < length; i++) {
          if (isTimeChunk) {
            final long time = ReadWriteIOUtils.readLong(stream);
            timePageBatch[i] = time;
            chunkWriter.writeTime(time);
          } else {
            final boolean isNull = ReadWriteIOUtils.readBool(stream);
            switch (chunkHeader.getDataType()) {
              case INT32:
              case DATE:
                final int int32Value = isNull ? DEFAULT_INT32 : ReadWriteIOUtils.readInt(stream);
                chunkWriter.write(timePageBatch[i], int32Value, isNull, valueChunkIndex);
                break;
              case INT64:
              case TIMESTAMP:
                final long int64Value = isNull ? DEFAULT_INT64 : ReadWriteIOUtils.readLong(stream);
                chunkWriter.write(timePageBatch[i], int64Value, isNull, valueChunkIndex);
                break;
              case FLOAT:
                final float floatValue =
                    isNull ? DEFAULT_FLOAT : ReadWriteIOUtils.readFloat(stream);
                chunkWriter.write(timePageBatch[i], floatValue, isNull, valueChunkIndex);
                break;
              case DOUBLE:
                final double doubleValue =
                    isNull ? DEFAULT_DOUBLE : ReadWriteIOUtils.readDouble(stream);
                chunkWriter.write(timePageBatch[i], doubleValue, isNull, valueChunkIndex);
                break;
              case BOOLEAN:
                final boolean boolValue =
                    isNull ? DEFAULT_BOOLEAN : ReadWriteIOUtils.readBool(stream);
                chunkWriter.write(timePageBatch[i], boolValue, isNull, valueChunkIndex);
                break;
              case TEXT:
              case BLOB:
              case STRING:
                final Binary binaryValue =
                    isNull ? DEFAULT_BINARY : ReadWriteIOUtils.readBinary(stream);
                chunkWriter.write(timePageBatch[i], binaryValue, isNull, valueChunkIndex);
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", chunkHeader.getDataType()));
            }
          }
        }
        if (isTimeChunk) {
          timeBatch.add(timePageBatch);
        }
        decodePageIndex += 1;

        if (isTimeChunk) {
          chunkWriter.sealCurrentTimePage();
        } else {
          chunkWriter.sealCurrentValuePage(valueChunkIndex);
        }
      } else {
        final PageHeader pageHeader =
            PageHeader.deserializeFrom(stream, chunkHeader.getDataType(), true);
        if (isTimeChunk) {
          chunkWriter.writePageHeaderAndDataIntoTimeBuff(
              ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream)),
              pageHeader);
        } else {
          chunkWriter.writePageHeaderAndDataIntoValueBuff(
              ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream)),
              pageHeader,
              valueChunkIndex);
        }
      }
    }
  }

  public static AlignedChunkData deserialize(final InputStream stream)
      throws IOException, PageException {
    final TTimePartitionSlot timePartitionSlot =
        TimePartitionUtils.getTimePartitionSlot(ReadWriteIOUtils.readLong(stream));
    final String device = ReadWriteIOUtils.readString(stream);
    final long dataSize = ReadWriteIOUtils.readLong(stream);
    final boolean needDecodeChunk = ReadWriteIOUtils.readBool(stream);
    final int chunkHeaderListSize = ReadWriteIOUtils.readInt(stream);
    final List<ChunkHeader> chunkHeaderList = new ArrayList<>();
    for (int i = 0; i < chunkHeaderListSize; i++) {
      final byte chunkType = ReadWriteIOUtils.readByte(stream);
      chunkHeaderList.add(ChunkHeader.deserializeFrom(stream, chunkType));
    }
    final List<Integer> pageNumbers = new ArrayList<>();
    if (needDecodeChunk) {
      for (int i = 0; i < chunkHeaderListSize; i++) {
        pageNumbers.add(ReadWriteIOUtils.readInt(stream));
      }
    }

    final AlignedChunkData chunkData;
    if (chunkHeaderList.get(0).getMeasurementID().equals("")) {
      chunkData = new AlignedChunkData(device, chunkHeaderList.get(0), timePartitionSlot);
    } else {
      chunkData = new BatchedAlignedValueChunkData(device, timePartitionSlot);
    }
    chunkData.needDecodeChunk = needDecodeChunk;
    chunkData.chunkHeaderList = chunkHeaderList;
    chunkData.pageNumbers = pageNumbers;
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
    return "AlignedChunkData{"
        + "timePartitionSlot="
        + timePartitionSlot
        + ", device='"
        + device
        + '\''
        + ", chunkHeaderList="
        + chunkHeaderList
        + ", totalDataSize="
        + dataSize
        + ", needDecodeChunk="
        + needDecodeChunk
        + '}';
  }
}
