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
import org.apache.iotdb.db.utils.TimePartitionUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

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
  private static final int DEFAULT_INT32 = 0;
  private static final long DEFAULT_INT64 = 0L;
  private static final float DEFAULT_FLOAT = 0;
  private static final double DEFAULT_DOUBLE = 0.0;
  private static final boolean DEFAULT_BOOLEAN = false;
  private static final Binary DEFAULT_BINARY = null;

  private final TTimePartitionSlot timePartitionSlot;
  private final String device;
  private List<ChunkHeader> chunkHeaderList;

  private final PublicBAOS byteStream;
  private final DataOutputStream stream;
  private List<long[]> timeBatch;
  private long dataSize;
  private boolean needDecodeChunk;
  private List<Integer> pageNumbers;
  private Queue<Integer> satisfiedLengthQueue;

  private AlignedChunkWriterImpl chunkWriter;
  private List<Chunk> chunkList;

  public AlignedChunkData(
      String device, ChunkHeader chunkHeader, TTimePartitionSlot timePartitionSlot) {
    this.dataSize = 0;
    this.device = device;
    this.chunkHeaderList = new ArrayList<>();
    this.timePartitionSlot = timePartitionSlot;
    this.needDecodeChunk = true;
    this.pageNumbers = new ArrayList<>();
    this.satisfiedLengthQueue = new LinkedList<>();
    this.byteStream = new PublicBAOS();
    this.stream = new DataOutputStream(byteStream);

    chunkHeaderList.add(chunkHeader);
    pageNumbers.add(0);
    addAttrDataSize();
  }

  private void addAttrDataSize() { // should be init before serialize, corresponding serializeAttr
    dataSize += 2 * Byte.BYTES; // isModification and isAligned
    dataSize += Long.BYTES; // timePartitionSlot
    int deviceLength = device.getBytes(TSFileConfig.STRING_CHARSET).length;
    dataSize += ReadWriteForEncodingUtils.varIntSize(deviceLength);
    dataSize += deviceLength; // device
    dataSize += Integer.BYTES; // chunkHeaderListSize
    dataSize += chunkHeaderList.get(0).getSerializedSize(); // timeChunkHeader
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
  public void writeToFileWriter(TsFileIOWriter writer) throws IOException {
    if (chunkList != null) {
      for (Chunk chunk : chunkList) {
        writer.writeChunk(chunk);
      }
    } else {
      chunkWriter.writeToFileWriter(writer);
    }
  }

  public void addValueChunk(ChunkHeader chunkHeader) {
    this.chunkHeaderList.add(chunkHeader);
    this.pageNumbers.add(0);
    dataSize += chunkHeader.getSerializedSize();
    if (needDecodeChunk) {
      dataSize += Integer.BYTES; // pageNumber
    }
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(isModification(), stream);
    ReadWriteIOUtils.write(isAligned(), stream);
    serializeAttr(stream);
    byteStream.writeTo(stream);
    close();
  }

  private void serializeAttr(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(timePartitionSlot.getStartTime(), stream);
    ReadWriteIOUtils.write(device, stream);
    ReadWriteIOUtils.write(needDecodeChunk, stream);
    ReadWriteIOUtils.write(chunkHeaderList.size(), stream);
    for (ChunkHeader chunkHeader : chunkHeaderList) {
      chunkHeader.serializeTo(stream); // chunk header already serialize chunk type
    }
    if (needDecodeChunk) {
      for (Integer pageNumber : pageNumbers) {
        ReadWriteIOUtils.write(pageNumber, stream);
      }
    }
  }

  @Override
  public void writeEntireChunk(ByteBuffer chunkData, IChunkMetadata chunkMetadata)
      throws IOException {
    dataSize += ReadWriteIOUtils.write(chunkData, stream);
    dataSize += chunkMetadata.getStatistics().serialize(stream);
  }

  @Override
  public void writeEntirePage(PageHeader pageHeader, ByteBuffer pageData) throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    dataSize += ReadWriteIOUtils.write(false, stream);
    dataSize += pageHeader.serializeTo(stream);
    dataSize += ReadWriteIOUtils.write(pageData, stream);
  }

  @Override
  public void writeDecodePage(long[] times, Object[] values, int satisfiedLength)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    satisfiedLengthQueue.offer(satisfiedLength);
    long startTime = timePartitionSlot.getStartTime();
    long endTime = startTime + TimePartitionUtils.getTimePartitionInterval();
    dataSize += ReadWriteIOUtils.write(true, stream);
    dataSize += ReadWriteIOUtils.write(satisfiedLength, stream);

    for (int i = 0; i < times.length; i++) {
      if (times[i] < startTime) {
        continue;
      } else if (times[i] >= endTime) {
        break;
      }
      dataSize += ReadWriteIOUtils.write(times[i], stream);
    }
  }

  public void writeDecodeValuePage(long[] times, TsPrimitiveType[] values, TSDataType dataType)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    long startTime = timePartitionSlot.getStartTime();
    long endTime = startTime + TimePartitionUtils.getTimePartitionInterval();
    int satisfiedLength = satisfiedLengthQueue.poll();
    dataSize += ReadWriteIOUtils.write(true, stream);
    dataSize += ReadWriteIOUtils.write(satisfiedLength, stream);
    satisfiedLengthQueue.offer(satisfiedLength);

    for (int i = 0; i < times.length; i++) {
      if (times[i] < startTime) {
        continue;
      } else if (times[i] >= endTime) {
        break;
      }

      if (values[i] == null) {
        dataSize += ReadWriteIOUtils.write(true, stream);
        continue;
      }
      dataSize += ReadWriteIOUtils.write(false, stream);
      switch (dataType) {
        case INT32:
          dataSize += ReadWriteIOUtils.write(values[i].getInt(), stream);
          break;
        case INT64:
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
          dataSize += ReadWriteIOUtils.write(values[i].getBinary(), stream);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
    }
  }

  private void deserializeTsFileData(InputStream stream) throws IOException, PageException {
    if (needDecodeChunk) {
      buildChunkWriter(stream);
    } else {
      deserializeEntireChunk(stream);
    }
  }

  private void deserializeEntireChunk(InputStream stream) throws IOException {
    chunkList = new ArrayList<>();
    int chunkSize = chunkHeaderList.size();
    for (int i = 0; i < chunkSize; i++) {
      ByteBuffer chunkData =
          ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream));
      Statistics<? extends Serializable> statistics =
          Statistics.deserialize(stream, chunkHeaderList.get(i).getDataType());
      chunkList.add(new Chunk(chunkHeaderList.get(i), chunkData, null, statistics));
    }
  }

  private void buildChunkWriter(InputStream stream) throws IOException, PageException {
    List<IMeasurementSchema> measurementSchemaList = new ArrayList<>();
    for (ChunkHeader chunkHeader : chunkHeaderList) {
      if (TSDataType.VECTOR.equals(chunkHeader.getDataType())) {
        continue;
      }
      measurementSchemaList.add(
          new MeasurementSchema(
              chunkHeader.getMeasurementID(),
              chunkHeader.getDataType(),
              chunkHeader.getEncodingType(),
              chunkHeader.getCompressionType()));
    }
    chunkWriter = new AlignedChunkWriterImpl(measurementSchemaList);
    timeBatch = new ArrayList<>();
    int chunkHeaderSize = chunkHeaderList.size();
    for (int i = 0; i < chunkHeaderSize; i++) {
      buildChunk(stream, chunkHeaderList.get(i), pageNumbers.get(i), i - 1, i == 0);
    }
    timeBatch = null;
  }

  private void buildChunk(
      InputStream stream,
      ChunkHeader chunkHeader,
      int pageNumber,
      int valueChunkIndex,
      boolean isTimeChunk)
      throws IOException, PageException {
    boolean needDecode;
    int decodePageIndex = 0;
    for (int j = 0; j < pageNumber; j++) {
      needDecode = ReadWriteIOUtils.readBool(stream);
      if (needDecode) {
        int length = ReadWriteIOUtils.readInt(stream);
        long[] timePageBatch = new long[length];
        if (!isTimeChunk) {
          timePageBatch = timeBatch.get(decodePageIndex);
        }
        for (int i = 0; i < length; i++) {
          if (isTimeChunk) {
            long time = ReadWriteIOUtils.readLong(stream);
            timePageBatch[i] = time;
            chunkWriter.writeTime(time);
          } else {
            boolean isNull = ReadWriteIOUtils.readBool(stream);
            switch (chunkHeader.getDataType()) {
              case INT32:
                int int32Value = isNull ? DEFAULT_INT32 : ReadWriteIOUtils.readInt(stream);
                chunkWriter.write(timePageBatch[i], int32Value, isNull, valueChunkIndex);
                break;
              case INT64:
                long int64Value = isNull ? DEFAULT_INT64 : ReadWriteIOUtils.readLong(stream);
                chunkWriter.write(timePageBatch[i], int64Value, isNull, valueChunkIndex);
                break;
              case FLOAT:
                float floatValue = isNull ? DEFAULT_FLOAT : ReadWriteIOUtils.readFloat(stream);
                chunkWriter.write(timePageBatch[i], floatValue, isNull, valueChunkIndex);
                break;
              case DOUBLE:
                double doubleValue = isNull ? DEFAULT_DOUBLE : ReadWriteIOUtils.readDouble(stream);
                chunkWriter.write(timePageBatch[i], doubleValue, isNull, valueChunkIndex);
                break;
              case BOOLEAN:
                boolean boolValue = isNull ? DEFAULT_BOOLEAN : ReadWriteIOUtils.readBool(stream);
                chunkWriter.write(timePageBatch[i], boolValue, isNull, valueChunkIndex);
                break;
              case TEXT:
                Binary binaryValue = isNull ? DEFAULT_BINARY : ReadWriteIOUtils.readBinary(stream);
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
        PageHeader pageHeader = PageHeader.deserializeFrom(stream, chunkHeader.getDataType(), true);
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

  public static AlignedChunkData deserialize(InputStream stream) throws IOException, PageException {
    TTimePartitionSlot timePartitionSlot =
        TimePartitionUtils.getTimePartition(ReadWriteIOUtils.readLong(stream));
    String device = ReadWriteIOUtils.readString(stream);
    boolean needDecodeChunk = ReadWriteIOUtils.readBool(stream);
    int chunkHeaderListSize = ReadWriteIOUtils.readInt(stream);
    List<ChunkHeader> chunkHeaderList = new ArrayList<>();
    for (int i = 0; i < chunkHeaderListSize; i++) {
      byte chunkType = ReadWriteIOUtils.readByte(stream);
      chunkHeaderList.add(ChunkHeader.deserializeFrom(stream, chunkType));
    }
    List<Integer> pageNumbers = new ArrayList<>();
    if (needDecodeChunk) {
      for (int i = 0; i < chunkHeaderListSize; i++) {
        pageNumbers.add(ReadWriteIOUtils.readInt(stream));
      }
    }

    AlignedChunkData chunkData =
        new AlignedChunkData(device, chunkHeaderList.get(0), timePartitionSlot);
    chunkData.needDecodeChunk = needDecodeChunk;
    chunkData.chunkHeaderList = chunkHeaderList;
    chunkData.pageNumbers = pageNumbers;
    chunkData.deserializeTsFileData(stream);
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
