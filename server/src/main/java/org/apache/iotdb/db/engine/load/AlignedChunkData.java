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
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.DataOutputStream;
import java.io.File;
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

  private List<Long> offset;
  private List<Long> dataSize;
  private boolean isHeadPageNeedDecode;
  private boolean isTailPageNeedDecode;

  private TTimePartitionSlot timePartitionSlot;
  private String device;
  private List<ChunkHeader> chunkHeaderList;
  private List<IChunkMetadata> chunkMetadataList;

  private List<long[]> timeBatch;
  private List<Integer> satisfiedTimeBatchLength;

  private AlignedChunkWriterImpl chunkWriter;
  private List<Chunk> chunkList;

  private long totalDataSize;
  private PublicBAOS byteStream;
  private DataOutputStream stream;
  private boolean needDecodeChunk;
  private List<Integer> pageNumbers;
  private Queue<Integer> satisfiedLengthQueue;

  public AlignedChunkData(long timeOffset, String device, ChunkHeader chunkHeader) {
    this.offset = new ArrayList<>();
    this.dataSize = new ArrayList<>();
    this.isHeadPageNeedDecode = false;
    this.isTailPageNeedDecode = false;
    this.device = device;
    this.chunkHeaderList = new ArrayList<>();

    offset.add(timeOffset);
    dataSize.add(0L);
    chunkHeaderList.add(chunkHeader);
  }

  public AlignedChunkData(
      String device, ChunkHeader chunkHeader, TTimePartitionSlot timePartitionSlot) {
    this.totalDataSize = 0;
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
    return dataSize.stream().mapToLong(o -> o).sum();
  }

  @Override
  public void addDataSize(long pageSize) {
    dataSize.set(0, dataSize.get(0) + pageSize);
  }

  @Override
  public void setNotDecode(IChunkMetadata chunkMetadata) {
    chunkMetadataList = new ArrayList<>();
    chunkMetadataList.add(chunkMetadata);
  }

  @Override
  public void setNotDecode() {
    needDecodeChunk = false;
  }

  @Override
  public boolean needDecodeChunk() {
    return chunkMetadataList == null;
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

  public void addValueChunk(long offset, ChunkHeader chunkHeader, IChunkMetadata chunkMetadata) {
    this.offset.add(offset);
    this.dataSize.add(0L);
    this.chunkHeaderList.add(chunkHeader);
    if (chunkMetadataList != null) {
      chunkMetadataList.add(chunkMetadata);
    }
  }

  public void addValueChunk(ChunkHeader chunkHeader) {
    this.chunkHeaderList.add(chunkHeader);
    this.pageNumbers.add(0);
  }

  public void addValueChunkDataSize(long dataSize) {
    int lastIndex = this.dataSize.size() - 1;
    this.dataSize.set(lastIndex, this.dataSize.get(lastIndex) + dataSize);
  }

  @Override
  public void serialize(DataOutputStream stream, File tsFile) throws IOException {
    ReadWriteIOUtils.write(isModification(), stream);
    ReadWriteIOUtils.write(isAligned(), stream);
    serializeAttr(stream);
    serializeTsFileData(stream, tsFile);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(isModification(), stream);
    ReadWriteIOUtils.write(isAligned(), stream);
    serializeAttr(stream);
    byteStream.writeTo(stream);
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

  private void serializeTsFileData(DataOutputStream stream, File tsFile) throws IOException {
    timeBatch = new ArrayList<>();
    satisfiedTimeBatchLength = new ArrayList<>();
    ReadWriteIOUtils.write(needDecodeChunk(), stream);
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      int chunkSize = offset.size();
      for (int i = 0; i < chunkSize; i++) {
        if (needDecodeChunk()) {
          serializeDecodeChunk(stream, reader, chunkHeaderList.get(i), i);
        } else {
          serializeEntireChunk(stream, reader, chunkHeaderList.get(i), chunkMetadataList.get(i));
        }
      }
    }
    timeBatch = null;
    satisfiedTimeBatchLength = null;
  }

  @Override
  public void writeEntireChunk(ByteBuffer chunkData, IChunkMetadata chunkMetadata)
      throws IOException {
    totalDataSize += ReadWriteIOUtils.write(chunkData, stream);
    chunkMetadata.getStatistics().serialize(stream);
  }

  private void serializeEntireChunk(
      DataOutputStream stream,
      TsFileSequenceReader reader,
      ChunkHeader chunkHeader,
      IChunkMetadata chunkMetadata)
      throws IOException {
    ByteBuffer chunkData =
        reader.readChunk(
            chunkMetadata.getOffsetOfChunkHeader() + chunkHeader.getSerializedSize(),
            chunkHeader.getDataSize());
    ReadWriteIOUtils.write(chunkData, stream);
    chunkMetadata.getStatistics().serialize(stream);
  }

  @Override
  public void writeEntirePage(PageHeader pageHeader, ByteBuffer pageData) throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    totalDataSize += ReadWriteIOUtils.write(false, stream);
    pageHeader.serializeTo(stream);
    totalDataSize += ReadWriteIOUtils.write(pageData, stream);
  }

  @Override
  public void writeDecodePage(long[] times, Object[] values, int satisfiedLength)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    satisfiedLengthQueue.offer(satisfiedLength);
    long startTime = timePartitionSlot.getStartTime();
    long endTime = startTime + TimePartitionUtils.getTimePartitionIntervalForRouting();
    totalDataSize += ReadWriteIOUtils.write(true, stream);
    totalDataSize += ReadWriteIOUtils.write(satisfiedLength, stream);

    for (int i = 0; i < times.length; i++) {
      if (times[i] < startTime) {
        continue;
      } else if (times[i] >= endTime) {
        break;
      }
      totalDataSize += ReadWriteIOUtils.write(times[i], stream);
    }
  }

  public void writeDecodeValuePage(long[] times, TsPrimitiveType[] values, TSDataType dataType)
      throws IOException {
    long startTime = timePartitionSlot.getStartTime();
    long endTime = startTime + TimePartitionUtils.getTimePartitionIntervalForRouting();
    int satisfiedLength = satisfiedLengthQueue.poll();
    totalDataSize += ReadWriteIOUtils.write(true, stream);
    totalDataSize += ReadWriteIOUtils.write(satisfiedLength, stream);
    satisfiedLengthQueue.offer(satisfiedLength);

    for (int i = 0; i < times.length; i++) {
      if (times[i] < startTime) {
        continue;
      } else if (times[i] >= endTime) {
        break;
      }

      if (values[i] == null) {
        totalDataSize += ReadWriteIOUtils.write(true, stream);
        continue;
      }
      totalDataSize += ReadWriteIOUtils.write(false, stream);
      switch (dataType) {
        case INT32:
          totalDataSize += ReadWriteIOUtils.write(values[i].getInt(), stream);
          break;
        case INT64:
          totalDataSize += ReadWriteIOUtils.write(values[i].getLong(), stream);
          break;
        case FLOAT:
          totalDataSize += ReadWriteIOUtils.write(values[i].getFloat(), stream);
          break;
        case DOUBLE:
          totalDataSize += ReadWriteIOUtils.write(values[i].getDouble(), stream);
          break;
        case BOOLEAN:
          totalDataSize += ReadWriteIOUtils.write(values[i].getBoolean(), stream);
          break;
        case TEXT:
          totalDataSize += ReadWriteIOUtils.write(values[i].getBinary(), stream);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
    }
  }

  private void serializeDecodeChunk(
      DataOutputStream stream, TsFileSequenceReader reader, ChunkHeader chunkHeader, int chunkIndex)
      throws IOException {
    Decoder defaultTimeDecoder =
        Decoder.getDecoderByType(
            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
            TSDataType.INT64);
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());

    reader.position(offset.get(chunkIndex));
    int decodePageIndex = 0; // should be 0,1 or 2
    long dataSize = this.dataSize.get(chunkIndex);
    while (dataSize > 0) {
      boolean hasStatistic = (chunkHeader.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER;
      PageHeader pageHeader = reader.readPageHeader(chunkHeader.getDataType(), hasStatistic);
      long pageDataSize = pageHeader.getSerializedPageSize();
      if ((dataSize == this.dataSize.get(chunkIndex) && isHeadPageNeedDecode) // decode head page
          || (dataSize == pageDataSize && isTailPageNeedDecode)) { // decode tail page
        ReadWriteIOUtils.write(true, stream); // decode
        if (chunkIndex == 0) {
          decodeTimePage(reader, chunkHeader, pageHeader, defaultTimeDecoder, valueDecoder, stream);
        } else {
          decodeValuePage(reader, chunkHeader, pageHeader, decodePageIndex, valueDecoder, stream);
        }
        decodePageIndex += 1;
      } else { // entire page
        ReadWriteIOUtils.write(false, stream); // don't decode
        pageHeader.serializeTo(stream);
        ByteBuffer pageData = reader.readCompressedPage(pageHeader);
        ReadWriteIOUtils.write(pageData, stream);
      }
      dataSize -= pageDataSize;
    }

    ReadWriteIOUtils.write(true, stream); // means ending
    ReadWriteIOUtils.write(-1, stream);
  }

  private void decodeTimePage(
      TsFileSequenceReader reader,
      ChunkHeader chunkHeader,
      PageHeader pageHeader,
      Decoder timeDecoder,
      Decoder valueDecoder,
      DataOutputStream stream)
      throws IOException {
    valueDecoder.reset();
    ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
    TimePageReader timePageReader = new TimePageReader(pageHeader, pageData, timeDecoder);
    long[] decodeTime = timePageReader.getNextTimeBatch();
    int satisfiedLength = 0;
    long[] time = new long[decodeTime.length];
    for (int i = 0; i < decodeTime.length; i++) {
      if (decodeTime[i] < timePartitionSlot.getStartTime()) {
        continue;
      } else if (!timePartitionSlot.equals(
          TimePartitionUtils.getTimePartitionForRouting(decodeTime[i]))) {
        break;
      }
      time[satisfiedLength++] = decodeTime[i];
    }
    ReadWriteIOUtils.write(satisfiedLength, stream);
    for (int i = 0; i < satisfiedLength; i++) {
      ReadWriteIOUtils.write(time[i], stream);
    }
    timeBatch.add(decodeTime);
    satisfiedTimeBatchLength.add(satisfiedLength);
  }

  private void decodeValuePage(
      TsFileSequenceReader reader,
      ChunkHeader chunkHeader,
      PageHeader pageHeader,
      int pageIndex,
      Decoder valueDecoder,
      DataOutputStream stream)
      throws IOException {
    valueDecoder.reset();
    ByteBuffer pageData = reader.readPage(pageHeader, chunkHeader.getCompressionType());
    ValuePageReader valuePageReader =
        new ValuePageReader(pageHeader, pageData, chunkHeader.getDataType(), valueDecoder);
    long[] time = timeBatch.get(pageIndex);
    TsPrimitiveType[] valueBatch =
        valuePageReader.nextValueBatch(
            time); // should be origin time, so recording satisfied length is necessary
    ReadWriteIOUtils.write(satisfiedTimeBatchLength.get(pageIndex), stream);
    for (int i = 0; i < valueBatch.length; i++) {
      if (time[i] < timePartitionSlot.getStartTime()) {
        continue;
      } else if (!timePartitionSlot.equals(
          TimePartitionUtils.getTimePartitionForRouting(time[i]))) {
        break;
      }
      if (valueBatch[i] == null) {
        ReadWriteIOUtils.write(true, stream);
        continue;
      }
      ReadWriteIOUtils.write(false, stream);
      switch (chunkHeader.getDataType()) {
        case INT32:
          ReadWriteIOUtils.write(valueBatch[i].getInt(), stream);
          break;
        case INT64:
          ReadWriteIOUtils.write(valueBatch[i].getLong(), stream);
          break;
        case FLOAT:
          ReadWriteIOUtils.write(valueBatch[i].getFloat(), stream);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write(valueBatch[i].getDouble(), stream);
          break;
        case BOOLEAN:
          ReadWriteIOUtils.write(valueBatch[i].getBoolean(), stream);
          break;
        case TEXT:
          ReadWriteIOUtils.write(valueBatch[i].getBinary(), stream);
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", chunkHeader.getDataType()));
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
        TimePartitionUtils.getTimePartitionForRouting(ReadWriteIOUtils.readLong(stream));
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
    return chunkData;
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
        + totalDataSize
        + ", needDecodeChunk="
        + needDecodeChunk
        + '}';
  }
}
