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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.writer.TsFileIOWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to be compatible with the new distribution of aligned series in chunk group.
 * In past versions, a time chunk and all its corresponding value chunks were continuous within the
 * chunk group. In order to solve the problem of excessive memory usage during compaction, the value
 * column is grouped and merged, which will cause the above rules to be broken.
 */
public class BatchedAlignedValueChunkData extends AlignedChunkData {

  private List<ValueChunkWriter> valueChunkWriters;

  // Used for splitter
  public BatchedAlignedValueChunkData(AlignedChunkData alignedChunkData) {
    super(alignedChunkData);
  }

  // Used for deserialize
  public BatchedAlignedValueChunkData(String device, TTimePartitionSlot timePartitionSlot) {
    super(device, timePartitionSlot);
    valueChunkWriters = new ArrayList<>();
  }

  @Override
  public void writeDecodeValuePage(long[] times, TsPrimitiveType[] values, TSDataType dataType)
      throws IOException {
    pageNumbers.set(pageNumbers.size() - 1, pageNumbers.get(pageNumbers.size() - 1) + 1);
    final long startTime = timePartitionSlot.getStartTime();
    final long endTime = startTime + TimePartitionUtils.getTimePartitionInterval();
    final int satisfiedLength = satisfiedLengthQueue.poll();
    // serialize needDecode==true
    dataSize += ReadWriteIOUtils.write(true, stream);
    dataSize += ReadWriteIOUtils.write(satisfiedLength, stream);
    satisfiedLengthQueue.offer(satisfiedLength);

    // The time column data is not included after serialization, so each value page needs to record
    // its start time and end time.
    long pageStartTime = Long.MAX_VALUE, pageEndTime = Long.MIN_VALUE;
    for (int i = 0; i < times.length; i++) {
      if (times[i] >= endTime) {
        break;
      }
      if (times[i] >= startTime) {
        if (values.length == 0 || values[i] == null) {
          dataSize += ReadWriteIOUtils.write(true, stream);
        } else {
          pageStartTime = Math.min(pageStartTime, times[i]);
          pageEndTime = Math.max(pageEndTime, times[i]);
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
    dataSize += ReadWriteIOUtils.write(pageStartTime, stream);
    dataSize += ReadWriteIOUtils.write(pageEndTime, stream);
  }

  @Override
  protected void buildChunkWriter(final InputStream stream) throws IOException, PageException {
    for (int i = 0; i < chunkHeaderList.size(); i++) {
      ChunkHeader chunkHeader = chunkHeaderList.get(i);
      MeasurementSchema measurementSchema =
          new MeasurementSchema(
              chunkHeader.getMeasurementID(),
              chunkHeader.getDataType(),
              chunkHeader.getEncodingType(),
              chunkHeader.getCompressionType());
      ValueChunkWriter valueChunkWriter =
          new ValueChunkWriter(
              measurementSchema.getMeasurementId(),
              measurementSchema.getCompressor(),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getValueEncoder());
      valueChunkWriters.add(valueChunkWriter);
      buildValueChunkWriter(stream, chunkHeader, pageNumbers.get(i), valueChunkWriter);
    }
  }

  private void buildValueChunkWriter(
      final InputStream stream,
      final ChunkHeader chunkHeader,
      final int pageNumber,
      final ValueChunkWriter valueChunkWriter)
      throws IOException, PageException {
    boolean needDecode;
    for (int i = 0; i < pageNumber; i++) {
      needDecode = ReadWriteIOUtils.readBool(stream);
      if (!needDecode) {
        final PageHeader pageHeader =
            PageHeader.deserializeFrom(stream, chunkHeader.getDataType(), true);
        valueChunkWriter.writePageHeaderAndDataIntoBuff(
            ByteBuffer.wrap(ReadWriteIOUtils.readBytesWithSelfDescriptionLength(stream)),
            pageHeader);
        continue;
      }

      // Since there is no data in the time column, we cannot provide the corresponding time for
      // each value.
      // However, if we only need to construct ValueChunk, we only need to provide the start time
      // and end time of each page to construct the statistic. Therefore, when writing data to
      // ValueChunkWriter, first use the wrong time. Before sealing each page, modify the statistic
      // in the page writer and set the correct start time and end time.
      final int length = ReadWriteIOUtils.readInt(stream);
      for (int j = 0; j < length; j++) {
        final boolean isNull = ReadWriteIOUtils.readBool(stream);
        switch (chunkHeader.getDataType()) {
          case INT32:
          case DATE:
            final int int32Value = isNull ? DEFAULT_INT32 : ReadWriteIOUtils.readInt(stream);
            valueChunkWriter.write(0, int32Value, isNull);
            break;
          case INT64:
          case TIMESTAMP:
            final long int64Value = isNull ? DEFAULT_INT64 : ReadWriteIOUtils.readLong(stream);
            valueChunkWriter.write(0, int64Value, isNull);
            break;
          case FLOAT:
            final float floatValue = isNull ? DEFAULT_FLOAT : ReadWriteIOUtils.readFloat(stream);
            valueChunkWriter.write(0, floatValue, isNull);
            break;
          case DOUBLE:
            final double doubleValue =
                isNull ? DEFAULT_DOUBLE : ReadWriteIOUtils.readDouble(stream);
            valueChunkWriter.write(0, doubleValue, isNull);
            break;
          case BOOLEAN:
            final boolean boolValue = isNull ? DEFAULT_BOOLEAN : ReadWriteIOUtils.readBool(stream);
            valueChunkWriter.write(0, boolValue, isNull);
            break;
          case TEXT:
          case BLOB:
          case STRING:
            final Binary binaryValue =
                isNull ? DEFAULT_BINARY : ReadWriteIOUtils.readBinary(stream);
            valueChunkWriter.write(0, binaryValue, isNull);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", chunkHeader.getDataType()));
        }
      }
      Statistics<? extends Serializable> statistics =
          valueChunkWriter.getPageWriter().getStatistics();

      // set the correct start time and end time in statistics
      long startTime = ReadWriteIOUtils.readLong(stream);
      long endTime = ReadWriteIOUtils.readLong(stream);
      statistics.setStartTime(startTime);
      statistics.setEndTime(endTime);

      valueChunkWriter.sealCurrentPage();
    }
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter writer) throws IOException {
    if (chunkList != null) {
      for (final Chunk chunk : chunkList) {
        writer.writeChunk(chunk);
      }
    } else {
      for (ValueChunkWriter valueChunkWriter : valueChunkWriters) {
        valueChunkWriter.writeToFileWriter(writer);
      }
    }
  }
}
