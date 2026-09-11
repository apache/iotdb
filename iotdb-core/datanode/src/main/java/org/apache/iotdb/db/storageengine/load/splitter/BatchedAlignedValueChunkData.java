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
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.apache.tsfile.common.constant.TsFileConstant.VALUE_COLUMN_MASK;

/**
 * This class is used to be compatible with the new distribution of aligned series in chunk group.
 * In past versions, a time chunk and all its corresponding value chunks were continuous within the
 * chunk group. In order to solve the problem of excessive memory usage during compaction, the value
 * column is grouped and merged, which will cause the above rules to be broken.
 */
public class BatchedAlignedValueChunkData extends AlignedChunkData {

  // Used for splitter
  public BatchedAlignedValueChunkData(AlignedChunkData alignedChunkData) {
    super(alignedChunkData);
  }

  // Used for deserialize
  public BatchedAlignedValueChunkData(IDeviceID device, TTimePartitionSlot timePartitionSlot) {
    super(device, timePartitionSlot);
  }

  /**
   * Override the encoding process to handle standalone Value Chunks without a Time Chunk. It uses
   * ValueChunkWriter directly, writes a dummy time (0), and manually corrects the start/end time in
   * the statistics.
   */
  protected void encodeAndBuildChunks() throws IOException, PageException {
    final long startTime = timePartitionSlot.getStartTime();
    long endTime = startTime + TimePartitionUtils.getTimePartitionInterval() - 1;
    if (endTime <= startTime) {
      endTime = Long.MAX_VALUE;
    }

    // Process each standalone value chunk
    for (int i = 0; i < chunkHeaderList.size(); i++) {
      final ChunkHeader chunkHeader = chunkHeaderList.get(i);
      MeasurementSchema measurementSchema =
          new MeasurementSchema(
              chunkHeader.getMeasurementID(),
              chunkHeader.getDataType(),
              chunkHeader.getEncodingType(),
              chunkHeader.getCompressionType());

      ValueChunkWriter valueChunkWriter =
          new ValueChunkWriter(
              measurementSchema.getMeasurementName(),
              measurementSchema.getCompressor(),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getValueEncoder());

      // Filter and process the pages that belong to the current value chunk (index 'i')
      for (PageBuffer page : pageBuffers) {
        if (page.chunkIndex != i) {
          continue;
        }

        if (!page.needDecode) {
          valueChunkWriter.writePageHeaderAndDataIntoBuff(page.pageData, page.pageHeader);
          continue;
        }

        // Keep track of actual bounds since we will write '0' as dummy time
        long pageStartTime = Long.MAX_VALUE;
        long pageEndTime = Long.MIN_VALUE;

        for (int j = 0; j < page.satisfiedLength; j++) {
          long time = page.timeBatch[j];

          // Apply time partition bounds check
          if (time > endTime) {
            break;
          }

          if (time >= startTime) {
            boolean isNull =
                (page.valueBatch == null
                    || page.valueBatch.length == 0
                    || page.valueBatch[j] == null);
            if (!isNull) {
              pageStartTime = Math.min(pageStartTime, time);
              pageEndTime = Math.max(pageEndTime, time);

              TsPrimitiveType val = page.valueBatch[j];
              // Write 0 as a dummy time, valueChunkWriter will ignore it anyway for value columns
              switch (chunkHeader.getDataType()) {
                case INT32:
                case DATE:
                  valueChunkWriter.write(0, val.getInt(), false);
                  break;
                case INT64:
                case TIMESTAMP:
                  valueChunkWriter.write(0, val.getLong(), false);
                  break;
                case FLOAT:
                  valueChunkWriter.write(0, val.getFloat(), false);
                  break;
                case DOUBLE:
                  valueChunkWriter.write(0, val.getDouble(), false);
                  break;
                case BOOLEAN:
                  valueChunkWriter.write(0, val.getBoolean(), false);
                  break;
                case TEXT:
                case BLOB:
                case STRING:
                  valueChunkWriter.write(0, val.getBinary(), false);
                  break;
                default:
                  throw new UnSupportedDataTypeException(
                      String.format(
                          StorageEngineMessages
                              .STORAGE_EXCEPTION_DATA_TYPE_S_IS_NOT_SUPPORTED_5D5C02E4,
                          chunkHeader.getDataType()));
              }
            } else {
              valueChunkWriter.write(0, 0L, true); // Dummy write for NULL
            }
          }
        }

        // Correct the page statistics with the actual Start/End time
        Statistics<? extends Serializable> statistics =
            valueChunkWriter.getPageWriter().getStatistics();
        if (pageStartTime <= pageEndTime) {
          statistics.setStartTime(pageStartTime);
          statistics.setEndTime(pageEndTime);
        }

        valueChunkWriter.sealCurrentPage();
      }

      // Directly assemble the final standard Chunk into memory
      final ByteBuffer valueData = valueChunkWriter.getByteBuffer();
      final Statistics<?> valueStat = valueChunkWriter.getStatistics();
      final ChunkHeader encodedHeader =
          createEncodedChunkHeader(
              chunkHeader,
              valueData.remaining(),
              valueChunkWriter.getNumOfPages(),
              VALUE_COLUMN_MASK);

      entireChunks.add(new Chunk(encodedHeader, valueData, null, valueStat));

      // Update global size (optional, since it will be fully recalculated during serialize anyway)
      dataSize += valueData.remaining() + valueStat.getSerializedSize();
    }
  }
}
