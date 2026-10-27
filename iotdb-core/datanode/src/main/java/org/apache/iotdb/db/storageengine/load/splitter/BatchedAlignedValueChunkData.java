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
import org.apache.iotdb.db.utils.TypeServices;

import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.write.chunk.ValueChunkWriter;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tsfile.common.constant.TsFileConstant.VALUE_COLUMN_MASK;

/**
 * This class is used to be compatible with the new distribution of aligned series in chunk group.
 * In past versions, a time chunk and all its corresponding value chunks were continuous within the
 * chunk group. In order to solve the problem of excessive memory usage during compaction, the value
 * column is grouped and merged, which will cause the above rules to be broken.
 */
public class BatchedAlignedValueChunkData extends AlignedChunkData {

  private static final ByteBuffer EMPTY_CHUNK_DATA = ByteBuffer.allocate(0);

  // Used for splitter
  public BatchedAlignedValueChunkData(AlignedChunkData alignedChunkData) {
    super(alignedChunkData);
  }

  // Used for deserialize
  public BatchedAlignedValueChunkData(IDeviceID device, TTimePartitionSlot timePartitionSlot) {
    super(device, timePartitionSlot);
  }

  /**
   * Encodes standalone value chunks while preserving the row and page positions of their time
   * chunks.
   */
  protected void encodeAndBuildChunks() throws IOException, PageException {
    final long startTime = timePartitionSlot.getStartTime();
    // Overflow-safe partition end: "start + interval - 1" wraps around for the last representable
    // partition and equals the start when interval == 1.
    final long endTime = TimePartitionUtils.getTimePartitionEndTime(startTime);

    // Group the buffered pages by chunk once; scanning the whole page list once per chunk turns
    // batched aligned tables into O(columns * pages).
    final Map<Integer, List<PageBuffer>> pagesByChunkIndex = new LinkedHashMap<>();
    for (final PageBuffer pageBuffer : pageBuffers) {
      pagesByChunkIndex
          .computeIfAbsent(pageBuffer.chunkIndex, ignored -> new ArrayList<>())
          .add(pageBuffer);
    }

    // Process each standalone value chunk
    for (int i = 0; i < chunkHeaderList.size(); i++) {
      final ChunkHeader chunkHeader = chunkHeaderList.get(i);
      final List<PageBuffer> chunkPages =
          pagesByChunkIndex.getOrDefault(i, Collections.emptyList());
      final Chunk directlyBuiltChunk =
          buildChunkFromUndecodedPages(chunkHeader, VALUE_COLUMN_MASK, chunkPages);
      if (directlyBuiltChunk != null) {
        entireChunks.add(directlyBuiltChunk);
        dataSize +=
            directlyBuiltChunk.getData().remaining()
                + directlyBuiltChunk.getChunkStatistic().getSerializedSize();
        continue;
      }
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

      // Filter and process the pages that belong to the current value chunk (index 'i').
      // A chunk header can exist for a physical chunk whose points all belong to another time
      // partition. Such a chunk stays empty here and must be emitted without any page: sealing an
      // untouched writer would produce a zero-point page whose data size does not match the bytes
      // actually written, and the resulting TsFile could not be parsed back.
      boolean hasBufferedData = false;
      for (PageBuffer page : chunkPages) {
        if (!page.needDecode) {
          // Empty pages still occupy a slot opposite the corresponding time page. If every page
          // is empty, the whole chunk is emitted without a body below.
          hasBufferedData |= page.pageData.remaining() > 0;
          valueChunkWriter.writePageHeaderAndDataIntoBuff(page.pageData, page.pageHeader);
          continue;
        }

        boolean pageHasRows = false;

        final TypeServices.TsPrimitiveValueChunkWriter valueChunkValueWriter =
            TypeServices.StorageEngine.TS_PRIMITIVE_VALUE_CHUNK_WRITER_SERVICE.call(
                Type.fromTsDataType(chunkHeader.getDataType()));

        for (int j = 0; j < page.timeBatch.length; j++) {
          long time = page.timeBatch[j];
          if (time >= startTime && time <= endTime) {
            boolean isNull =
                (page.valueBatch == null
                    || page.valueBatch.length == 0
                    || page.valueBatch[j] == null);
            if (!isNull) {
              hasBufferedData = true;
            }
            // The bitmap has one position per timestamp, including nulls. Use the real timestamp
            // so the writer also computes the statistics of the non-null values correctly.
            valueChunkValueWriter.write(
                valueChunkWriter, time, isNull ? null : page.valueBatch[j], isNull);
            pageHasRows = true;
          }
        }

        if (pageHasRows) {
          valueChunkWriter.sealCurrentPage();
        }
      }

      // Directly assemble the final standard Chunk into memory
      final ByteBuffer valueData =
          hasBufferedData ? valueChunkWriter.getByteBuffer() : EMPTY_CHUNK_DATA;
      final Statistics<?> valueStat = valueChunkWriter.getStatistics();
      final ChunkHeader encodedHeader =
          createEncodedChunkHeader(
              chunkHeader,
              valueData.remaining(),
              hasBufferedData ? valueChunkWriter.getNumOfPages() : 0,
              VALUE_COLUMN_MASK);

      entireChunks.add(new Chunk(encodedHeader, valueData, null, valueStat));

      // Update global size (optional, since it will be fully recalculated during serialize anyway)
      dataSize += valueData.remaining() + valueStat.getSerializedSize();
    }
  }
}
