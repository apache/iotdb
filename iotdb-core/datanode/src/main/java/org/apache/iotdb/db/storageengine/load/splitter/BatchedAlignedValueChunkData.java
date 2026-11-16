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
import org.apache.tsfile.file.header.PageHeader;
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

  // An empty page holds no page body and no page statistics: it serializes to the single 0 varint
  // that stands for it. The header must therefore not carry statistics, otherwise the page writer
  // appends a compressed-size varint to a page without data and the reader sees a phantom page.
  private static final ByteBuffer EMPTY_PAGE_DATA = ByteBuffer.allocate(0);
  private static final PageHeader EMPTY_PAGE_HEADER = new PageHeader(0, 0, null);

  // Used for splitter
  public BatchedAlignedValueChunkData(AlignedChunkData alignedChunkData) {
    super(alignedChunkData);
  }

  // Used for deserialize
  public BatchedAlignedValueChunkData(IDeviceID device, TTimePartitionSlot timePartitionSlot) {
    super(device, timePartitionSlot);
  }

  /**
   * Encodes standalone value chunks while preserving the row and the page positions of their time
   * chunks: a row whose value is null still occupies a bitmap position, and an all-null page still
   * occupies a page slot.
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

      // Whether this chunk covers any row of this time partition. Such a chunk writes one page for
      // every page slot of its time chunk, all-null pages included: the aligned reader pairs the
      // pages of a value chunk with the pages of its time chunk by position, so a missing page
      // shifts every later page onto the wrong timestamps. A chunk that covers no row at all keeps
      // the zero-page layout of an all-null column, which the metadata reader reports as null.
      final boolean chunkHasRows = hasPageWithRows(chunkPages);
      for (final PageBuffer page : chunkPages) {
        if (!page.needDecode) {
          if (page.pageData.remaining() == 0) {
            // An all-null page is stored as an empty page and keeps its slot. Its header is
            // normalized because the page writer decides on the presence of the statistics instead
            // of the page size: the placeholder page built by
            // TsFileSplitter#handleEmptyValueChunk carries the statistics of an empty chunk, and
            // those would turn the empty page into two bytes that the reader parses as a page
            // followed by a phantom one.
            if (chunkHasRows) {
              valueChunkWriter.writePageHeaderAndDataIntoBuff(EMPTY_PAGE_DATA, EMPTY_PAGE_HEADER);
            }
            continue;
          }
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
                    || j >= page.valueBatch.length
                    || page.valueBatch[j] == null);
            // The bitmap has one position per timestamp, including nulls. Use the real timestamp
            // so the writer also computes the statistics of the non-null values correctly.
            valueChunkValueWriter.write(
                valueChunkWriter, time, isNull ? null : page.valueBatch[j], isNull);
            pageHasRows = true;
          }
        }

        // Sealing an untouched writer would count a page that holds no byte.
        if (pageHasRows) {
          valueChunkWriter.sealCurrentPage();
        }
      }

      // Directly assemble the final standard Chunk into memory. A chunk without any row of this
      // partition stays empty: sealing its untouched writer would produce a page whose data size
      // does not match the bytes actually written, and the resulting TsFile could not be parsed
      // back.
      final ByteBuffer valueData =
          chunkHasRows ? valueChunkWriter.getByteBuffer() : EMPTY_CHUNK_DATA;
      final Statistics<?> valueStat = valueChunkWriter.getStatistics();
      final ChunkHeader encodedHeader =
          createEncodedChunkHeader(
              chunkHeader,
              valueData.remaining(),
              chunkHasRows ? valueChunkWriter.getNumOfPages() : 0,
              VALUE_COLUMN_MASK);

      entireChunks.add(new Chunk(encodedHeader, valueData, null, valueStat));

      // Update global size (optional, since it will be fully recalculated during serialize anyway)
      dataSize += valueData.remaining() + valueStat.getSerializedSize();
    }
  }

  /**
   * Whether any page of the chunk holds rows of this time partition. Only such a chunk mirrors the
   * page slots of its time chunk, because only then does the time chunk have a page to pair with; a
   * chunk that holds no row at all keeps the zero-page layout. This is the counterpart of the rule
   * applied by the base class to the chunks it writes itself.
   */
  private static boolean hasPageWithRows(final List<PageBuffer> chunkPages) {
    for (final PageBuffer page : chunkPages) {
      // A decoded page already records how many of its rows belong to this partition, and a page
      // copied as it is was read from this partition, so a body means rows and only an all-null
      // page has no body.
      if (page.needDecode ? page.satisfiedLength > 0 : page.pageData.remaining() > 0) {
        return true;
      }
    }
    return false;
  }
}
