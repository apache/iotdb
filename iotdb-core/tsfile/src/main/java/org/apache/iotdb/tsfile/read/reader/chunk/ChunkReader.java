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

package org.apache.iotdb.tsfile.read.reader.chunk;

import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

public class ChunkReader extends AbstractChunkReader {

  private final ChunkHeader chunkHeader;
  private final ByteBuffer chunkDataBuffer;
  private final List<TimeRange> deleteIntervalList;

  @SuppressWarnings("unchecked")
  public ChunkReader(Chunk chunk, long readStopTime, Filter queryFilter) throws IOException {
    super(readStopTime, queryFilter);
    this.chunkHeader = chunk.getHeader();
    this.chunkDataBuffer = chunk.getData();
    this.deleteIntervalList = chunk.getDeleteIntervalList();

    initAllPageReaders(chunk.getChunkStatistic());
  }

  public ChunkReader(Chunk chunk) throws IOException {
    this(chunk, Long.MIN_VALUE, null);
  }

  public ChunkReader(Chunk chunk, Filter queryFilter) throws IOException {
    this(chunk, Long.MIN_VALUE, queryFilter);
  }

  /**
   * Constructor of ChunkReader by timestamp. This constructor is used to accelerate queries by
   * filtering out pages whose endTime is less than current timestamp.
   *
   * @throws IOException exception when initAllPageReaders
   */
  public ChunkReader(Chunk chunk, long readStopTime) throws IOException {
    this(chunk, readStopTime, null);
  }

  private void initAllPageReaders(Statistics<? extends Serializable> chunkStatistic)
      throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkStatistic);
        // when there is only one page in the chunk, the page statistic is the same as the chunk, so
        // we needn't filter the page again
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
        // if the current page satisfies
        if (pageCanSkip(pageHeader)) {
          skipCurrentPage(pageHeader);
          continue;
        }
      }

      if (pageDeleted(pageHeader)) {
        skipCurrentPage(pageHeader);
      } else {
        pageReaderList.add(constructPageReader(pageHeader));
      }
    }
  }

  private boolean pageCanSkip(PageHeader pageHeader) {
    return queryFilter != null
        && !queryFilter.satisfyStartEndTime(pageHeader.getStartTime(), pageHeader.getEndTime());
  }

  private boolean pageDeleted(PageHeader pageHeader) {
    if (readStopTime > pageHeader.getEndTime()) {
      // used for chunk reader by timestamp
      return true;
    }

    long startTime = pageHeader.getStartTime();
    long endTime = pageHeader.getEndTime();
    if (deleteIntervalList != null) {
      for (TimeRange range : deleteIntervalList) {
        if (range.contains(startTime, endTime)) {
          return true;
        }
        if (range.overlaps(new TimeRange(startTime, endTime))) {
          pageHeader.setModified(true);
        }
      }
    }
    return false;
  }

  private void skipCurrentPage(PageHeader pageHeader) {
    chunkDataBuffer.position(chunkDataBuffer.position() + pageHeader.getCompressedSize());
  }

  private PageReader constructPageReader(PageHeader pageHeader) throws IOException {
    ByteBuffer pageData = deserializePageData(pageHeader, chunkDataBuffer, chunkHeader);
    PageReader reader =
        new PageReader(
            pageHeader,
            pageData,
            chunkHeader.getDataType(),
            Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType()),
            defaultTimeDecoder,
            queryFilter);
    reader.setDeleteIntervalList(deleteIntervalList);
    return reader;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // util methods
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public static ByteBuffer readCompressedPageData(PageHeader pageHeader, ByteBuffer chunkBuffer)
      throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];
    // doesn't have a complete page body
    if (compressedPageBodyLength > chunkBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkBuffer.remaining());
    }
    chunkBuffer.get(compressedPageBody);
    return ByteBuffer.wrap(compressedPageBody);
  }

  public static ByteBuffer uncompressPageData(
      PageHeader pageHeader, IUnCompressor unCompressor, ByteBuffer compressedPageData)
      throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    try {
      unCompressor.uncompress(
          compressedPageData.array(), 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage());
    }

    return ByteBuffer.wrap(uncompressedPageData);
  }

  public static ByteBuffer deserializePageData(
      PageHeader pageHeader, ByteBuffer chunkBuffer, ChunkHeader chunkHeader) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    ByteBuffer compressedPageBody = readCompressedPageData(pageHeader, chunkBuffer);
    return uncompressPageData(pageHeader, unCompressor, compressedPageBody);
  }
}
