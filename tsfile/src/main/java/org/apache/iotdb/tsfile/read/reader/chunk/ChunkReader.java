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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.v2.file.header.PageHeaderV2;
import org.apache.iotdb.tsfile.v2.read.reader.page.PageReaderV2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class ChunkReader implements IChunkReader {

  private ChunkHeader chunkHeader;
  private ByteBuffer chunkDataBuffer;
  private IUnCompressor unCompressor;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  protected Filter filter;

  private List<IPageReader> pageReaderList = new LinkedList<>();

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  /**
   * constructor of ChunkReader.
   *
   * @param chunk input Chunk object
   * @param filter filter
   */
  public ChunkReader(Chunk chunk, Filter filter) throws IOException {
    this.filter = filter;
    this.chunkDataBuffer = chunk.getData();
    this.deleteIntervalList = chunk.getDeleteIntervalList();
    chunkHeader = chunk.getHeader();
    this.unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    if (chunk.isFromOldFile()) {
      initAllPageReadersV2();
    } else {
      initAllPageReaders(chunk.getChunkStatistic());
    }
  }

  private void initAllPageReaders(Statistics chunkStatistic) throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkStatistic);
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      // if the current page satisfies
      if (pageSatisfied(pageHeader)) {
        pageReaderList.add(constructPageReaderForNextPage(pageHeader));
      } else {
        skipBytesInStreamByLength(pageHeader.getCompressedSize());
      }
    }
  }

  /** judge if has next page whose page header satisfies the filter. */
  @Override
  public boolean hasNextSatisfiedPage() {
    return !pageReaderList.isEmpty();
  }

  /**
   * get next data batch.
   *
   * @return next data batch
   * @throws IOException IOException
   */
  @Override
  public BatchData nextPageData() throws IOException {
    if (pageReaderList.isEmpty()) {
      throw new IOException("No more page");
    }
    return pageReaderList.remove(0).getAllSatisfiedPageData();
  }

  private void skipBytesInStreamByLength(int length) {
    chunkDataBuffer.position(chunkDataBuffer.position() + length);
  }

  protected boolean pageSatisfied(PageHeader pageHeader) {
    if (deleteIntervalList != null) {
      for (TimeRange range : deleteIntervalList) {
        if (range.contains(pageHeader.getStartTime(), pageHeader.getEndTime())) {
          return false;
        }
        if (range.overlaps(new TimeRange(pageHeader.getStartTime(), pageHeader.getEndTime()))) {
          pageHeader.setModified(true);
        }
      }
    }
    return filter == null || filter.satisfy(pageHeader.getStatistics());
  }

  private PageReader constructPageReaderForNextPage(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    try {
      unCompressor.uncompress(
          compressedPageBody, 0, compressedPageBodyLength, uncompressedPageData, 0);
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

    ByteBuffer pageData = ByteBuffer.wrap(uncompressedPageData);
    PageReader reader =
        new PageReader(
            pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, filter);
    reader.setDeleteIntervalList(deleteIntervalList);
    return reader;
  }

  @Override
  public void close() {}

  public ChunkHeader getChunkHeader() {
    return chunkHeader;
  }

  @Override
  public List<IPageReader> loadPageReaderList() {
    return pageReaderList;
  }

  // For reading TsFile V2
  private void initAllPageReadersV2() throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader =
          PageHeaderV2.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      // if the current page satisfies
      if (pageSatisfied(pageHeader)) {
        pageReaderList.add(constructPageReaderForNextPageV2(pageHeader));
      } else {
        skipBytesInStreamByLength(pageHeader.getCompressedSize());
      }
    }
  }

  // For reading TsFile V2
  private PageReader constructPageReaderForNextPageV2(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    Decoder valueDecoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    unCompressor.uncompress(
        compressedPageBody, 0, compressedPageBodyLength, uncompressedPageData, 0);
    ByteBuffer pageData = ByteBuffer.wrap(uncompressedPageData);
    PageReader reader =
        new PageReaderV2(
            pageHeader, pageData, chunkHeader.getDataType(), valueDecoder, timeDecoder, filter);
    reader.setDeleteIntervalList(deleteIntervalList);
    return reader;
  }
}
