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
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.reader.page.AlignedPageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class AlignedChunkReader implements IChunkReader {

  // chunk header of the time column
  private final ChunkHeader timeChunkHeader;
  // chunk headers of all the sub sensors
  private final List<ChunkHeader> valueChunkHeaderList = new ArrayList<>();
  // chunk data of the time column
  private final ByteBuffer timeChunkDataBuffer;
  // chunk data of all the sub sensors
  private final List<ByteBuffer> valueChunkDataBufferList = new ArrayList<>();
  private final IUnCompressor unCompressor;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);
  private long currentTimestamp;

  protected Filter filter;

  private final List<IPageReader> pageReaderList = new LinkedList<>();

  /** A list of deleted intervals. */
  private final List<List<TimeRange>> valueDeleteIntervalList;

  /**
   * constructor of ChunkReader.
   *
   * @param filter filter
   */
  public AlignedChunkReader(Chunk timeChunk, List<Chunk> valueChunkList, Filter filter)
      throws IOException {
    this.filter = filter;
    this.timeChunkDataBuffer = timeChunk.getData();
    this.valueDeleteIntervalList = new ArrayList<>();
    this.timeChunkHeader = timeChunk.getHeader();
    this.unCompressor = IUnCompressor.getUnCompressor(timeChunkHeader.getCompressionType());
    this.currentTimestamp = Long.MIN_VALUE;
    List<Statistics> valueChunkStatisticsList = new ArrayList<>();
    valueChunkList.forEach(
        chunk -> {
          valueChunkHeaderList.add(chunk == null ? null : chunk.getHeader());
          valueChunkDataBufferList.add(chunk == null ? null : chunk.getData());
          valueChunkStatisticsList.add(chunk == null ? null : chunk.getChunkStatistic());
          valueDeleteIntervalList.add(chunk == null ? null : chunk.getDeleteIntervalList());
        });
    initAllPageReaders(timeChunk.getChunkStatistic(), valueChunkStatisticsList);
  }

  /**
   * Constructor of ChunkReader by timestamp. This constructor is used to accelerate queries by
   * filtering out pages whose endTime is less than current timestamp.
   */
  public AlignedChunkReader(
      Chunk timeChunk, List<Chunk> valueChunkList, Filter filter, long currentTimestamp)
      throws IOException {
    this.filter = filter;
    this.timeChunkDataBuffer = timeChunk.getData();
    this.valueDeleteIntervalList = new ArrayList<>();
    this.timeChunkHeader = timeChunk.getHeader();
    this.unCompressor = IUnCompressor.getUnCompressor(timeChunkHeader.getCompressionType());
    this.currentTimestamp = currentTimestamp;
    List<Statistics> valueChunkStatisticsList = new ArrayList<>();
    valueChunkList.forEach(
        chunk -> {
          valueChunkHeaderList.add(chunk == null ? null : chunk.getHeader());
          valueChunkDataBufferList.add(chunk == null ? null : chunk.getData());
          valueChunkStatisticsList.add(chunk == null ? null : chunk.getChunkStatistic());
          valueDeleteIntervalList.add(chunk == null ? null : chunk.getDeleteIntervalList());
        });
    initAllPageReaders(timeChunk.getChunkStatistic(), valueChunkStatisticsList);
  }

  /** construct all the page readers in this chunk */
  private void initAllPageReaders(
      Statistics timeChunkStatistics, List<Statistics> valueChunkStatisticsList)
      throws IOException {
    // construct next satisfied page header
    while (timeChunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader timePageHeader;
      List<PageHeader> valuePageHeaderList = new ArrayList<>();

      boolean exits = false;
      // this chunk has only one page
      if ((timeChunkHeader.getChunkType() & 0x3F) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        timePageHeader = PageHeader.deserializeFrom(timeChunkDataBuffer, timeChunkStatistics);
        for (int i = 0; i < valueChunkDataBufferList.size(); i++) {
          if (valueChunkDataBufferList.get(i) != null) {
            exits = true;
            valuePageHeaderList.add(
                PageHeader.deserializeFrom(
                    valueChunkDataBufferList.get(i), valueChunkStatisticsList.get(i)));
          } else {
            valuePageHeaderList.add(null);
          }
        }
      } else { // this chunk has more than one page
        timePageHeader =
            PageHeader.deserializeFrom(timeChunkDataBuffer, timeChunkHeader.getDataType());
        for (int i = 0; i < valueChunkDataBufferList.size(); i++) {
          if (valueChunkDataBufferList.get(i) != null) {
            exits = true;
            valuePageHeaderList.add(
                PageHeader.deserializeFrom(
                    valueChunkDataBufferList.get(i), valueChunkHeaderList.get(i).getDataType()));
          } else {
            valuePageHeaderList.add(null);
          }
        }
      }
      // if the current page satisfies
      if (exits && timePageSatisfied(timePageHeader)) {
        AlignedPageReader alignedPageReader =
            constructPageReaderForNextPage(timePageHeader, valuePageHeaderList);
        if (alignedPageReader != null) {
          pageReaderList.add(alignedPageReader);
        }
      } else {
        skipBytesInStreamByLength(timePageHeader, valuePageHeaderList);
      }
    }
  }

  /** used for time page filter */
  private boolean timePageSatisfied(PageHeader timePageHeader) {
    long startTime = timePageHeader.getStatistics().getStartTime();
    long endTime = timePageHeader.getStatistics().getEndTime();
    return filter == null || filter.satisfyStartEndTime(startTime, endTime);
  }

  /** used for value page filter */
  protected boolean pageSatisfied(PageHeader pageHeader, List<TimeRange> valueDeleteInterval) {
    if (currentTimestamp > pageHeader.getEndTime()) {
      // used for chunk reader by timestamp
      return false;
    }
    if (valueDeleteInterval != null) {
      for (TimeRange range : valueDeleteInterval) {
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

  private AlignedPageReader constructPageReaderForNextPage(
      PageHeader timePageHeader, List<PageHeader> valuePageHeader) throws IOException {
    PageInfo timePageInfo = new PageInfo();
    getPageInfo(timePageHeader, timeChunkDataBuffer, timeChunkHeader, timePageInfo);
    PageInfo valuePageInfo = new PageInfo();
    List<PageHeader> valuePageHeaderList = new ArrayList<>();
    List<ByteBuffer> valuePageDataList = new ArrayList<>();
    List<TSDataType> valueDataTypeList = new ArrayList<>();
    List<Decoder> valueDecoderList = new ArrayList<>();
    boolean exist = false;
    for (int i = 0; i < valuePageHeader.size(); i++) {
      if (valuePageHeader.get(i) == null
          || valuePageHeader.get(i).getUncompressedSize() == 0) { // Empty Page
        valuePageHeaderList.add(null);
        valuePageDataList.add(null);
        valueDataTypeList.add(null);
        valueDecoderList.add(null);
      } else if (pageSatisfied(
          valuePageHeader.get(i),
          valueDeleteIntervalList.get(i))) { // if the page is satisfied, deserialize it
        getPageInfo(
            valuePageHeader.get(i),
            valueChunkDataBufferList.get(i),
            valueChunkHeaderList.get(i),
            valuePageInfo);
        valuePageHeaderList.add(valuePageInfo.pageHeader);
        valuePageDataList.add(valuePageInfo.pageData);
        valueDataTypeList.add(valuePageInfo.dataType);
        valueDecoderList.add(valuePageInfo.decoder);
        exist = true;
      } else { // if the page is not satisfied, just skip it
        valueChunkDataBufferList
            .get(i)
            .position(
                valueChunkDataBufferList.get(i).position()
                    + valuePageHeader.get(i).getCompressedSize());
        valuePageHeaderList.add(null);
        valuePageDataList.add(null);
        valueDataTypeList.add(null);
        valueDecoderList.add(null);
      }
    }
    if (!exist) {
      return null;
    }
    AlignedPageReader alignedPageReader =
        new AlignedPageReader(
            timePageHeader,
            timePageInfo.pageData,
            timeDecoder,
            valuePageHeaderList,
            valuePageDataList,
            valueDataTypeList,
            valueDecoderList,
            filter);
    alignedPageReader.setDeleteIntervalList(valueDeleteIntervalList);
    return alignedPageReader;
  }

  /** Read data from compressed page data. Uncompress the page and decode it to tsblock data. */
  public TsBlock readPageData(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer compressedTimePageData,
      List<ByteBuffer> compressedValuePageDatas)
      throws IOException {

    // uncompress time page data
    ByteBuffer uncompressedTimePageData =
        uncompressPageData(timePageHeader, compressedTimePageData);

    // uncompress value page datas
    List<ByteBuffer> uncompressedValuePageDatas = new ArrayList<>();
    List<TSDataType> valueTypes = new ArrayList<>();
    List<Decoder> valueDecoders = new ArrayList<>();
    for (int i = 0; i < valuePageHeaders.size(); i++) {
      if (valuePageHeaders.get(i) == null) {
        uncompressedValuePageDatas.add(null);
        valueTypes.add(TSDataType.BOOLEAN);
        valueDecoders.add(null);
      } else {
        uncompressedValuePageDatas.add(
            uncompressPageData(valuePageHeaders.get(i), compressedValuePageDatas.get(i)));
        ChunkHeader valueChunkHeader = valueChunkHeaderList.get(i);
        TSDataType valueType = valueChunkHeader.getDataType();
        valueDecoders.add(Decoder.getDecoderByType(valueChunkHeader.getEncodingType(), valueType));
        valueTypes.add(valueType);
      }
    }

    // decode page data
    AlignedPageReader alignedPageReader =
        new AlignedPageReader(
            timePageHeader,
            uncompressedTimePageData,
            timeDecoder,
            valuePageHeaders,
            uncompressedValuePageDatas,
            valueTypes,
            valueDecoders,
            null);
    alignedPageReader.initTsBlockBuilder(valueTypes);
    alignedPageReader.setDeleteIntervalList(valueDeleteIntervalList);
    return alignedPageReader.getAllSatisfiedData();
  }

  private ByteBuffer uncompressPageData(PageHeader pageHeader, ByteBuffer compressedPageData)
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

  /**
   * deserialize the page
   *
   * @param pageHeader PageHeader for current page
   * @param chunkBuffer current chunk data buffer
   * @param chunkHeader current chunk header
   * @param pageInfo A struct to put the deserialized page into.
   */
  private void getPageInfo(
      PageHeader pageHeader, ByteBuffer chunkBuffer, ChunkHeader chunkHeader, PageInfo pageInfo)
      throws IOException {
    pageInfo.pageHeader = pageHeader;
    pageInfo.dataType = chunkHeader.getDataType();
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];
    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkBuffer.remaining());
    }

    chunkBuffer.get(compressedPageBody);
    pageInfo.decoder =
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    try {
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
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
    pageInfo.pageData = ByteBuffer.wrap(uncompressedPageData);
  }

  private static class PageInfo {

    PageHeader pageHeader;
    ByteBuffer pageData;
    TSDataType dataType;
    Decoder decoder;
  }

  private void skipBytesInStreamByLength(
      PageHeader timePageHeader, List<PageHeader> valuePageHeader) {
    timeChunkDataBuffer.position(
        timeChunkDataBuffer.position() + timePageHeader.getCompressedSize());
    for (int i = 0; i < valuePageHeader.size(); i++) {
      if (valuePageHeader.get(i) != null) {
        valueChunkDataBufferList
            .get(i)
            .position(
                valueChunkDataBufferList.get(i).position()
                    + valuePageHeader.get(i).getCompressedSize());
      }
    }
  }

  @Override
  public boolean hasNextSatisfiedPage() {
    return !pageReaderList.isEmpty();
  }

  @Override
  public BatchData nextPageData() throws IOException {
    if (pageReaderList.isEmpty()) {
      throw new IOException("No more page");
    }
    return pageReaderList.remove(0).getAllSatisfiedPageData();
  }

  @Override
  public void close() throws IOException {}

  @Override
  public List<IPageReader> loadPageReaderList() {
    return pageReaderList;
  }
}
