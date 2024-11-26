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

package org.apache.tsfile.read.reader.chunk;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.page.AbstractAlignedPageReader;
import org.apache.tsfile.read.reader.page.LazyLoadPageData;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractAlignedChunkReader extends AbstractChunkReader {
  // chunk header of the time column
  private final ChunkHeader timeChunkHeader;
  // chunk data of the time column
  private final ByteBuffer timeChunkDataBuffer;

  // chunk headers of all the sub sensors
  private final List<ChunkHeader> valueChunkHeaderList = new ArrayList<>();
  // chunk data of all the sub sensors
  private final List<ByteBuffer> valueChunkDataBufferList = new ArrayList<>();
  // deleted intervals of all the sub sensors
  private final List<List<TimeRange>> valueDeleteIntervalsList = new ArrayList<>();
  // deleted intervals of time column
  protected final List<TimeRange> timeDeleteIntervalList;

  private final EncryptParameter encryptParam;

  @SuppressWarnings("unchecked")
  AbstractAlignedChunkReader(
      Chunk timeChunk, List<Chunk> valueChunkList, long readStopTime, Filter queryFilter)
      throws IOException {
    super(readStopTime, queryFilter);
    this.timeChunkHeader = timeChunk.getHeader();
    this.timeChunkDataBuffer = timeChunk.getData();
    this.timeDeleteIntervalList = timeChunk.getDeleteIntervalList();

    List<Statistics<? extends Serializable>> valueChunkStatisticsList = new ArrayList<>();
    valueChunkList.forEach(
        chunk -> {
          this.valueChunkHeaderList.add(chunk == null ? null : chunk.getHeader());
          this.valueChunkDataBufferList.add(chunk == null ? null : chunk.getData());
          this.valueDeleteIntervalsList.add(chunk == null ? null : chunk.getDeleteIntervalList());

          valueChunkStatisticsList.add(chunk == null ? null : chunk.getChunkStatistic());
        });
    this.encryptParam = timeChunk.getEncryptParam();
    initAllPageReaders(timeChunk.getChunkStatistic(), valueChunkStatisticsList);
  }

  /** construct all the page readers in this chunk */
  private void initAllPageReaders(
      Statistics<? extends Serializable> timeChunkStatistics,
      List<Statistics<? extends Serializable>> valueChunkStatisticsList)
      throws IOException {
    // construct next satisfied page header
    while (timeChunkDataBuffer.remaining() > 0) {
      // deserialize PageHeader from chunkDataBuffer
      AbstractAlignedPageReader alignedPageReader =
          isSinglePageChunk()
              ? deserializeFromSinglePageChunk(timeChunkStatistics, valueChunkStatisticsList)
              : deserializeFromMultiPageChunk();
      if (alignedPageReader != null) {
        pageReaderList.add(alignedPageReader);
      }
    }
  }

  private boolean isSinglePageChunk() {
    return (timeChunkHeader.getChunkType() & 0x3F) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER;
  }

  private AbstractAlignedPageReader deserializeFromSinglePageChunk(
      Statistics<? extends Serializable> timeChunkStatistics,
      List<Statistics<? extends Serializable>> valueChunkStatisticsList)
      throws IOException {
    PageHeader timePageHeader =
        PageHeader.deserializeFrom(timeChunkDataBuffer, timeChunkStatistics);
    List<PageHeader> valuePageHeaderList = new ArrayList<>();

    boolean isAllNull = true;
    for (int i = 0; i < valueChunkDataBufferList.size(); i++) {
      if (valueChunkDataBufferList.get(i) != null) {
        isAllNull = false;
        valuePageHeaderList.add(
            PageHeader.deserializeFrom(
                valueChunkDataBufferList.get(i), valueChunkStatisticsList.get(i)));
      } else {
        valuePageHeaderList.add(null);
      }
    }

    if (needSkipForSinglePageChunk(isAllNull, timePageHeader)) {
      // when there is only one page in the chunk, the page statistic is the same as the chunk, so
      // we needn't filter the page again
      skipCurrentPage(timePageHeader, valuePageHeaderList);
      return null;
    }
    return constructAlignedPageReader(timePageHeader, valuePageHeaderList);
  }

  abstract boolean needSkipForSinglePageChunk(boolean isAllNull, PageHeader timePageHeader);

  private AbstractAlignedPageReader deserializeFromMultiPageChunk() throws IOException {
    PageHeader timePageHeader =
        PageHeader.deserializeFrom(timeChunkDataBuffer, timeChunkHeader.getDataType());
    List<PageHeader> valuePageHeaderList = new ArrayList<>();

    boolean isAllNull = true;
    for (int i = 0; i < valueChunkDataBufferList.size(); i++) {
      if (valueChunkDataBufferList.get(i) != null) {
        isAllNull = false;
        valuePageHeaderList.add(
            PageHeader.deserializeFrom(
                valueChunkDataBufferList.get(i), valueChunkHeaderList.get(i).getDataType()));
      } else {
        valuePageHeaderList.add(null);
      }
    }

    if (needSkipForMultiPageChunk(isAllNull, timePageHeader)) {
      skipCurrentPage(timePageHeader, valuePageHeaderList);
      return null;
    }
    return constructAlignedPageReader(timePageHeader, valuePageHeaderList);
  }

  abstract boolean needSkipForMultiPageChunk(boolean isAllNull, PageHeader timePageHeader);

  protected boolean isEarlierThanReadStopTime(final PageHeader timePageHeader) {
    return timePageHeader.getEndTime() < readStopTime;
  }

  protected boolean pageCanSkip(PageHeader pageHeader) {
    return queryFilter != null
        && !queryFilter.satisfyStartEndTime(pageHeader.getStartTime(), pageHeader.getEndTime());
  }

  private void skipCurrentPage(PageHeader timePageHeader, List<PageHeader> valuePageHeader) {
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

  private AbstractAlignedPageReader constructAlignedPageReader(
      PageHeader timePageHeader, List<PageHeader> rawValuePageHeaderList) throws IOException {
    IDecryptor decrytor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer timePageData =
        ChunkReader.deserializePageData(
            timePageHeader, timeChunkDataBuffer, timeChunkHeader, decrytor);

    List<PageHeader> valuePageHeaderList = new ArrayList<>();
    LazyLoadPageData[] lazyLoadPageDataArray = new LazyLoadPageData[rawValuePageHeaderList.size()];
    List<TSDataType> valueDataTypeList = new ArrayList<>();
    List<Decoder> valueDecoderList = new ArrayList<>();

    boolean isAllNull = true;
    for (int i = 0; i < rawValuePageHeaderList.size(); i++) {
      PageHeader valuePageHeader = rawValuePageHeaderList.get(i);

      if (valuePageHeader == null || valuePageHeader.getUncompressedSize() == 0) {
        // Empty Page
        valuePageHeaderList.add(null);
        lazyLoadPageDataArray[i] = null;
        valueDataTypeList.add(null);
        valueDecoderList.add(null);
      } else if (pageDeleted(valuePageHeader, valueDeleteIntervalsList.get(i))) {
        valueChunkDataBufferList
            .get(i)
            .position(
                valueChunkDataBufferList.get(i).position() + valuePageHeader.getCompressedSize());
        valuePageHeaderList.add(null);
        lazyLoadPageDataArray[i] = null;
        valueDataTypeList.add(null);
        valueDecoderList.add(null);
      } else {
        ChunkHeader valueChunkHeader = valueChunkHeaderList.get(i);
        int currentPagePosition = valueChunkDataBufferList.get(i).position();
        // adjust position as if we have read the page data even if it is just lazy-loaded
        valueChunkDataBufferList
            .get(i)
            .position(
                valueChunkDataBufferList.get(i).position() + valuePageHeader.getCompressedSize());

        valuePageHeaderList.add(valuePageHeader);
        lazyLoadPageDataArray[i] =
            new LazyLoadPageData(
                valueChunkDataBufferList.get(i).array(),
                currentPagePosition,
                IUnCompressor.getUnCompressor(valueChunkHeader.getCompressionType()),
                encryptParam);
        valueDataTypeList.add(valueChunkHeader.getDataType());
        valueDecoderList.add(
            Decoder.getDecoderByType(
                valueChunkHeader.getEncodingType(), valueChunkHeader.getDataType()));
        isAllNull = false;
      }
    }
    if (canSkip(isAllNull, timePageHeader)) {
      return null;
    }
    return constructPageReader(
        timePageHeader,
        timePageData,
        defaultTimeDecoder,
        valuePageHeaderList,
        lazyLoadPageDataArray,
        valueDataTypeList,
        valueDecoderList,
        queryFilter,
        valueDeleteIntervalsList);
  }

  abstract boolean canSkip(boolean isAllNull, PageHeader timePageHeader);

  abstract AbstractAlignedPageReader constructPageReader(
      PageHeader timePageHeader,
      ByteBuffer timePageData,
      Decoder timeDecoder,
      List<PageHeader> valuePageHeaderList,
      LazyLoadPageData[] lazyLoadPageDataArray,
      List<TSDataType> valueDataTypeList,
      List<Decoder> valueDecoderList,
      Filter queryFilter,
      List<List<TimeRange>> valueDeleteIntervalsList);

  protected boolean pageDeleted(PageHeader pageHeader, List<TimeRange> deleteIntervals) {
    if (pageHeader.getEndTime() < readStopTime) {
      return true;
    }
    if (deleteIntervals != null) {
      for (TimeRange range : deleteIntervals) {
        if (range.contains(pageHeader.getStartTime(), pageHeader.getEndTime())) {
          return true;
        }
        if (range.overlaps(new TimeRange(pageHeader.getStartTime(), pageHeader.getEndTime()))) {
          pageHeader.setModified(true);
        }
      }
    }
    return false;
  }
}
