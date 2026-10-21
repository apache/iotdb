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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.chunk.AbstractChunkReader;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.read.reader.page.AlignedPageReader;
import org.apache.tsfile.read.reader.page.LazyLoadPageData;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * The {@link AlignedSinglePageWholeChunkReader} is used to read a whole single page aligned chunk
 * with need to pass in the statistics.
 */
public class AlignedSinglePageWholeChunkReader extends AbstractChunkReader
    implements EstimatedMemoryChunkReader {

  // chunk header of the time column
  private final ChunkHeader timeChunkHeader;
  // chunk data of the time column
  private final ByteBuffer timeChunkDataBuffer;

  private final EncryptParameter encryptParam;

  // chunk headers of all the sub sensors
  private final List<ChunkHeader> valueChunkHeaderList = new ArrayList<>();
  // chunk data of all the sub sensors
  private final List<ByteBuffer> valueChunkDataBufferList = new ArrayList<>();
  // deleted intervals of all the sub sensors
  private final List<List<TimeRange>> valueDeleteIntervalsList = new ArrayList<>();
  private final long pageEstimatedMemoryUsageInBytes;

  public AlignedSinglePageWholeChunkReader(
      Chunk timeChunk, List<Chunk> valueChunkList, LongConsumer filteredRowsRecord)
      throws IOException {
    super(Long.MIN_VALUE, null, filteredRowsRecord);
    this.timeChunkHeader = timeChunk.getHeader();
    this.timeChunkDataBuffer = timeChunk.getData();
    this.encryptParam = timeChunk.getEncryptParam();
    this.pageEstimatedMemoryUsageInBytes =
        calculateMaxPageEstimatedMemoryUsageInBytesWithBatchData(timeChunk, valueChunkList);

    valueChunkList.forEach(
        chunk -> {
          this.valueChunkHeaderList.add(chunk == null ? null : chunk.getHeader());
          this.valueChunkDataBufferList.add(chunk == null ? null : chunk.getData());
          this.valueDeleteIntervalsList.add(chunk == null ? null : chunk.getDeleteIntervalList());
        });

    initAllPageReaders();
  }

  private void initAllPageReaders() throws IOException {
    while (timeChunkDataBuffer.remaining() > 0) {
      AlignedPageReader alignedPageReader = deserializeFromSinglePageChunk();
      if (alignedPageReader != null) {
        pageReaderList.add(alignedPageReader);
      }
    }
  }

  private AlignedPageReader deserializeFromSinglePageChunk() throws IOException {
    PageHeader timePageHeader =
        PageHeader.deserializeFrom(timeChunkDataBuffer, (Statistics<? extends Serializable>) null);
    List<PageHeader> valuePageHeaderList = new ArrayList<>();

    boolean isAllNull = true;
    for (ByteBuffer byteBuffer : valueChunkDataBufferList) {
      if (byteBuffer != null) {
        isAllNull = false;
        valuePageHeaderList.add(
            PageHeader.deserializeFrom(byteBuffer, (Statistics<? extends Serializable>) null));
      } else {
        valuePageHeaderList.add(null);
      }
    }

    if (isAllNull) {
      // when there is only one page in the chunk, the page statistic is the same as the chunk, so
      // we needn't filter the page again
      skipCurrentPage(timePageHeader, valuePageHeaderList);
      return null;
    }
    return constructAlignedPageReader(timePageHeader, valuePageHeaderList);
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

  private AlignedPageReader constructAlignedPageReader(
      PageHeader timePageHeader, List<PageHeader> rawValuePageHeaderList) throws IOException {
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer timePageData =
        ChunkReader.deserializePageData(
            timePageHeader, timeChunkDataBuffer, timeChunkHeader, decryptor);

    List<PageHeader> valuePageHeaderList = new ArrayList<>();
    LazyLoadPageData[] valuePageDataArray = new LazyLoadPageData[rawValuePageHeaderList.size()];
    List<TSDataType> valueDataTypeList = new ArrayList<>();
    List<Decoder> valueDecoderList = new ArrayList<>();

    boolean isAllNull = true;
    for (int i = 0; i < rawValuePageHeaderList.size(); i++) {
      PageHeader valuePageHeader = rawValuePageHeaderList.get(i);

      if (valuePageHeader == null || valuePageHeader.getUncompressedSize() == 0) {
        // Empty Page
        valuePageHeaderList.add(null);
        valuePageDataArray[i] = null;
        valueDataTypeList.add(null);
        valueDecoderList.add(null);
      } else {
        ChunkHeader valueChunkHeader = valueChunkHeaderList.get(i);
        int currentPagePosition = valueChunkDataBufferList.get(i).position();
        valueChunkDataBufferList
            .get(i)
            .position(currentPagePosition + valuePageHeader.getCompressedSize());
        valuePageHeaderList.add(valuePageHeader);
        valuePageDataArray[i] =
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
    if (isAllNull) {
      return null;
    }
    AlignedPageReader alignedPageReader =
        new AlignedPageReader(
            timePageHeader,
            timePageData,
            getTimeDecoder(timeChunkHeader.getEncodingType()),
            valuePageHeaderList,
            valuePageDataArray,
            valueDataTypeList,
            valueDecoderList,
            queryFilter);
    alignedPageReader.setDeleteIntervalList(valueDeleteIntervalsList);
    return alignedPageReader;
  }

  @Override
  public long getCurrentPageEstimatedMemoryUsageInBytes() {
    return pageEstimatedMemoryUsageInBytes;
  }

  public static long calculatePageEstimatedMemoryUsageInBytes(
      final Chunk timeChunk, final List<Chunk> valueChunkList) throws IOException {
    final ByteBuffer timeChunkDataBuffer = timeChunk.getData().duplicate();
    long estimatedMemoryUsageInBytes =
        PageHeader.deserializeFrom(timeChunkDataBuffer, (Statistics<? extends Serializable>) null)
            .getUncompressedSize();

    for (final Chunk valueChunk : valueChunkList) {
      if (valueChunk == null) {
        continue;
      }

      final ByteBuffer valueChunkDataBuffer = valueChunk.getData().duplicate();
      estimatedMemoryUsageInBytes +=
          PageHeader.deserializeFrom(
                  valueChunkDataBuffer, (Statistics<? extends Serializable>) null)
              .getUncompressedSize();
    }

    return estimatedMemoryUsageInBytes;
  }

  public static long calculateMaxPageEstimatedMemoryUsageInBytesWithBatchData(
      final Chunk timeChunk, final List<Chunk> valueChunkList) throws IOException {
    final List<Long> pageEstimatedMemoryUsageInBytesList =
        calculatePageEstimatedMemoryUsageInBytesWithBatchDataList(timeChunk, valueChunkList);
    return pageEstimatedMemoryUsageInBytesList.isEmpty()
        ? 0
        : pageEstimatedMemoryUsageInBytesList.get(0);
  }

  public static List<Long> calculatePageEstimatedMemoryUsageInBytesWithBatchDataList(
      final Chunk timeChunk, final List<Chunk> valueChunkList) throws IOException {
    final PageMemoryUsageEstimator estimator = new PageMemoryUsageEstimator(timeChunk);
    for (final Chunk valueChunk : valueChunkList) {
      estimator.addValueChunk(valueChunk);
    }
    return estimator.getPageEstimatedMemoryUsageInBytesList();
  }

  static final class PageMemoryUsageEstimator {

    private final List<Long> pageUncompressedSizeInBytesList = new ArrayList<>();
    private final List<Integer> pageRowCountList = new ArrayList<>();
    private List<Long> pageEstimatedMemoryUsageInBytesList = new ArrayList<>();

    private int valueCount;
    private TSDataType singleValueDataType;
    private long vectorValueMemoryUsageInBytes;

    PageMemoryUsageEstimator(final Chunk timeChunk) throws IOException {
      final ByteBuffer timeChunkDataBuffer = timeChunk.getData().duplicate();
      timeChunkDataBuffer.rewind();
      while (timeChunkDataBuffer.remaining() > 0) {
        final PageHeader timePageHeader =
            SinglePageWholeChunkReader.deserializePageHeader(
                timeChunkDataBuffer, timeChunk.getHeader());
        pageUncompressedSizeInBytesList.add((long) timePageHeader.getUncompressedSize());
        pageRowCountList.add(SinglePageWholeChunkReader.getPageRowCount(timePageHeader, timeChunk));
        SinglePageWholeChunkReader.skipCompressedPageData(timeChunkDataBuffer, timePageHeader);
      }
      pageEstimatedMemoryUsageInBytesList = calculatePageEstimatedMemoryUsageInBytesList();
    }

    long calculateMaxPageEstimatedMemoryUsageInBytesWithValueChunk(final Chunk valueChunk)
        throws IOException {
      final List<Long> pageEstimatedMemoryUsageInBytesList =
          calculatePageEstimatedMemoryUsageInBytesList(valueChunk, false);
      return pageEstimatedMemoryUsageInBytesList.isEmpty()
          ? 0
          : pageEstimatedMemoryUsageInBytesList.get(0);
    }

    void addValueChunk(final Chunk valueChunk) throws IOException {
      pageEstimatedMemoryUsageInBytesList =
          calculatePageEstimatedMemoryUsageInBytesList(valueChunk, true);
    }

    List<Long> getPageEstimatedMemoryUsageInBytesList() {
      return pageEstimatedMemoryUsageInBytesList;
    }

    private List<Long> calculatePageEstimatedMemoryUsageInBytesList() {
      final List<Long> pageEstimatedMemoryUsageInBytesList =
          new ArrayList<>(pageUncompressedSizeInBytesList.size());
      for (int i = 0; i < pageUncompressedSizeInBytesList.size(); ++i) {
        pageEstimatedMemoryUsageInBytesList.add(
            SinglePageWholeChunkReader.estimatePageMemoryUsageInBytesWithBatchData(
                pageUncompressedSizeInBytesList.get(i),
                pageRowCountList.get(i),
                valueCount,
                singleValueDataType,
                vectorValueMemoryUsageInBytes));
      }
      return SinglePageWholeChunkReader.toSuffixMaxList(pageEstimatedMemoryUsageInBytesList);
    }

    private List<Long> calculatePageEstimatedMemoryUsageInBytesList(
        final Chunk valueChunk, final boolean commit) throws IOException {
      final ByteBuffer valueChunkDataBuffer;
      final TSDataType valueDataType;
      if (Objects.isNull(valueChunk)) {
        valueChunkDataBuffer = null;
        valueDataType = null;
      } else {
        valueChunkDataBuffer = valueChunk.getData().duplicate();
        valueChunkDataBuffer.rewind();
        valueDataType = valueChunk.getHeader().getDataType();
      }

      final int newValueCount = valueCount + 1;
      final TSDataType newSingleValueDataType = valueCount == 0 ? valueDataType : null;
      final long newVectorValueMemoryUsageInBytes =
          vectorValueMemoryUsageInBytes
              + SinglePageWholeChunkReader.estimateVectorValueMemoryUsageInBytes(valueDataType);
      final List<Long> newPageUncompressedSizeInBytesList =
          new ArrayList<>(pageUncompressedSizeInBytesList);
      final List<Long> pageEstimatedMemoryUsageInBytesList =
          new ArrayList<>(pageUncompressedSizeInBytesList.size());

      for (int i = 0; i < newPageUncompressedSizeInBytesList.size(); ++i) {
        if (Objects.nonNull(valueChunkDataBuffer)) {
          final PageHeader valuePageHeader =
              SinglePageWholeChunkReader.deserializePageHeader(
                  valueChunkDataBuffer, valueChunk.getHeader());
          newPageUncompressedSizeInBytesList.set(
              i, newPageUncompressedSizeInBytesList.get(i) + valuePageHeader.getUncompressedSize());
          SinglePageWholeChunkReader.skipCompressedPageData(valueChunkDataBuffer, valuePageHeader);
        }
        pageEstimatedMemoryUsageInBytesList.add(
            SinglePageWholeChunkReader.estimatePageMemoryUsageInBytesWithBatchData(
                newPageUncompressedSizeInBytesList.get(i),
                pageRowCountList.get(i),
                newValueCount,
                newSingleValueDataType,
                newVectorValueMemoryUsageInBytes));
      }

      if (commit) {
        pageUncompressedSizeInBytesList.clear();
        pageUncompressedSizeInBytesList.addAll(newPageUncompressedSizeInBytesList);
        valueCount = newValueCount;
        singleValueDataType = newSingleValueDataType;
        vectorValueMemoryUsageInBytes = newVectorValueMemoryUsageInBytes;
      }

      return SinglePageWholeChunkReader.toSuffixMaxList(pageEstimatedMemoryUsageInBytesList);
    }
  }
}
