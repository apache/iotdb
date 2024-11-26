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
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.reader.page.LazyLoadPageData;
import org.apache.tsfile.read.reader.page.PageReader;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

public class ChunkReader extends AbstractChunkReader {

  private final ChunkHeader chunkHeader;
  private final ByteBuffer chunkDataBuffer;
  private final List<TimeRange> deleteIntervalList;

  private final EncryptParameter encryptParam;

  @SuppressWarnings("unchecked")
  public ChunkReader(Chunk chunk, long readStopTime, Filter queryFilter) {
    super(readStopTime, queryFilter);
    this.chunkHeader = chunk.getHeader();
    this.chunkDataBuffer = chunk.getData();
    this.deleteIntervalList = chunk.getDeleteIntervalList();
    this.encryptParam = chunk.getEncryptParam();
    initAllPageReaders(chunk.getChunkStatistic());
  }

  public ChunkReader(Chunk chunk) throws IOException {
    this(chunk, Long.MIN_VALUE, null);
  }

  public ChunkReader(Chunk chunk, Filter queryFilter) {
    this(chunk, Long.MIN_VALUE, queryFilter);
  }

  /**
   * Constructor of ChunkReader by timestamp. This constructor is used to accelerate queries by
   * filtering out pages whose endTime is less than current timestamp.
   */
  public ChunkReader(Chunk chunk, long readStopTime) {
    this(chunk, readStopTime, null);
  }

  private void initAllPageReaders(Statistics<? extends Serializable> chunkStatistic) {
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

  protected boolean pageDeleted(PageHeader pageHeader) {
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

  private PageReader constructPageReader(PageHeader pageHeader) {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    // record the current position of chunkDataBuffer, use this to get the page data in PageReader
    // through directly accessing the buffer array
    int currentPagePosition = chunkDataBuffer.position();
    skipCurrentPage(pageHeader);
    PageReader reader =
        new PageReader(
            pageHeader,
            new LazyLoadPageData(
                chunkDataBuffer.array(), currentPagePosition, unCompressor, encryptParam),
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
          compressedPageData.array(),
          compressedPageData.arrayOffset() + compressedPageData.position(),
          compressedPageBodyLength,
          uncompressedPageData,
          0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage(),
          e);
    }
    compressedPageData.position(compressedPageData.position() + compressedPageBodyLength);
    return ByteBuffer.wrap(uncompressedPageData);
  }

  public static ByteBuffer decryptAndUncompressPageData(
      PageHeader pageHeader,
      IUnCompressor unCompressor,
      ByteBuffer compressedPageData,
      IDecryptor decryptor)
      throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    try {
      byte[] decryptedPageData =
          decryptor.decrypt(
              compressedPageData.array(),
              compressedPageData.arrayOffset() + compressedPageData.position(),
              compressedPageBodyLength);
      unCompressor.uncompress(
          decryptedPageData, 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage(),
          e);
    }
    compressedPageData.position(compressedPageData.position() + compressedPageBodyLength);
    return ByteBuffer.wrap(uncompressedPageData);
  }

  public static ByteBuffer deserializePageData(
      PageHeader pageHeader, ByteBuffer chunkBuffer, ChunkHeader chunkHeader, IDecryptor decryptor)
      throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    ByteBuffer compressedPageBody = readCompressedPageData(pageHeader, chunkBuffer);
    if (decryptor == null || decryptor.getEncryptionType() == EncryptionType.UNENCRYPTED) {
      return uncompressPageData(pageHeader, unCompressor, compressedPageBody);
    } else {
      return decryptAndUncompressPageData(pageHeader, unCompressor, compressedPageBody, decryptor);
    }
  }

  public static ByteBuffer deserializePageData(
      PageHeader pageHeader, ByteBuffer chunkBuffer, ChunkHeader chunkHeader) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    ByteBuffer compressedPageBody = readCompressedPageData(pageHeader, chunkBuffer);
    return uncompressPageData(pageHeader, unCompressor, compressedPageBody);
  }
}
