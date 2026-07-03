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

import org.apache.iotdb.db.i18n.DataNodePipeMessages;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.AbstractChunkReader;
import org.apache.tsfile.read.reader.page.LazyLoadPageData;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.tsfile.file.metadata.enums.CompressionType.UNCOMPRESSED;

public class SinglePageWholeChunkReader extends AbstractChunkReader
    implements EstimatedMemoryChunkReader {
  private final ChunkHeader chunkHeader;
  private final ByteBuffer chunkDataBuffer;
  private final EncryptParameter encryptParam;
  private final long pageEstimatedMemoryUsageInBytes;

  public SinglePageWholeChunkReader(Chunk chunk) throws IOException {
    super(Long.MIN_VALUE, null, null);

    this.chunkHeader = chunk.getHeader();
    this.chunkDataBuffer = chunk.getData();
    this.encryptParam = chunk.getEncryptParam();
    this.pageEstimatedMemoryUsageInBytes =
        calculateMaxPageEstimatedMemoryUsageInBytesWithBatchData(chunk);
    initAllPageReaders();
  }

  private void initAllPageReaders() throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      pageReaderList.add(
          constructPageReader(
              PageHeader.deserializeFrom(
                  chunkDataBuffer, (Statistics<? extends Serializable>) null)));
    }
  }

  private PageReader constructPageReader(PageHeader pageHeader) throws IOException {
    final int currentPagePosition = chunkDataBuffer.position();
    chunkDataBuffer.position(currentPagePosition + pageHeader.getCompressedSize());
    return new PageReader(
        pageHeader,
        new LazyLoadPageData(
            chunkDataBuffer.array(),
            currentPagePosition,
            IUnCompressor.getUnCompressor(chunkHeader.getCompressionType()),
            encryptParam),
        chunkHeader.getDataType(),
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType()),
        getConfiguredTimeDecoder(),
        null);
  }

  @Override
  public long getCurrentPageEstimatedMemoryUsageInBytes() {
    return pageEstimatedMemoryUsageInBytes;
  }

  public static long calculatePageEstimatedMemoryUsageInBytes(final Chunk chunk)
      throws IOException {
    final ByteBuffer chunkDataBuffer = chunk.getData().duplicate();
    final PageHeader pageHeader = deserializePageHeader(chunkDataBuffer, chunk.getHeader());
    return pageHeader.getUncompressedSize();
  }

  public static long calculateMaxPageEstimatedMemoryUsageInBytes(final Chunk chunk)
      throws IOException {
    final ByteBuffer chunkDataBuffer = chunk.getData().duplicate();
    long maxPageEstimatedMemoryUsageInBytes = 0;
    while (chunkDataBuffer.remaining() > 0) {
      final PageHeader pageHeader = deserializePageHeader(chunkDataBuffer, chunk.getHeader());
      maxPageEstimatedMemoryUsageInBytes =
          Math.max(maxPageEstimatedMemoryUsageInBytes, pageHeader.getUncompressedSize());
      skipCompressedPageData(chunkDataBuffer, pageHeader);
    }
    return maxPageEstimatedMemoryUsageInBytes;
  }

  public static long calculateMaxPageEstimatedMemoryUsageInBytesWithBatchData(final Chunk chunk)
      throws IOException {
    final List<Long> pageEstimatedMemoryUsageInBytesList =
        calculatePageEstimatedMemoryUsageInBytesWithBatchDataList(chunk);
    return pageEstimatedMemoryUsageInBytesList.isEmpty()
        ? 0
        : pageEstimatedMemoryUsageInBytesList.get(0);
  }

  public static List<Long> calculatePageEstimatedMemoryUsageInBytesWithBatchDataList(
      final Chunk chunk) throws IOException {
    final ByteBuffer chunkDataBuffer = chunk.getData().duplicate();
    final List<Long> pageEstimatedMemoryUsageInBytesList = new ArrayList<>();
    while (chunkDataBuffer.remaining() > 0) {
      final PageHeader pageHeader = deserializePageHeader(chunkDataBuffer, chunk.getHeader());
      pageEstimatedMemoryUsageInBytesList.add(
          estimatePageMemoryUsageInBytesWithBatchData(
              pageHeader, chunk, Collections.singletonList(chunk.getHeader().getDataType())));
      skipCompressedPageData(chunkDataBuffer, pageHeader);
    }
    return toSuffixMaxList(pageEstimatedMemoryUsageInBytesList);
  }

  static PageHeader deserializePageHeader(
      final ByteBuffer chunkDataBuffer, final ChunkHeader chunkHeader) throws IOException {
    return isSinglePageChunk(chunkHeader)
        ? PageHeader.deserializeFrom(chunkDataBuffer, (Statistics<? extends Serializable>) null)
        : PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
  }

  static boolean isSinglePageChunk(final ChunkHeader chunkHeader) {
    return (chunkHeader.getChunkType() & 0x3F) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER;
  }

  static void skipCompressedPageData(
      final ByteBuffer chunkDataBuffer, final PageHeader pageHeader) {
    chunkDataBuffer.position(chunkDataBuffer.position() + pageHeader.getCompressedSize());
  }

  static List<Long> toSuffixMaxList(final List<Long> pageEstimatedMemoryUsageInBytesList) {
    long suffixMaxPageEstimatedMemoryUsageInBytes = 0;
    for (int i = pageEstimatedMemoryUsageInBytesList.size() - 1; i >= 0; --i) {
      suffixMaxPageEstimatedMemoryUsageInBytes =
          Math.max(
              suffixMaxPageEstimatedMemoryUsageInBytes, pageEstimatedMemoryUsageInBytesList.get(i));
      pageEstimatedMemoryUsageInBytesList.set(i, suffixMaxPageEstimatedMemoryUsageInBytes);
    }
    return pageEstimatedMemoryUsageInBytesList;
  }

  static long estimatePageMemoryUsageInBytesWithBatchData(
      final PageHeader timePageHeader,
      final Chunk timeChunk,
      final List<TSDataType> valueDataTypeList)
      throws IOException {
    return estimatePageMemoryUsageInBytesWithBatchData(
        timePageHeader.getUncompressedSize(),
        getPageRowCount(timePageHeader, timeChunk),
        valueDataTypeList);
  }

  static int getPageRowCount(final PageHeader pageHeader, final Chunk chunk) throws IOException {
    if (isSinglePageChunk(chunk.getHeader())) {
      if (Objects.nonNull(chunk.getChunkStatistic())) {
        return saturateToInt(chunk.getChunkStatistic().getCount());
      }
      return isTimeChunk(chunk.getHeader()) ? countSinglePageTimeValues(chunk) : 0;
    }
    return saturateToInt(pageHeader.getNumOfValues());
  }

  private static int countSinglePageTimeValues(final Chunk chunk) throws IOException {
    final ByteBuffer chunkDataBuffer = chunk.getData().duplicate();
    final PageHeader pageHeader = deserializePageHeader(chunkDataBuffer, chunk.getHeader());
    final ByteBuffer pageData =
        deserializePageData(
            pageHeader,
            chunkDataBuffer,
            chunk.getHeader(),
            IDecryptor.getDecryptor(chunk.getEncryptParam()));
    final Decoder decoder =
        Decoder.getDecoderByType(chunk.getHeader().getEncodingType(), TSDataType.INT64);

    int rowCount = 0;
    while (decoder.hasNext(pageData)) {
      decoder.readLong(pageData);
      ++rowCount;
    }
    return rowCount;
  }

  private static boolean isTimeChunk(final ChunkHeader chunkHeader) {
    return (chunkHeader.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
        == TsFileConstant.TIME_COLUMN_MASK;
  }

  private static int saturateToInt(final long value) {
    return (int) Math.min(Integer.MAX_VALUE, value);
  }

  static long estimatePageMemoryUsageInBytesWithBatchData(
      final long pageUncompressedSizeInBytes,
      final int rowCount,
      final List<TSDataType> valueDataTypeList) {
    return pageUncompressedSizeInBytes
        + estimateBatchDataMemoryUsageInBytes(rowCount, valueDataTypeList);
  }

  private static long estimateBatchDataMemoryUsageInBytes(
      final int rowCount, final List<TSDataType> valueDataTypeList) {
    final int valueCount = valueDataTypeList.size();
    final long segmentCount = Math.max(1, (rowCount + 15L) / 16);
    long estimatedMemoryUsageInBytes = RamUsageEstimator.sizeOfLongArray(16) * segmentCount;

    if (valueCount == 1) {
      estimatedMemoryUsageInBytes +=
          estimateSingleValueArrayMemoryUsageInBytes(rowCount, valueDataTypeList.get(0));
    } else if (valueCount > 1) {
      estimatedMemoryUsageInBytes += RamUsageEstimator.sizeOfObjectArray(16) * segmentCount;
      estimatedMemoryUsageInBytes +=
          (long) rowCount
              * (RamUsageEstimator.sizeOfObjectArray(valueCount)
                  + estimateVectorValueMemoryUsageInBytes(valueDataTypeList));
    }

    return estimatedMemoryUsageInBytes;
  }

  private static long estimateSingleValueArrayMemoryUsageInBytes(
      final int rowCount, final TSDataType dataType) {
    final long segmentCount = Math.max(1, (rowCount + 15L) / 16);
    if (Objects.isNull(dataType)) {
      return 0;
    }

    switch (dataType) {
      case BOOLEAN:
        return RamUsageEstimator.sizeOfBooleanArray(16) * segmentCount;
      case INT32:
      case DATE:
        return RamUsageEstimator.sizeOfIntArray(16) * segmentCount;
      case INT64:
      case TIMESTAMP:
        return RamUsageEstimator.sizeOfLongArray(16) * segmentCount;
      case FLOAT:
        return RamUsageEstimator.sizeOfFloatArray(16) * segmentCount;
      case DOUBLE:
        return RamUsageEstimator.sizeOfDoubleArray(16) * segmentCount;
      case TEXT:
      case BLOB:
      case STRING:
        return RamUsageEstimator.sizeOfObjectArray(16) * segmentCount;
      default:
        return 0;
    }
  }

  private static long estimateVectorValueMemoryUsageInBytes(
      final List<TSDataType> valueDataTypeList) {
    long estimatedMemoryUsageInBytes = 0;
    for (final TSDataType dataType : valueDataTypeList) {
      if (Objects.isNull(dataType)) {
        continue;
      }

      estimatedMemoryUsageInBytes +=
          RamUsageEstimator.alignObjectSize(
              RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                  + estimateTsPrimitiveTypeValueMemoryUsageInBytes(dataType));
    }
    return estimatedMemoryUsageInBytes;
  }

  private static long estimateTsPrimitiveTypeValueMemoryUsageInBytes(final TSDataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return 1;
      case INT32:
      case DATE:
      case FLOAT:
        return Integer.BYTES;
      case INT64:
      case TIMESTAMP:
      case DOUBLE:
        return Long.BYTES;
      case TEXT:
      case BLOB:
      case STRING:
        return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      default:
        return 0;
    }
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
          DataNodePipeMessages.DO_NOT_HAS_A_COMPLETE_PAGE_BODY
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
    if (unCompressor.getCodecName() == UNCOMPRESSED) {
      return compressedPageData;
    }
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    ByteBuffer uncompressedPageData = ByteBuffer.allocate(pageHeader.getUncompressedSize());
    try {
      unCompressor.uncompress(
          compressedPageData.array(), 0, compressedPageBodyLength, uncompressedPageData.array(), 0);
    } catch (Exception e) {
      throw new IOException(
          DataNodePipeMessages.UNCOMPRESS_ERROR_UNCOMPRESS_SIZE
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage(),
          e);
    }

    return uncompressedPageData;
  }

  public static ByteBuffer decrypt(IDecryptor decryptor, ByteBuffer buffer) {
    if (decryptor == null || decryptor.getEncryptionType() == EncryptionType.UNENCRYPTED) {
      return buffer;
    }
    return ByteBuffer.wrap(
        decryptor.decrypt(
            buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining()));
  }

  public static ByteBuffer decryptAndUncompressPageData(
      PageHeader pageHeader,
      IUnCompressor unCompressor,
      ByteBuffer compressedPageData,
      IDecryptor decryptor)
      throws IOException {
    return uncompressPageData(pageHeader, unCompressor, decrypt(decryptor, compressedPageData));
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
}
