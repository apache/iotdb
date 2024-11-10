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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.reader;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.reader.page.PageReader;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.read.reader.chunk.ChunkReader.decryptAndUncompressPageData;

public class CompactionChunkReader {

  private final ChunkHeader chunkHeader;
  private ByteBuffer chunkDataBuffer;
  private final IUnCompressor unCompressor;

  private final EncryptParameter encryptParam;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  private final Statistics chunkStatistic;

  // A list of deleted intervals.
  private final List<TimeRange> deleteIntervalList;

  /**
   * Constructor of ChunkReader without deserializing chunk into page. This is used for fast
   * compaction.
   */
  public CompactionChunkReader(Chunk chunk) {
    this.chunkHeader = chunk.getHeader();
    this.chunkDataBuffer = chunk.getData();
    this.unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    this.deleteIntervalList = chunk.getDeleteIntervalList();
    this.chunkStatistic = chunk.getChunkStatistic();
    this.encryptParam = chunk.getEncryptParam();
  }

  /**
   * Read page data without uncompressing it.
   *
   * @return compressed page data
   * @throws IOException exception thrown when reading page data
   */
  public ByteBuffer readPageDataWithoutUncompressing(PageHeader pageHeader) throws IOException {
    return readCompressedPageData(pageHeader, chunkDataBuffer);
  }

  public List<Pair<PageHeader, ByteBuffer>> readPageDataWithoutUncompressing() throws IOException {
    List<Pair<PageHeader, ByteBuffer>> pages = new ArrayList<>();
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader;
      if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkStatistic);
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      }
      if (pageHeader.getCompressedSize() == 0) {
        // empty value page
        pages.add(null);
      } else {
        ByteBuffer compressedPageData = readCompressedPageData(pageHeader, chunkDataBuffer);
        Pair<PageHeader, ByteBuffer> page = new Pair<>(pageHeader, compressedPageData);
        pages.add(page);
      }
    }
    // clear chunk data to release memory
    chunkDataBuffer = null;
    return pages;
  }

  public static ByteBuffer readCompressedPageData(PageHeader pageHeader, ByteBuffer chunkBuffer)
      throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    if (compressedPageBodyLength > chunkBuffer.remaining()) {
      throw new IOException(
          "do not have a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkBuffer.remaining());
    }
    ByteBuffer pageBodyBuffer = chunkBuffer.slice();
    pageBodyBuffer.limit(compressedPageBodyLength);
    chunkBuffer.position(chunkBuffer.position() + compressedPageBodyLength);
    return pageBodyBuffer;
  }

  /**
   * Read data from compressed page data. Uncompress the page and decode it to batch data.
   *
   * @param compressedPageData Compressed page data
   * @throws IOException exception thrown when reading page data
   */
  public TsBlock readPageData(PageHeader pageHeader, ByteBuffer compressedPageData)
      throws IOException {
    // decrypt and uncompress page data
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer pageData =
        decryptAndUncompressPageData(pageHeader, unCompressor, compressedPageData, decryptor);
    // decode page data
    TSDataType dataType = chunkHeader.getDataType();
    Decoder valueDecoder = Decoder.getDecoderByType(chunkHeader.getEncodingType(), dataType);
    PageReader pageReader =
        new PageReader(pageHeader, pageData, dataType, valueDecoder, timeDecoder, null);
    pageReader.setDeleteIntervalList(deleteIntervalList);
    return pageReader.getAllSatisfiedData();
  }
}
