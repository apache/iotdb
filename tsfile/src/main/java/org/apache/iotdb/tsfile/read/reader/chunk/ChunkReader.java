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

import java.util.ArrayList;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.v1.file.utils.HeaderUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class ChunkReader implements IChunkReader {

  private ChunkHeader chunkHeader;
  private ByteBuffer chunkDataBuffer;
  private IUnCompressor unCompressor;
  private Decoder timeDecoder = Decoder.getDecoderByType(
      TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
      TSDataType.INT64);

  protected Filter filter;

  private List<IPageReader> pageReaderList = new LinkedList<>();
  
  private boolean isFromOldTsFile = false;

  /**
   * Data whose timestamp <= deletedAt should be considered deleted(not be returned).
   */
  protected long deletedAt;

  private List<Pair<Long, Long>> deleteRangeList = new ArrayList<>();

  /**
   * constructor of ChunkReader.
   *
   * @param chunk  input Chunk object
   * @param filter filter
   */
  public ChunkReader(Chunk chunk, Filter filter) throws IOException {
    this.filter = filter;
    this.chunkDataBuffer = chunk.getData();
    this.deletedAt = chunk.getDeletedAt();
    this.deleteRangeList = chunk.getDeleteRangeList();
    chunkHeader = chunk.getHeader();
    this.unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());


    initAllPageReaders();
  }

  public ChunkReader(Chunk chunk, Filter filter, boolean isFromOldFile) throws IOException {
    this.filter = filter;
    this.chunkDataBuffer = chunk.getData();
    this.deletedAt = chunk.getDeletedAt();
    this.deleteRangeList = chunk.getDeleteRangeList();
    chunkHeader = chunk.getHeader();
    this.unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    this.isFromOldTsFile = isFromOldFile;

    initAllPageReaders();
  }

  private void initAllPageReaders() throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      // deserialize a PageHeader from chunkDataBuffer
      PageHeader pageHeader = isFromOldTsFile ? HeaderUtils.deserializePageHeaderV1(chunkDataBuffer, chunkHeader.getDataType()) :
          PageHeader.deserializeFrom(chunkDataBuffer, chunkHeader.getDataType());
      // if the current page satisfies
      if (pageSatisfied(pageHeader)) {
        pageReaderList.add(constructPageReaderForNextPage(pageHeader));
      } else {
        skipBytesInStreamByLength(pageHeader.getCompressedSize());
      }
    }
  }



  /**
   * judge if has next page whose page header satisfies the filter.
   */
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

  private void skipBytesInStreamByLength(long length) {
    chunkDataBuffer.position(chunkDataBuffer.position() + (int) length);
  }

  public boolean pageSatisfied(PageHeader pageHeader) {
    long lower = pageHeader.getStartTime();
    long upper = pageHeader.getEndTime();
    // deleteRangeList is sorted in terms of startTime
    for (Pair<Long, Long> range : deleteRangeList) {
      if (upper < range.left) {
        break;
      }
      if (range.left <= lower && lower <= range.right) {
        pageHeader.setModified(true);
        if (upper <= range.right) {
          return true;
        }
        lower = range.right;
      } else if (lower < range.left) {
        pageHeader.setModified(true);
        break;
      }
    }
    return filter == null || filter.satisfy(pageHeader.getStatistics());
  }

  private PageReader constructPageReaderForNextPage(PageHeader pageHeader)
      throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];

    // doesn't has a complete page body
    if (compressedPageBodyLength > chunkDataBuffer.remaining()) {
      throw new IOException("do not has a complete page body. Expected:" + compressedPageBodyLength
          + ". Actual:" + chunkDataBuffer.remaining());
    }

    chunkDataBuffer.get(compressedPageBody);
    Decoder valueDecoder = Decoder
            .getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType());
    ByteBuffer pageData = ByteBuffer.wrap(unCompressor.uncompress(compressedPageBody));
    PageReader reader = new PageReader(pageHeader, pageData, chunkHeader.getDataType(),
        valueDecoder, timeDecoder, filter);
    reader.setDeletedAt(deletedAt);
    reader.setDeleteRangeList(deleteRangeList);
    return reader;
  }

  @Override
  public void close() {
  }

  public ChunkHeader getChunkHeader() {
    return chunkHeader;
  }

  @Override
  public List<IPageReader> loadPageReaderList() {
    return pageReaderList;
  }
}
