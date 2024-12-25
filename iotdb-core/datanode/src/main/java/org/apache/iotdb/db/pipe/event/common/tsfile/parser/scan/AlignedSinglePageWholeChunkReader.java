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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * The {@link AlignedSinglePageWholeChunkReader} is used to read a whole single page aligned chunk
 * with need to pass in the statistics.
 */
public class AlignedSinglePageWholeChunkReader extends AbstractChunkReader {

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

  public AlignedSinglePageWholeChunkReader(Chunk timeChunk, List<Chunk> valueChunkList)
      throws IOException {
    super(Long.MIN_VALUE, null);
    this.timeChunkHeader = timeChunk.getHeader();
    this.timeChunkDataBuffer = timeChunk.getData();
    this.encryptParam = timeChunk.getEncryptParam();

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
    List<ByteBuffer> valuePageDataList = new ArrayList<>();
    List<TSDataType> valueDataTypeList = new ArrayList<>();
    List<Decoder> valueDecoderList = new ArrayList<>();

    boolean isAllNull = true;
    for (int i = 0; i < rawValuePageHeaderList.size(); i++) {
      PageHeader valuePageHeader = rawValuePageHeaderList.get(i);

      if (valuePageHeader == null || valuePageHeader.getUncompressedSize() == 0) {
        // Empty Page
        valuePageHeaderList.add(null);
        valuePageDataList.add(null);
        valueDataTypeList.add(null);
        valueDecoderList.add(null);
      } else {
        ChunkHeader valueChunkHeader = valueChunkHeaderList.get(i);
        valuePageHeaderList.add(valuePageHeader);
        valuePageDataList.add(
            ChunkReader.deserializePageData(
                valuePageHeader, valueChunkDataBufferList.get(i), valueChunkHeader, decryptor));
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
            defaultTimeDecoder,
            valuePageHeaderList,
            valuePageDataList,
            valueDataTypeList,
            valueDecoderList,
            queryFilter);
    alignedPageReader.setDeleteIntervalList(valueDeleteIntervalsList);
    return alignedPageReader;
  }
}
