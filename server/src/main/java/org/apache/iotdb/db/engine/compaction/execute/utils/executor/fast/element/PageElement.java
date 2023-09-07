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
package org.apache.iotdb.db.engine.compaction.execute.utils.executor.fast.element;

import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.IChunkReader;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.page.LazyLoadAlignedPagePointReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PageElement {

  public PageHeader pageHeader;

  public List<PageHeader> valuePageHeaders;

  public TsBlock batchData;

  // pointReader is used to replace batchData to get rid of huge memory cost by loading data point
  // in a lazy way
  public IPointReader pointReader;

  // compressed page data
  public ByteBuffer pageData;

  public List<ByteBuffer> valuePageDatas;

  public IChunkReader iChunkReader;

  public long priority;

  public long startTime;

  public boolean isSelected = false;

  public boolean isLastPage;

  public ChunkMetadataElement chunkMetadataElement;

  public boolean needForceDecoding;

  public PageElement(
      PageHeader pageHeader,
      ByteBuffer pageData,
      ChunkReader chunkReader,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      long priority) {
    this.pageHeader = pageHeader;
    this.pageData = pageData;
    this.priority = priority;
    this.iChunkReader = chunkReader;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
    this.needForceDecoding = chunkMetadataElement.needForceDecoding;
  }

  public PageElement(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer timePageData,
      List<ByteBuffer> valuePageDatas,
      ChunkMetadataElement chunkMetadataElement,
      boolean isLastPage,
      long priority) {
    this.pageHeader = timePageHeader;
    this.valuePageHeaders = valuePageHeaders;
    this.pageData = timePageData;
    this.valuePageDatas = valuePageDatas;
    this.priority = priority;
    this.startTime = pageHeader.getStartTime();
    this.chunkMetadataElement = chunkMetadataElement;
    this.isLastPage = isLastPage;
    this.needForceDecoding = chunkMetadataElement.needForceDecoding;
  }

  public void deserializePage() throws IOException {
    if (this.iChunkReader == null) {
      this.pointReader = getAlignedPagePointReader();
    } else {
      this.batchData = ((ChunkReader) iChunkReader).readPageData(pageHeader, pageData);
    }
  }

  private IPointReader getAlignedPagePointReader() throws IOException {
    // construct time page reader
    ChunkHeader timeChunkHeader = chunkMetadataElement.chunk.getHeader();
    IUnCompressor timeUnCompressor =
        IUnCompressor.getUnCompressor(timeChunkHeader.getCompressionType());
    Decoder timeDecoder =
        Decoder.getDecoderByType(timeChunkHeader.getEncodingType(), timeChunkHeader.getDataType());
    ByteBuffer uncompressedTimePageData =
        uncompressPageData(pageHeader, timeUnCompressor, pageData);
    TimePageReader timePageReader =
        new TimePageReader(pageHeader, uncompressedTimePageData, timeDecoder);
    // release compressed time page data
    pageData = null;

    // construct value page readers
    List<ValuePageReader> valuePageReaders = new ArrayList<>(valuePageHeaders.size());
    for (int i = 0; i < valuePageHeaders.size(); i++) {
      if (valuePageHeaders.get(i) == null) {
        valuePageReaders.add(null);
      } else {
        // the data buffer of all chunk has been released
        Chunk valueChunk = chunkMetadataElement.valueChunks.get(i);
        ChunkHeader valueChunkHeader = valueChunk.getHeader();
        List<TimeRange> valueDeleteIntervalList = valueChunk.getDeleteIntervalList();
        TSDataType valueType = valueChunkHeader.getDataType();
        Decoder valuePageDecoder =
            Decoder.getDecoderByType(valueChunkHeader.getEncodingType(), valueType);
        ByteBuffer uncompressedPageData =
            uncompressPageData(
                valuePageHeaders.get(i),
                IUnCompressor.getUnCompressor(valueChunkHeader.getCompressionType()),
                valuePageDatas.get(i));
        ValuePageReader valuePageReader =
            new ValuePageReader(
                valuePageHeaders.get(i), uncompressedPageData, valueType, valuePageDecoder);
        valuePageReader.setDeleteIntervalList(valueDeleteIntervalList);
        valuePageReaders.add(valuePageReader);
        // release compressed value page data
        valuePageDatas.set(i, null);
      }
    }

    return new LazyLoadAlignedPagePointReader(timePageReader, valuePageReaders);
  }

  private ByteBuffer uncompressPageData(
      PageHeader header, IUnCompressor unCompressor, ByteBuffer compressedPageData)
      throws IOException {
    int compressedPageBodyLength = header.getCompressedSize();
    byte[] uncompressedPageData = new byte[header.getUncompressedSize()];
    try {
      unCompressor.uncompress(
          compressedPageData.array(), 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + header.getUncompressedSize()
              + "compressed size: "
              + header.getCompressedSize()
              + "page header: "
              + header
              + e.getMessage());
    }

    return ByteBuffer.wrap(uncompressedPageData);
  }
}
