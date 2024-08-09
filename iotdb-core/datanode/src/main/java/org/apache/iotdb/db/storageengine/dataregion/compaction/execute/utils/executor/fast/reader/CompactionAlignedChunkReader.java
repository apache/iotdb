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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.BatchedCompactionAlignedPagePointReader;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.page.AlignedPageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.read.reader.chunk.ChunkReader.uncompressPageData;

public class CompactionAlignedChunkReader {

  // chunk headers of all the sub sensors
  private final List<ChunkHeader> valueChunkHeaderList = new ArrayList<>();

  private final IUnCompressor timeUnCompressor;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  // A list of deleted intervals
  private final List<List<TimeRange>> valueDeleteIntervalList = new ArrayList<>();

  /**
   * Constructor of ChunkReader without deserializing chunk into page. This is used for fast
   * compaction.
   */
  public CompactionAlignedChunkReader(Chunk timeChunk, List<Chunk> valueChunkList) {
    ChunkHeader timeChunkHeader = timeChunk.getHeader();
    this.timeUnCompressor = IUnCompressor.getUnCompressor(timeChunkHeader.getCompressionType());

    valueChunkList.forEach(
        chunk -> {
          this.valueChunkHeaderList.add(chunk == null ? null : chunk.getHeader());
          this.valueDeleteIntervalList.add(chunk == null ? null : chunk.getDeleteIntervalList());
        });
  }

  /**
   * Read data from compressed page data. Uncompress the page and decode it to tsblock data.
   *
   * @throws IOException exception thrown when reading page data
   */
  public IPointReader getPagePointReader(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer compressedTimePageData,
      List<ByteBuffer> compressedValuePageDatas)
      throws IOException {
    AlignedPageReader alignedPageReader =
        getAlignedPageReader(
            timePageHeader, valuePageHeaders, compressedTimePageData, compressedValuePageDatas);
    return alignedPageReader.getLazyPointReader();
  }

  public IPointReader getBatchedPagePointReader(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer compressedTimePageData,
      List<ByteBuffer> compressedValuePageDatas)
      throws IOException {
    AlignedPageReader alignedPageReader =
        getAlignedPageReader(
            timePageHeader, valuePageHeaders, compressedTimePageData, compressedValuePageDatas);
    return new BatchedCompactionAlignedPagePointReader(
        alignedPageReader.getTimePageReader(), alignedPageReader.getValuePageReaderList());
  }

  public AlignedPageReader getAlignedPageReader(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer compressedTimePageData,
      List<ByteBuffer> compressedValuePageDatas)
      throws IOException {

    // uncompress time page data
    ByteBuffer uncompressedTimePageData =
        uncompressPageData(timePageHeader, timeUnCompressor, compressedTimePageData);
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
        ChunkHeader valueChunkHeader = valueChunkHeaderList.get(i);
        uncompressedValuePageDatas.add(
            uncompressPageData(
                valuePageHeaders.get(i),
                IUnCompressor.getUnCompressor(valueChunkHeader.getCompressionType()),
                compressedValuePageDatas.get(i)));
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
    return alignedPageReader;
  }
}
