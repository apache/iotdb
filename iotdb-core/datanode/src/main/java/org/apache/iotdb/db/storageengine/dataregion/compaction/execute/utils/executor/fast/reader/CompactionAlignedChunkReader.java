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

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.batch.utils.CompactionAlignedPageLazyLoadPointReader;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.read.reader.page.TimePageReader;
import org.apache.tsfile.read.reader.page.ValuePageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.read.reader.chunk.ChunkReader.decryptAndUncompressPageData;

public class CompactionAlignedChunkReader {

  // chunk headers of all the sub sensors
  private final List<ChunkHeader> valueChunkHeaderList = new ArrayList<>();

  private final IUnCompressor timeUnCompressor;

  private final EncryptParameter encryptParam;
  private final boolean ignoreAllNullRows;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  private final List<TimeRange> timeDeleteIntervalList;
  // A list of deleted intervals
  private final List<List<TimeRange>> valueDeleteIntervalList;

  /**
   * Constructor of ChunkReader without deserializing chunk into page. This is used for fast
   * compaction.
   */
  public CompactionAlignedChunkReader(
      Chunk timeChunk, List<Chunk> valueChunkList, boolean ignoreAllNullRows) {
    ChunkHeader timeChunkHeader = timeChunk.getHeader();
    this.timeUnCompressor = IUnCompressor.getUnCompressor(timeChunkHeader.getCompressionType());
    this.encryptParam = timeChunk.getEncryptParam();
    this.timeDeleteIntervalList = timeChunk.getDeleteIntervalList();
    this.valueDeleteIntervalList = new ArrayList<>(valueChunkList.size());

    valueChunkList.forEach(
        chunk -> {
          this.valueChunkHeaderList.add(chunk == null ? null : chunk.getHeader());
          this.valueDeleteIntervalList.add(chunk == null ? null : chunk.getDeleteIntervalList());
        });
    this.ignoreAllNullRows = ignoreAllNullRows;
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
    return getPontReader(
        timePageHeader,
        valuePageHeaders,
        compressedTimePageData,
        compressedValuePageDatas,
        ignoreAllNullRows);
  }

  public IPointReader getBatchedPagePointReader(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer compressedTimePageData,
      List<ByteBuffer> compressedValuePageDatas)
      throws IOException {
    return getPontReader(
        timePageHeader, valuePageHeaders, compressedTimePageData, compressedValuePageDatas, false);
  }

  private IPointReader getPontReader(
      PageHeader timePageHeader,
      List<PageHeader> valuePageHeaders,
      ByteBuffer compressedTimePageData,
      List<ByteBuffer> compressedValuePageDatas,
      boolean ignoreAllNullRows)
      throws IOException {

    // decrypt and uncompress time page data
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer uncompressedTimePageData =
        decryptAndUncompressPageData(
            timePageHeader, timeUnCompressor, compressedTimePageData, decryptor);
    TimePageReader timePageReader =
        new TimePageReader(timePageHeader, uncompressedTimePageData, timeDecoder);
    timePageReader.setDeleteIntervalList(timeDeleteIntervalList);

    // uncompress value page datas
    List<ValuePageReader> valuePageReaders = new ArrayList<>(valuePageHeaders.size());
    for (int i = 0; i < valuePageHeaders.size(); i++) {
      if (valuePageHeaders.get(i) == null) {
        valuePageReaders.add(null);
      } else {
        ChunkHeader valueChunkHeader = valueChunkHeaderList.get(i);
        ByteBuffer uncompressedPageData =
            decryptAndUncompressPageData(
                valuePageHeaders.get(i),
                IUnCompressor.getUnCompressor(valueChunkHeader.getCompressionType()),
                compressedValuePageDatas.get(i),
                decryptor);
        TSDataType valueType = valueChunkHeader.getDataType();
        ValuePageReader valuePageReader =
            new ValuePageReader(
                valuePageHeaders.get(i),
                uncompressedPageData,
                valueType,
                Decoder.getDecoderByType(valueChunkHeader.getEncodingType(), valueType));
        valuePageReader.setDeleteIntervalList(valueDeleteIntervalList.get(i));
        valuePageReaders.add(valuePageReader);
      }
    }

    return new CompactionAlignedPageLazyLoadPointReader(
        timePageReader, valuePageReaders, ignoreAllNullRows);
  }
}
