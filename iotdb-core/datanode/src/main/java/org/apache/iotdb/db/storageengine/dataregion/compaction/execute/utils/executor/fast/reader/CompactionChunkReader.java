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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader.readCompressedPageData;
import static org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader.uncompressPageData;

public class CompactionChunkReader {

  private final ChunkHeader chunkHeader;
  private final ByteBuffer chunkDataBuffer;
  private final IUnCompressor unCompressor;
  private final Decoder timeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

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

  /**
   * Read data from compressed page data. Uncompress the page and decode it to batch data.
   *
   * @param compressedPageData Compressed page data
   * @throws IOException exception thrown when reading page data
   */
  public TsBlock readPageData(PageHeader pageHeader, ByteBuffer compressedPageData)
      throws IOException {
    // uncompress page data
    ByteBuffer pageData = uncompressPageData(pageHeader, unCompressor, compressedPageData);

    // decode page data
    TSDataType dataType = chunkHeader.getDataType();
    Decoder valueDecoder = Decoder.getDecoderByType(chunkHeader.getEncodingType(), dataType);
    PageReader pageReader =
        new PageReader(pageHeader, pageData, dataType, valueDecoder, timeDecoder, null);
    pageReader.setDeleteIntervalList(deleteIntervalList);
    return pageReader.getAllSatisfiedData();
  }
}
