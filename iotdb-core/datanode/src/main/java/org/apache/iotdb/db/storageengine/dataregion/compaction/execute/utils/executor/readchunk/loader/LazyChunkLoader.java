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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LazyChunkLoader extends ChunkLoader {
  private CompactionTsFileReader reader;
  private Chunk chunk;
  private ChunkHeader chunkHeader;

  public LazyChunkLoader(CompactionTsFileReader reader, ChunkMetadata chunkMetadata) {
    super(chunkMetadata);
    this.reader = reader;
  }

  public LazyChunkLoader() {}

  @Override
  public Chunk getChunk() throws IOException {
    if (reader == null) {
      return null;
    }
    if (chunkHeader == null) {
      this.chunk = reader.readMemChunk(chunkMetadata);
      this.chunkHeader = this.chunk.getHeader();
      return this.chunk;
    }
    ByteBuffer buffer =
        reader.readChunk(
            chunkMetadata.getOffsetOfChunkHeader() + chunkHeader.getSerializedSize(),
            chunkHeader.getDataSize());
    this.chunk =
        new Chunk(
            chunkHeader,
            buffer,
            chunkMetadata.getDeleteIntervalList(),
            chunkMetadata.getStatistics());
    return this.chunk;
  }

  @Override
  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }

  @Override
  public boolean isEmpty() {
    return reader == null || chunkMetadata == null || chunkMetadata.getStatistics().getCount() == 0;
  }

  @Override
  public ChunkHeader getHeader() throws IOException {
    if (isEmpty()) {
      return null;
    }
    if (chunkHeader != null) {
      return chunkHeader;
    }
    chunkHeader = reader.readChunkHeader(chunkMetadata.getOffsetOfChunkHeader());
    return chunkHeader;
  }

  @Override
  public ModifiedStatus getModifiedStatus() {
    return this.modifiedStatus;
  }

  @Override
  public List<PageLoader> getPages() throws IOException {
    if (getChunk() == null) {
      return Collections.emptyList();
    }
    long chunkDataStartOffset =
        chunkMetadata.getOffsetOfChunkHeader() + chunkHeader.getSerializedSize();
    long chunkEndOffset = chunkDataStartOffset + chunkHeader.getDataSize();
    long index = chunkDataStartOffset;
    boolean hasPageStatistics =
        ((byte) (chunkHeader.getChunkType() & 0x3F)) != MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER;
    List<PageLoader> pageLoaders = new ArrayList<>();
    reader.position(index);
    InputStream inputStream = reader.wrapAsInputStream();
    while (index < chunkEndOffset) {
      PageHeader pageHeader;
      if (hasPageStatistics) {
        pageHeader = PageHeader.deserializeFrom(inputStream, chunkHeader.getDataType(), true);
      } else {
        pageHeader = PageHeader.deserializeFrom(inputStream, chunkMetadata.getStatistics());
      }
      int serializedPageSize = pageHeader.getSerializedPageSize();
      int headerSize = serializedPageSize - pageHeader.getCompressedSize();
      ModifiedStatus pageModifiedStatus = calculatePageModifiedStatus(pageHeader);
      pageLoaders.add(
          new LazyPageLoader(
              reader,
              pageHeader,
              index + headerSize,
              chunk.getHeader().getCompressionType(),
              chunk.getHeader().getDataType(),
              chunk.getHeader().getEncodingType(),
              chunkMetadata.getDeleteIntervalList(),
              pageModifiedStatus));
      index += serializedPageSize;
      inputStream.skip(pageHeader.getCompressedSize());
    }
    return pageLoaders;
  }

  @Override
  public void clear() {
    this.chunkHeader = null;
    this.chunkMetadata = null;
    this.chunk = null;
  }
}
