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

import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.read.common.Chunk;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InstantChunkLoader extends ChunkLoader {

  private Chunk chunk;

  public InstantChunkLoader() {}

  public InstantChunkLoader(String fileName, ChunkMetadata chunkMetadata, Chunk chunk) {
    super(fileName, chunkMetadata);
    this.chunk = chunk;
  }

  @Override
  public Chunk getChunk() {
    return chunk;
  }

  @Override
  public boolean isEmpty() {
    return chunkMetadata == null
        || chunk == null
        || chunkMetadata.getNumOfPoints() == 0
        || this.modifiedStatus == ModifiedStatus.ALL_DELETED;
  }

  @Override
  public ChunkHeader getHeader() {
    if (chunk == null) {
      return null;
    }
    return chunk.getHeader();
  }

  @Override
  public List<PageLoader> getPages() {
    if (chunk == null) {
      return Collections.emptyList();
    }

    ByteBuffer chunkData = chunk.getData();
    List<PageLoader> pageList = new ArrayList<>();
    while (chunkData.hasRemaining()) {
      PageHeader pageHeader;
      if (((byte) (chunk.getHeader().getChunkType() & 0x3F))
          == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        pageHeader = PageHeader.deserializeFrom(chunkData, chunk.getChunkStatistic());
      } else {
        pageHeader = PageHeader.deserializeFrom(chunkData, chunk.getChunkStatistic().getType());
      }
      int pageSize = pageHeader.getCompressedSize();
      ByteBuffer pageBodyBuffer = getPageBodyBuffer(chunkData, pageSize);
      ModifiedStatus pageModifiedStatus = calculatePageModifiedStatus(pageHeader);
      pageHeader.setModified(pageModifiedStatus != ModifiedStatus.NONE_DELETED);
      pageList.add(
          new InstantPageLoader(
              file,
              pageHeader,
              pageBodyBuffer,
              chunk.getHeader().getCompressionType(),
              chunk.getHeader().getDataType(),
              chunk.getHeader().getEncodingType(),
              chunkMetadata,
              pageModifiedStatus,
              chunk.getEncryptParam()));
    }
    return pageList;
  }

  private ByteBuffer getPageBodyBuffer(ByteBuffer chunkDataBuffer, int pageSize) {
    ByteBuffer pageBodyBuffer = chunkDataBuffer.slice();
    pageBodyBuffer.limit(pageSize);
    chunkDataBuffer.position(chunkDataBuffer.position() + pageSize);
    return pageBodyBuffer;
  }

  @Override
  public void clear() {
    this.chunk = null;
    this.chunkMetadata = null;
  }
}
