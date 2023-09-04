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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader;

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LazyChunkLoader {

  private TsFileSequenceReader reader;
  private ChunkMetadata chunkMetadata;
  private ChunkHeader chunkHeaderCache;

  public LazyChunkLoader(TsFileSequenceReader reader, ChunkMetadata chunkMetadata) {
    this.reader = reader;
    this.chunkMetadata = chunkMetadata;
  }

  public LazyChunkLoader() {}

  public ChunkHeader loadChunkHeader() throws IOException {
    if (reader == null) {
      return null;
    }
    if (chunkHeaderCache != null) {
      return chunkHeaderCache;
    }
    int chunkHeaderSize = ChunkHeader.getSerializedSize(chunkMetadata.getMeasurementUid());
    chunkHeaderCache =
        reader.readChunkHeader(chunkMetadata.getOffsetOfChunkHeader(), chunkHeaderSize);
    return chunkHeaderCache;
  }

  public Chunk loadChunk() throws IOException {
    if (reader == null) {
      return null;
    }
    if (chunkHeaderCache == null) {
      return reader.readMemChunk(chunkMetadata);
    }
    ByteBuffer buffer =
        reader.readChunk(
            chunkMetadata.getOffsetOfChunkHeader() + chunkHeaderCache.getSerializedSize(),
            chunkHeaderCache.getDataSize());
    return new Chunk(
        chunkHeaderCache,
        buffer,
        chunkMetadata.getDeleteIntervalList(),
        chunkMetadata.getStatistics());
  }

  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }

  public boolean isEmpty() {
    return reader == null;
  }
}
