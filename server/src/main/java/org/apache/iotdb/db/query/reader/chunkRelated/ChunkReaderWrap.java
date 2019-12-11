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
package org.apache.iotdb.db.query.reader.chunkRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.reader.chunk.AbstractChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;

public class ChunkReaderWrap {

  private ChunkReaderType type;
  private Filter filter;

  // attributes for disk chunk
  private ChunkMetaData chunkMetaData;
  private IChunkLoader chunkLoader;

  // attributes for mem chunk
  private ReadOnlyMemChunk readOnlyMemChunk;

  /**
   * This is used in test.
   */
  protected ChunkReaderWrap() {

  }

  /**
   * constructor of diskChunkReader
   */
  public ChunkReaderWrap(ChunkMetaData metaData, IChunkLoader chunkLoader, Filter filter) {
    this.type = ChunkReaderType.DISK_CHUNK;
    this.chunkMetaData = metaData;
    this.chunkLoader = chunkLoader;
    this.filter = filter;
  }

  /**
   * constructor of MemChunkReader
   */
  public ChunkReaderWrap(ReadOnlyMemChunk readOnlyMemChunk, Filter filter) {
    type = ChunkReaderType.MEM_CHUNK;
    this.readOnlyMemChunk = readOnlyMemChunk;
    this.filter = filter;
  }

  public IPointReader getIPointReader() throws IOException {
    if (type.equals(ChunkReaderType.DISK_CHUNK)) {
      Chunk chunk = chunkLoader.getChunk(chunkMetaData);
      AbstractChunkReader AbstractChunkReader = new ChunkReader(chunk, filter);
      return new DiskChunkReader(AbstractChunkReader);
    } else {
      return new MemChunkReader(readOnlyMemChunk, filter);
    }
  }

  public IBatchReader getIBatchReader() throws IOException {
    if (type.equals(ChunkReaderType.DISK_CHUNK)) {
      Chunk chunk = chunkLoader.getChunk(chunkMetaData);
      AbstractChunkReader AbstractChunkReader = new ChunkReader(chunk, filter);
      return new DiskChunkReader(AbstractChunkReader);
    } else {
      return new MemChunkReader(readOnlyMemChunk, filter);
    }
  }

  public IReaderByTimestamp getIReaderByTimestamp() throws IOException {
    if (type.equals(ChunkReaderType.DISK_CHUNK)) {
      Chunk chunk = chunkLoader.getChunk(chunkMetaData);
      ChunkReaderByTimestamp chunkReader = new ChunkReaderByTimestamp(chunk);
      return new DiskChunkReaderByTimestamp(chunkReader);
    } else {
      return new MemChunkReaderByTimestamp(readOnlyMemChunk);
    }
  }

  public String getMeasurementUid() {
    if (chunkMetaData != null) {
      return chunkMetaData.getMeasurementUid();
    } else {
      return null;
    }
  }

  enum ChunkReaderType {
    DISK_CHUNK, MEM_CHUNK
  }

}
