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
package org.apache.iotdb.db.query.reader.chunk;

import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.query.externalsort.adapter.ByTimestampReaderAdapter;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.common.Chunk;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReader;
import org.apache.iotdb.tsfile.read.reader.chunk.ChunkReaderByTimestamp;

import java.io.IOException;

public class ChunkReaderWrap {

  private ChunkReaderType type;
  private Filter filter;

  // attributes for disk chunk
  private ChunkMetadata chunkMetaData;
  private IChunkLoader chunkLoader;

  // attributes for mem chunk
  private ReadOnlyMemChunk readOnlyMemChunk;

  /** This is used in test. */
  protected ChunkReaderWrap() {}

  /** constructor of diskChunkReader */
  public ChunkReaderWrap(ChunkMetadata metaData, IChunkLoader chunkLoader, Filter filter) {
    this.type = ChunkReaderType.DISK_CHUNK;
    this.chunkMetaData = metaData;
    this.chunkLoader = chunkLoader;
    this.filter = filter;
  }

  /** constructor of MemChunkReader */
  public ChunkReaderWrap(ReadOnlyMemChunk readOnlyMemChunk, Filter filter) {
    type = ChunkReaderType.MEM_CHUNK;
    this.readOnlyMemChunk = readOnlyMemChunk;
    this.filter = filter;
  }

  public IPointReader getIPointReader() throws IOException {
    if (type.equals(ChunkReaderType.DISK_CHUNK)) {
      Chunk chunk = chunkLoader.loadChunk(chunkMetaData);
      ChunkReader chunkReader = new ChunkReader(chunk, filter);
      return new ChunkDataIterator(chunkReader);
    } else {
      return new MemChunkReader(readOnlyMemChunk, filter);
    }
  }

  public IReaderByTimestamp getIReaderByTimestamp() throws IOException {
    if (type.equals(ChunkReaderType.DISK_CHUNK)) {
      Chunk chunk = chunkLoader.loadChunk(chunkMetaData);
      ChunkReaderByTimestamp chunkReader = new ChunkReaderByTimestamp(chunk);
      return new DiskChunkReaderByTimestamp(chunkReader);
    } else {
      return new ByTimestampReaderAdapter(readOnlyMemChunk.getPointReader());
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
    DISK_CHUNK,
    MEM_CHUNK
  }
}
