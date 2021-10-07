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
package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.common.cache.LRUCache;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

import java.io.IOException;

/** Read one Chunk and cache it into a LRUCache, only used in tsfile module. */
public class CachedChunkLoaderImpl implements IChunkLoader {

  private static final int DEFAULT_CHUNK_CACHE_SIZE = 1000;
  private TsFileSequenceReader reader;
  private LRUCache<ChunkMetadata, Chunk> chunkCache;

  public CachedChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
    this(fileSequenceReader, DEFAULT_CHUNK_CACHE_SIZE);
  }

  /**
   * constructor of ChunkLoaderImpl.
   *
   * @param fileSequenceReader file sequence reader
   * @param cacheSize cache size
   */
  public CachedChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {

    this.reader = fileSequenceReader;

    chunkCache =
        new LRUCache<ChunkMetadata, Chunk>(cacheSize) {

          @Override
          public Chunk loadObjectByKey(ChunkMetadata metaData) throws IOException {
            return reader.readMemChunk(metaData);
          }
        };
  }

  @Override
  public Chunk loadChunk(ChunkMetadata chunkMetaData) throws IOException {
    chunkMetaData.setFilePath(reader.getFileName());
    Chunk chunk = chunkCache.get(chunkMetaData);
    return new Chunk(
        chunk.getHeader(),
        chunk.getData().duplicate(),
        chunkMetaData.getDeleteIntervalList(),
        chunkMetaData.getStatistics());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
