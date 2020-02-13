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

import java.io.IOException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Chunk;

/**
 * Read one Chunk and cache it into a LRUCache.
 */
public class ChunkLoaderImpl implements IChunkLoader {

  private TsFileSequenceReader reader;

  /**
   * constructor of ChunkLoaderImpl.
   *
   * @param fileSequenceReader file sequence reader
   */
  public ChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
    this.reader = fileSequenceReader;
  }

  @Override
  public Chunk getChunk(ChunkMetaData chunkMetaData) throws IOException {
    Chunk chunk = reader.readMemChunk(chunkMetaData);
    return new Chunk(chunk.getHeader(), chunk.getData().duplicate(), chunk.getDeletedAt(),
        reader.getEndianType());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
