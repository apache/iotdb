/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.tsfile.read.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.common.util.ChunkProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiskChunk extends Chunk {

  private static final Logger logger = LoggerFactory.getLogger(DiskChunk.class);

  private boolean onDisk = true;
  private ChunkMetaData metaData;
  private ChunkProvider provider;

  public DiskChunk(ChunkMetaData metaData,
      ChunkProvider provider) {
    this.metaData = metaData;
    this.provider = provider;
  }

  private void load() {
    if (onDisk) {
      Chunk chunk;
      try {
        chunk = provider.require(metaData);
        this.chunkHeader = chunk.chunkHeader;
        this.chunkData = chunk.chunkData;
        onDisk = false;
      } catch (InterruptedException | IOException e) {
        logger.error("Cannot load a chunk, metadata: {}", metaData, e);
      }
    }
  }

  @Override
  public ChunkHeader getHeader() {

    return super.getHeader();
  }

  @Override
  public ByteBuffer getData() {
    return super.getData();
  }

  @Override
  public long getDeletedAt() {
    return super.getDeletedAt();
  }
}