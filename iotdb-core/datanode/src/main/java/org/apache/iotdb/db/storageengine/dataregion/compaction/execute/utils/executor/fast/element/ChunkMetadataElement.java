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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.fast.element;

import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.MergeReaderPriority;

import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.read.common.Chunk;

import java.util.List;

@SuppressWarnings("squid:S1104")
public class ChunkMetadataElement {
  public IChunkMetadata chunkMetadata;

  public long startTime;

  public boolean isLastChunk;

  public FileElement fileElement;

  public Chunk chunk;

  public List<Chunk> valueChunks;

  public boolean needForceDecodingPage;

  public ChunkMetadataElement(
      IChunkMetadata chunkMetadata, boolean isLastChunk, FileElement fileElement) {
    this.chunkMetadata = chunkMetadata;
    this.startTime = chunkMetadata.getStartTime();
    this.isLastChunk = isLastChunk;
    this.fileElement = fileElement;
  }

  public void clearChunks() {
    chunk = null;
    valueChunks = null;
  }

  public MergeReaderPriority getPriority() {
    return this.fileElement.getPriority();
  }
}
