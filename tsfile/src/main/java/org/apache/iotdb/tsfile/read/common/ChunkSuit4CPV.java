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

package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;

import java.util.ArrayList;
import java.util.List;

public class ChunkSuit4CPV {

  private ChunkMetadata
      chunkMetadata; // this.version = new MergeReaderPriority(chunkMetadata.getVersion(),
  // chunkMetadata.getOffsetOfChunkHeader());
  private BatchData batchData;
  private List<Long> mergeVersionList = new ArrayList<>();
  private List<Long> mergeOffsetList = new ArrayList<>();

  public ChunkSuit4CPV(ChunkMetadata chunkMetadata) {
    this.chunkMetadata = chunkMetadata;
    this.batchData = null;
  }

  public ChunkSuit4CPV(ChunkMetadata chunkMetadata, BatchData batchData) {
    this.chunkMetadata = chunkMetadata;
    this.batchData = batchData;
  }

  public ChunkMetadata getChunkMetadata() {
    return chunkMetadata;
  }

  public BatchData getBatchData() {
    return batchData;
  }

  public void setBatchData(BatchData batchData) {
    this.batchData = batchData;
  }

  public void setChunkMetadata(ChunkMetadata chunkMetadata) {
    this.chunkMetadata = chunkMetadata;
  }

  public void addMergeVersionList(long version) {
    this.mergeVersionList.add(version);
  }

  public void addMergeOffsetList(long offset) {
    this.mergeOffsetList.add(offset);
  }

  public List<Long> getMergeVersionList() {
    return mergeVersionList;
  }

  public List<Long> getMergeOffsetList() {
    return mergeOffsetList;
  }

  public long getVersion() {
    return this.getChunkMetadata().getVersion();
  }

  public long getOffset() {
    return this.getChunkMetadata().getOffsetOfChunkHeader();
  }
}
