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

package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;

import java.util.List;

/**
 * 读取 ChunkGroup 下的 Chunk 时，需要更新的信息
 *
 * @author shenguanchu
 */
public class ChunkGroupMetaInfo {
  private byte marker;
  private List<IChunkMetadata> chunkMetadataList;
  private List<ChunkHeader> chunkHeaderList;
  private List<long[]> timeBatch;
  private IChunkMetadata alignedTimeChunkMetadata;
  private List<IChunkMetadata> alignedValueChunkMetadata;

  public ChunkGroupMetaInfo() {}

  public ChunkGroupMetaInfo(
      byte marker,
      List<IChunkMetadata> chunkMetadataList,
      List<ChunkHeader> chunkHeaderList,
      List<long[]> timeBatch,
      IChunkMetadata alignedTimeChunkMetadata,
      List<IChunkMetadata> alignedValueChunkMetadata) {
    this.marker = marker;
    this.chunkMetadataList = chunkMetadataList;
    this.chunkHeaderList = chunkHeaderList;
    this.timeBatch = timeBatch;
    this.alignedTimeChunkMetadata = alignedTimeChunkMetadata;
    this.alignedValueChunkMetadata = alignedValueChunkMetadata;
  }

  public byte getMarker() {
    return marker;
  }

  public void setMarker(byte marker) {
    this.marker = marker;
  }

  public List<IChunkMetadata> getChunkMetadataList() {
    return chunkMetadataList;
  }

  public void setChunkMetadataList(List<IChunkMetadata> chunkMetadataList) {
    this.chunkMetadataList = chunkMetadataList;
  }

  public List<ChunkHeader> getChunkHeaderList() {
    return chunkHeaderList;
  }

  public void setChunkHeaderList(List<ChunkHeader> chunkHeaderList) {
    this.chunkHeaderList = chunkHeaderList;
  }

  public List<long[]> getTimeBatch() {
    return timeBatch;
  }

  public void setTimeBatch(List<long[]> timeBatch) {
    this.timeBatch = timeBatch;
  }

  public IChunkMetadata getAlignedTimeChunkMetadata() {
    return alignedTimeChunkMetadata;
  }

  public void setAlignedTimeChunkMetadata(IChunkMetadata alignedTimeChunkMetadata) {
    this.alignedTimeChunkMetadata = alignedTimeChunkMetadata;
  }

  public List<IChunkMetadata> getAlignedValueChunkMetadata() {
    return alignedValueChunkMetadata;
  }

  public void setAlignedValueChunkMetadata(List<IChunkMetadata> alignedValueChunkMetadata) {
    this.alignedValueChunkMetadata = alignedValueChunkMetadata;
  }
}
