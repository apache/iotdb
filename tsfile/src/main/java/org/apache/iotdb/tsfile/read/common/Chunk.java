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

import java.nio.ByteBuffer;

import java.util.List;
import org.apache.iotdb.tsfile.common.cache.Accountable;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;

/**
 * used in query.
 */
public class Chunk implements Accountable {

  private ChunkHeader chunkHeader;
  private ByteBuffer chunkData;
  /**
   * A list of deleted intervals.
   */
  private List<TimeRange> deleteIntervalList;

  private long ramSize;

  public Chunk(ChunkHeader header, ByteBuffer buffer, List<TimeRange> deleteIntervalList) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.deleteIntervalList = deleteIntervalList;
  }

  public ChunkHeader getHeader() {
    return chunkHeader;
  }

  public ByteBuffer getData() {
    return chunkData;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public void mergeChunk(Chunk chunk) {
    chunkHeader.mergeChunkHeader(chunk.chunkHeader);
    ByteBuffer newChunkData = ByteBuffer
        .allocate(chunkData.array().length + chunk.chunkData.array().length);
    newChunkData.put(chunkData.array());
    newChunkData.put(chunk.chunkData.array());
    chunkData = newChunkData;
  }

  @Override
  public void setRamSize(long size) {
    this.ramSize = size;
  }

  @Override
  public long getRamSize() {
    return ramSize;
  }
}
