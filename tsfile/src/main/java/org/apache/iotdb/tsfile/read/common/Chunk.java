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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iotdb.tsfile.common.cache.Accountable;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

/**
 * used in query.
 */
public class Chunk implements Accountable {

  private ChunkHeader chunkHeader;
  private Statistics chunkStatistic;
  private ByteBuffer chunkData;
  /**
   * A list of deleted intervals.
   */
  private List<TimeRange> deleteIntervalList;

  private long ramSize;

  public Chunk(ChunkHeader header, ByteBuffer buffer, List<TimeRange> deleteIntervalList, Statistics chunkStatistic) {
    this.chunkHeader = header;
    this.chunkData = buffer;
    this.deleteIntervalList = deleteIntervalList;
    this.chunkStatistic = chunkStatistic;
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

  public void mergeChunk(Chunk chunk) throws IOException {
    int dataSize = 0;
    int offset1 = -1;
    if (chunk.chunkHeader.getChunkType() == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunk.chunkData);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunk.chunkData);
      offset1 = chunk.chunkData.position();
      chunk.chunkData.flip();
      dataSize += (chunk.chunkData.array().length + chunk.chunkStatistic.getSerializedSize());
    } else {
      dataSize += chunk.chunkData.array().length;
    }
    int offset2 = -1;
    if (chunkHeader.getChunkType() == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      chunkHeader.setChunkType(MetaMarker.CHUNK_HEADER);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunkData);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunkData);
      offset2 = chunkData.position();
      chunkData.flip();
      dataSize += (chunkData.array().length + chunkStatistic.getSerializedSize());
    } else {
      dataSize += chunkData.array().length;
    }
    chunkHeader.setDataSize(dataSize);
    ByteBuffer newChunkData = ByteBuffer.allocate(dataSize);
    if (offset2 == -1) {
      newChunkData.put(chunkData.array());
    } else {
      byte[] b = chunkData.array();
      newChunkData.put(b, 0, offset2);
      PublicBAOS a = new PublicBAOS();
      chunkStatistic.serialize(a);
      newChunkData.put(a.getBuf(), 0, a.size());
      newChunkData.put(b, offset2, b.length - offset2);
    }
    if (offset1 == -1) {
      newChunkData.put(chunk.chunkData.array());
    } else {
      byte[] b = chunk.chunkData.array();
      newChunkData.put(b, 0, offset1);
      PublicBAOS a = new PublicBAOS();
      chunk.chunkStatistic.serialize(a);
      newChunkData.put(a.getBuf(), 0, a.size());
      newChunkData.put(b, offset1, b.length - offset1);
    }
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

  public Statistics getChunkStatistic() {
    return chunkStatistic;
  }
}
