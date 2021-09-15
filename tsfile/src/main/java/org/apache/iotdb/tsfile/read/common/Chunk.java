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

import org.apache.iotdb.tsfile.common.cache.Accountable;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** used in query. */
public class Chunk implements Accountable {

  private ChunkHeader chunkHeader;
  private Statistics chunkStatistic;
  private ByteBuffer chunkData;
  private boolean isFromOldFile = false;
  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private long ramSize;

  public Chunk(
      ChunkHeader header,
      ByteBuffer buffer,
      List<TimeRange> deleteIntervalList,
      Statistics chunkStatistic) {
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
    // from where the page data of the merged chunk starts, if -1, it means the merged chunk has
    // more than one page
    int offset1 = -1;
    // if the merged chunk has only one page, after merge with current chunk ,it will have more than
    // page
    // so we should add page statistics for it
    if (((byte) (chunk.chunkHeader.getChunkType() & 0x3F))
        == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      // read the uncompressedSize and compressedSize of this page
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunk.chunkData);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunk.chunkData);
      // record the position from which we can reuse
      offset1 = chunk.chunkData.position();
      chunk.chunkData.flip();
      // the actual size should add another page statistics size
      dataSize += (chunk.chunkData.array().length + chunk.chunkStatistic.getSerializedSize());
    } else {
      // if the merge chunk already has more than one page, we can reuse all the part of its data
      // the dataSize is equal to the before
      dataSize += chunk.chunkData.array().length;
    }
    // from where the page data of the current chunk starts, if -1, it means the current chunk has
    // more than one page
    int offset2 = -1;
    // if the current chunk has only one page, after merge with the merged chunk ,it will have more
    // than page
    // so we should add page statistics for it
    if (((byte) (chunkHeader.getChunkType() & 0x3F)) == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
      // change the chunk type
      chunkHeader.setChunkType(MetaMarker.CHUNK_HEADER);
      // read the uncompressedSize and compressedSize of this page
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunkData);
      ReadWriteForEncodingUtils.readUnsignedVarInt(chunkData);
      // record the position from which we can reuse
      offset2 = chunkData.position();
      chunkData.flip();
      // the actual size should add another page statistics size
      dataSize += (chunkData.array().length + chunkStatistic.getSerializedSize());
    } else {
      // if the current chunk already has more than one page, we can reuse all the part of its data
      // the dataSize is equal to the before
      dataSize += chunkData.array().length;
    }
    chunkHeader.setDataSize(dataSize);
    ByteBuffer newChunkData = ByteBuffer.allocate(dataSize);
    // the current chunk has more than one page, we can use its data part directly without any
    // changes
    if (offset2 == -1) {
      newChunkData.put(chunkData.array());
    } else { // the current chunk has only one page, we need to add one page statistics for it
      byte[] b = chunkData.array();
      // put the uncompressedSize and compressedSize of this page
      newChunkData.put(b, 0, offset2);
      // add page statistics
      PublicBAOS a = new PublicBAOS();
      chunkStatistic.serialize(a);
      newChunkData.put(a.getBuf(), 0, a.size());
      // put the remaining page data
      newChunkData.put(b, offset2, b.length - offset2);
    }
    // the merged chunk has more than one page, we can use its data part directly without any
    // changes
    if (offset1 == -1) {
      newChunkData.put(chunk.chunkData.array());
    } else {
      // put the uncompressedSize and compressedSize of this page
      byte[] b = chunk.chunkData.array();
      newChunkData.put(b, 0, offset1);
      // add page statistics
      PublicBAOS a = new PublicBAOS();
      chunk.chunkStatistic.serialize(a);
      newChunkData.put(a.getBuf(), 0, a.size());
      // put the remaining page data
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

  public boolean isFromOldFile() {
    return isFromOldFile;
  }

  public void setFromOldFile(boolean isFromOldFile) {
    this.isFromOldFile = isFromOldFile;
  }
}
