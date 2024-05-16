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

package org.apache.iotdb.db.storageengine.dataregion.read.filescan.impl;

import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;

import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.ChunkReader;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/** It will receive a list of offset and execute sequential scan of TsFile for chunkData. */
public class DiskChunkHandleImpl implements IChunkHandle {

  protected ChunkHeader currentChunkHeader;
  protected PageHeader currentPageHeader;
  protected ByteBuffer currentChunkDataBuffer;
  protected TsFileSequenceReader reader;
  protected long offset;

  // Page will reuse chunkStatistics if there is only one page in chunk
  protected final Statistics<? extends Serializable> chunkStatistic;

  public DiskChunkHandleImpl(
      TsFileSequenceReader reader,
      long offset,
      Statistics<? extends Serializable> chunkStatistics) {
    this.chunkStatistic = chunkStatistics;
    this.reader = reader;
    this.offset = offset;
  }

  protected void init() throws IOException {
    if (currentChunkDataBuffer != null) {
      return;
    }
    Chunk chunk = reader.readMemChunk(offset);
    this.currentChunkDataBuffer = chunk.getData();
    this.currentChunkHeader = chunk.getHeader();
  }

  // Check if there is more pages to be scanned in Chunk.
  // If so, deserialize the page header
  @Override
  public boolean hasNextPage() throws IOException {
    // read chunk from disk if needed
    init();
    if (!currentChunkDataBuffer.hasRemaining()) {
      return false;
    }
    // If there is only one page, page statistics is not stored in the chunk header, which is the
    // same as chunkStatistics
    if ((byte) (this.currentChunkHeader.getChunkType() & 63) == 5) {
      currentPageHeader =
          PageHeader.deserializeFrom(this.currentChunkDataBuffer, this.chunkStatistic);
    } else {
      currentPageHeader =
          PageHeader.deserializeFrom(
              this.currentChunkDataBuffer, this.currentChunkHeader.getDataType());
    }
    return true;
  }

  @Override
  public void skipCurrentPage() {
    currentChunkDataBuffer.position(
        currentChunkDataBuffer.position() + currentPageHeader.getCompressedSize());
  }

  @Override
  public long[] getPageStatisticsTime() {
    return new long[] {currentPageHeader.getStartTime(), currentPageHeader.getEndTime()};
  }

  @Override
  public long[] getDataTime() throws IOException {
    ByteBuffer currentPageDataBuffer =
        ChunkReader.deserializePageData(
            currentPageHeader, this.currentChunkDataBuffer, this.currentChunkHeader);
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(currentPageDataBuffer);
    ByteBuffer timeBuffer = currentPageDataBuffer.slice();
    timeBuffer.limit(timeBufferLength);

    return convertToTimeArray(timeBuffer, timeBufferLength);
  }

  private long[] convertToTimeArray(ByteBuffer timeBuffer, int timeBufferLength) {
    long[] timeArray = new long[timeBufferLength / 8];
    for (int i = 0; i < timeArray.length; i++) {
      timeArray[i] = timeBuffer.getLong();
    }
    return timeArray;
  }
}
