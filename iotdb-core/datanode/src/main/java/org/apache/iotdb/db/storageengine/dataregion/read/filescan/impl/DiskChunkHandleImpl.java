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

import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.read.filescan.IChunkHandle;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
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
  private final boolean tsFileClosed;
  private final IDeviceID deviceID;
  private final String measurement;
  private final String filePath;
  private EncryptParameter encryptParam;
  protected ChunkHeader currentChunkHeader;
  protected PageHeader currentPageHeader;
  protected ByteBuffer currentChunkDataBuffer;
  protected long offset;

  private final Decoder defaultTimeDecoder =
      Decoder.getDecoderByType(
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
          TSDataType.INT64);

  // Page will reuse chunkStatistics if there is only one page in chunk
  protected final Statistics<? extends Serializable> chunkStatistic;

  public DiskChunkHandleImpl(
      IDeviceID deviceID,
      String measurement,
      String filePath,
      boolean isTsFileClosed,
      long offset,
      Statistics<? extends Serializable> chunkStatistics) {
    this.deviceID = deviceID;
    this.measurement = measurement;
    this.chunkStatistic = chunkStatistics;
    this.offset = offset;
    this.filePath = filePath;
    this.tsFileClosed = isTsFileClosed;
  }

  protected void init(TsFileSequenceReader reader) throws IOException {
    if (currentChunkDataBuffer != null) {
      return;
    }
    Chunk chunk = reader.readMemChunk(offset);
    this.currentChunkDataBuffer = chunk.getData();
    this.currentChunkHeader = chunk.getHeader();
    this.encryptParam = chunk.getEncryptParam();
  }

  // Check if there is more pages to be scanned in Chunk.
  // If currentChunkDataBuffer is equals to null, it means nextPage() is not called and needed
  @Override
  public boolean hasNextPage() throws IOException {
    return currentChunkDataBuffer == null || currentChunkDataBuffer.hasRemaining();
  }

  @Override
  public void nextPage() throws IOException {
    // read chunk from disk if needed
    if (currentChunkDataBuffer == null) {
      TsFileSequenceReader reader = FileReaderManager.getInstance().get(filePath, tsFileClosed);
      init(reader);
    }
    if (currentChunkDataBuffer.hasRemaining()) {
      // If there is only one page, page statistics is not stored in the chunk header, which is the
      // same as chunkStatistics
      if ((byte) (this.currentChunkHeader.getChunkType() & 0x3F)
          == MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER) {
        currentPageHeader =
            PageHeader.deserializeFrom(this.currentChunkDataBuffer, this.chunkStatistic);
      } else {
        currentPageHeader =
            PageHeader.deserializeFrom(
                this.currentChunkDataBuffer, this.currentChunkHeader.getDataType());
      }
    }
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
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    ByteBuffer currentPageDataBuffer =
        ChunkReader.deserializePageData(
            currentPageHeader, this.currentChunkDataBuffer, this.currentChunkHeader, decryptor);
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(currentPageDataBuffer);
    ByteBuffer timeBuffer = currentPageDataBuffer.slice();
    timeBuffer.limit(timeBufferLength);

    return convertToTimeArray(timeBuffer);
  }

  @Override
  public IDeviceID getDeviceID() {
    return deviceID;
  }

  @Override
  public String getMeasurement() {
    return measurement;
  }

  private long[] convertToTimeArray(ByteBuffer timeBuffer) throws IOException {
    long[] timeArray = new long[(int) currentPageHeader.getNumOfValues()];
    int index = 0;
    while (defaultTimeDecoder.hasNext(timeBuffer)) {
      timeArray[index++] = defaultTimeDecoder.readLong(timeBuffer);
    }
    return timeArray;
  }
}
