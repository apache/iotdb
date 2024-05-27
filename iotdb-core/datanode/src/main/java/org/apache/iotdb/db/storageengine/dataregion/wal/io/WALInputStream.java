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
package org.apache.iotdb.db.storageengine.dataregion.wal.io;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class WALInputStream extends InputStream implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(WALInputStream.class);
  private final FileChannel channel;
  private final ByteBuffer headerBuffer = ByteBuffer.allocate(Integer.BYTES + 1);
  private final ByteBuffer compressedHeader = ByteBuffer.allocate(Integer.BYTES);
  private ByteBuffer dataBuffer = null;
  private long fileSize;
  File logFile;
  private long endOffset = -1;

  enum FileVersion {
    V1,
    V2,
    UNKNOWN
  };

  FileVersion version;

  public WALInputStream(File logFile) throws IOException {
    channel = FileChannel.open(logFile.toPath());
    fileSize = channel.size();
    analyzeFileVersion();
    getEndOffset();
    this.logFile = logFile;
  }

  private void getEndOffset() throws IOException {
    if (channel.size() < WALWriter.MAGIC_STRING_BYTES + Integer.BYTES) {
      endOffset = channel.size();
      return;
    }
    ByteBuffer metadataSizeBuf = ByteBuffer.allocate(Integer.BYTES);
    long position;
    try {
      if (version == FileVersion.V2) {
        ByteBuffer magicStringBuffer = ByteBuffer.allocate(WALWriter.MAGIC_STRING_BYTES);
        channel.read(magicStringBuffer, channel.size() - WALWriter.MAGIC_STRING_BYTES);
        magicStringBuffer.flip();
        if (!new String(magicStringBuffer.array()).equals(WALWriter.MAGIC_STRING)) {
          // this is a broken wal file
          endOffset = channel.size();
          return;
        }
        position = channel.size() - WALWriter.MAGIC_STRING_BYTES - Integer.BYTES;
      } else {
        ByteBuffer magicStringBuffer =
            ByteBuffer.allocate(WALWriter.MAGIC_STRING_V1.getBytes().length);
        channel.read(
            magicStringBuffer, channel.size() - WALWriter.MAGIC_STRING_V1.getBytes().length);
        magicStringBuffer.flip();
        if (!new String(magicStringBuffer.array()).equals(WALWriter.MAGIC_STRING_V1)) {
          // this is a broken wal file
          endOffset = channel.size();
          return;
        }
        position = channel.size() - WALWriter.MAGIC_STRING_V1.getBytes().length - Integer.BYTES;
      }
      channel.read(metadataSizeBuf, position);
      metadataSizeBuf.flip();
      int metadataSize = metadataSizeBuf.getInt();
      endOffset = channel.size() - WALWriter.MAGIC_STRING_BYTES - Integer.BYTES - metadataSize - 1;
    } finally {
      channel.position(WALWriter.MAGIC_STRING_BYTES);
    }
  }

  private void analyzeFileVersion() throws IOException {
    if (channel.size() < WALWriter.MAGIC_STRING_BYTES) {
      version = FileVersion.UNKNOWN;
      return;
    }
    if (isCurrentVersion()) {
      this.version = FileVersion.V2;
      return;
    }
    this.version = FileVersion.V1;
  }

  private boolean isCurrentVersion() throws IOException {
    channel.position(0);
    ByteBuffer buffer = ByteBuffer.allocate(WALWriter.MAGIC_STRING_BYTES);
    channel.read(buffer);
    return new String(buffer.array()).equals(WALWriter.MAGIC_STRING);
  }

  @Override
  public int read() throws IOException {
    if (Objects.isNull(dataBuffer) || dataBuffer.position() >= dataBuffer.limit()) {
      loadNextSegment();
    }
    return dataBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (Objects.isNull(dataBuffer) || dataBuffer.position() >= dataBuffer.limit()) {
      loadNextSegment();
    }
    if (dataBuffer.remaining() >= len) {
      dataBuffer.get(b, off, len);
      return len;
    }
    int toBeRead = len;
    while (toBeRead > 0) {
      int remaining = dataBuffer.remaining();
      int bytesRead = Math.min(remaining, toBeRead);
      dataBuffer.get(b, off, bytesRead);
      off += bytesRead;
      toBeRead -= bytesRead;
      if (toBeRead > 0) {
        loadNextSegment();
      }
    }
    return len;
  }

  @Override
  public void close() throws IOException {
    channel.close();
    dataBuffer = null;
  }

  @Override
  public int available() throws IOException {
    long size = (endOffset - channel.position());
    if (!Objects.isNull(dataBuffer)) {
      size += dataBuffer.limit() - dataBuffer.position();
    }
    return (int) size;
  }

  private void loadNextSegment() throws IOException {
    if (channel.position() >= endOffset) {
      throw new IOException("End of file");
    }
    if (version == FileVersion.V2) {
      loadNextSegmentV2();
    } else if (version == FileVersion.V1) {
      loadNextSegmentV1();
    } else {
      tryLoadSegment();
    }
  }

  private void loadNextSegmentV1() throws IOException {
    // just read raw data as input
    if (channel.position() >= fileSize) {
      throw new IOException("Unexpected end of file");
    }
    if (Objects.isNull(dataBuffer)) {
      // read 128 KB
      dataBuffer = ByteBuffer.allocate(128 * 1024);
    }
    dataBuffer.clear();
    channel.read(dataBuffer);
    dataBuffer.flip();
  }

  private void loadNextSegmentV2() throws IOException {
    headerBuffer.clear();
    if (channel.read(headerBuffer) != Integer.BYTES + 1) {
      throw new IOException("Unexpected end of file");
    }
    // compressionType originalSize compressedSize
    headerBuffer.flip();
    CompressionType compressionType = CompressionType.deserialize(headerBuffer.get());
    int dataBufferSize = headerBuffer.getInt();
    if (compressionType != CompressionType.UNCOMPRESSED) {
      compressedHeader.clear();
      if (channel.read(compressedHeader) != Integer.BYTES) {
        throw new IOException("Unexpected end of file");
      }
      compressedHeader.flip();
      int uncompressedSize = compressedHeader.getInt();
      dataBuffer = ByteBuffer.allocateDirect(uncompressedSize);
      ByteBuffer compressedData = ByteBuffer.allocateDirect(dataBufferSize);
      if (channel.read(compressedData) != dataBufferSize) {
        throw new IOException("Unexpected end of file");
      }
      compressedData.flip();
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
      dataBuffer.clear();
      unCompressor.uncompress(compressedData, dataBuffer);
    } else {
      dataBuffer = ByteBuffer.allocate(dataBufferSize);
      if (channel.read(dataBuffer) != dataBufferSize) {
        throw new IOException("Unexpected end of file");
      }
    }
    dataBuffer.flip();
  }

  private void tryLoadSegment() throws IOException {
    long originPosition = channel.position();
    try {
      loadNextSegmentV2();
      version = FileVersion.V2;
    } catch (Throwable e) {
      // failed to load in V2 way, try in V1 way
      logger.warn("Failed to load WAL segment in V2 way, try in V1 way", e);
      channel.position(originPosition);
    }

    if (version == FileVersion.UNKNOWN) {
      loadNextSegmentV1();
      version = FileVersion.V1;
    }
  }

  public void skipToGivenPosition(long pos) throws IOException {
    if (version == FileVersion.V2) {
      channel.position(WALWriter.MAGIC_STRING_BYTES);
      ByteBuffer buffer = ByteBuffer.allocate(Byte.BYTES + Integer.BYTES);
      long posRemain = pos;
      int currSegmentSize = 0;
      while (posRemain > 0) {
        buffer.clear();
        channel.read(buffer);
        buffer.flip();
        buffer.get();
        currSegmentSize = buffer.getInt();
        if (posRemain >= currSegmentSize) {
          posRemain -= currSegmentSize;
        } else {
          break;
        }
      }
      dataBuffer = ByteBuffer.allocate(currSegmentSize);
      channel.read(dataBuffer);
      dataBuffer.position((int) posRemain);
    } else {
      dataBuffer.clear();
      channel.position(pos);
    }
  }

  public void read(ByteBuffer buffer) throws IOException {
    int totalBytesToBeRead = buffer.remaining();
    int currReadBytes = Math.min(dataBuffer.remaining(), buffer.remaining());
    dataBuffer.get(buffer.array(), buffer.position(), currReadBytes);
    if (totalBytesToBeRead - currReadBytes > 0) {
      loadNextSegment();
      read(buffer);
    }
  }

  public long getFileCurrentPos() throws IOException {
    return channel.position();
  }
}
