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
  File logFile;

  enum FileVersion {
    V1,
    V2,
    UNKNOWN
  };

  FileVersion version;

  public WALInputStream(File logFile) throws IOException {
    channel = FileChannel.open(logFile.toPath());
    analyzeFileVersion();
    this.logFile = logFile;
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
    return (int) (channel.size() - channel.position());
  }

  private void loadNextSegment() throws IOException {
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
      dataBuffer = ByteBuffer.allocateDirect(dataBufferSize);
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
}
