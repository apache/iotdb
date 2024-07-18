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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.dataregion.wal.checkpoint.Checkpoint;

import org.apache.tsfile.compress.ICompressor;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * LogWriter writes the binary logs into a file, including writing {@link WALEntry} into .wal file
 * and writing {@link Checkpoint} into .checkpoint file.
 */
public abstract class LogWriter implements ILogWriter {
  private static final Logger logger = LoggerFactory.getLogger(LogWriter.class);

  protected final File logFile;
  protected final FileOutputStream logStream;
  protected final FileChannel logChannel;
  protected long size = 0;
  protected long originalSize = 0;

  /**
   * 1 byte for whether enable compression, 4 byte for compressedSize, 4 byte for uncompressedSize
   */
  private final int COMPRESSED_HEADER_SIZE = Byte.BYTES + Integer.BYTES * 2;

  /** 1 byte for whether enable compression, 4 byte for uncompressedSize */
  private final int UN_COMPRESSED_HEADER_SIZE = Byte.BYTES + Integer.BYTES;

  private final ByteBuffer headerBuffer = ByteBuffer.allocate(COMPRESSED_HEADER_SIZE);
  private ICompressor compressor =
      ICompressor.getCompressor(
          IoTDBDescriptor.getInstance().getConfig().getWALCompressionAlgorithm());
  private ByteBuffer compressedByteBuffer;

  /** Minimum size to compress, use magic number 32 KB */
  private static long MIN_COMPRESSION_SIZE = 32 * 1024L;

  protected LogWriter(File logFile, WALFileVersion version) throws IOException {
    this.logFile = logFile;
    this.logStream = new FileOutputStream(logFile, true);
    this.logChannel = this.logStream.getChannel();
    if (!logFile.exists() || logFile.length() == 0) {
      this.logChannel.write(
          ByteBuffer.wrap(
              version == WALFileVersion.V1
                  ? WALWriter.MAGIC_STRING_V1.getBytes(StandardCharsets.UTF_8)
                  : WALWriter.MAGIC_STRING_V2.getBytes(StandardCharsets.UTF_8)));
      size += logChannel.position();
    }
  }

  @Override
  public double write(ByteBuffer buffer) throws IOException {
    // To support hot loading, we can't define it as a variable,
    // because we need to dynamically check whether wal compression is enabled
    // each time the buffer is serialized
    CompressionType compressionType =
        IoTDBDescriptor.getInstance().getConfig().getWALCompressionAlgorithm();
    int bufferSize = buffer.position();
    if (bufferSize == 0) {
      return 1.0;
    }
    originalSize += bufferSize;
    buffer.flip();
    boolean compressed = false;
    int uncompressedSize = bufferSize;
    if (compressionType != CompressionType.UNCOMPRESSED
        /* Do not compress buffer that is less than min size */
        && bufferSize > MIN_COMPRESSION_SIZE
        && compressedByteBuffer != null) {
      compressedByteBuffer.clear();
      if (compressor.getType() != compressionType) {
        compressor = ICompressor.getCompressor(compressionType);
      }
      compressor.compress(buffer, compressedByteBuffer);
      buffer = compressedByteBuffer;
      bufferSize = buffer.position();
      buffer.flip();
      compressed = true;
      size += COMPRESSED_HEADER_SIZE;
    } else {
      size += UN_COMPRESSED_HEADER_SIZE;
    }
    size += bufferSize;
    /*
     Header structure:
     [CompressionType(1 byte)][dataBufferSize(4 bytes)][uncompressedSize(4 bytes)]
    */
    headerBuffer.clear();
    headerBuffer.put(
        compressed ? compressionType.serialize() : CompressionType.UNCOMPRESSED.serialize());
    headerBuffer.putInt(bufferSize);
    if (compressed) {
      headerBuffer.putInt(uncompressedSize);
    }
    size += headerBuffer.position();
    try {
      headerBuffer.flip();
      logChannel.write(headerBuffer);
      logChannel.write(buffer);
    } catch (ClosedChannelException e) {
      logger.warn("Cannot write to {}", logFile, e);
    }
    return ((double) bufferSize / uncompressedSize);
  }

  @Override
  public void force() throws IOException {
    force(true);
  }

  @Override
  public void force(boolean metaData) throws IOException {
    if (logChannel != null && logChannel.isOpen()) {
      logChannel.force(metaData);
    }
  }

  @Override
  public long size() {
    return size;
  }

  public long originalSize() {
    return originalSize;
  }

  @Override
  public File getLogFile() {
    return logFile;
  }

  @Override
  public void close() throws IOException {
    if (logChannel != null) {
      try {
        if (logChannel.isOpen()) {
          logChannel.force(true);
        }
      } finally {
        logChannel.close();
        logStream.close();
      }
    }
  }

  public void setCompressedByteBuffer(ByteBuffer compressedByteBuffer) {
    this.compressedByteBuffer = compressedByteBuffer;
  }

  public long getOffset() throws IOException {
    return logChannel.position();
  }
}
