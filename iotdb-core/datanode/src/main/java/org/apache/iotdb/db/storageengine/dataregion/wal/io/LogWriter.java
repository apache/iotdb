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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

/**
 * LogWriter writes the binary logs into a file, including writing {@link WALEntry} into .wal file
 * and writing {@link Checkpoint} into .checkpoint file.
 */
public abstract class LogWriter implements ILogWriter {
  private static final Logger logger = LoggerFactory.getLogger(LogWriter.class);

  protected final File logFile;
  protected final FileOutputStream logStream;
  protected final FileChannel logChannel;
  protected long size;
  protected boolean isEndFile = false;
  private final ByteBuffer headerBuffer = ByteBuffer.allocate(Integer.BYTES * 2 + 1);
  private static final CompressionType compressionType = CompressionType.GZIP;
  private final ICompressor compressor = ICompressor.getCompressor(CompressionType.GZIP);
  private final ByteBuffer compressedByteBuffer;

  protected LogWriter(File logFile) throws FileNotFoundException {
    this.logFile = logFile;
    this.logStream = new FileOutputStream(logFile, true);
    this.logChannel = this.logStream.getChannel();
    if (IoTDBDescriptor.getInstance().getConfig().isEnableWALCompression()) {
      compressedByteBuffer =
          ByteBuffer.allocate(
              compressor.getMaxBytesForCompression(
                  IoTDBDescriptor.getInstance().getConfig().getWalBufferSize()));
    } else {
      compressedByteBuffer = null;
    }
  }

  @Override
  public void write(ByteBuffer buffer) throws IOException {
    int bufferSize = buffer.position();
    buffer.flip();
    boolean compressed = false;
    int uncompressedSize = bufferSize;
    if (!isEndFile && IoTDBDescriptor.getInstance().getConfig().isEnableWALCompression()
    /* && bufferSize > 1024 * 512 Do not compress buffer that is less than 512KB */ ) {
      compressedByteBuffer.clear();
      compressor.compress(buffer, compressedByteBuffer);
      buffer = compressedByteBuffer;
      bufferSize = buffer.position();
      buffer.flip();
      compressed = true;
    }
    size += bufferSize;
    headerBuffer.clear();
    headerBuffer.put(
        compressed ? compressionType.serialize() : CompressionType.UNCOMPRESSED.serialize());
    headerBuffer.putInt(bufferSize);
    if (compressed) {
      headerBuffer.putInt(uncompressedSize);
      size += 4;
    }
    size += bufferSize;
    size += 5;
    try {
      headerBuffer.flip();
      logChannel.write(headerBuffer);
      logChannel.write(buffer);
    } catch (ClosedChannelException e) {
      logger.warn("Cannot write to {}", logFile, e);
    }
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
}
