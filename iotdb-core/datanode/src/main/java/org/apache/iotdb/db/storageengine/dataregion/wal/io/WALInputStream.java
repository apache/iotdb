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
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;

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
  private ByteBuffer dataBuffer =
      ByteBuffer.allocate(
          IoTDBDescriptor.getInstance().getConfig().getWalBufferSize()); // uncompressed data buffer

  public WALInputStream(File logFile) throws IOException {
    channel = FileChannel.open(logFile.toPath());
  }

  @Override
  public int read() throws IOException {
    if (Objects.isNull(dataBuffer) || dataBuffer.position() == dataBuffer.limit()) {
      loadNextSegment();
    }
    return dataBuffer.get() & 0xFF;
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
    headerBuffer.clear();
    if (channel.read(headerBuffer) != Integer.BYTES + 1) {
      throw new IOException("Unexpected end of file");
    }
    headerBuffer.flip();
    int dataSize = headerBuffer.getInt();
    boolean isCompressed = headerBuffer.get() == 1;
    if (isCompressed) {
      compressedHeader.clear();
      if (channel.read(compressedHeader) != Integer.BYTES) {
        throw new IOException("Unexpected end of file");
      }
      compressedHeader.flip();
      int uncompressedSize = compressedHeader.getInt();
      if (uncompressedSize > dataBuffer.capacity()) {
        // enlarge buffer
        dataBuffer = ByteBuffer.allocateDirect(uncompressedSize);
      }
      ByteBuffer compressedData = ByteBuffer.allocateDirect(dataSize);
      if (channel.read(compressedData) != dataSize) {
        throw new IOException("Unexpected end of file");
      }
      compressedData.flip();
      IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.LZ4);
      dataBuffer.clear();
      unCompressor.uncompress(compressedData, dataBuffer);
    } else {
      dataBuffer = ByteBuffer.allocateDirect(dataSize);
      if (channel.read(dataBuffer) != dataSize) {
        throw new IOException("Unexpected end of file");
      }
    }
    dataBuffer.flip();
  }
}
