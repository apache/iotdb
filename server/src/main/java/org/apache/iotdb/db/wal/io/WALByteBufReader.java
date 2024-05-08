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
package org.apache.iotdb.db.wal.io;

import org.apache.iotdb.db.wal.buffer.WALEntry;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

import static org.apache.iotdb.db.wal.io.WALWriter.MAGIC_STRING;
import static org.apache.iotdb.db.wal.io.WALWriter.MAGIC_STRING_BYTES;

/**
 * This reader returns {@link WALEntry} as {@link ByteBuffer}, the usage of WALByteBufReader is like
 * {@link Iterator}.
 */
public class WALByteBufReader implements Closeable {
  private final File logFile;
  private final FileChannel channel;
  private final WALMetaData metaData;
  private final Iterator<Integer> sizeIterator;

  public WALByteBufReader(File logFile) throws IOException {
    this.logFile = logFile;
    this.channel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ);
    if (channel.size() < MAGIC_STRING_BYTES || !readTailMagic().equals(MAGIC_STRING)) {
      throw new IOException(String.format("Broken wal file %s", logFile));
    }
    // load metadata size
    ByteBuffer metadataSizeBuf = ByteBuffer.allocate(Integer.BYTES);
    long position = channel.size() - MAGIC_STRING_BYTES - Integer.BYTES;
    channel.read(metadataSizeBuf, position);
    metadataSizeBuf.flip();
    // load metadata
    int metadataSize = metadataSizeBuf.getInt();
    ByteBuffer metadataBuf = ByteBuffer.allocate(metadataSize);
    channel.read(metadataBuf, position - metadataSize);
    metadataBuf.flip();
    metaData = WALMetaData.deserialize(metadataBuf);
    // init iterator
    sizeIterator = metaData.getBuffersSize().iterator();
    channel.position(0);
  }

  /** Like {@link Iterator#hasNext()} */
  public boolean hasNext() {
    return sizeIterator.hasNext();
  }

  /** Like {@link Iterator#next()} */
  public ByteBuffer next() throws IOException {
    int size = sizeIterator.next();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    channel.read(buffer);
    buffer.clear();
    return buffer;
  }

  private String readTailMagic() throws IOException {
    ByteBuffer magicStringBytes = ByteBuffer.allocate(MAGIC_STRING_BYTES);
    channel.read(magicStringBytes, channel.size() - MAGIC_STRING_BYTES);
    magicStringBytes.flip();
    return new String(magicStringBytes.array());
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  public long getFirstSearchIndex() {
    return metaData.getFirstSearchIndex();
  }
}
