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

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * This reader returns {@link WALEntry} as {@link ByteBuffer}, the usage of WALByteBufReader is like
 * {@link Iterator}.
 */
public class WALByteBufReader implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(WALByteBufReader.class);
  private final File logFile;
  private final FileChannel channel;
  private final WALMetaData metaData;
  private final DataInputStream logStream;
  private final Iterator<Integer> sizeIterator;

  public WALByteBufReader(File logFile) throws IOException {
    this(logFile, FileChannel.open(logFile.toPath(), StandardOpenOption.READ));
  }

  public WALByteBufReader(File logFile, FileChannel channel) throws IOException {
    this.logFile = logFile;
    this.channel = channel;
    this.logStream = new DataInputStream(new WALInputStream(logFile));
    this.metaData = WALMetaData.readFromWALFile(logFile, channel);
    this.sizeIterator = metaData.getBuffersSize().iterator();
    logger.info("{} Buffer sizes is {}", logFile.getAbsolutePath(), metaData.getBuffersSize());
    channel.position(0);
  }

  /** Like {@link Iterator#hasNext()}. */
  public boolean hasNext() {
    return sizeIterator.hasNext();
  }

  /**
   * Like {@link Iterator#next()}.
   *
   * @throws IOException when failing to read from channel.
   */
  public ByteBuffer next() throws IOException {
    int size = sizeIterator.next();
    ByteBuffer buffer = ByteBuffer.allocate(size);
    /*
     Notice, we don't need to flip the buffer after calling
     logStream.readFully, since this function does not change
     the position of the buffer.
    */
    logStream.readFully(buffer.array(), 0, size);
    return buffer;
  }

  public WALMetaData getMetaData() {
    return metaData;
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  public long getFirstSearchIndex() {
    return metaData.getFirstSearchIndex();
  }
}
