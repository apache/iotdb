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
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryPosition;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * This reader returns {@link WALEntry} as {@link ByteBuffer}, the usage of WALByteBufReader is like
 * {@link Iterator}.
 */
public class WALByteBufReader implements Closeable {
  private WALMetaData metaData;
  private DataInputStream logStream;
  private Iterator<Integer> sizeIterator;

  public WALByteBufReader(File logFile) throws IOException {
    WALInputStream walInputStream = new WALInputStream(logFile);
    try {
      this.logStream = new DataInputStream(walInputStream);
      this.metaData = walInputStream.getWALMetaData();
      this.sizeIterator = metaData.getBuffersSize().iterator();
    } catch (Exception e) {
      walInputStream.close();
      throw e;
    }
  }

  public WALByteBufReader(WALEntryPosition walEntryPosition) throws IOException {
    WALInputStream walInputStream = walEntryPosition.openReadFileStream();
    try {
      this.logStream = new DataInputStream(walInputStream);
      this.metaData = walInputStream.getWALMetaData();
      this.sizeIterator = metaData.getBuffersSize().iterator();
    } catch (Exception e) {
      walInputStream.close();
      throw e;
    }
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
    // TODO: Reuse this buffer
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
    logStream.close();
  }

  public long getFirstSearchIndex() {
    return metaData.getFirstSearchIndex();
  }
}
