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

import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryPosition;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class WALInsertNodeReader implements Closeable {
  private final WALMetaData metaData;
  private final WALInputStream walInputStream;
  private final long readSize;
  private final Iterator<Integer> sizeIterator;

  public WALInsertNodeReader(WALEntryPosition walEntryPosition, long readSize) throws IOException {
    this.readSize = readSize;
    walInputStream = walEntryPosition.openReadFileStream();
    try {
      this.metaData = walInputStream.getWALMetaData();
      sizeIterator = metaData.getBuffersSize().listIterator(1);
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
  //    public ByteBuffer next() throws IOException {
  //        int size = sizeIterator.next();
  //        // TODO: Reuse this buffer
  //        ByteBuffer buffer = ByteBuffer.allocate(size);
  //    /*
  //     Notice, we don't need to flip the buffer after calling
  //     logStream.readFully, since this function does not change
  //     the position of the buffer.
  //    */
  //        logStream.readFully(buffer.array(), 0, size);
  //        return buffer;
  //    }

  public WALMetaData getMetaData() {
    return metaData;
  }

  @Override
  public void close() throws IOException {
    walInputStream.close();
  }
}
