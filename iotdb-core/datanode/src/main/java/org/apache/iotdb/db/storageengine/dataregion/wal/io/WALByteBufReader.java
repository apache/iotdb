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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * This reader returns {@link WALEntry} as {@link ByteBuffer}, the usage of WALByteBufReader is like
 * {@link Iterator}.
 */
public class WALByteBufReader implements Closeable {
  private WALMetaData metaData;
  private WALInputStream walInputStream;
  private DataInputStream logStream;
  private Iterator<Integer> sizeIterator;
  // V3: track current entry index to provide per-entry progress metadata
  private int currentEntryIndex = -1;

  public WALByteBufReader(File logFile) throws IOException {
    WALInputStream walInputStream = new WALInputStream(logFile);
    try {
      this.walInputStream = walInputStream;
      this.logStream = new DataInputStream(walInputStream);
      this.metaData = walInputStream.getWALMetaData();
      this.sizeIterator = metaData.getBuffersSize().iterator();
    } catch (Exception e) {
      walInputStream.close();
      throw e;
    }
  }

  public WALByteBufReader(File logFile, WALMetaData metaDataSnapshot) throws IOException {
    WALInputStream walInputStream = new WALInputStream(logFile);
    try {
      this.walInputStream = walInputStream;
      this.logStream = new DataInputStream(walInputStream);
      this.metaData = metaDataSnapshot == null ? new WALMetaData() : metaDataSnapshot;
      this.sizeIterator = this.metaData.getBuffersSize().iterator();
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
    currentEntryIndex++;
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

  public boolean skipToEntryIndex(int entryIndex) throws IOException {
    if (entryIndex < 0 || entryIndex > metaData.getBuffersSize().size()) {
      return false;
    }
    long logicalPosition = 0L;
    for (int i = 0; i < entryIndex; i++) {
      logicalPosition += metaData.getBuffersSize().get(i);
    }
    if (entryIndex < metaData.getBuffersSize().size()) {
      walInputStream.skipToGivenLogicalPosition(logicalPosition);
    }
    sizeIterator = metaData.getBuffersSize().listIterator(entryIndex);
    currentEntryIndex = entryIndex - 1;
    return true;
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

  public long getCurrentEntryPhysicalTime() {
    List<Long> physicalTimes = metaData.getPhysicalTimes();
    if (currentEntryIndex >= 0 && currentEntryIndex < physicalTimes.size()) {
      return physicalTimes.get(currentEntryIndex);
    }
    return 0L;
  }

  public int getCurrentEntryNodeId() {
    List<Short> nodeIds = metaData.getNodeIds();
    if (currentEntryIndex >= 0 && currentEntryIndex < nodeIds.size()) {
      return nodeIds.get(currentEntryIndex);
    }
    return -1;
  }

  public long getCurrentEntryLocalSeq() {
    List<Long> localSeqs = metaData.getLocalSeqs();
    if (currentEntryIndex >= 0 && currentEntryIndex < localSeqs.size()) {
      return localSeqs.get(currentEntryIndex);
    }
    return metaData.getFirstSearchIndex() + currentEntryIndex;
  }

  /** Returns the current entry index (0-based). */
  public int getCurrentEntryIndex() {
    return currentEntryIndex;
  }
}
