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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reader dedicated to the new writer-based subscription progress model.
 *
 * <p>It keeps the original WAL entry body untouched and exposes per-entry writer metadata from WAL
 * footer arrays alongside the current entry buffer.
 */
public class ProgressWALReader implements Closeable {

  private final WALByteBufReader delegate;

  public ProgressWALReader(File logFile) throws IOException {
    this.delegate = new WALByteBufReader(logFile);
  }

  public boolean hasNext() {
    return delegate.hasNext();
  }

  public ByteBuffer next() throws IOException {
    return delegate.next();
  }

  public WALMetaData getMetaData() {
    return delegate.getMetaData();
  }

  public long getCurrentEntryPhysicalTime() {
    return delegate.getCurrentEntryPhysicalTime();
  }

  public int getCurrentEntryNodeId() {
    return delegate.getCurrentEntryNodeId();
  }

  public long getCurrentEntryWriterEpoch() {
    return delegate.getCurrentEntryWriterEpoch();
  }

  public long getCurrentEntryLocalSeq() {
    return delegate.getCurrentEntryLocalSeq();
  }

  public int getCurrentEntryIndex() {
    return delegate.getCurrentEntryIndex();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
