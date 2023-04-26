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
package org.apache.iotdb.db.wal.utils;

import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * This class uses the tuple(file, position, size) to denote the position of the wal entry, and give
 * some methods to read the content from the disk.
 */
public class WALEntryPosition {
  private static final WALInsertNodeCache CACHE = WALInsertNodeCache.getInstance();
  private volatile File walFile;
  private volatile long position;
  private volatile int size;

  public WALEntryPosition() {}

  public WALEntryPosition(File walFile, long position, int size) {
    this.walFile = walFile;
    this.position = position;
    this.size = size;
  }

  /** Read the wal entry and parse it to the InsertNode. Use LRU cache to accelerate read. */
  public InsertNode readInsertNodeViaCache() throws IOException {
    if (!canRead()) {
      throw new IOException("Target file hasn't been specified.");
    }
    return CACHE.get(this);
  }

  /** Read the byte buffer directly. */
  ByteBuffer read() throws IOException {
    if (!canRead()) {
      throw new IOException("Target file hasn't been specified.");
    }
    try (FileChannel channel = FileChannel.open(walFile.toPath(), StandardOpenOption.READ)) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      channel.position(position);
      channel.read(buffer);
      buffer.clear();
      return buffer;
    }
  }

  /** Return true only when the tuple(file, position, size) is ready. */
  public boolean canRead() {
    return walFile != null;
  }

  public void setEntryPosition(File walFile, long position) {
    this.position = position;
    this.walFile = walFile;
  }

  public File getWalFile() {
    return walFile;
  }

  public long getPosition() {
    return position;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  @Override
  public int hashCode() {
    return Objects.hash(walFile, position);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WALEntryPosition that = (WALEntryPosition) o;
    return walFile.equals(that.walFile) && position == that.position;
  }
}
