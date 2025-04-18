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

package org.apache.iotdb.db.storageengine.dataregion.wal.utils;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.dataregion.wal.io.WALInputStream;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;

import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * This class uses the tuple(identifier, file, position) to denote the position of the wal entry,
 * and give some methods to read the content from the disk.
 */
public class WALEntryPosition {
  private volatile String identifier = "";
  private volatile long walFileVersionId = -1;
  private volatile long position;
  private volatile int size;
  // wal node, null when wal is disabled
  private WALNode walNode = null;
  // wal file is not null when openReadFileChannel method has been called
  private File walFile = null;
  // cache for wal entry
  private WALInsertNodeCache cache = null;

  private static final String ENTRY_NOT_READY_MESSAGE = "This entry isn't ready for read.";

  public WALEntryPosition() {}

  public WALEntryPosition(String identifier, long walFileVersionId, long position, int size) {
    this.identifier = identifier;
    this.walFileVersionId = walFileVersionId;
    this.position = position;
    this.size = size;
  }

  /**
   * Try to read the wal entry directly from the cache. No need to check if the wal entry is ready
   * for read.
   */
  public Pair<ByteBuffer, InsertNode> getByteBufferOrInsertNodeIfPossible() {
    return cache.getByteBufferOrInsertNodeIfPossible(this);
  }

  /**
   * Read the wal entry and parse it to the InsertNode. Use LRU cache to accelerate read.
   *
   * @throws IOException failing to read.
   */
  public InsertNode readInsertNodeViaCacheAfterCanRead() throws IOException {
    if (!canRead()) {
      throw new IOException(ENTRY_NOT_READY_MESSAGE);
    }
    return cache.getInsertNode(this);
  }

  /**
   * Read the wal entry and get the raw bytebuffer. Use LRU cache to accelerate read.
   *
   * @throws IOException failing to read.
   */
  public ByteBuffer readByteBufferViaCacheAfterCanRead() throws IOException {
    if (!canRead()) {
      throw new IOException(ENTRY_NOT_READY_MESSAGE);
    }
    return cache.getByteBuffer(this);
  }

  /**
   * Read the byte buffer directly.
   *
   * @throws IOException failing to read.
   */
  ByteBuffer read() throws IOException {
    if (!canRead()) {
      throw new IOException("Target file hasn't been specified.");
    }
    // TODO: Reuse the file stream
    try (WALInputStream is = openReadFileStream()) {
      is.skipToGivenLogicalPosition(position);
      ByteBuffer buffer = ByteBuffer.allocate(size);
      is.read(buffer);
      return buffer;
    }
  }

  /**
   * Open the read file channel for this wal entry, this method will retry automatically when the
   * file is sealed when opening the file channel.
   *
   * @throws IOException failing to open the file channel.
   */
  public FileChannel openReadFileChannel() throws IOException {
    if (isInSealedFile()) {
      walFile = walNode.getWALFile(walFileVersionId);
      return FileChannel.open(walFile.toPath(), StandardOpenOption.READ);
    } else {
      try {
        walFile = walNode.getWALFile(walFileVersionId);
        return FileChannel.open(walFile.toPath(), StandardOpenOption.READ);
      } catch (IOException e) {
        // unsealed file may be renamed after sealed, so we should try again
        if (isInSealedFile()) {
          walFile = walNode.getWALFile(walFileVersionId);
          return FileChannel.open(walFile.toPath(), StandardOpenOption.READ);
        } else {
          throw e;
        }
      }
    }
  }

  public WALInputStream openReadFileStream() throws IOException {
    // TODO: Refactor this part of code
    if (isInSealedFile()) {
      walFile = walNode.getWALFile(walFileVersionId);
      return new WALInputStream(walFile);
    } else {
      try {
        walFile = walNode.getWALFile(walFileVersionId);
        return new WALInputStream(walFile);
      } catch (IOException e) {
        // unsealed file may be renamed after sealed, so we should try again
        if (isInSealedFile()) {
          walFile = walNode.getWALFile(walFileVersionId);
          return new WALInputStream(walFile);
        } else {
          throw e;
        }
      }
    }
  }

  public File getWalFile() {
    return walFile;
  }

  /** Return true only when the tuple(file, position, size) is ready. */
  public boolean canRead() {
    return walFileVersionId >= 0;
  }

  /** Return true only when this wal file is sealed. */
  public boolean isInSealedFile() {
    if (walNode == null || !canRead()) {
      throw new RuntimeException(ENTRY_NOT_READY_MESSAGE);
    }
    return walFileVersionId < walNode.getCurrentWALFileVersion();
  }

  public void setWalNode(WALNode walNode, long memTableId) {
    this.walNode = walNode;
    identifier = walNode.getIdentifier();
    cache = WALInsertNodeCache.getInstance(walNode.getRegionId(memTableId));
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setEntryPosition(long walFileVersionId, long position, WALEntryValue value) {
    this.position = position;
    this.walFileVersionId = walFileVersionId;
    if (cache != null && value instanceof InsertNode) {
      cache.cacheInsertNodeIfNeeded(this, (InsertNode) value);
    }
  }

  public long getPosition() {
    return position;
  }

  public long getWalFileVersionId() {
    return walFileVersionId;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier, walFileVersionId, position);
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
    return identifier.equals(that.identifier)
        && walFileVersionId == that.walFileVersionId
        && position == that.position;
  }
}
