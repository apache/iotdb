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
package org.apache.iotdb.db.wal.cache;

import org.apache.iotdb.db.wal.node.WALNode;

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
  private static final WALEntryCache CACHE = WALEntryCache.getInstance();
  private volatile String identifier = "";
  private volatile long walFileVersionId = -1;
  private volatile long position;
  private volatile int size;
  /** wal node, null when wal is disabled */
  private WALNode walNode = null;
  /** wal file is not null when openReadFileChannel method has been called */
  private File walFile = null;

  public WALEntryPosition() {}

  public WALEntryPosition(String identifier, long walFileVersionId, long position, int size) {
    this.identifier = identifier;
    this.walFileVersionId = walFileVersionId;
    this.position = position;
    this.size = size;
  }

  /** Read the wal entry buffer. Use LRU cache to accelerate read. */
  public WALEntryCacheValue readViaCache() throws IOException {
    if (!canRead()) {
      throw new IOException("This entry isn't ready for read.");
    }
    return CACHE.get(this);
  }

  /** Read the byte buffer directly. */
  ByteBuffer read() throws IOException {
    if (!canRead()) {
      throw new IOException("Target file hasn't been specified.");
    }
    try (FileChannel channel = openReadFileChannel()) {
      ByteBuffer buffer = ByteBuffer.allocate(size);
      channel.position(position);
      channel.read(buffer);
      buffer.clear();
      return buffer;
    }
  }

  /**
   * open the read file channel for this wal entry, this method will retry automatically when the
   * file is sealed when opening the file channel
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

  /** Return true only when the tuple(file, position, size) is ready. */
  public boolean canRead() {
    return walFileVersionId >= 0;
  }

  /** Return true only when this wal file is sealed. */
  public boolean isInSealedFile() {
    if (walNode == null || !canRead()) {
      throw new RuntimeException("This entry isn't ready for read.");
    }
    return walFileVersionId < walNode.getCurrentWALFileVersion();
  }

  public void setWalNode(WALNode walNode) {
    this.walNode = walNode;
    this.identifier = walNode.getIdentifier();
  }

  public void setEntryPosition(long walFileVersionId, long position) {
    this.position = position;
    this.walFileVersionId = walFileVersionId;
  }

  public String getIdentifier() {
    return identifier;
  }

  public long getWalFileVersionId() {
    return walFileVersionId;
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
