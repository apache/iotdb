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
package org.apache.iotdb.db.storageengine.rescon.memory;

import org.apache.iotdb.db.i18n.StorageEngineMessages;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A named owner's estimated write-memory usage. All changes propagate to its accounting scope. The
 * block does not allocate heap memory or impose a per-owner quota.
 */
public final class WriteMemoryBlock implements AutoCloseable {
  public enum Category {
    MEMTABLE_ARRAY,
    MEMTABLE_STRUCTURE,
    MEMTABLE_BINARY,
    // WAL snapshots contain only a total; do not guess an array/Binary breakdown on recovery.
    MEMTABLE_RECOVERED,
    TSFILE_METADATA,
    FLUSH_TEMPORARY
  }

  private static final AtomicLong NEXT_ID = new AtomicLong();
  private final long id = NEXT_ID.incrementAndGet();
  private final Category category;
  private final String name;
  // Guarded by this block's monitor, just like reservations and ownership.
  private long usedBytes;
  private WriteMemoryManager manager;
  private long reservedBytes;
  private boolean closed;

  public WriteMemoryBlock(String name, Category category) {
    this.category = category;
    this.name = name;
  }

  /** Standalone/recovery MemTables are attached only when owned by a live writer. */
  public synchronized void attachTo(WriteMemoryManager manager) {
    checkOpen();
    if (this.manager == manager) {
      return;
    }
    if (this.manager != null) {
      throw new IllegalStateException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_WRITE_MEMORY_BLOCK_ARG_ALREADY_BELONGS_TO_ANOTHER_MANAGER_A05A8229,
              getName()));
    }
    this.manager = manager;
    manager.addMemory(getAccountedMemoryInBytes());
    manager.register(this);
  }

  public synchronized void add(long bytes) {
    checkOpen();
    checkNonNegative(bytes);
    usedBytes += bytes;

    if (manager != null) {
      manager.addMemory(bytes);
    }
  }

  public synchronized void release(long bytes) {
    checkOpen();
    checkNonNegative(bytes);
    if (bytes > usedBytes) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages
                  .EXCEPTION_CANNOT_RELEASE_ARG_BYTES_FROM_WRITE_MEMORY_BLOCK_ARG_WITH_ARG_BYTES_3783A614,
              bytes,
              getName(),
              usedBytes));
    }
    usedBytes -= bytes;
    if (manager != null) {
      manager.releaseMemory(bytes);
    }
  }

  /** A pending allocation counts towards rejection but is not yet flushable MemTable memory. */
  public synchronized Reservation reserve(long bytes) {
    checkOpen();
    checkNonNegative(bytes);
    reservedBytes += bytes;
    if (manager != null) {
      manager.addMemory(bytes);
    }
    return new Reservation(bytes);
  }

  public synchronized long getUsedMemoryInBytes() {
    return usedBytes;
  }

  public synchronized long getReservedMemoryInBytes() {
    return reservedBytes;
  }

  public synchronized long getAccountedMemoryInBytes() {
    return usedBytes + reservedBytes;
  }

  /** Clears live accounting, for a MemTable that is explicitly cleared and reused. */
  public synchronized void clear() {
    checkOpen();
    // Reservations must be settled by their caller before a live owner is reused.
    release(usedBytes);
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    if (manager != null) {
      manager.releaseMemory(getAccountedMemoryInBytes());
      manager.unregister(this);
      manager = null;
    }
    usedBytes = 0;
    reservedBytes = 0;
    closed = true;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Category getCategory() {
    return category;
  }

  public synchronized boolean isClosed() {
    return closed;
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException(
          String.format(
              StorageEngineMessages.EXCEPTION_WRITE_MEMORY_BLOCK_ARG_IS_CLOSED_9E2561DF,
              getName()));
    }
  }

  private static void checkNonNegative(long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages.EXCEPTION_WRITE_MEMORY_SIZE_MUST_BE_NON_NEGATIVE_ARG_2CBCF358,
              bytes));
    }
  }

  /** Closing an uncommitted reservation rolls it back; committing changes no region total. */
  public final class Reservation implements AutoCloseable {
    private final long bytes;
    private boolean completed;

    private Reservation(long bytes) {
      this.bytes = bytes;
    }

    public void commit() {
      synchronized (WriteMemoryBlock.this) {
        if (completed) {
          return;
        }
        checkOpen();
        reservedBytes -= bytes;
        usedBytes += bytes;

        completed = true;
      }
    }

    @Override
    public void close() {
      synchronized (WriteMemoryBlock.this) {
        if (completed) {
          return;
        }
        if (!closed) {
          reservedBytes -= bytes;
          if (manager != null) {
            manager.releaseMemory(bytes);
          }
        }
        completed = true;
      }
    }
  }
}
