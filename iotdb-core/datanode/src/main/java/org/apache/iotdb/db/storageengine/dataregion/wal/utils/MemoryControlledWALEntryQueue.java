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

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.rpc.TSStatusCode.WAL_ENTRY_TOO_LARGE;

public class MemoryControlledWALEntryQueue {
  private final BlockingQueue<ChargedEntry> queue;
  private static final Object nonFullCondition = new Object();

  public MemoryControlledWALEntryQueue() {
    queue = new LinkedBlockingQueue<>();
  }

  public WALEntry poll(long timeout, TimeUnit unit) throws InterruptedException {
    ChargedEntry entry = queue.poll(timeout, unit);
    if (entry != null) {
      SystemInfo.getInstance().getWalBufferQueueMemoryBlock().release(entry.bytes);
      synchronized (nonFullCondition) {
        nonFullCondition.notifyAll();
      }
    }
    return entry == null ? null : entry.entry;
  }

  public void put(WALEntry e) throws InterruptedException {
    long elementSize = getElementSize(e);
    synchronized (nonFullCondition) {
      while (!SystemInfo.getInstance().getWalBufferQueueMemoryBlock().allocate(elementSize)) {
        if (elementSize
            > SystemInfo.getInstance().getWalBufferQueueMemoryBlock().getTotalMemorySizeInBytes()) {
          throw new IoTDBRuntimeException(
              String.format(
                  StorageEngineMessages
                      .STORAGE_EXCEPTION_THE_ELEMENT_SIZE_OF_WALENTRY_S_IS_LARGER_THAN_THE_TOTAL_E494520D,
                  elementSize,
                  SystemInfo.getInstance()
                      .getWalBufferQueueMemoryBlock()
                      .getTotalMemorySizeInBytes()),
              WAL_ENTRY_TOO_LARGE.getStatusCode());
        }
        nonFullCondition.wait();
      }
    }
    boolean enqueued = false;
    try {
      queue.put(new ChargedEntry(e, elementSize));
      enqueued = true;
    } finally {
      if (!enqueued) {
        SystemInfo.getInstance().getWalBufferQueueMemoryBlock().release(elementSize);
        synchronized (nonFullCondition) {
          nonFullCondition.notifyAll();
        }
      }
    }
  }

  public WALEntry take() throws InterruptedException {
    ChargedEntry entry = queue.take();
    SystemInfo.getInstance().getWalBufferQueueMemoryBlock().release(entry.bytes);
    synchronized (nonFullCondition) {
      nonFullCondition.notifyAll();
    }
    return entry.entry;
  }

  public int size() {
    return queue.size();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  private static final class ChargedEntry {
    private final WALEntry entry;
    private final long bytes;

    private ChargedEntry(WALEntry entry, long bytes) {
      this.entry = entry;
      this.bytes = bytes;
    }
  }

  private long getElementSize(WALEntry walEntry) {
    return walEntry.getMemorySize();
  }
}
