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

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntry;
import org.apache.iotdb.db.storageengine.rescon.memory.SystemInfo;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MemoryControlledWALEntryQueue {

  private final BlockingQueue<WALEntry> queue;
  private static final Object nonFullCondition = new Object();

  public MemoryControlledWALEntryQueue() {
    queue = new LinkedBlockingQueue<>();
  }

  public WALEntry poll(long timeout, TimeUnit unit) throws InterruptedException {
    WALEntry e = queue.poll(timeout, unit);
    if (e != null) {
      SystemInfo.getInstance().updateWalQueueMemoryCost(-getElementSize(e));
      synchronized (nonFullCondition) {
        nonFullCondition.notifyAll();
      }
    }
    return e;
  }

  public void put(WALEntry e) throws InterruptedException {
    long elementSize = getElementSize(e);
    synchronized (nonFullCondition) {
      while (SystemInfo.getInstance().cannotReserveMemoryForWalEntry(elementSize)) {
        nonFullCondition.wait();
      }
    }
    queue.put(e);
    SystemInfo.getInstance().updateWalQueueMemoryCost(elementSize);
  }

  public WALEntry take() throws InterruptedException {
    WALEntry e = queue.take();
    SystemInfo.getInstance().updateWalQueueMemoryCost(-getElementSize(e));
    synchronized (nonFullCondition) {
      nonFullCondition.notifyAll();
    }

    return e;
  }

  public int size() {
    return queue.size();
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  private long getElementSize(WALEntry walEntry) {
    return walEntry.getMemorySize();
  }
}
