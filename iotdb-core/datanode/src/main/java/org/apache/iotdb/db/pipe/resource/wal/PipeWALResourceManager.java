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

package org.apache.iotdb.db.pipe.resource.wal;

import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class PipeWALResourceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeWALResourceManager.class);

  protected final Map<Long, PipeWALResource> memtableIdToPipeWALResourceMap;

  private static final int SEGMENT_LOCK_COUNT = 32;
  private final ReentrantLock[] memtableIdSegmentLocks;

  protected PipeWALResourceManager() {
    // memtableIdToPipeWALResourceMap can be concurrently accessed by multiple threads
    memtableIdToPipeWALResourceMap = new ConcurrentHashMap<>();

    memtableIdSegmentLocks = new ReentrantLock[SEGMENT_LOCK_COUNT];
    for (int i = 0; i < SEGMENT_LOCK_COUNT; i++) {
      memtableIdSegmentLocks[i] = new ReentrantLock();
    }

    PipeAgent.runtime()
        .registerPeriodicalJob(
            "PipeWALResourceManager#ttlCheck()",
            this::ttlCheck,
            Math.max(PipeWALResource.WAL_MIN_TIME_TO_LIVE_IN_MS / 1000, 1));
  }

  private void ttlCheck() {
    final Iterator<Map.Entry<Long, PipeWALResource>> iterator =
        memtableIdToPipeWALResourceMap.entrySet().iterator();
    try {
      while (iterator.hasNext()) {
        final Map.Entry<Long, PipeWALResource> entry = iterator.next();
        final ReentrantLock lock =
            memtableIdSegmentLocks[(int) (entry.getKey() % SEGMENT_LOCK_COUNT)];

        lock.lock();
        try {
          if (entry.getValue().invalidateIfPossible()) {
            iterator.remove();
          } else if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "WAL (memtableId {}) is still referenced {} times",
                entry.getKey(),
                entry.getValue().getReferenceCount());
          }
        } finally {
          lock.unlock();
        }
      }
    } catch (ConcurrentModificationException e) {
      LOGGER.error(
          "Concurrent modification issues happened, skipping the WAL in this round of ttl check",
          e);
    }
  }

  public final void pin(final WALEntryHandler walEntryHandler) throws IOException {
    final long memtableId = walEntryHandler.getMemTableId();
    final ReentrantLock lock = memtableIdSegmentLocks[(int) (memtableId % SEGMENT_LOCK_COUNT)];

    lock.lock();
    try {
      pinInternal(memtableId, walEntryHandler);
    } finally {
      lock.unlock();
    }
  }

  protected abstract void pinInternal(long memtableId, WALEntryHandler walEntryHandler)
      throws IOException;

  public final void unpin(final WALEntryHandler walEntryHandler) throws IOException {
    final long memtableId = walEntryHandler.getMemTableId();
    final ReentrantLock lock = memtableIdSegmentLocks[(int) (memtableId % SEGMENT_LOCK_COUNT)];

    lock.lock();
    try {
      unpinInternal(memtableId, walEntryHandler);
    } finally {
      lock.unlock();
    }
  }

  protected abstract void unpinInternal(long memtableId, WALEntryHandler walEntryHandler)
      throws IOException;

  public int getPinnedWalCount() {
    return Objects.nonNull(memtableIdToPipeWALResourceMap)
        ? memtableIdToPipeWALResourceMap.size()
        : 0;
  }
}
