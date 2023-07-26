/*  * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.  */ package org.apache.iotdb.db.pipe.resource.wal;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.db.pipe.resource.wal.selfhost.PipeWALSelfHostResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public abstract class PipeWALResourceManager {

  protected final Map<Long, PipeWALResource> memtableIdToPipeWALResourceMap;

  private static final int SEGMENT_LOCK_COUNT = 32;
  private final ReentrantLock[] memtableIdSegmentLocks;

  private static final ScheduledExecutorService PIPE_WAL_RESOURCE_TTL_CHECKER =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_WAL_RESOURCE_TTL_CHECKER.getName());

  protected PipeWALResourceManager() {
    // memtableIdToPipeWALResourceMap can be concurrently accessed by multiple threads
    memtableIdToPipeWALResourceMap = new ConcurrentHashMap<>();

    memtableIdSegmentLocks = new ReentrantLock[SEGMENT_LOCK_COUNT];
    for (int i = 0; i < SEGMENT_LOCK_COUNT; i++) {
      memtableIdSegmentLocks[i] = new ReentrantLock();
    }

    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        PIPE_WAL_RESOURCE_TTL_CHECKER,
        () -> {
          Iterator<Map.Entry<Long, PipeWALResource>> iterator =
              memtableIdToPipeWALResourceMap.entrySet().iterator();
          while (iterator.hasNext()) {
            final Map.Entry<Long, PipeWALResource> entry = iterator.next();
            final ReentrantLock lock =
                memtableIdSegmentLocks[(int) (entry.getKey() % SEGMENT_LOCK_COUNT)];

            lock.lock();
            try {
              if (entry.getValue().invalidateIfPossible()) {
                iterator.remove();
              }
            } finally {
              lock.unlock();
            }
          }
        },
        PipeWALSelfHostResource.MIN_TIME_TO_LIVE_IN_MS,
        PipeWALSelfHostResource.MIN_TIME_TO_LIVE_IN_MS,
        TimeUnit.MILLISECONDS);
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
}
