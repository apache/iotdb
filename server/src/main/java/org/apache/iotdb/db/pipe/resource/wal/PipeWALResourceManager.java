/*  * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.  */ package org.apache.iotdb.db.pipe.resource.wal;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.db.wal.utils.WALPipeHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class PipeWALResourceManager implements AutoCloseable {

  private final Map<Long, PipeWALResource> memtableIdToPipeWALResourceMap;

  private static final int SEGMENT_LOCK_COUNT = 32;
  private final ReentrantLock[] memtableIdSegmentLocks;

  private static final ScheduledExecutorService PIPE_WAL_RESOURCE_TTL_CHECKER =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_WAL_RESOURCE_TTL_CHECKER_SERVICE.getName());
  private final ScheduledFuture<?> ttlCheckerFuture;

  public PipeWALResourceManager() {
    memtableIdToPipeWALResourceMap = new HashMap<>();

    memtableIdSegmentLocks = new ReentrantLock[SEGMENT_LOCK_COUNT];
    for (int i = 0; i < SEGMENT_LOCK_COUNT; i++) {
      memtableIdSegmentLocks[i] = new ReentrantLock();
    }

    ttlCheckerFuture =
        PIPE_WAL_RESOURCE_TTL_CHECKER.scheduleAtFixedRate(
            () -> {
              for (final long memtableId : memtableIdToPipeWALResourceMap.keySet()) {
                final ReentrantLock lock =
                    memtableIdSegmentLocks[(int) (memtableId % SEGMENT_LOCK_COUNT)];

                lock.lock();
                try {
                  if (memtableIdToPipeWALResourceMap.get(memtableId).invalidateIfPossible()) {
                    memtableIdToPipeWALResourceMap.remove(memtableId);
                  }
                } finally {
                  lock.unlock();
                }
              }
            },
            PipeWALResource.MIN_TIME_TO_LIVE_IN_MS,
            PipeWALResource.MIN_TIME_TO_LIVE_IN_MS,
            TimeUnit.MILLISECONDS);
  }

  public void pin(long memtableId, WALPipeHandler walPipeHandler) {
    final ReentrantLock lock = memtableIdSegmentLocks[(int) (memtableId % SEGMENT_LOCK_COUNT)];

    lock.lock();
    try {
      memtableIdToPipeWALResourceMap
          .computeIfAbsent(memtableId, id -> new PipeWALResource(walPipeHandler))
          .pin();
    } finally {
      lock.unlock();
    }
  }

  public void unpin(long memtableId) {
    final ReentrantLock lock = memtableIdSegmentLocks[(int) (memtableId % SEGMENT_LOCK_COUNT)];

    lock.lock();
    try {
      memtableIdToPipeWALResourceMap.get(memtableId).unpin();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws Exception {
    if (ttlCheckerFuture != null) {
      ttlCheckerFuture.cancel(true);
    }

    for (final long memtableId : memtableIdToPipeWALResourceMap.keySet()) {
      final ReentrantLock lock = memtableIdSegmentLocks[(int) (memtableId % SEGMENT_LOCK_COUNT)];

      lock.lock();
      try {
        memtableIdToPipeWALResourceMap.get(memtableId).close();
        memtableIdToPipeWALResourceMap.remove(memtableId);
      } finally {
        lock.unlock();
      }
    }
  }
}
