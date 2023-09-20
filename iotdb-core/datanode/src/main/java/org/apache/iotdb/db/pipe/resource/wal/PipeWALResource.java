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

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeCriticalException;
import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.MemTablePinException;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALEntryHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PipeWALResource implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeWALResource.class);

  protected final WALEntryHandler walEntryHandler;

  private final AtomicInteger referenceCount;

  public static final long MIN_TIME_TO_LIVE_IN_MS = 1000L * 60;
  private final AtomicLong lastLogicalPinTime;
  private final AtomicBoolean isPhysicallyPinned;

  protected PipeWALResource(WALEntryHandler walEntryHandler) {
    this.walEntryHandler = walEntryHandler;

    referenceCount = new AtomicInteger(0);

    lastLogicalPinTime = new AtomicLong(0);
    isPhysicallyPinned = new AtomicBoolean(false);
  }

  public final void pin() throws PipeRuntimeNonCriticalException {
    if (referenceCount.get() == 0) {
      if (!isPhysicallyPinned.get()) {
        try {
          pinInternal();
        } catch (MemTablePinException e) {
          throw new PipeRuntimeNonCriticalException(
              String.format(
                  "failed to pin wal %d, because %s",
                  walEntryHandler.getMemTableId(), e.getMessage()));
        }
        isPhysicallyPinned.set(true);
        LOGGER.info("wal {} is pinned by pipe engine", walEntryHandler.getMemTableId());
      } // else means the wal is already pinned, do nothing

      // no matter the wal is pinned or not, update the last pin time
      lastLogicalPinTime.set(System.currentTimeMillis());
    }

    referenceCount.incrementAndGet();
  }

  protected abstract void pinInternal()
      throws MemTablePinException, PipeRuntimeNonCriticalException;

  public final void unpin() throws PipeRuntimeNonCriticalException {
    final int finalReferenceCount = referenceCount.get();

    if (finalReferenceCount == 1) {
      unpinPhysicallyIfOutOfTimeToLive();
    } else if (finalReferenceCount < 1) {
      throw new PipeRuntimeCriticalException(
          String.format(
              "wal %d is unpinned more than pinned, this should not happen",
              walEntryHandler.getMemTableId()));
    }

    referenceCount.decrementAndGet();
  }

  protected abstract void unpinInternal()
      throws MemTablePinException, PipeRuntimeNonCriticalException;

  /**
   * Invalidate the wal if it is unpinned and out of time to live.
   *
   * @return true if the wal is invalidated, false otherwise
   */
  public final boolean invalidateIfPossible() {
    if (referenceCount.get() > 0) {
      return false;
    }

    // referenceCount.get() == 0
    return unpinPhysicallyIfOutOfTimeToLive();
  }

  /**
   * Unpin the wal if it is out of time to live.
   *
   * @return true if the wal is unpinned physically (then it can be invalidated), false otherwise
   * @throws PipeRuntimeNonCriticalException if failed to unpin WAL of memtable.
   */
  private boolean unpinPhysicallyIfOutOfTimeToLive() {
    if (isPhysicallyPinned.get()) {
      if (System.currentTimeMillis() - lastLogicalPinTime.get() > MIN_TIME_TO_LIVE_IN_MS) {
        try {
          unpinInternal();
        } catch (MemTablePinException e) {
          throw new PipeRuntimeNonCriticalException(
              String.format(
                  "failed to unpin wal %d, because %s",
                  walEntryHandler.getMemTableId(), e.getMessage()));
        }
        isPhysicallyPinned.set(false);
        LOGGER.info(
            "wal {} is unpinned by pipe engine when checking time to live",
            walEntryHandler.getMemTableId());
        return true;
      } else {
        return false;
      }
    } else {
      LOGGER.info(
          "wal {} is not pinned physically when checking time to live",
          walEntryHandler.getMemTableId());
      return true;
    }
  }

  @Override
  public final void close() {
    if (isPhysicallyPinned.get()) {
      try {
        unpinInternal();
      } catch (MemTablePinException e) {
        LOGGER.error(
            "failed to unpin wal {} when closing pipe wal resource, because {}",
            walEntryHandler.getMemTableId(),
            e.getMessage());
      }
      isPhysicallyPinned.set(false);
      LOGGER.info(
          "wal {} is unpinned by pipe engine when closing pipe wal resource",
          walEntryHandler.getMemTableId());
    }

    referenceCount.set(0);
  }

  public int getReferenceCount() {
    return referenceCount.get();
  }
}
