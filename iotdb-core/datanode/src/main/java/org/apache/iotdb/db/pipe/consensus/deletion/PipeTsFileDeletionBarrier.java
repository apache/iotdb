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

package org.apache.iotdb.db.pipe.consensus.deletion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A lightweight barrier for coordinating delete materialization and TsFile transfer.
 *
 * <p>In IoTDB, a {@code DELETE} is first published to pipe/consensus and then its modifications are
 * materialized to sealed TsFiles as mod files. Under concurrent flush and delete, it is possible
 * that a TsFile event gets pinned/transferred before its corresponding mod file is generated,
 * causing followers to miss deletions if they apply delete before loading that TsFile.
 *
 * <p>This barrier allows the delete path to mark a set of TsFiles as "pending deletion
 * materialization". A TsFile event will wait (best effort) for the pending count to drop to zero
 * before pinning/snapshotting the TsFile and its mod.
 */
public class PipeTsFileDeletionBarrier {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileDeletionBarrier.class);

  private final ConcurrentMap<String, PendingCounter> tsFilePath2PendingCounter =
      new ConcurrentHashMap<>();

  public void registerPendingDeletion(final Collection<String> tsFilePaths) {
    if (Objects.isNull(tsFilePaths) || tsFilePaths.isEmpty()) {
      return;
    }
    tsFilePaths.forEach(this::registerPendingDeletion);
  }

  public void registerPendingDeletion(final String tsFilePath) {
    if (Objects.isNull(tsFilePath)) {
      return;
    }

    tsFilePath2PendingCounter.compute(
        tsFilePath,
        (k, v) -> {
          final PendingCounter counter = v == null ? new PendingCounter() : v;
          counter.pendingCount.incrementAndGet();
          return counter;
        });
  }

  public void releasePendingDeletion(final Collection<String> tsFilePaths) {
    if (Objects.isNull(tsFilePaths) || tsFilePaths.isEmpty()) {
      return;
    }
    tsFilePaths.forEach(this::releasePendingDeletion);
  }

  public void releasePendingDeletion(final String tsFilePath) {
    if (Objects.isNull(tsFilePath)) {
      return;
    }

    final PendingCounter counter = tsFilePath2PendingCounter.get(tsFilePath);
    if (counter == null) {
      return;
    }

    final int remaining = counter.pendingCount.decrementAndGet();
    if (remaining > 0) {
      return;
    }

    if (remaining < 0) {
      LOGGER.warn(
          "PipeTsFileDeletionBarrier: pending deletion count becomes negative for {}, current {}. "
              + "This may indicate mismatched register/release.",
          tsFilePath,
          remaining);
    }

    tsFilePath2PendingCounter.remove(tsFilePath, counter);
    synchronized (counter.monitor) {
      counter.monitor.notifyAll();
    }
  }

  public void awaitPendingDeletionIfNecessary(final String tsFilePath) {
    if (Objects.isNull(tsFilePath)) {
      return;
    }

    final PendingCounter counter = tsFilePath2PendingCounter.get(tsFilePath);
    if (counter == null) {
      return;
    }

    synchronized (counter.monitor) {
      while (counter.pendingCount.get() > 0) {
        try {
          counter.monitor.wait();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          LOGGER.warn(
              "PipeTsFileDeletionBarrier: interrupted while waiting pending deletions for {}, "
                  + "will continue without waiting.",
              tsFilePath,
              e);
          return;
        }
      }
    }
  }

  private static class PendingCounter {
    private final AtomicInteger pendingCount = new AtomicInteger(0);
    private final Object monitor = new Object();
  }

  /////////////////////////////// singleton ///////////////////////////////

  private static class PipeTsFileDeletionBarrierHolder {
    private static final PipeTsFileDeletionBarrier INSTANCE = new PipeTsFileDeletionBarrier();

    private PipeTsFileDeletionBarrierHolder() {
      // empty constructor
    }
  }

  public static PipeTsFileDeletionBarrier getInstance() {
    return PipeTsFileDeletionBarrierHolder.INSTANCE;
  }

  private PipeTsFileDeletionBarrier() {
    // empty constructor
  }
}
