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

package org.apache.iotdb.db.schemaengine.lease;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.MetadataLeaseFencedException;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeSchemaMessages;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TreeDeviceSchemaCacheManager;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.function.LongSupplier;

import static org.apache.iotdb.commons.concurrent.ThreadName.CHECK_DN_LEASE_STATUS;
import static org.apache.iotdb.commons.concurrent.ThreadName.RELOAD_TABLE_METADATA_CACHE;

/**
 * Tracks the DataNode's "metadata lease" with the ConfigNode. The ConfigNode periodically sends
 * heartbeats to the DataNode; while these arrive the DataNode may trust its ConfigNode-pushed
 * metadata caches (table/tree schema, device attributes, templates, TTL, permissions, ...). If no
 * heartbeat is received within {@code metadata_lease_fence_ms} ({@code T_fence}), the lease has
 * expired and the DataNode must self-fence: stop trusting those caches so a partitioned DataNode
 * cannot serve stale schema and generate dirty data.
 *
 * <p>This class only tracks the lease state; wiring fail-closed behavior into the read/write/auth
 * paths and resync-on-recovery is done by the respective subsystems.
 *
 * <p>A monotonic clock ({@link System#nanoTime()}) is used so the lease is immune to wall-clock
 * adjustments. The clock and fence threshold are injectable for testing.
 */
public class MetadataLeaseManager {

  @FunctionalInterface
  interface MetadataAction {
    void execute();
  }

  private final Logger LOGGER = LoggerFactory.getLogger(MetadataLeaseManager.class);

  private final List<MetadataAction> clearCacheList;
  private final List<MetadataAction> pullMetaList;

  private final LongSupplier nanoClock;
  private final LongSupplier fenceThresholdMs;

  private volatile long lastConfigNodeHeartbeatNanos;

  private final AtomicBoolean hasPullTaskNowRef;
  private final AtomicStampedReference<MetadataState> metadataStateRef;
  private final ExecutorService pullExecutorService;
  private final ScheduledExecutorService checkLeaseStatusExecutor;

  private enum MetadataState {
    NORMAL,
    CACHE_CLEARING,
    CACHE_CLEARED,
    NEED_CLEAR,
    PULLING,
    PULL_OR_INIT_FAILED;
  }

  private MetadataLeaseManager() {
    this(
        System::nanoTime,
        () -> CommonDescriptor.getInstance().getConfig().getMetadataLeaseFenceMs(),
        defaultClearCacheList(),
        defaultPullMetaList(),
        IoTDBThreadPoolFactory.newCachedThreadPool(RELOAD_TABLE_METADATA_CACHE.getName()),
        IoTDBDescriptor.getInstance().getConfig().getCheckDnLeaseStatusIntervalMs(),
        IoTDBThreadPoolFactory.newScheduledThreadPool(1, CHECK_DN_LEASE_STATUS.getName()));
  }

  private static List<MetadataAction> defaultClearCacheList() {
    return Arrays.asList(
        () -> ClusterPartitionFetcher.getInstance().invalidAllCache(),
        () -> DataNodeTableCache.getInstance().invalidateAll(),
        () -> TreeDeviceSchemaCacheManager.getInstance().cleanUp());
  }

  private static List<MetadataAction> defaultPullMetaList() {
    return Collections.singletonList(
        () -> DataNodeTableCache.getInstance().reloadTableCacheAfterLeaseRecovery());
  }

  MetadataLeaseManager(
      final LongSupplier nanoClock,
      final LongSupplier fenceThresholdMs,
      final List<MetadataAction> clearCacheList,
      final List<MetadataAction> pullMetaList,
      final ExecutorService pullExecutorService,
      final long checkDnLeaseStatusIntervalMs,
      final ScheduledExecutorService checkLeaseStatusExecutor) {
    this.nanoClock = nanoClock;
    this.fenceThresholdMs = fenceThresholdMs;
    this.clearCacheList = new ArrayList<>(clearCacheList);
    this.pullMetaList = new ArrayList<>(pullMetaList);
    // Startup registration performs a full re-sync, so treat construction time as a fresh contact.
    this.lastConfigNodeHeartbeatNanos = nanoClock.getAsLong();

    metadataStateRef = new AtomicStampedReference<>(MetadataState.NORMAL, 0);
    hasPullTaskNowRef = new AtomicBoolean(false);
    this.pullExecutorService = pullExecutorService;
    this.checkLeaseStatusExecutor = checkLeaseStatusExecutor;
    if (this.checkLeaseStatusExecutor != null) {
      ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
          this.checkLeaseStatusExecutor,
          this::checkLeaseStatus,
          0,
          checkDnLeaseStatusIntervalMs,
          TimeUnit.MILLISECONDS);
    }
  }

  /** Renew the lease: record that a ConfigNode heartbeat has just been received */
  public void triggerCheckWithHeartBeat() {
    if (metadataStateRef.getReference() == MetadataState.NORMAL && !hasOutOfLease()) {
      // If the lease is about to expire, a cache-clear thread may race with a new CN heartbeat.
      //  In that case the heartbeat only refreshes the timestamp;
      //  And the state is no longer NORMAL, so the next heartbeat will schedule
      //  metadata pulling and make the cache available again
      this.lastConfigNodeHeartbeatNanos = nanoClock.getAsLong();
      return;
    }
    boolean hasPullTaskNow = hasPullTaskNowRef.get();
    if (hasPullTaskNow) {
      return;
    }
    if (hasPullTaskNowRef.compareAndSet(false, true)) {
      try {
        pullExecutorService.submit(this::clearCacheAndPullMetaData);
      } catch (Exception e) {
        LOGGER.error(DataNodeSchemaMessages.FAILED_TO_SUBMIT_METADATA_PULL_TASK, e);
        hasPullTaskNowRef.set(false);
      }
    }
  }

  private void clearCacheAndPullMetaData() {
    try {
      int[] stamp = new int[1];
      MetadataState metadataState = metadataStateRef.get(stamp);
      if (metadataState == MetadataState.PULLING || metadataState == MetadataState.CACHE_CLEARING) {
        LOGGER.error(DataNodeSchemaMessages.UNEXPECTED_METADATA_STATE, metadataState);
        return;
      }

      // clear the cache
      if (metadataState == MetadataState.NORMAL || metadataState == MetadataState.NEED_CLEAR) {
        if (!tryClearCache(metadataState, stamp[0])) {
          LOGGER.warn(
              DataNodeSchemaMessages.METADATA_LEASE_CACHE_CLEARING_IN_PROGRESS, metadataState);
          return;
        }
      }

      pullMetaDataAndInit();
    } finally {
      hasPullTaskNowRef.set(false);
    }
  }

  /**
   * Attempts to CAS {@code currentState} → {@code CACHE_CLEARING}, then executes all cache-clear
   * actions. Sets state to {@code CACHE_CLEARED} on success, or {@code NEED_CLEAR} on failure.
   */
  private boolean tryClearCache(final MetadataState currentState, final int currentStamp) {
    if (!metadataStateRef.compareAndSet(
        currentState, MetadataState.CACHE_CLEARING, currentStamp, currentStamp + 1)) {
      return false;
    }
    try {
      clearCacheList.forEach(MetadataAction::execute);
    } catch (Throwable t) {
      metadataStateRef.set(MetadataState.NEED_CLEAR, metadataStateRef.getStamp() + 1);
      LOGGER.error(DataNodeSchemaMessages.FAILED_TO_CLEAR_METADATA_CACHE, t);
      rethrowUnchecked(t);
    }
    metadataStateRef.set(MetadataState.CACHE_CLEARED, metadataStateRef.getStamp() + 1);
    return true;
  }

  private void pullMetaDataAndInit() {
    int[] stamp = new int[1];
    MetadataState metadataState = metadataStateRef.get(stamp);
    if (metadataState != MetadataState.CACHE_CLEARED
        && metadataState != MetadataState.PULL_OR_INIT_FAILED) {
      LOGGER.error(DataNodeSchemaMessages.UNEXPECTED_METADATA_STATE, metadataState);
      return;
    }

    if (!metadataStateRef.compareAndSet(
        metadataState, MetadataState.PULLING, stamp[0], stamp[0] + 1)) {
      LOGGER.error(DataNodeSchemaMessages.FAILED_TO_MARK_METADATA_STATE_AS_PULLING, metadataState);
      return;
    }

    for (final MetadataAction action : pullMetaList) {
      try {
        action.execute();
      } catch (final Throwable t) {
        metadataStateRef.set(MetadataState.PULL_OR_INIT_FAILED, metadataStateRef.getStamp() + 1);
        LOGGER.error(DataNodeSchemaMessages.FAILED_TO_PULL_OR_INIT_METADATA, t);
        rethrowUnchecked(t);
      }
    }
    this.lastConfigNodeHeartbeatNanos = nanoClock.getAsLong();
    metadataStateRef.set(MetadataState.NORMAL, metadataStateRef.getStamp() + 1);
  }

  private static void rethrowUnchecked(final Throwable t) {
    if (t instanceof Error) {
      throw (Error) t;
    }
    if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    }
    throw new RuntimeException(t);
  }

  private boolean hasOutOfLease() {
    return getMillisSinceLastConfigNodeHeartbeat() > fenceThresholdMs.getAsLong();
  }

  /** Milliseconds elapsed since the last ConfigNode heartbeat was received (never negative). */
  public long getMillisSinceLastConfigNodeHeartbeat() {
    final long elapsedNanos = nanoClock.getAsLong() - lastConfigNodeHeartbeatNanos;
    return elapsedNanos > 0 ? elapsedNanos / 1_000_000L : 0L;
  }

  public boolean isFenced() {
    return metadataStateRef.getReference() != MetadataState.NORMAL;
  }

  void checkLeaseStatus() {
    int[] stampHolder = new int[1];
    MetadataState metadataState = metadataStateRef.get(stampHolder);
    if (metadataState != MetadataState.NORMAL) {
      return;
    }
    if (hasOutOfLease()) {
      metadataStateRef.compareAndSet(
          MetadataState.NORMAL, MetadataState.NEED_CLEAR, stampHolder[0], stampHolder[0] + 1);
    }
  }

  /**
   * Fail closed when the metadata lease has expired: a fenced DataNode may hold a stale
   * table-schema cache (it could have missed a ConfigNode invalidation while partitioned), so
   * refuse to serve it rather than risk validating writes/queries against stale schema and
   * producing dirty data.
   */
  public void failIfMetadataLeaseFenced() {
    if (isFenced()) {
      throw new MetadataLeaseFencedException(DataNodeSchemaMessages.METADATA_LEASE_IS_FENCED);
    }
  }

  /** Force the lease to appear expired, for tests that exercise fail-closed behavior. */
  @TestOnly
  public void recoveryLeaseForTest(boolean recovery) {
    if (recovery) {
      this.lastConfigNodeHeartbeatNanos = nanoClock.getAsLong();
      metadataStateRef.set(MetadataState.NORMAL, 0);
    } else {
      this.lastConfigNodeHeartbeatNanos =
          nanoClock.getAsLong() - (fenceThresholdMs.getAsLong() + 1_000L) * 1_000_000L;
    }
  }

  public static MetadataLeaseManager getInstance() {
    return MetadataLeaseManagerHolder.INSTANCE;
  }

  private static final class MetadataLeaseManagerHolder {
    private static final MetadataLeaseManager INSTANCE = new MetadataLeaseManager();

    private MetadataLeaseManagerHolder() {}
  }
}
