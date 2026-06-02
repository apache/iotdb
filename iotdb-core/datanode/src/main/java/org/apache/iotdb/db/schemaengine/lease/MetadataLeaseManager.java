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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.TestOnly;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongSupplier;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataLeaseManager.class);

  private final List<Runnable> leaseRecoveryListeners = new CopyOnWriteArrayList<>();

  private final LongSupplier nanoClock;
  private final LongSupplier fenceThresholdMsSupplier;

  private volatile long lastConfigNodeHeartbeatNanos;

  private MetadataLeaseManager() {
    this(
        System::nanoTime,
        () -> CommonDescriptor.getInstance().getConfig().getMetadataLeaseFenceMs());
  }

  MetadataLeaseManager(final LongSupplier nanoClock, final LongSupplier fenceThresholdMsSupplier) {
    this.nanoClock = nanoClock;
    this.fenceThresholdMsSupplier = fenceThresholdMsSupplier;
    // Startup registration performs a full resync, so treat construction time as a fresh contact.
    this.lastConfigNodeHeartbeatNanos = nanoClock.getAsLong();
  }

  /**
   * Register a listener to run when the lease recovers, i.e. a ConfigNode heartbeat arrives after
   * the lease had expired. Push-maintained caches (e.g. {@code DataNodeTableCache}) register here
   * to invalidate themselves on recovery, since they may have missed ConfigNode pushes while
   * fenced; subsequent lookups then re-fetch fresh state instead of trusting stale entries.
   */
  public void addLeaseRecoveryListener(final Runnable listener) {
    leaseRecoveryListeners.add(listener);
  }

  /**
   * Renew the lease: record that a ConfigNode heartbeat has just been received. If the lease had
   * expired (the DataNode was fenced), this heartbeat is a recovery, so the registered recovery
   * listeners run to drop possibly-stale ConfigNode-pushed caches before they are trusted again.
   */
  public void recordConfigNodeHeartbeat() {
    final boolean wasFenced = isFenced();
    this.lastConfigNodeHeartbeatNanos = nanoClock.getAsLong();
    if (wasFenced) {
      for (final Runnable listener : leaseRecoveryListeners) {
        try {
          listener.run();
        } catch (final Exception e) {
          // A misbehaving listener must not break heartbeat processing / lease renewal.
          LOGGER.warn("Metadata lease recovery listener failed", e);
        }
      }
    }
  }

  /** Milliseconds elapsed since the last ConfigNode heartbeat was received (never negative). */
  public long getMillisSinceLastConfigNodeHeartbeat() {
    final long elapsedNanos = nanoClock.getAsLong() - lastConfigNodeHeartbeatNanos;
    return elapsedNanos > 0 ? elapsedNanos / 1_000_000L : 0L;
  }

  /** Whether the metadata lease has expired (no ConfigNode heartbeat within {@code T_fence}). */
  public boolean isFenced() {
    return getMillisSinceLastConfigNodeHeartbeat() > fenceThresholdMsSupplier.getAsLong();
  }

  /** Force the lease to appear expired, for tests that exercise fail-closed behavior. */
  @TestOnly
  public void expireLeaseForTest() {
    this.lastConfigNodeHeartbeatNanos =
        nanoClock.getAsLong() - (fenceThresholdMsSupplier.getAsLong() + 1_000L) * 1_000_000L;
  }

  public static MetadataLeaseManager getInstance() {
    return MetadataLeaseManagerHolder.INSTANCE;
  }

  private static final class MetadataLeaseManagerHolder {
    private static final MetadataLeaseManager INSTANCE = new MetadataLeaseManager();

    private MetadataLeaseManagerHolder() {}
  }
}
