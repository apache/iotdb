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

package org.apache.iotdb.confignode.manager.lease;

import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import static org.apache.iotdb.confignode.manager.lease.ClusterCachePropagator.DEFAULT_PROCEED_MARGIN_MS;

/**
 * Tracks, per DataNode, the time the ConfigNode last received a <em>successful heartbeat
 * response</em> from it, stamped with the ConfigNode's own monotonic clock at receipt.
 *
 * <p>This is the sound signal for deciding whether an unreachable DataNode has self-fenced (used by
 * the metadata-lease verdict). It must be kept separate from the load-cache {@code
 * NodeHeartbeatSample}s, which (a) record the heartbeat <em>send</em> time echoed back by the
 * DataNode — not response receipt — and (b) are advanced to the current time by failure ({@code
 * onError}) samples. Either property would break the verdict: send-time can make the ConfigNode
 * believe a DataNode is fenced while it just renewed from a delayed heartbeat, and failure-advanced
 * time would keep the age from ever growing.
 *
 * <p>By construction there is no method that advances the time on failure: only {@link
 * #recordSuccessfulResponse(int)} updates it. A never-contacted DataNode reads as age 0 (treated as
 * just-contacted) so the verdict never wrongly declares an unknown DataNode fenced.
 */
public class DataNodeContactTracker {

  private final LongSupplier nanoClock;

  private final Map<Integer, Long> lastSuccessfulResponseNanos = new ConcurrentHashMap<>();

  private final Map<Integer, Long> lastSentFenceThresholdMs = new ConcurrentHashMap<>();

  private DataNodeContactTracker() {
    this(System::nanoTime);
  }

  DataNodeContactTracker(final LongSupplier nanoClock) {
    this.nanoClock = nanoClock;
  }

  /** Record that a successful heartbeat response from the DataNode was just received. */
  public void recordSuccessfulResponse(final int dataNodeId) {
    lastSuccessfulResponseNanos.put(dataNodeId, nanoClock.getAsLong());
    lastSentFenceThresholdMs.put(dataNodeId, getCurrentFenceThresholdMs());
  }

  /**
   * Milliseconds since the ConfigNode last received a successful heartbeat response from the
   * DataNode. Returns 0 (treated as just-contacted) if never recorded — conservative, so an unknown
   * DataNode is never declared fenced.
   */
  public long getMillisSinceLastSuccessfulResponse(final int dataNodeId) {
    final Long lastNanos = lastSuccessfulResponseNanos.get(dataNodeId);
    if (lastNanos == null) {
      return 0L;
    }
    final long elapsedNanos = nanoClock.getAsLong() - lastNanos;
    return elapsedNanos > 0 ? elapsedNanos / 1_000_000L : 0L;
  }

  public boolean isDataNodeFenced(final int dataNodeId) {
    return getMillisSinceLastSuccessfulResponse(dataNodeId)
        > (ConfigNodeDescriptor.getInstance().getConf().getMetadataLeaseFenceMs()
            + DEFAULT_PROCEED_MARGIN_MS);
  }

  /**
   * On leadership acquisition, conservatively reset contact ages so this leader does not infer any
   * dataNode as fencing state from its previous record history.
   */
  public void onLeadershipAcquired(final Collection<Integer> registeredDataNodeIds) {
    final long now = nanoClock.getAsLong();
    for (final Integer dataNodeId : registeredDataNodeIds) {
      lastSuccessfulResponseNanos.put(dataNodeId, now);
    }
    lastSentFenceThresholdMs.clear();
  }

  public void removeDataNode(final int dataNodeId) {
    lastSuccessfulResponseNanos.remove(dataNodeId);
    lastSentFenceThresholdMs.remove(dataNodeId);
  }

  /** The current fence threshold value from the ConfigNode's own configuration. */
  public long getCurrentFenceThresholdMs() {
    return ConfigNodeDescriptor.getInstance().getConf().getMetadataLeaseFenceMs();
  }

  /**
   * Returns true if this DataNode has already received the current (confirmed by a successful
   * heartbeat response sent after that value was loaded).
   */
  public boolean hasDeliveredCurrentFenceThreshold(final int dataNodeId) {
    final Long lastSent = lastSentFenceThresholdMs.get(dataNodeId);
    return lastSent != null && lastSent == getCurrentFenceThresholdMs();
  }

  public static DataNodeContactTracker getInstance() {
    return DataNodeContactTrackerHolder.INSTANCE;
  }

  private static final class DataNodeContactTrackerHolder {
    private static final DataNodeContactTracker INSTANCE = new DataNodeContactTracker();

    private DataNodeContactTrackerHolder() {}
  }
}
