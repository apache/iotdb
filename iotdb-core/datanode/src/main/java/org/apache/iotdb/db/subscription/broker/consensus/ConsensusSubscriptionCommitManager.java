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

package org.apache.iotdb.db.subscription.broker.consensus;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.mpp.rpc.thrift.TSyncSubscriptionProgressReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Manages commit state for consensus-based subscriptions.
 *
 * <p>This manager tracks which events have been committed by consumers using their end search
 * indices directly (no intermediate commitId mapping). It maintains the progress for each
 * (consumerGroup, topic, region) triple and supports persistence and recovery.
 *
 * <p>Progress is tracked <b>per-region</b> because searchIndex is region-local — each DataRegion
 * has its own independent WAL with its own searchIndex namespace. Using a single state per topic
 * would cause TreeSet deduplication bugs when different regions emit the same searchIndex value.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Track outstanding (dispatched but not-yet-committed) events by searchIndex
 *   <li>Handle commit/ack from consumers
 *   <li>Persist and recover progress state
 * </ul>
 */
public class ConsensusSubscriptionCommitManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusSubscriptionCommitManager.class);

  private static final String PROGRESS_FILE_PREFIX = "consensus_subscription_progress_";
  private static final String PROGRESS_FILE_SUFFIX = ".dat";

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  /** Client manager for DataNode-to-DataNode RPC (progress broadcast). */
  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      SYNC_DN_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  /** Minimum interval (ms) between broadcasts for the same (consumerGroup, topic, region). */
  private static final long MIN_BROADCAST_INTERVAL_MS = 5000;

  /** Rate-limiting: last broadcast timestamp per key. */
  private final Map<String, Long> lastBroadcastTime = new ConcurrentHashMap<>();

  /** Single-threaded executor for fire-and-forget broadcasts. */
  private final ExecutorService broadcastExecutor =
      Executors.newSingleThreadExecutor(
          r -> {
            final Thread t = new Thread(r, "SubscriptionProgressBroadcast");
            t.setDaemon(true);
            return t;
          });

  /** Key: "consumerGroupId##topicName##regionId" -> progress tracking state */
  private final Map<String, ConsensusSubscriptionCommitState> commitStates =
      new ConcurrentHashMap<>();

  private final String persistDir;

  private ConsensusSubscriptionCommitManager() {
    this.persistDir =
        IoTDBDescriptor.getInstance().getConfig().getSystemDir()
            + File.separator
            + "subscription"
            + File.separator
            + "consensus_progress";
    final File dir = new File(persistDir);
    if (!dir.exists()) {
      dir.mkdirs();
    }
  }

  /**
   * Gets or creates the commit state for a specific (consumerGroup, topic, region) triple.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID
   * @return the commit state
   */
  public ConsensusSubscriptionCommitState getOrCreateState(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    return commitStates.computeIfAbsent(
        key,
        k -> {
          // Try to recover from persisted local state
          final ConsensusSubscriptionCommitState recovered = tryRecover(key);
          if (recovered != null) {
            return recovered;
          }
          // Fallback: query ConfigNode for the last known committed search index
          final long fallbackSearchIndex =
              queryCommitProgressFromConfigNode(consumerGroupId, topicName, regionId);
          return new ConsensusSubscriptionCommitState(
              new SubscriptionConsensusProgress(0L, fallbackSearchIndex, 0L));
        });
  }

  public boolean hasPersistedState(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    return getProgressFile(generateKey(consumerGroupId, topicName, regionId)).exists();
  }

  /**
   * Records a dispatched event's (epoch, syncIndex) for commit tracking.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID
   * @param epoch the epoch of the dispatched event
   * @param syncIndex the syncIndex of the dispatched event
   */
  public void recordMapping(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final long epoch,
      final long syncIndex) {
    final ConsensusSubscriptionCommitState state =
        getOrCreateState(consumerGroupId, topicName, regionId);
    state.recordMapping(epoch, syncIndex);
  }

  /**
   * Handles commit (ack) for an event. Updates the progress and potentially advances the committed
   * position.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID
   * @param epoch the epoch of the committed event
   * @param syncIndex the syncIndex of the committed event
   * @return true if commit handled successfully
   */
  public boolean commit(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final long epoch,
      final long syncIndex) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      LOGGER.warn(
          "ConsensusSubscriptionCommitManager: Cannot commit for unknown state, "
              + "consumerGroupId={}, topicName={}, regionId={}, epoch={}, syncIndex={}",
          consumerGroupId,
          topicName,
          regionId,
          epoch,
          syncIndex);
      return false;
    }
    final boolean success = state.commit(epoch, syncIndex);
    if (success) {
      // Periodically persist progress
      persistProgressIfNeeded(key, state);
      // Broadcast to followers (rate-limited, async, fire-and-forget)
      maybeBroadcast(
          key,
          consumerGroupId,
          topicName,
          regionId,
          state.getCommittedEpoch(),
          state.getCommittedSyncIndex());
    }
    return success;
  }

  /**
   * Gets the current committed search index for a specific region's state.
   *
   * @deprecated Use {@link #getCommittedEpoch} and {@link #getCommittedSyncIndex} instead.
   * @return the committed sync index, or -1 if no state exists
   */
  @Deprecated
  public long getCommittedSearchIndex(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      return -1;
    }
    return state.getCommittedSyncIndex();
  }

  public long getCommittedEpoch(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    return state != null ? state.getCommittedEpoch() : 0;
  }

  public long getCommittedSyncIndex(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    return state != null ? state.getCommittedSyncIndex() : -1;
  }

  /**
   * Removes state for a specific (consumerGroup, topic, region) triple.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID
   */
  public void removeState(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    commitStates.remove(key);
    // Clean up persisted file
    final File file = getProgressFile(key);
    if (file.exists()) {
      file.delete();
    }
  }

  /**
   * Removes all states for a given (consumerGroup, topic) pair across all regions. Used during
   * subscription teardown when the individual regionIds may not be readily available.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   */
  public void removeAllStatesForTopic(final String consumerGroupId, final String topicName) {
    final String prefix = consumerGroupId + KEY_SEPARATOR + topicName + KEY_SEPARATOR;
    final Iterator<Map.Entry<String, ConsensusSubscriptionCommitState>> it =
        commitStates.entrySet().iterator();
    while (it.hasNext()) {
      final Map.Entry<String, ConsensusSubscriptionCommitState> entry = it.next();
      if (entry.getKey().startsWith(prefix)) {
        it.remove();
        final File file = getProgressFile(entry.getKey());
        if (file.exists()) {
          file.delete();
        }
      }
    }
  }

  /**
   * Resets the commit state for a specific (consumerGroup, topic, region) triple. Used by seek
   * operations to discard all outstanding commit tracking and restart from the specified position.
   */
  public void resetState(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final long epoch,
      final long syncIndex) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      LOGGER.warn(
          "ConsensusSubscriptionCommitManager: Cannot reset unknown state, "
              + "consumerGroupId={}, topicName={}, regionId={}",
          consumerGroupId,
          topicName,
          regionId);
      return;
    }
    state.resetForSeek(epoch, syncIndex);
    persistProgress(key, state);
  }

  /** Persists all states. Should be called during graceful shutdown. */
  public void persistAll() {
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      persistProgress(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Collects all current committed progress for reporting to ConfigNode. Returns syncIndex values
   * for backward compatibility; epoch information is available via the state objects directly.
   */
  public Map<String, Long> collectAllProgress(final int dataNodeId) {
    final Map<String, Long> result = new ConcurrentHashMap<>();
    final String suffix = KEY_SEPARATOR + dataNodeId;
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      result.put(entry.getKey() + suffix, entry.getValue().getCommittedSyncIndex());
    }
    return result;
  }

  // ======================== Progress Broadcast (Leader → Follower) ========================

  /**
   * Broadcasts committed progress to followers if enough time has elapsed since the last broadcast
   * for this key. The broadcast is async and fire-and-forget.
   */
  private void maybeBroadcast(
      final String key,
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final long committedEpoch,
      final long committedSyncIndex) {
    final long now = System.currentTimeMillis();
    final Long last = lastBroadcastTime.get(key);
    if (last != null && now - last < MIN_BROADCAST_INTERVAL_MS) {
      return;
    }
    lastBroadcastTime.put(key, now);
    broadcastExecutor.submit(
        () ->
            doBroadcast(consumerGroupId, topicName, regionId, committedEpoch, committedSyncIndex));
  }

  /**
   * Sends committed progress to all follower replicas of the given region. Uses the partition cache
   * to discover replica endpoints and skips the local DataNode.
   */
  private void doBroadcast(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final long epoch,
      final long syncIndex) {
    final int localDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    try {
      final List<TRegionReplicaSet> replicaSets =
          ClusterPartitionFetcher.getInstance()
              .getRegionReplicaSet(
                  Collections.singletonList(regionId.convertToTConsensusGroupId()));
      if (replicaSets.isEmpty()) {
        return;
      }
      final String regionIdStr = regionId.toString();
      final TSyncSubscriptionProgressReq req =
          new TSyncSubscriptionProgressReq(
              consumerGroupId, topicName, regionIdStr, epoch, syncIndex);

      for (final TDataNodeLocation location : replicaSets.get(0).getDataNodeLocations()) {
        if (location.getDataNodeId() == localDataNodeId) {
          continue; // skip self
        }
        final TEndPoint endpoint = location.getInternalEndPoint();
        try (final SyncDataNodeInternalServiceClient client =
            SYNC_DN_CLIENT_MANAGER.borrowClient(endpoint)) {
          client.syncSubscriptionProgress(req);
        } catch (final ClientManagerException | TException e) {
          LOGGER.debug(
              "Failed to broadcast subscription progress to DataNode {} at {}: {}",
              location.getDataNodeId(),
              endpoint,
              e.getMessage());
        }
      }
    } catch (final Exception e) {
      LOGGER.debug(
          "Failed to broadcast subscription progress for region {}: {}", regionId, e.getMessage());
    }
  }

  /**
   * Receives a committed progress broadcast from another DataNode (Leader). Updates local state if
   * the broadcast progress is ahead of the current local progress.
   */
  public void receiveProgressBroadcast(
      final String consumerGroupId,
      final String topicName,
      final String regionIdStr,
      final long epoch,
      final long syncIndex) {
    final String key = consumerGroupId + KEY_SEPARATOR + topicName + KEY_SEPARATOR + regionIdStr;
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state != null) {
      // Update only if broadcast is ahead
      state.updateFromBroadcast(epoch, syncIndex);
      persistProgressIfNeeded(key, state);
    } else {
      // Create a new state from the broadcast progress
      final ConsensusSubscriptionCommitState newState =
          new ConsensusSubscriptionCommitState(
              new SubscriptionConsensusProgress(epoch, syncIndex, 0L));
      commitStates.putIfAbsent(key, newState);
      persistProgress(key, commitStates.get(key));
    }
    LOGGER.debug(
        "Received subscription progress broadcast: consumerGroupId={}, topicName={}, "
            + "regionId={}, epoch={}, syncIndex={}",
        consumerGroupId,
        topicName,
        regionIdStr,
        epoch,
        syncIndex);
  }

  // ======================== Helper Methods ========================

  // Use a separator that cannot appear in consumerGroupId, topicName, or regionId
  // to prevent key collisions (e.g., "a_b" + "c" vs "a" + "b_c").
  private static final String KEY_SEPARATOR = "##";

  private String generateKey(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    return consumerGroupId + KEY_SEPARATOR + topicName + KEY_SEPARATOR + regionId.toString();
  }

  private File getProgressFile(final String key) {
    return new File(persistDir, PROGRESS_FILE_PREFIX + key + PROGRESS_FILE_SUFFIX);
  }

  private ConsensusSubscriptionCommitState tryRecover(final String key) {
    final File file = getProgressFile(key);
    if (!file.exists()) {
      return null;
    }
    try (final FileInputStream fis = new FileInputStream(file)) {
      final byte[] bytes = new byte[(int) file.length()];
      fis.read(bytes);
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      return ConsensusSubscriptionCommitState.deserialize(buffer);
    } catch (final IOException e) {
      LOGGER.warn("Failed to recover consensus subscription progress from {}", file, e);
      return null;
    }
  }

  private long queryCommitProgressFromConfigNode(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    try (final ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TGetCommitProgressReq req =
          new TGetCommitProgressReq(
              consumerGroupId,
              topicName,
              regionId.getId(),
              IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
      final TGetCommitProgressResp resp = configNodeClient.getCommitProgress(req);
      if (resp.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && resp.isSetCommittedSearchIndex()) {
        LOGGER.info(
            "ConsensusSubscriptionCommitManager: recovered committedSearchIndex={} from "
                + "ConfigNode for consumerGroupId={}, topicName={}, regionId={}",
            resp.committedSearchIndex,
            consumerGroupId,
            topicName,
            regionId);
        return resp.committedSearchIndex;
      }
    } catch (final ClientManagerException | TException e) {
      LOGGER.warn(
          "ConsensusSubscriptionCommitManager: failed to query commit progress from ConfigNode "
              + "for consumerGroupId={}, topicName={}, regionId={}, starting from 0",
          consumerGroupId,
          topicName,
          regionId,
          e);
    }
    return 0L;
  }

  private void persistProgressIfNeeded(
      final String key, final ConsensusSubscriptionCommitState state) {
    final int interval =
        SubscriptionConfig.getInstance().getSubscriptionConsensusCommitPersistInterval();
    if (interval > 0 && state.getProgress().getCommitIndex() % interval == 0) {
      persistProgress(key, state);
    }
  }

  private void persistProgress(final String key, final ConsensusSubscriptionCommitState state) {
    final File file = getProgressFile(key);
    try (final FileOutputStream fos = new FileOutputStream(file);
        final DataOutputStream dos = new DataOutputStream(fos)) {
      state.serialize(dos);
      dos.flush();
      if (SubscriptionConfig.getInstance().isSubscriptionConsensusCommitFsyncEnabled()) {
        fos.getFD().sync();
      }
    } catch (final IOException e) {
      LOGGER.warn("Failed to persist consensus subscription progress to {}", file, e);
    }
  }

  // ======================== Inner State Class ========================

  /**
   * Tracks commit state for a single (consumerGroup, topic, region) triple using (epoch, syncIndex)
   * pairs for cross-leader-migration consistency. Outstanding and committed positions are tracked
   * as ProgressKey objects (epoch, syncIndex) rather than raw searchIndex values.
   */
  public static class ConsensusSubscriptionCommitState {

    private final SubscriptionConsensusProgress progress;

    /** LRU set of recently committed keys for idempotent re-commit detection. */
    private static final int RECENTLY_COMMITTED_CAPACITY = 1024;

    private final Set<ProgressKey> recentlyCommittedKeys =
        Collections.newSetFromMap(
            new LinkedHashMap<ProgressKey, Boolean>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry<ProgressKey, Boolean> eldest) {
                return size() > RECENTLY_COMMITTED_CAPACITY;
              }
            });

    /**
     * Tracks the safe recovery position as (epoch, syncIndex). Only advances contiguously — never
     * jumps over uncommitted gaps.
     */
    private volatile long committedEpoch;

    private volatile long committedSyncIndex;

    /**
     * Tracks the maximum committed position (may be ahead of committed when out-of-order commits
     * exist).
     */
    private ProgressKey maxCommittedKey;

    /**
     * Tracks (epoch, syncIndex) pairs of dispatched but not-yet-committed events. On commit, the
     * frontier advances to just before the earliest uncommitted entry.
     */
    private final TreeSet<ProgressKey> outstandingKeys = new TreeSet<>();

    public ConsensusSubscriptionCommitState(final SubscriptionConsensusProgress progress) {
      this.progress = progress;
      this.committedEpoch = progress.getEpoch();
      this.committedSyncIndex = progress.getSyncIndex();
      this.maxCommittedKey = new ProgressKey(committedEpoch, committedSyncIndex);
    }

    public SubscriptionConsensusProgress getProgress() {
      return progress;
    }

    public long getCommittedEpoch() {
      return committedEpoch;
    }

    public long getCommittedSyncIndex() {
      return committedSyncIndex;
    }

    /**
     * @deprecated Use {@link #getCommittedSyncIndex()} instead.
     */
    @Deprecated
    public long getCommittedSearchIndex() {
      return committedSyncIndex;
    }

    /** Threshold for warning about outstanding (uncommitted) entries accumulation. */
    private static final int OUTSTANDING_SIZE_WARN_THRESHOLD = 10000;

    public void recordMapping(final long epoch, final long syncIndex) {
      synchronized (this) {
        outstandingKeys.add(new ProgressKey(epoch, syncIndex));
        final int size = outstandingKeys.size();
        if (size > OUTSTANDING_SIZE_WARN_THRESHOLD && size % OUTSTANDING_SIZE_WARN_THRESHOLD == 1) {
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: outstanding size ({}) exceeds threshold ({}), "
                  + "consumers may not be committing. committed=({},{}), maxCommitted={}",
              size,
              OUTSTANDING_SIZE_WARN_THRESHOLD,
              committedEpoch,
              committedSyncIndex,
              maxCommittedKey);
        }
      }
    }

    /**
     * Commits the specified event and advances the committed position contiguously.
     *
     * @param epoch the epoch of the event to commit
     * @param syncIndex the syncIndex of the event to commit
     * @return true if successfully committed
     */
    public boolean commit(final long epoch, final long syncIndex) {
      progress.incrementCommitIndex();
      final ProgressKey key = new ProgressKey(epoch, syncIndex);

      synchronized (this) {
        if (!outstandingKeys.remove(key)) {
          if (recentlyCommittedKeys.contains(key)) {
            LOGGER.debug(
                "ConsensusSubscriptionCommitState: idempotent re-commit for ({},{})",
                epoch,
                syncIndex);
            return true;
          }
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: unknown key ({},{}) for commit", epoch, syncIndex);
          return false;
        }
        recentlyCommittedKeys.add(key);
        if (key.compareTo(maxCommittedKey) > 0) {
          maxCommittedKey = key;
        }

        if (outstandingKeys.isEmpty()) {
          committedEpoch = maxCommittedKey.epoch;
          committedSyncIndex = maxCommittedKey.syncIndex;
        } else {
          // Can only advance to just before the earliest outstanding entry.
          // Within the same epoch, syncIndex is contiguous, so (epoch, syncIndex-1) is valid.
          // Across epochs, we cannot advance past the epoch boundary.
          final ProgressKey firstOutstanding = outstandingKeys.first();
          final ProgressKey candidate;
          if (firstOutstanding.syncIndex > 0) {
            candidate = new ProgressKey(firstOutstanding.epoch, firstOutstanding.syncIndex - 1);
          } else {
            // Edge case: syncIndex=0 means beginning of an epoch; committed stays at current
            candidate = new ProgressKey(committedEpoch, committedSyncIndex);
          }
          if (candidate.compareTo(new ProgressKey(committedEpoch, committedSyncIndex)) > 0) {
            committedEpoch = candidate.epoch;
            committedSyncIndex = candidate.syncIndex;
          }
        }
        progress.setEpoch(committedEpoch);
        progress.setSyncIndex(committedSyncIndex);
      }

      return true;
    }

    /**
     * Resets all commit tracking state for a seek operation. Clears all outstanding mappings and
     * resets progress to the new position.
     */
    public void resetForSeek(final long epoch, final long syncIndex) {
      synchronized (this) {
        outstandingKeys.clear();
        recentlyCommittedKeys.clear();
        committedEpoch = epoch;
        committedSyncIndex = syncIndex;
        maxCommittedKey = new ProgressKey(epoch, syncIndex);
        progress.setEpoch(epoch);
        progress.setSyncIndex(syncIndex);
      }
    }

    /**
     * Updates committed progress from a Leader broadcast. Only advances if the broadcast position
     * is ahead of the current local position.
     */
    public void updateFromBroadcast(final long epoch, final long syncIndex) {
      synchronized (this) {
        final ProgressKey incoming = new ProgressKey(epoch, syncIndex);
        if (incoming.compareTo(maxCommittedKey) > 0) {
          committedEpoch = epoch;
          committedSyncIndex = syncIndex;
          maxCommittedKey = incoming;
          progress.setEpoch(epoch);
          progress.setSyncIndex(syncIndex);
        }
      }
    }

    public void serialize(final DataOutputStream stream) throws IOException {
      progress.serialize(stream);
      stream.writeLong(committedEpoch);
      stream.writeLong(committedSyncIndex);
    }

    public static ConsensusSubscriptionCommitState deserialize(final ByteBuffer buffer) {
      final SubscriptionConsensusProgress progress =
          SubscriptionConsensusProgress.deserialize(buffer);
      final ConsensusSubscriptionCommitState state = new ConsensusSubscriptionCommitState(progress);
      state.committedEpoch = buffer.getLong();
      state.committedSyncIndex = buffer.getLong();
      state.maxCommittedKey = new ProgressKey(state.committedEpoch, state.committedSyncIndex);
      return state;
    }
  }

  // ======================== ProgressKey ========================

  /**
   * Comparable key for tracking commit progress: (epoch, syncIndex). Epoch takes priority; within
   * the same epoch, syncIndex determines order.
   */
  static final class ProgressKey implements Comparable<ProgressKey> {
    final long epoch;
    final long syncIndex;

    ProgressKey(final long epoch, final long syncIndex) {
      this.epoch = epoch;
      this.syncIndex = syncIndex;
    }

    @Override
    public int compareTo(final ProgressKey o) {
      final int cmp = Long.compare(epoch, o.epoch);
      return cmp != 0 ? cmp : Long.compare(syncIndex, o.syncIndex);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProgressKey)) {
        return false;
      }
      final ProgressKey that = (ProgressKey) o;
      return epoch == that.epoch && syncIndex == that.syncIndex;
    }

    @Override
    public int hashCode() {
      return Objects.hash(epoch, syncIndex);
    }

    @Override
    public String toString() {
      return "(" + epoch + "," + syncIndex + ")";
    }
  }

  // ======================== Singleton ========================

  private static class Holder {
    private static final ConsensusSubscriptionCommitManager INSTANCE =
        new ConsensusSubscriptionCommitManager();
  }

  public static ConsensusSubscriptionCommitManager getInstance() {
    return Holder.INSTANCE;
  }
}
