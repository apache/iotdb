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
import org.apache.iotdb.rpc.subscription.payload.poll.RegionProgress;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterId;
import org.apache.iotdb.rpc.subscription.payload.poll.WriterProgress;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
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
    final String regionIdString = regionId.toString();
    return commitStates.computeIfAbsent(
        key,
        k -> {
          // Try to recover from persisted local state
          final ConsensusSubscriptionCommitState recovered = tryRecover(key, regionIdString);
          if (recovered != null) {
            return recovered;
          }
          final ConsensusSubscriptionCommitState recoveredFromConfigNode =
              queryCommitProgressStateFromConfigNode(consumerGroupId, topicName, regionId);
          if (Objects.nonNull(recoveredFromConfigNode)) {
            return recoveredFromConfigNode;
          }
          return new ConsensusSubscriptionCommitState(
              regionIdString, new SubscriptionConsensusProgress());
        });
  }

  public boolean hasPersistedState(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    return getProgressFile(generateKey(consumerGroupId, topicName, regionId)).exists();
  }

  public void recordMapping(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final WriterId writerId,
      final WriterProgress writerProgress) {
    final ConsensusSubscriptionCommitState state =
        getOrCreateState(consumerGroupId, topicName, regionId);
    state.recordMapping(writerId, writerProgress);
  }

  public boolean commit(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final WriterId writerId,
      final WriterProgress writerProgress) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      LOGGER.warn(
          "ConsensusSubscriptionCommitManager: Cannot commit for unknown state, "
              + "consumerGroupId={}, topicName={}, regionId={}, writerId={}, writerProgress={}",
          consumerGroupId,
          topicName,
          regionId,
          writerId,
          writerProgress);
      return false;
    }
    final CommitOperationResult result = state.commitAndGetResult(writerId, writerProgress);
    if (result.isHandled()) {
      // Periodically persist progress
      persistProgressIfNeeded(key, state);
      if (result.hasAdvancedWriter()) {
        maybeBroadcast(
            key,
            consumerGroupId,
            topicName,
            regionId,
            result.getAdvancedWriterProgress(),
            result.getAdvancedWriterId());
      }
    }
    return result.isHandled();
  }

  public boolean commitWithoutOutstanding(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final WriterId writerId,
      final WriterProgress writerProgress) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      LOGGER.warn(
          "ConsensusSubscriptionCommitManager: Cannot direct-commit for unknown state, "
              + "consumerGroupId={}, topicName={}, regionId={}, writerId={}, writerProgress={}",
          consumerGroupId,
          topicName,
          regionId,
          writerId,
          writerProgress);
      return false;
    }
    final CommitOperationResult result =
        state.commitWithoutOutstandingAndGetResult(writerId, writerProgress);
    if (result.isHandled()) {
      persistProgressIfNeeded(key, state);
      if (result.hasAdvancedWriter()) {
        maybeBroadcast(
            key,
            consumerGroupId,
            topicName,
            regionId,
            result.getAdvancedWriterProgress(),
            result.getAdvancedWriterId());
      }
    }
    return result.isHandled();
  }

  public long getCommittedPhysicalTime(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    return state != null ? state.getCommittedPhysicalTime() : 0L;
  }

  public long getCommittedLocalSeq(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    return state != null ? state.getCommittedLocalSeq() : -1L;
  }

  public int getCommittedWriterNodeId(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    return state != null ? state.getCommittedWriterNodeId() : -1;
  }

  public long getCommittedWriterEpoch(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    return state != null ? state.getCommittedWriterEpoch() : 0L;
  }

  public WriterId getCommittedWriterId(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    return state != null ? state.getCommittedWriterId() : null;
  }

  public WriterProgress getCommittedWriterProgress(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    return state != null ? state.getCommittedWriterProgress() : null;
  }

  public RegionProgress getCommittedRegionProgress(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      return new RegionProgress(Collections.emptyMap());
    }
    return state.getCommittedRegionProgress();
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

  public void resetState(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final WriterId writerId,
      final WriterProgress writerProgress) {
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
    state.resetForSeek(writerId, writerProgress);
    persistProgress(key, state);
  }

  public void resetState(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final RegionProgress regionProgress) {
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
    state.resetForSeek(regionProgress);
    persistProgress(key, state);
  }

  /** Persists all states. Should be called during graceful shutdown. */
  public void persistAll() {
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      persistProgress(entry.getKey(), entry.getValue());
    }
  }

  public Map<String, ByteBuffer> collectAllRegionProgress(final int dataNodeId) {
    final Map<String, ByteBuffer> result = new ConcurrentHashMap<>();
    final String suffix = KEY_SEPARATOR + dataNodeId;
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      final RegionProgress regionProgress = entry.getValue().getCommittedRegionProgress();
      final ByteBuffer serialized = serializeRegionProgress(regionProgress);
      if (Objects.nonNull(serialized)) {
        result.put(entry.getKey() + suffix, serialized);
      }
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
      final WriterProgress committedWriterProgress,
      final WriterId committedWriterId) {
    if (Objects.isNull(committedWriterId) || Objects.isNull(committedWriterProgress)) {
      return;
    }
    final String broadcastKey = buildBroadcastKey(key, committedWriterId);
    final long now = System.currentTimeMillis();
    final Long last = lastBroadcastTime.get(broadcastKey);
    if (last != null && now - last < MIN_BROADCAST_INTERVAL_MS) {
      return;
    }
    lastBroadcastTime.put(broadcastKey, now);
    broadcastExecutor.submit(
        () ->
            doBroadcast(
                consumerGroupId, topicName, regionId, committedWriterProgress, committedWriterId));
  }

  /**
   * Sends committed progress to all follower replicas of the given region. Uses the partition cache
   * to discover replica endpoints and skips the local DataNode.
   */
  private void doBroadcast(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final WriterProgress writerProgress,
      final WriterId writerId) {
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
              consumerGroupId,
              topicName,
              regionIdStr,
              Objects.nonNull(writerProgress) ? writerProgress.getPhysicalTime() : 0L,
              Objects.nonNull(writerProgress) ? writerProgress.getLocalSeq() : -1L);
      if (Objects.nonNull(writerId) && writerId.getNodeId() >= 0) {
        req.setWriterNodeId(writerId.getNodeId());
      }
      if (Objects.nonNull(writerId) && writerId.getWriterEpoch() > 0) {
        req.setWriterEpoch(writerId.getWriterEpoch());
      }

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
      final long physicalTime,
      final long localSeq,
      final int writerNodeId,
      final long writerEpoch) {
    receiveProgressBroadcast(
        consumerGroupId,
        topicName,
        regionIdStr,
        buildWriterId(regionIdStr, writerNodeId, writerEpoch),
        new WriterProgress(physicalTime, localSeq));
  }

  public void receiveProgressBroadcast(
      final String consumerGroupId,
      final String topicName,
      final String regionIdStr,
      final WriterId writerId,
      final WriterProgress writerProgress) {
    if (Objects.isNull(writerId) || Objects.isNull(writerProgress)) {
      LOGGER.warn(
          "ConsensusSubscriptionCommitManager: ignore broadcast without writer identity, "
              + "consumerGroupId={}, topicName={}, regionId={}, writerId={}, writerProgress={}",
          consumerGroupId,
          topicName,
          regionIdStr,
          writerId,
          writerProgress);
      return;
    }
    final String key = consumerGroupId + KEY_SEPARATOR + topicName + KEY_SEPARATOR + regionIdStr;
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state != null) {
      // Update only if broadcast is ahead
      state.updateFromBroadcast(writerId, writerProgress);
      persistProgressIfNeeded(key, state);
    } else {
      // Create a new state from the broadcast progress
      final ConsensusSubscriptionCommitState newState =
          new ConsensusSubscriptionCommitState(
              regionIdStr,
              new SubscriptionConsensusProgress(
                  new RegionProgress(Collections.singletonMap(writerId, writerProgress)), 0L));
      newState.updateFromBroadcast(writerId, writerProgress);
      commitStates.putIfAbsent(key, newState);
      persistProgress(key, commitStates.get(key));
    }
    LOGGER.debug(
        "Received subscription progress broadcast: consumerGroupId={}, topicName={}, "
            + "regionId={}, physicalTime={}, localSeq={}",
        consumerGroupId,
        topicName,
        regionIdStr,
        writerProgress != null ? writerProgress.getPhysicalTime() : 0L,
        writerProgress != null ? writerProgress.getLocalSeq() : -1L);
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

  private ConsensusSubscriptionCommitState tryRecover(final String key, final String regionIdStr) {
    final File file = getProgressFile(key);
    if (!file.exists()) {
      return null;
    }
    try (final FileInputStream fis = new FileInputStream(file)) {
      final byte[] bytes = new byte[(int) file.length()];
      fis.read(bytes);
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      return ConsensusSubscriptionCommitState.deserialize(regionIdStr, buffer);
    } catch (final IOException e) {
      LOGGER.warn("Failed to recover consensus subscription progress from {}", file, e);
      return null;
    }
  }

  private static WriterId buildWriterId(
      final String regionIdStr, final int writerNodeId, final long writerEpoch) {
    return writerNodeId >= 0 ? new WriterId(regionIdStr, writerNodeId, writerEpoch) : null;
  }

  static String buildBroadcastKey(final String key, final WriterId writerId) {
    return key
        + KEY_SEPARATOR
        + (Objects.nonNull(writerId) ? writerId.getNodeId() : -1)
        + KEY_SEPARATOR
        + (Objects.nonNull(writerId) ? writerId.getWriterEpoch() : 0L);
  }

  private ConsensusSubscriptionCommitState queryCommitProgressStateFromConfigNode(
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
      if (resp.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return null;
      }
      if (resp.isSetCommittedRegionProgress()) {
        final RegionProgress committedRegionProgress =
            deserializeRegionProgress(
                ByteBuffer.wrap(resp.getCommittedRegionProgress()).asReadOnlyBuffer());
        if (Objects.nonNull(committedRegionProgress)
            && !committedRegionProgress.getWriterPositions().isEmpty()) {
          LOGGER.info(
              "ConsensusSubscriptionCommitManager: recovered committedRegionProgress={} from "
                  + "ConfigNode for consumerGroupId={}, topicName={}, regionId={}",
              committedRegionProgress,
              consumerGroupId,
              topicName,
              regionId);
          final ConsensusSubscriptionCommitState recoveredState =
              new ConsensusSubscriptionCommitState(
                  regionId.toString(), new SubscriptionConsensusProgress());
          recoveredState.resetForSeek(committedRegionProgress);
          return recoveredState;
        }
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
    return null;
  }

  private static ByteBuffer serializeRegionProgress(final RegionProgress regionProgress) {
    if (Objects.isNull(regionProgress)) {
      return null;
    }
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos)) {
      regionProgress.serialize(dos);
      dos.flush();
      return ByteBuffer.wrap(baos.toByteArray());
    } catch (final IOException e) {
      LOGGER.warn("Failed to serialize committed region progress {}", regionProgress, e);
      return null;
    }
  }

  private static RegionProgress deserializeRegionProgress(final ByteBuffer buffer) {
    if (Objects.isNull(buffer)) {
      return null;
    }
    final ByteBuffer duplicate = buffer.asReadOnlyBuffer();
    duplicate.rewind();
    return RegionProgress.deserialize(duplicate);
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
   * Tracks commit state for a single (consumerGroup, topic, region) triple using (physicalTime,
   * localSeq) pairs for cross-leader-migration consistency. Outstanding and committed positions are
   * tracked as ProgressKey objects rather than raw searchIndex values.
   */
  public static class ConsensusSubscriptionCommitState {

    private final String regionId;

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

    /** Real committed checkpoint per writer. */
    private final Map<WriterId, WriterProgress> committedWriterPositions = new LinkedHashMap<>();

    /** Tracks dispatched but not-yet-committed events by writer-local slot. */
    private final Map<ProgressSlot, ProgressKey> outstandingKeys = new ConcurrentHashMap<>();

    /** Tracks committed dispatched entries that cannot yet advance the frontier because of gaps. */
    private final Map<ProgressSlot, ProgressKey> committedPendingKeys = new LinkedHashMap<>();

    public ConsensusSubscriptionCommitState(
        final String regionId, final SubscriptionConsensusProgress progress) {
      this.regionId = regionId;
      this.progress = progress;
      committedWriterPositions.putAll(progress.getCommittedRegionProgress().getWriterPositions());
      syncPersistedProgress();
    }

    public SubscriptionConsensusProgress getProgress() {
      return progress;
    }

    public long getCommittedPhysicalTime() {
      return getDerivedCommittedFrontierKey().physicalTime;
    }

    public long getCommittedLocalSeq() {
      return getDerivedCommittedFrontierKey().localSeq;
    }

    public int getCommittedWriterNodeId() {
      final WriterId committedWriterId = getCommittedWriterId();
      return Objects.nonNull(committedWriterId) ? committedWriterId.getNodeId() : -1;
    }

    public long getCommittedWriterEpoch() {
      final WriterId committedWriterId = getCommittedWriterId();
      return Objects.nonNull(committedWriterId) ? committedWriterId.getWriterEpoch() : 0L;
    }

    public WriterId getCommittedWriterId() {
      return getDerivedCommittedFrontierKey().toWriterId(regionId);
    }

    public WriterProgress getCommittedWriterProgress() {
      return getDerivedCommittedFrontierKey().toWriterProgress();
    }

    public RegionProgress getCommittedRegionProgress() {
      synchronized (this) {
        return new RegionProgress(new LinkedHashMap<>(committedWriterPositions));
      }
    }

    /** Threshold for warning about outstanding (uncommitted) entries accumulation. */
    private static final int OUTSTANDING_SIZE_WARN_THRESHOLD = 10000;

    public void recordMapping(final WriterId writerId, final WriterProgress writerProgress) {
      if (Objects.isNull(writerId) || Objects.isNull(writerProgress)) {
        LOGGER.warn(
            "ConsensusSubscriptionCommitState: ignore mapping without writer identity, "
                + "writerId={}, writerProgress={}",
            writerId,
            writerProgress);
        return;
      }
      final ProgressKey key = new ProgressKey(writerId, writerProgress);
      final ProgressSlot slot = ProgressSlot.from(key);
      synchronized (this) {
        final ProgressKey previous = outstandingKeys.put(slot, key);
        if (Objects.nonNull(previous) && !previous.equals(key)) {
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: duplicate outstanding mapping for slot={}, "
                  + "previous={}, current={}",
              slot,
              previous,
              key);
        }
        final int size = outstandingKeys.size();
        if (size > OUTSTANDING_SIZE_WARN_THRESHOLD && size % OUTSTANDING_SIZE_WARN_THRESHOLD == 1) {
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: outstanding size ({}) exceeds threshold ({}), "
                  + "consumers may not be committing. committed=({},{}), writer=({}, {})",
              size,
              OUTSTANDING_SIZE_WARN_THRESHOLD,
              getCommittedPhysicalTime(),
              getCommittedLocalSeq(),
              getCommittedWriterNodeId(),
              getCommittedWriterEpoch());
        }
      }
    }

    /**
     * Commits the specified event and advances the committed position contiguously.
     *
     * @param writerProgress the writer progress of the event to commit
     * @return true if successfully committed
     */
    public boolean commit(final WriterId writerId, final WriterProgress writerProgress) {
      return commitAndGetResult(writerId, writerProgress).isHandled();
    }

    CommitOperationResult commitAndGetResult(
        final WriterId writerId, final WriterProgress writerProgress) {
      if (Objects.isNull(writerId) || Objects.isNull(writerProgress)) {
        LOGGER.warn(
            "ConsensusSubscriptionCommitState: missing writer identity for commit, "
                + "writerId={}, writerProgress={}",
            writerId,
            writerProgress);
        return CommitOperationResult.unhandled();
      }
      final ProgressKey key = new ProgressKey(writerId, writerProgress);

      synchronized (this) {
        final ProgressKey recordedKey = outstandingKeys.remove(ProgressSlot.from(key));
        if (recordedKey == null) {
          if (recentlyCommittedKeys.contains(key)) {
            LOGGER.debug(
                "ConsensusSubscriptionCommitState: idempotent re-commit for ({},{},{},{})",
                key.physicalTime,
                key.localSeq,
                key.writerNodeId,
                key.writerEpoch);
            progress.incrementCommitIndex();
            return CommitOperationResult.handledWithoutAdvance();
          }
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: unknown key ({},{},{},{}) for commit",
              key.physicalTime,
              key.localSeq,
              key.writerNodeId,
              key.writerEpoch);
          return CommitOperationResult.unhandled();
        }
        final ProgressKey effectiveKey = recordedKey.resolveMissingFields(writerId, writerProgress);
        final WriterId effectiveWriterId = effectiveKey.toWriterId(regionId);
        final WriterProgress before = getCommittedWriterProgressForWriter(effectiveWriterId);
        recentlyCommittedKeys.add(effectiveKey);
        stageCommittedAndAdvance(effectiveKey);
        progress.incrementCommitIndex();
        syncPersistedProgress();
        return buildCommitOperationResult(
            effectiveWriterId, before, getCommittedWriterProgressForWriter(effectiveWriterId));
      }
    }

    public boolean commitWithoutOutstanding(
        final WriterId writerId, final WriterProgress writerProgress) {
      return commitWithoutOutstandingAndGetResult(writerId, writerProgress).isHandled();
    }

    CommitOperationResult commitWithoutOutstandingAndGetResult(
        final WriterId writerId, final WriterProgress writerProgress) {
      if (Objects.isNull(writerId) || Objects.isNull(writerProgress)) {
        LOGGER.warn(
            "ConsensusSubscriptionCommitState: missing writer identity for direct commit, "
                + "writerId={}, writerProgress={}",
            writerId,
            writerProgress);
        return CommitOperationResult.unhandled();
      }
      final ProgressKey incomingKey = new ProgressKey(writerId, writerProgress);

      synchronized (this) {
        if (recentlyCommittedKeys.contains(incomingKey)) {
          LOGGER.debug(
              "ConsensusSubscriptionCommitState: idempotent direct commit for ({},{},{},{})",
              incomingKey.physicalTime,
              incomingKey.localSeq,
              incomingKey.writerNodeId,
              incomingKey.writerEpoch);
          progress.incrementCommitIndex();
          return CommitOperationResult.handledWithoutAdvance();
        }

        final ProgressKey outstandingKey = outstandingKeys.remove(ProgressSlot.from(incomingKey));
        if (Objects.isNull(outstandingKey)) {
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: reject direct commit without outstanding mapping "
                  + "for ({},{},{},{})",
              incomingKey.physicalTime,
              incomingKey.localSeq,
              incomingKey.writerNodeId,
              incomingKey.writerEpoch);
          return CommitOperationResult.unhandled();
        }
        final ProgressKey effectiveKey =
            outstandingKey.resolveMissingFields(writerId, writerProgress);
        final WriterId effectiveWriterId = effectiveKey.toWriterId(regionId);
        final WriterProgress before = getCommittedWriterProgressForWriter(effectiveWriterId);
        recentlyCommittedKeys.add(effectiveKey);
        stageCommittedAndAdvance(effectiveKey);
        progress.incrementCommitIndex();
        syncPersistedProgress();
        return buildCommitOperationResult(
            effectiveWriterId, before, getCommittedWriterProgressForWriter(effectiveWriterId));
      }
    }

    /**
     * Resets all commit tracking state for a seek operation. Clears all outstanding mappings and
     * resets progress to the new position.
     */
    public void resetForSeek(final WriterId writerId, final WriterProgress writerProgress) {
      synchronized (this) {
        outstandingKeys.clear();
        committedPendingKeys.clear();
        recentlyCommittedKeys.clear();
        committedWriterPositions.clear();
        if (Objects.nonNull(writerId) && Objects.nonNull(writerProgress)) {
          committedWriterPositions.put(writerId, writerProgress);
        } else if (Objects.nonNull(writerProgress)) {
          LOGGER.info(
              "ConsensusSubscriptionCommitState: dropping non-per-writer seek baseline, "
                  + "regionId={}, writerId={}, writerProgress={}",
              regionId,
              writerId,
              writerProgress);
        }
        syncPersistedProgress();
      }
    }

    public void resetForSeek(final RegionProgress regionProgress) {
      synchronized (this) {
        outstandingKeys.clear();
        committedPendingKeys.clear();
        recentlyCommittedKeys.clear();
        committedWriterPositions.clear();
        if (Objects.nonNull(regionProgress)) {
          for (final Map.Entry<WriterId, WriterProgress> entry :
              regionProgress.getWriterPositions().entrySet()) {
            if (Objects.nonNull(entry.getKey()) && Objects.nonNull(entry.getValue())) {
              committedWriterPositions.put(entry.getKey(), entry.getValue());
            }
          }
        }
        syncPersistedProgress();
      }
    }

    /**
     * Updates committed progress from a Leader broadcast. Only advances if the broadcast position
     * is ahead of the current local position.
     */
    public void updateFromBroadcast(final WriterId writerId, final WriterProgress writerProgress) {
      if (Objects.isNull(writerId) || Objects.isNull(writerProgress)) {
        return;
      }
      synchronized (this) {
        final ProgressKey incoming = new ProgressKey(writerId, writerProgress);
        final WriterId incomingWriterId = incoming.toWriterId(regionId);
        final WriterProgress currentWriterProgress =
            getCommittedWriterProgressForWriter(incomingWriterId);
        final ProgressKey current = new ProgressKey(incomingWriterId, currentWriterProgress);
        if (incoming.compareTo(current) > 0) {
          committedWriterPositions.put(incomingWriterId, incoming.toWriterProgress());
          syncPersistedProgress();
        }
      }
    }

    private void advanceCommitted(final ProgressKey key) {
      final WriterId writerId = key.toWriterId(regionId);
      if (Objects.isNull(writerId)) {
        return;
      }
      committedWriterPositions.put(writerId, key.toWriterProgress());
    }

    private WriterProgress getCommittedWriterProgressForWriter(final WriterId writerId) {
      return Objects.nonNull(writerId)
          ? committedWriterPositions.getOrDefault(writerId, new WriterProgress(0L, -1L))
          : new WriterProgress(0L, -1L);
    }

    private void stageCommittedAndAdvance(final ProgressKey key) {
      committedPendingKeys.put(ProgressSlot.from(key), key);
      final WriterId writerId = key.toWriterId(regionId);
      if (Objects.isNull(writerId)) {
        committedPendingKeys.remove(ProgressSlot.from(key));
        return;
      }
      ProgressKey current =
          new ProgressKey(writerId, getCommittedWriterProgressForWriter(writerId));
      while (true) {
        final ProgressKey nextCommitted = findNextCommittedKey(writerId, current);
        if (Objects.isNull(nextCommitted)) {
          return;
        }
        final ProgressKey nextOutstanding = findNextOutstandingKey(writerId, current);
        if (Objects.nonNull(nextOutstanding) && nextOutstanding.compareTo(nextCommitted) < 0) {
          return;
        }
        committedPendingKeys.remove(ProgressSlot.from(nextCommitted));
        advanceCommitted(nextCommitted);
        current = nextCommitted;
      }
    }

    private void advanceCommittedIfAhead(final ProgressKey key) {
      final WriterId writerId = key.toWriterId(regionId);
      if (Objects.isNull(writerId)) {
        return;
      }
      final WriterProgress currentWriterProgress = getCommittedWriterProgressForWriter(writerId);
      final ProgressKey currentKey = new ProgressKey(writerId, currentWriterProgress);
      if (key.compareTo(currentKey) > 0) {
        advanceCommitted(key);
      }
    }

    private ProgressKey findNextCommittedKey(final WriterId writerId, final ProgressKey current) {
      ProgressKey next = null;
      for (final ProgressKey candidate : committedPendingKeys.values()) {
        if (!sameWriter(writerId, candidate)) {
          continue;
        }
        if (candidate.compareTo(current) <= 0) {
          continue;
        }
        if (Objects.isNull(next) || candidate.compareTo(next) < 0) {
          next = candidate;
        }
      }
      return next;
    }

    private ProgressKey findNextOutstandingKey(final WriterId writerId, final ProgressKey current) {
      ProgressKey next = null;
      for (final ProgressKey candidate : outstandingKeys.values()) {
        if (!sameWriter(writerId, candidate)) {
          continue;
        }
        if (candidate.compareTo(current) <= 0) {
          continue;
        }
        if (Objects.isNull(next) || candidate.compareTo(next) < 0) {
          next = candidate;
        }
      }
      return next;
    }

    private boolean sameWriter(final WriterId writerId, final ProgressKey key) {
      return Objects.nonNull(writerId)
          && writerId.getNodeId() == key.writerNodeId
          && writerId.getWriterEpoch() == key.writerEpoch;
    }

    private CommitOperationResult buildCommitOperationResult(
        final WriterId writerId, final WriterProgress before, final WriterProgress after) {
      if (Objects.isNull(writerId)) {
        return CommitOperationResult.handledWithoutAdvance();
      }
      final ProgressKey beforeKey = new ProgressKey(writerId, before);
      final ProgressKey afterKey = new ProgressKey(writerId, after);
      return afterKey.compareTo(beforeKey) > 0
          ? CommitOperationResult.handledWithAdvance(writerId, after)
          : CommitOperationResult.handledWithoutAdvance();
    }

    private ProgressKey getDerivedCommittedFrontierKey() {
      ProgressKey maxKey = null;
      synchronized (this) {
        for (final Map.Entry<WriterId, WriterProgress> entry :
            committedWriterPositions.entrySet()) {
          final ProgressKey candidate = new ProgressKey(entry.getKey(), entry.getValue());
          if (Objects.isNull(maxKey) || candidate.compareTo(maxKey) > 0) {
            maxKey = candidate;
          }
        }
      }
      return Objects.nonNull(maxKey) ? maxKey : new ProgressKey(0L, -1L, -1, 0L);
    }

    private void syncPersistedProgress() {
      progress.setCommittedRegionProgress(
          new RegionProgress(new LinkedHashMap<>(committedWriterPositions)));
    }

    public void serialize(final DataOutputStream stream) throws IOException {
      synchronized (this) {
        syncPersistedProgress();
        progress.serialize(stream);
      }
    }

    public static ConsensusSubscriptionCommitState deserialize(
        final String regionId, final ByteBuffer buffer) {
      final SubscriptionConsensusProgress progress =
          SubscriptionConsensusProgress.deserialize(buffer);
      return new ConsensusSubscriptionCommitState(regionId, progress);
    }
  }

  private static final class CommitOperationResult {

    private static final CommitOperationResult UNHANDLED =
        new CommitOperationResult(false, null, null);

    private static final CommitOperationResult HANDLED_WITHOUT_ADVANCE =
        new CommitOperationResult(true, null, null);

    private final boolean handled;

    private final WriterId advancedWriterId;

    private final WriterProgress advancedWriterProgress;

    private CommitOperationResult(
        final boolean handled,
        final WriterId advancedWriterId,
        final WriterProgress advancedWriterProgress) {
      this.handled = handled;
      this.advancedWriterId = advancedWriterId;
      this.advancedWriterProgress = advancedWriterProgress;
    }

    private static CommitOperationResult unhandled() {
      return UNHANDLED;
    }

    private static CommitOperationResult handledWithoutAdvance() {
      return HANDLED_WITHOUT_ADVANCE;
    }

    private static CommitOperationResult handledWithAdvance(
        final WriterId advancedWriterId, final WriterProgress advancedWriterProgress) {
      return new CommitOperationResult(true, advancedWriterId, advancedWriterProgress);
    }

    private boolean isHandled() {
      return handled;
    }

    private boolean hasAdvancedWriter() {
      return Objects.nonNull(advancedWriterId) && Objects.nonNull(advancedWriterProgress);
    }

    private WriterId getAdvancedWriterId() {
      return advancedWriterId;
    }

    private WriterProgress getAdvancedWriterProgress() {
      return advancedWriterProgress;
    }
  }

  static final class ProgressSlot {
    final int writerNodeId;
    final long writerEpoch;
    final long localSeq;

    private ProgressSlot(final int writerNodeId, final long writerEpoch, final long localSeq) {
      this.writerNodeId = writerNodeId;
      this.writerEpoch = writerEpoch;
      this.localSeq = localSeq;
    }

    static ProgressSlot of(final int writerNodeId, final long writerEpoch, final long localSeq) {
      return new ProgressSlot(writerNodeId, writerEpoch, localSeq);
    }

    static ProgressSlot from(final ProgressKey key) {
      return new ProgressSlot(key.writerNodeId, key.writerEpoch, key.localSeq);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProgressSlot)) {
        return false;
      }
      final ProgressSlot that = (ProgressSlot) o;
      return writerNodeId == that.writerNodeId
          && writerEpoch == that.writerEpoch
          && localSeq == that.localSeq;
    }

    @Override
    public int hashCode() {
      return Objects.hash(writerNodeId, writerEpoch, localSeq);
    }

    @Override
    public String toString() {
      return "(" + writerNodeId + "," + writerEpoch + "," + localSeq + ")";
    }
  }

  // ======================== ProgressKey ========================

  /**
   * Comparable key for tracking commit progress: (physicalTime, localSeq). Physical time takes
   * priority; within the same physical time, writer identity and local sequence determine order.
   */
  static final class ProgressKey implements Comparable<ProgressKey> {
    final long physicalTime;
    final long localSeq;
    final int writerNodeId;
    final long writerEpoch;

    ProgressKey(final long physicalTime, final long localSeq) {
      this(physicalTime, localSeq, -1, 0L);
    }

    ProgressKey(final WriterId writerId, final WriterProgress writerProgress) {
      this(
          Objects.nonNull(writerProgress) ? writerProgress.getPhysicalTime() : 0L,
          Objects.nonNull(writerProgress) ? writerProgress.getLocalSeq() : -1L,
          Objects.nonNull(writerId) ? writerId.getNodeId() : -1,
          Objects.nonNull(writerId) ? writerId.getWriterEpoch() : 0L);
    }

    ProgressKey(
        final long physicalTime,
        final long localSeq,
        final int writerNodeId,
        final long writerEpoch) {
      this.physicalTime = physicalTime;
      this.localSeq = localSeq;
      this.writerNodeId = writerNodeId;
      this.writerEpoch = writerEpoch;
    }

    ProgressKey resolveMissingFields(final WriterId writerId, final WriterProgress writerProgress) {
      final long effectivePhysicalTime =
          this.physicalTime > 0
              ? this.physicalTime
              : Objects.nonNull(writerProgress)
                  ? writerProgress.getPhysicalTime()
                  : this.physicalTime;
      final long effectiveLocalSeq =
          this.localSeq >= 0
              ? this.localSeq
              : Objects.nonNull(writerProgress) ? writerProgress.getLocalSeq() : this.localSeq;
      final int effectiveWriterNodeId =
          this.writerNodeId >= 0
              ? this.writerNodeId
              : Objects.nonNull(writerId) ? writerId.getNodeId() : this.writerNodeId;
      final long effectiveWriterEpoch =
          this.writerEpoch > 0
              ? this.writerEpoch
              : Objects.nonNull(writerId) ? writerId.getWriterEpoch() : this.writerEpoch;
      if (effectivePhysicalTime == this.physicalTime
          && effectiveLocalSeq == this.localSeq
          && effectiveWriterNodeId == this.writerNodeId
          && effectiveWriterEpoch == this.writerEpoch) {
        return this;
      }
      return new ProgressKey(
          effectivePhysicalTime, effectiveLocalSeq, effectiveWriterNodeId, effectiveWriterEpoch);
    }

    WriterId toWriterId(final String regionId) {
      return writerNodeId >= 0 ? new WriterId(regionId, writerNodeId, writerEpoch) : null;
    }

    WriterProgress toWriterProgress() {
      return new WriterProgress(physicalTime, localSeq);
    }

    @Override
    public int compareTo(final ProgressKey o) {
      int cmp = Long.compare(physicalTime, o.physicalTime);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Integer.compare(writerNodeId, o.writerNodeId);
      if (cmp != 0) {
        return cmp;
      }
      cmp = Long.compare(writerEpoch, o.writerEpoch);
      if (cmp != 0) {
        return cmp;
      }
      return Long.compare(localSeq, o.localSeq);
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
      return physicalTime == that.physicalTime
          && localSeq == that.localSeq
          && writerNodeId == that.writerNodeId
          && writerEpoch == that.writerEpoch;
    }

    @Override
    public int hashCode() {
      return Objects.hash(physicalTime, localSeq, writerNodeId, writerEpoch);
    }

    @Override
    public String toString() {
      return "(" + physicalTime + "," + writerNodeId + "," + writerEpoch + "," + localSeq + ")";
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
