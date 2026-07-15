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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.commons.subscription.meta.consumer.CommitProgressKeeper;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
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
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Manages committed progress for consensus-based subscriptions.
 *
 * <p>State is maintained per {@code (consumerGroup, topic, region)} so each DataRegion can recover
 * independently.
 *
 * <p>Committed progress is represented in per-writer terms via {@link WriterId} and {@link
 * WriterProgress}. Outstanding deliveries are tracked by writer-local slots, while commit
 * advancement is computed with ordered progress keys derived from {@code physicalTime}, {@code
 * writerNodeId}, and {@code localSeq}. {@code searchIndex} is not the committed frontier here; it
 * only remains an implementation aid for WAL positioning elsewhere.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Track dispatched but uncommitted mappings per writer
 *   <li>Advance committed progress idempotently and contiguously on ack/commit
 *   <li>Persist, recover, and broadcast committed region progress
 * </ul>
 */
public class ConsensusSubscriptionCommitManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusSubscriptionCommitManager.class);

  private static final String PROGRESS_FILE_PREFIX = "consensus_subscription_progress_";
  private static final String PROGRESS_META_FILE_SUFFIX = ".meta";
  private static final String LEGACY_PROGRESS_INDEX_FILE_SUFFIX = ".meta.index";
  private static final String PROGRESS_TMP_FILE_SUFFIX = ".tmp";
  private static final int TOPIC_PROGRESS_FILE_VERSION = 1;

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
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.SUBSCRIPTION_CONSENSUS_PROGRESS_BROADCASTER.getName());

  /** Key: versioned, encoded (consumerGroupId, topicName, regionId) -> progress tracking state. */
  private final Map<String, ConsensusSubscriptionCommitState> commitStates =
      new ConcurrentHashMap<>();

  /** Key: encoded commit-state key -> decoded (consumerGroupId, topicName, regionId) fields. */
  private final Map<String, CommitStateKey> commitStateKeys = new ConcurrentHashMap<>();

  private final Set<String> recoveredTopicKeys = ConcurrentHashMap.newKeySet();

  private final Map<String, Object> topicPersistLocks = new ConcurrentHashMap<>();

  // Runtime random-write index. The outer key is the topic file key
  // "base64(consumerGroupId).base64(topicName)"; the inner key is regionId. The on-disk .meta
  // file is the only source of truth, and this map is rebuilt from .meta during recovery.
  private final Map<String, Map<String, ProgressIndexEntry>> topicProgressIndexes =
      new ConcurrentHashMap<>();

  /** Fresh local-tail proposals waiting for ConfigNode to publish an explicit per-region entry. */
  private final Set<String> pendingInitialProgressKeys = ConcurrentHashMap.newKeySet();

  /** New regions that may start from the beginning once ConfigNode explicitly reports no entry. */
  private final Set<String> pendingNewRegionProgressKeys = ConcurrentHashMap.newKeySet();

  /**
   * Regions whose latest initialization/refresh obtained an authoritative ConfigNode resolution.
   * This includes an explicitly present field and explicit absence for a newly-created region.
   */
  private final Set<String> configNodeProgressAvailableKeys = ConcurrentHashMap.newKeySet();

  /** Prevents duplicate queue binding from proposing a second local tail in the same process. */
  private final Set<String> tailProposalInitializedKeys = ConcurrentHashMap.newKeySet();

  private final ConfigNodeProgressFetcher configNodeProgressFetcher;

  private final String persistDir;

  private ConsensusSubscriptionCommitManager() {
    this(null);
  }

  ConsensusSubscriptionCommitManager(final ConfigNodeProgressFetcher configNodeProgressFetcher) {
    this.configNodeProgressFetcher =
        Objects.nonNull(configNodeProgressFetcher)
            ? configNodeProgressFetcher
            : this::queryCommitProgressFromConfigNode;
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
    final String regionIdString = regionId.toString();
    final CommitStateKey stateKey = getCommitStateKey(consumerGroupId, topicName, regionIdString);
    final ConsensusSubscriptionCommitState existing = commitStates.get(stateKey.encodedStateKey);
    if (Objects.nonNull(existing)) {
      return existing;
    }
    recoverTopicStatesIfNeeded(stateKey);
    final ConsensusSubscriptionCommitState recovered = commitStates.get(stateKey.encodedStateKey);
    if (Objects.nonNull(recovered)) {
      return recovered;
    }
    return commitStates.computeIfAbsent(
        stateKey.encodedStateKey,
        k -> {
          final ConfigNodeProgressQueryResult queryResult =
              configNodeProgressFetcher.fetch(consumerGroupId, topicName, regionId);
          final ConsensusSubscriptionCommitState state =
              new ConsensusSubscriptionCommitState(
                  regionIdString, new SubscriptionConsensusProgress());
          if (queryResult.isAvailable()) {
            state.resetForSeek(queryResult.getRegionProgress());
            configNodeProgressAvailableKeys.add(stateKey.encodedStateKey);
          }
          return state;
        });
  }

  public boolean hasPersistedState(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final CommitStateKey stateKey =
        getCommitStateKey(consumerGroupId, topicName, regionId.toString());
    recoverTopicStatesIfNeeded(stateKey);
    return topicProgressIndexes
        .getOrDefault(stateKey.topicFileKey, Collections.emptyMap())
        .containsKey(stateKey.regionIdStr);
  }

  /** Captures all local commit state that setup may replace for the requested topics. */
  SetupSnapshot captureSetupSnapshot(final String consumerGroupId, final Set<String> topicNames) {
    final Map<String, TopicSetupSnapshot> topicSnapshots = new LinkedHashMap<>();
    for (final String topicName : topicNames) {
      final String topicFileKey = generateTopicFileKey(consumerGroupId, topicName);
      recoverTopicStatesIfNeeded(topicFileKey, consumerGroupId, topicName);
      synchronized (getTopicPersistLock(topicFileKey)) {
        final Map<String, StateSetupSnapshot> stateSnapshots = new LinkedHashMap<>();
        for (final Map.Entry<String, CommitStateKey> entry : commitStateKeys.entrySet()) {
          final CommitStateKey stateKey = entry.getValue();
          if (!stateKey.sameTopic(consumerGroupId, topicName)) {
            continue;
          }
          final ConsensusSubscriptionCommitState state = commitStates.get(entry.getKey());
          if (Objects.isNull(state)) {
            continue;
          }
          stateSnapshots.put(
              stateKey.regionIdStr,
              new StateSetupSnapshot(
                  new SubscriptionConsensusProgress(
                      copyRegionProgress(state.getCommittedRegionProgress()),
                      state.getProgress().getPersistenceThrottleCounter()),
                  pendingInitialProgressKeys.contains(entry.getKey()),
                  pendingNewRegionProgressKeys.contains(entry.getKey()),
                  configNodeProgressAvailableKeys.contains(entry.getKey()),
                  tailProposalInitializedKeys.contains(entry.getKey())));
        }
        topicSnapshots.put(topicName, new TopicSetupSnapshot(topicFileKey, stateSnapshots));
      }
    }
    return new SetupSnapshot(consumerGroupId, topicSnapshots);
  }

  /** Restores only attempted topics to their exact state before setup began. */
  void restoreSetupSnapshot(
      final SetupSnapshot setupSnapshot, final Set<String> attemptedTopicNames) {
    for (final String topicName : attemptedTopicNames) {
      final TopicSetupSnapshot topicSnapshot = setupSnapshot.topicSnapshots.get(topicName);
      if (Objects.isNull(topicSnapshot)) {
        continue;
      }
      synchronized (getTopicPersistLock(topicSnapshot.topicFileKey)) {
        removeTopicStateFromMemory(setupSnapshot.consumerGroupId, topicName);

        CommitStateKey stateKeyForRewrite = null;
        for (final Map.Entry<String, StateSetupSnapshot> entry :
            topicSnapshot.stateSnapshots.entrySet()) {
          final CommitStateKey stateKey =
              getCommitStateKey(setupSnapshot.consumerGroupId, topicName, entry.getKey());
          final StateSetupSnapshot stateSnapshot = entry.getValue();
          commitStates.put(
              stateKey.encodedStateKey,
              new ConsensusSubscriptionCommitState(
                  stateKey.regionIdStr,
                  new SubscriptionConsensusProgress(
                      copyRegionProgress(stateSnapshot.progress.getCommittedRegionProgress()),
                      stateSnapshot.progress.getPersistenceThrottleCounter())));
          restoreSetupMarker(
              pendingInitialProgressKeys,
              stateKey.encodedStateKey,
              stateSnapshot.initialProgressPending);
          restoreSetupMarker(
              pendingNewRegionProgressKeys,
              stateKey.encodedStateKey,
              stateSnapshot.newRegionProgressPending);
          restoreSetupMarker(
              configNodeProgressAvailableKeys,
              stateKey.encodedStateKey,
              stateSnapshot.configNodeProgressAvailable);
          restoreSetupMarker(
              tailProposalInitializedKeys,
              stateKey.encodedStateKey,
              stateSnapshot.tailProposalInitialized);
          stateKeyForRewrite = stateKey;
        }

        if (Objects.isNull(stateKeyForRewrite)) {
          deleteTopicProgressFiles(topicSnapshot.topicFileKey);
          topicProgressIndexes.remove(topicSnapshot.topicFileKey);
          recoveredTopicKeys.add(topicSnapshot.topicFileKey);
          continue;
        }
        try {
          rewriteTopicProgressFilesUnderLock(stateKeyForRewrite);
        } catch (final IOException e) {
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_FAILED_TO_REWRITE_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_8B230D50,
              setupSnapshot.consumerGroupId,
              topicName,
              e);
        }
      }
    }
  }

  private void removeTopicStateFromMemory(final String consumerGroupId, final String topicName) {
    final Iterator<Map.Entry<String, CommitStateKey>> keyIterator =
        commitStateKeys.entrySet().iterator();
    while (keyIterator.hasNext()) {
      final Map.Entry<String, CommitStateKey> entry = keyIterator.next();
      if (!entry.getValue().sameTopic(consumerGroupId, topicName)) {
        continue;
      }
      commitStates.remove(entry.getKey());
      pendingInitialProgressKeys.remove(entry.getKey());
      pendingNewRegionProgressKeys.remove(entry.getKey());
      configNodeProgressAvailableKeys.remove(entry.getKey());
      tailProposalInitializedKeys.remove(entry.getKey());
      keyIterator.remove();
    }
  }

  private static void restoreSetupMarker(
      final Set<String> markerSet, final String stateKey, final boolean present) {
    if (present) {
      markerSet.add(stateKey);
    } else {
      markerSet.remove(stateKey);
    }
  }

  /**
   * Initializes an already-bound existing-region queue from its local WAL-tail proposal.
   *
   * <p>A fresh proposal is only a candidate frontier. It remains pending, and therefore the queue
   * remains dormant, until ConfigNode returns an explicitly present per-region progress field.
   */
  public void initializeStateFromTailProposal(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final RegionProgress tailProposal) {
    final CommitStateKey stateKey =
        getCommitStateKey(consumerGroupId, topicName, regionId.toString());
    recoverTopicStatesIfNeeded(stateKey);
    synchronized (getTopicPersistLock(stateKey.topicFileKey)) {
      if (!tailProposalInitializedKeys.add(stateKey.encodedStateKey)) {
        return;
      }
      pendingNewRegionProgressKeys.remove(stateKey.encodedStateKey);

      final boolean hasLocalPersistedState = hasPersistedStateUnderLock(stateKey);
      final ConsensusSubscriptionCommitState localState =
          commitStates.computeIfAbsent(
              stateKey.encodedStateKey,
              ignored ->
                  new ConsensusSubscriptionCommitState(
                      stateKey.regionIdStr, new SubscriptionConsensusProgress()));
      final ConfigNodeProgressQueryResult queryResult =
          configNodeProgressFetcher.fetch(consumerGroupId, topicName, regionId);
      if (queryResult.isAvailable()) {
        localState.resetForSeek(queryResult.getRegionProgress());
        pendingInitialProgressKeys.remove(stateKey.encodedStateKey);
        configNodeProgressAvailableKeys.add(stateKey.encodedStateKey);
      } else {
        if (!hasLocalPersistedState) {
          localState.resetForSeek(copyRegionProgress(tailProposal));
        }
        pendingInitialProgressKeys.add(stateKey.encodedStateKey);
        configNodeProgressAvailableKeys.remove(stateKey.encodedStateKey);
      }
      persistRegionProgress(stateKey, localState);
    }
  }

  /** Initializes a newly-created region without proposing the current local tail. */
  public void initializeStateWithoutTailProposal(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final CommitStateKey stateKey =
        getCommitStateKey(consumerGroupId, topicName, regionId.toString());
    recoverTopicStatesIfNeeded(stateKey);
    synchronized (getTopicPersistLock(stateKey.topicFileKey)) {
      final boolean hasLocalPersistedState = hasPersistedStateUnderLock(stateKey);
      final ConsensusSubscriptionCommitState localState =
          commitStates.computeIfAbsent(
              stateKey.encodedStateKey,
              ignored ->
                  new ConsensusSubscriptionCommitState(
                      stateKey.regionIdStr, new SubscriptionConsensusProgress()));
      final ConfigNodeProgressQueryResult queryResult =
          configNodeProgressFetcher.fetch(consumerGroupId, topicName, regionId);
      if (queryResult.isAvailable()) {
        localState.resetForSeek(queryResult.getRegionProgress());
        pendingInitialProgressKeys.remove(stateKey.encodedStateKey);
        pendingNewRegionProgressKeys.remove(stateKey.encodedStateKey);
        configNodeProgressAvailableKeys.add(stateKey.encodedStateKey);
        persistRegionProgress(stateKey, localState);
      } else if (queryResult.getAvailability() == ConfigNodeProgressAvailability.ABSENT
          && (!hasLocalPersistedState
              || pendingNewRegionProgressKeys.contains(stateKey.encodedStateKey))) {
        localState.resetForSeek(new RegionProgress(Collections.emptyMap()));
        pendingInitialProgressKeys.remove(stateKey.encodedStateKey);
        pendingNewRegionProgressKeys.add(stateKey.encodedStateKey);
        configNodeProgressAvailableKeys.add(stateKey.encodedStateKey);
        persistRegionProgress(stateKey, localState);
      } else {
        pendingInitialProgressKeys.add(stateKey.encodedStateKey);
        if (!hasLocalPersistedState) {
          pendingNewRegionProgressKeys.add(stateKey.encodedStateKey);
        }
        configNodeProgressAvailableKeys.remove(stateKey.encodedStateKey);
      }
    }
  }

  /**
   * Retries a pending fresh proposal. Returns {@code false} while ConfigNode still has no explicit
   * entry or cannot be queried, so the first poll can stay dormant.
   */
  public boolean refreshPendingInitialProgress(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final CommitStateKey stateKey =
        getCommitStateKey(consumerGroupId, topicName, regionId.toString());
    if (!pendingInitialProgressKeys.contains(stateKey.encodedStateKey)) {
      return true;
    }
    synchronized (getTopicPersistLock(stateKey.topicFileKey)) {
      if (!pendingInitialProgressKeys.contains(stateKey.encodedStateKey)) {
        return true;
      }
      final ConfigNodeProgressQueryResult queryResult =
          configNodeProgressFetcher.fetch(consumerGroupId, topicName, regionId);
      final boolean startNewRegionFromBeginning =
          queryResult.getAvailability() == ConfigNodeProgressAvailability.ABSENT
              && pendingNewRegionProgressKeys.contains(stateKey.encodedStateKey);
      if (!queryResult.isAvailable() && !startNewRegionFromBeginning) {
        return false;
      }
      final ConsensusSubscriptionCommitState state =
          commitStates.computeIfAbsent(
              stateKey.encodedStateKey,
              ignored ->
                  new ConsensusSubscriptionCommitState(
                      stateKey.regionIdStr, new SubscriptionConsensusProgress()));
      state.resetForSeek(
          startNewRegionFromBeginning
              ? new RegionProgress(Collections.emptyMap())
              : queryResult.getRegionProgress());
      persistRegionProgress(stateKey, state);
      pendingInitialProgressKeys.remove(stateKey.encodedStateKey);
      if (!startNewRegionFromBeginning) {
        pendingNewRegionProgressKeys.remove(stateKey.encodedStateKey);
      }
      configNodeProgressAvailableKeys.add(stateKey.encodedStateKey);
      return true;
    }
  }

  /** Refreshes an exact authoritative frontier for preferred-writer activation. */
  public ConfigNodeProgressQueryResult refreshAuthoritativeProgress(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final CommitStateKey stateKey =
        getCommitStateKey(consumerGroupId, topicName, regionId.toString());
    final ConfigNodeProgressQueryResult queryResult =
        configNodeProgressFetcher.fetch(consumerGroupId, topicName, regionId);
    if (!queryResult.isAvailable()
        && queryResult.getAvailability() != ConfigNodeProgressAvailability.ABSENT) {
      return queryResult;
    }

    recoverTopicStatesIfNeeded(stateKey);
    synchronized (getTopicPersistLock(stateKey.topicFileKey)) {
      final boolean startNewRegionFromBeginning =
          queryResult.getAvailability() == ConfigNodeProgressAvailability.ABSENT
              && pendingNewRegionProgressKeys.contains(stateKey.encodedStateKey);
      if (!queryResult.isAvailable() && !startNewRegionFromBeginning) {
        return queryResult;
      }
      final RegionProgress authoritativeProgress =
          startNewRegionFromBeginning
              ? new RegionProgress(Collections.emptyMap())
              : queryResult.getRegionProgress();
      final ConsensusSubscriptionCommitState state =
          commitStates.computeIfAbsent(
              stateKey.encodedStateKey,
              ignored ->
                  new ConsensusSubscriptionCommitState(
                      stateKey.regionIdStr, new SubscriptionConsensusProgress()));
      state.resetForSeek(authoritativeProgress);
      persistRegionProgress(stateKey, state);
      pendingInitialProgressKeys.remove(stateKey.encodedStateKey);
      if (!startNewRegionFromBeginning) {
        pendingNewRegionProgressKeys.remove(stateKey.encodedStateKey);
      }
      configNodeProgressAvailableKeys.add(stateKey.encodedStateKey);
      tailProposalInitializedKeys.add(stateKey.encodedStateKey);
      return ConfigNodeProgressQueryResult.available(authoritativeProgress);
    }
  }

  public boolean isConfigNodeProgressAvailable(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    return configNodeProgressAvailableKeys.contains(
        generateKey(consumerGroupId, topicName, regionId));
  }

  boolean isInitialProgressPending(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    return pendingInitialProgressKeys.contains(generateKey(consumerGroupId, topicName, regionId));
  }

  private boolean hasPersistedStateUnderLock(final CommitStateKey stateKey) {
    return topicProgressIndexes
        .getOrDefault(stateKey.topicFileKey, Collections.emptyMap())
        .containsKey(stateKey.regionIdStr);
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
    final CommitStateKey stateKey =
        getCommitStateKey(consumerGroupId, topicName, regionId.toString());
    final ConsensusSubscriptionCommitState state = commitStates.get(stateKey.encodedStateKey);
    if (state == null) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_COMMIT_FOR_UNKNOWN_751BD2A9,
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
      persistProgressIfNeeded(stateKey, state);
      if (result.hasAdvancedWriter()) {
        maybeBroadcast(
            stateKey.encodedStateKey,
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
    final CommitStateKey stateKey =
        getCommitStateKey(consumerGroupId, topicName, regionId.toString());
    final ConsensusSubscriptionCommitState state = commitStates.get(stateKey.encodedStateKey);
    if (state == null) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_DIRECT_COMMIT_D6AD7D96,
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
      persistProgressIfNeeded(stateKey, state);
      if (result.hasAdvancedWriter()) {
        maybeBroadcast(
            stateKey.encodedStateKey,
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
    final CommitStateKey stateKey =
        getCommitStateKey(consumerGroupId, topicName, regionId.toString());
    recoverTopicStatesIfNeeded(stateKey);
    commitStates.remove(stateKey.encodedStateKey);
    commitStateKeys.remove(stateKey.encodedStateKey);
    pendingInitialProgressKeys.remove(stateKey.encodedStateKey);
    pendingNewRegionProgressKeys.remove(stateKey.encodedStateKey);
    configNodeProgressAvailableKeys.remove(stateKey.encodedStateKey);
    tailProposalInitializedKeys.remove(stateKey.encodedStateKey);
    rewriteTopicProgressFiles(stateKey);
  }

  /**
   * Removes all states for a given (consumerGroup, topic) pair across all regions. Used during
   * subscription teardown when the individual regionIds may not be readily available.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   */
  public void removeAllStatesForTopic(final String consumerGroupId, final String topicName) {
    final String topicFileKey = generateTopicFileKey(consumerGroupId, topicName);
    final Iterator<Map.Entry<String, CommitStateKey>> keyIterator =
        commitStateKeys.entrySet().iterator();
    while (keyIterator.hasNext()) {
      final Map.Entry<String, CommitStateKey> entry = keyIterator.next();
      if (entry.getValue().sameTopic(consumerGroupId, topicName)) {
        commitStates.remove(entry.getKey());
        pendingInitialProgressKeys.remove(entry.getKey());
        pendingNewRegionProgressKeys.remove(entry.getKey());
        configNodeProgressAvailableKeys.remove(entry.getKey());
        tailProposalInitializedKeys.remove(entry.getKey());
        keyIterator.remove();
      }
    }
    synchronized (getTopicPersistLock(topicFileKey)) {
      deleteTopicProgressFiles(topicFileKey);
      topicProgressIndexes.remove(topicFileKey);
      recoveredTopicKeys.remove(topicFileKey);
    }
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
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_CANNOT_RESET_UNKNOWN_C469052F,
          consumerGroupId,
          topicName,
          regionId);
      return;
    }
    state.resetForSeek(regionProgress);
    persistRegionProgress(
        getCommitStateKey(consumerGroupId, topicName, regionId.toString()), state);
  }

  /** Persists all states. Should be called during graceful shutdown. */
  public void persistAll() {
    final Map<String, CommitStateKey> topicKeys = new LinkedHashMap<>();
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      final CommitStateKey stateKey = commitStateKeys.get(entry.getKey());
      if (Objects.nonNull(stateKey)) {
        topicKeys.putIfAbsent(stateKey.topicFileKey, stateKey);
      }
    }
    for (final CommitStateKey stateKey : topicKeys.values()) {
      rewriteTopicProgressFiles(stateKey);
    }
  }

  public Map<String, ByteBuffer> collectAllRegionProgress(final int dataNodeId) {
    recoverAllTopicStatesIfNeeded();
    final Map<String, ByteBuffer> result = new ConcurrentHashMap<>();
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      final CommitStateKey stateKey = commitStateKeys.get(entry.getKey());
      if (Objects.isNull(stateKey)) {
        continue;
      }
      final RegionProgress regionProgress = entry.getValue().getCommittedRegionProgress();
      final ByteBuffer serialized = serializeRegionProgress(regionProgress);
      if (Objects.nonNull(serialized)) {
        result.put(
            CommitProgressKeeper.generateKey(
                stateKey.consumerGroupId, stateKey.topicName, stateKey.regionIdStr, dataNodeId),
            serialized);
        if (CommitProgressKeeper.isLegacyKeyUnambiguous(
            stateKey.consumerGroupId, stateKey.topicName, stateKey.regionIdStr)) {
          result.put(
              CommitProgressKeeper.generateLegacyKey(
                  stateKey.consumerGroupId, stateKey.topicName, stateKey.regionIdStr, dataNodeId),
              serialized.duplicate());
        }
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
      final int writerNodeId =
          Objects.nonNull(writerId) && writerId.getNodeId() >= 0 ? writerId.getNodeId() : -1;
      final TSyncSubscriptionProgressReq req =
          new TSyncSubscriptionProgressReq(
              consumerGroupId,
              topicName,
              regionIdStr,
              Objects.nonNull(writerProgress) ? writerProgress.getPhysicalTime() : 0L,
              writerNodeId,
              Objects.nonNull(writerProgress) ? writerProgress.getLocalSeq() : -1L);

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
              DataNodePipeMessages
                  .PIPE_LOG_FAILED_TO_BROADCAST_SUBSCRIPTION_PROGRESS_TO_DATANODE_AT_7024F5B2,
              location.getDataNodeId(),
              endpoint,
              e.getMessage());
        }
      }
    } catch (final Exception e) {
      LOGGER.debug(
          DataNodePipeMessages
              .PIPE_LOG_FAILED_TO_BROADCAST_SUBSCRIPTION_PROGRESS_FOR_REGION_DE9074BD,
          regionId,
          e.getMessage());
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
      final int writerNodeId,
      final long localSeq) {
    receiveProgressBroadcast(
        consumerGroupId,
        topicName,
        regionIdStr,
        buildWriterId(regionIdStr, writerNodeId),
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
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_IGNORE_BROADCAST_WITHOUT_211DE477,
          consumerGroupId,
          topicName,
          regionIdStr,
          writerId,
          writerProgress);
      return;
    }
    final CommitStateKey stateKey = getCommitStateKey(consumerGroupId, topicName, regionIdStr);
    recoverTopicStatesIfNeeded(stateKey);
    final ConsensusSubscriptionCommitState state = commitStates.get(stateKey.encodedStateKey);
    if (state != null) {
      // Update only if broadcast is ahead
      state.updateFromBroadcast(writerId, writerProgress);
      persistProgressIfNeeded(stateKey, state);
    } else {
      // Create a new state from the broadcast progress
      final ConsensusSubscriptionCommitState newState =
          new ConsensusSubscriptionCommitState(
              regionIdStr,
              new SubscriptionConsensusProgress(
                  new RegionProgress(Collections.singletonMap(writerId, writerProgress)), 0L));
      newState.updateFromBroadcast(writerId, writerProgress);
      commitStates.putIfAbsent(stateKey.encodedStateKey, newState);
      persistRegionProgress(stateKey, commitStates.get(stateKey.encodedStateKey));
    }
    LOGGER.debug(
        DataNodePipeMessages
            .PIPE_LOG_RECEIVED_SUBSCRIPTION_PROGRESS_BROADCAST_CONSUMERGROUPID_CDAEF839,
        consumerGroupId,
        topicName,
        regionIdStr,
        writerProgress != null ? writerProgress.getPhysicalTime() : 0L,
        writerProgress != null ? writerProgress.getLocalSeq() : -1L);
  }

  // ======================== Helper Methods ========================

  // Used for encoded topic file keys and writer-scoped broadcast throttle keys.
  private static final String KEY_SEPARATOR = "##";

  private String generateKey(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    return generateKey(consumerGroupId, topicName, regionId.toString());
  }

  private String generateKey(
      final String consumerGroupId, final String topicName, final String regionIdStr) {
    return CommitProgressKeeper.generateRegionKey(consumerGroupId, topicName, regionIdStr);
  }

  private CommitStateKey getCommitStateKey(
      final String consumerGroupId, final String topicName, final String regionIdStr) {
    final String stateKey = generateKey(consumerGroupId, topicName, regionIdStr);
    return commitStateKeys.computeIfAbsent(
        stateKey,
        ignored ->
            new CommitStateKey(
                consumerGroupId,
                topicName,
                regionIdStr,
                stateKey,
                generateTopicFileKey(consumerGroupId, topicName)));
  }

  private String generateTopicFileKey(final String consumerGroupId, final String topicName) {
    return encodeFileNameComponent(consumerGroupId)
        + KEY_SEPARATOR
        + encodeFileNameComponent(topicName);
  }

  private static String encodeFileNameComponent(final String value) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
  }

  private static String decodeFileNameComponent(final String value) {
    switch (value.length() & 3) {
      case 0:
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
      case 2:
        return new String(Base64.getUrlDecoder().decode(value + "=="), StandardCharsets.UTF_8);
      case 3:
        return new String(Base64.getUrlDecoder().decode(value + "="), StandardCharsets.UTF_8);
      default:
        throw new IllegalArgumentException(
            DataNodePipeMessages.PIPE_EXCEPTION_INVALID_BASE64_URL_COMPONENT_LENGTH_F1F1B6BA);
    }
  }

  private static String[] decodeTopicFileKey(final String topicFileKey) {
    final int separatorIndex = topicFileKey.indexOf(KEY_SEPARATOR);
    if (separatorIndex < 0) {
      return null;
    }
    try {
      return new String[] {
        decodeFileNameComponent(topicFileKey.substring(0, separatorIndex)),
        decodeFileNameComponent(topicFileKey.substring(separatorIndex + KEY_SEPARATOR.length()))
      };
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  private Object getTopicPersistLock(final String topicFileKey) {
    return topicPersistLocks.computeIfAbsent(topicFileKey, ignored -> new Object());
  }

  private File getMetaFile(final String topicFileKey) {
    return new File(persistDir, PROGRESS_FILE_PREFIX + topicFileKey + PROGRESS_META_FILE_SUFFIX);
  }

  private File getLegacyIndexFile(final String topicFileKey) {
    return new File(
        persistDir, PROGRESS_FILE_PREFIX + topicFileKey + LEGACY_PROGRESS_INDEX_FILE_SUFFIX);
  }

  private void recoverAllTopicStatesIfNeeded() {
    final File dir = new File(persistDir);
    final File[] metaFiles =
        dir.listFiles(
            (ignored, name) ->
                name.startsWith(PROGRESS_FILE_PREFIX) && name.endsWith(PROGRESS_META_FILE_SUFFIX));
    if (Objects.isNull(metaFiles)) {
      return;
    }
    for (final File metaFile : metaFiles) {
      final String topicFileKey = extractTopicFileKey(metaFile.getName());
      if (Objects.isNull(topicFileKey) || recoveredTopicKeys.contains(topicFileKey)) {
        continue;
      }
      final String[] decodedTopicKey = decodeTopicFileKey(topicFileKey);
      if (Objects.isNull(decodedTopicKey)) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_SKIP_MALFORMED_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_NAME_BB4D75F0,
            metaFile.getName());
        continue;
      }
      recoverTopicStatesIfNeeded(topicFileKey, decodedTopicKey[0], decodedTopicKey[1]);
    }
  }

  private String extractTopicFileKey(final String metaFileName) {
    if (!metaFileName.startsWith(PROGRESS_FILE_PREFIX)
        || !metaFileName.endsWith(PROGRESS_META_FILE_SUFFIX)) {
      return null;
    }
    return metaFileName.substring(
        PROGRESS_FILE_PREFIX.length(), metaFileName.length() - PROGRESS_META_FILE_SUFFIX.length());
  }

  private void recoverTopicStatesIfNeeded(final CommitStateKey stateKey) {
    recoverTopicStatesIfNeeded(stateKey.topicFileKey, stateKey.consumerGroupId, stateKey.topicName);
  }

  private void recoverTopicStatesIfNeeded(
      final String topicFileKey, final String consumerGroupId, final String topicName) {
    if (recoveredTopicKeys.contains(topicFileKey)) {
      return;
    }
    synchronized (getTopicPersistLock(topicFileKey)) {
      if (recoveredTopicKeys.contains(topicFileKey)) {
        return;
      }
      try {
        final TopicProgressSnapshot snapshot = readTopicProgressSnapshot(topicFileKey);
        for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
            snapshot.states.entrySet()) {
          final CommitStateKey recoveredStateKey =
              getCommitStateKey(consumerGroupId, topicName, entry.getKey());
          commitStates.putIfAbsent(recoveredStateKey.encodedStateKey, entry.getValue());
        }
        topicProgressIndexes.put(topicFileKey, snapshot.indexEntries);
      } catch (final IOException e) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_FAILED_TO_RECOVER_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_DF30716B,
            consumerGroupId,
            topicName,
            e);
        topicProgressIndexes.put(topicFileKey, Collections.emptyMap());
      } finally {
        recoveredTopicKeys.add(topicFileKey);
      }
    }
  }

  // Recovery reads .meta sequentially and rebuilds the runtime offset index.
  private TopicProgressSnapshot readTopicProgressSnapshot(final String topicFileKey)
      throws IOException {
    final File metaFile = getMetaFile(topicFileKey);
    if (!metaFile.exists()) {
      return TopicProgressSnapshot.empty();
    }
    try {
      final ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(metaFile.toPath()));
      final int version = ReadWriteIOUtils.readInt(buffer);
      if (version != TOPIC_PROGRESS_FILE_VERSION) {
        throw new IOException(
            String.format(
                DataNodeMiscMessages.SUBSCRIPTION_UNSUPPORTED_CONSENSUS_PROGRESS_FILE_VERSION_FMT,
                version));
      }
      final int regionCount = ReadWriteIOUtils.readInt(buffer);
      if (regionCount < 0) {
        throw new IOException(
            String.format(
                DataNodePipeMessages
                    .PIPE_EXCEPTION_INVALID_CONSENSUS_SUBSCRIPTION_PROGRESS_REGION_COUNT_S_7CE4FD8E,
                regionCount));
      }
      final Map<String, ConsensusSubscriptionCommitState> states = new LinkedHashMap<>();
      final Map<String, ProgressIndexEntry> indexEntries = new LinkedHashMap<>();
      for (int i = 0; i < regionCount; i++) {
        final String regionId = ReadWriteIOUtils.readString(buffer);
        final int payloadLength = ReadWriteIOUtils.readInt(buffer);
        if (payloadLength < 0 || payloadLength > buffer.remaining()) {
          throw new IOException(
              String.format(
                  DataNodePipeMessages
                      .PIPE_EXCEPTION_INVALID_CONSENSUS_SUBSCRIPTION_PROGRESS_PAYLOAD_LENGTH_S_8C145986,
                  payloadLength));
        }
        final long payloadOffset = buffer.position();
        final byte[] payload = new byte[payloadLength];
        buffer.get(payload);
        states.put(
            regionId,
            ConsensusSubscriptionCommitState.deserialize(regionId, ByteBuffer.wrap(payload)));
        indexEntries.put(regionId, new ProgressIndexEntry(payloadOffset, payloadLength));
      }
      return new TopicProgressSnapshot(states, indexEntries);
    } catch (final RuntimeException e) {
      throw new IOException(
          String.format(
              DataNodePipeMessages
                  .PIPE_EXCEPTION_MALFORMED_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_S_83042847,
              metaFile),
          e);
    }
  }

  private void deleteTopicProgressFiles(final String topicFileKey) {
    deleteFileIfExists(getMetaFile(topicFileKey));
    deleteFileIfExists(getLegacyIndexFile(topicFileKey));
    deleteFileIfExists(
        new File(getMetaFile(topicFileKey).getAbsolutePath() + PROGRESS_TMP_FILE_SUFFIX));
    deleteFileIfExists(
        new File(getLegacyIndexFile(topicFileKey).getAbsolutePath() + PROGRESS_TMP_FILE_SUFFIX));
  }

  private static void deleteFileIfExists(final File file) {
    if (file.exists() && !file.delete()) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_FAILED_TO_DELETE_CONSENSUS_SUBSCRIPTION_PROGRESS_FILE_51C57096,
          file);
    }
  }

  private static WriterId buildWriterId(final String regionIdStr, final int writerNodeId) {
    return writerNodeId >= 0 ? new WriterId(regionIdStr, writerNodeId) : null;
  }

  static String buildBroadcastKey(final String key, final WriterId writerId) {
    return key + KEY_SEPARATOR + (Objects.nonNull(writerId) ? writerId.getNodeId() : -1);
  }

  private byte[] serializeCommitState(final ConsensusSubscriptionCommitState state)
      throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos)) {
      state.serialize(dos);
      return baos.toByteArray();
    }
  }

  private void persistProgressIfNeeded(
      final CommitStateKey stateKey, final ConsensusSubscriptionCommitState state) {
    final int interval =
        SubscriptionConfig.getInstance().getSubscriptionConsensusCommitPersistInterval();
    if (interval > 0 && state.getProgress().getPersistenceThrottleCounter() % interval == 0) {
      persistRegionProgress(stateKey, state);
    }
  }

  private void persistRegionProgress(
      final CommitStateKey stateKey, final ConsensusSubscriptionCommitState state) {
    recoverTopicStatesIfNeeded(stateKey);
    synchronized (getTopicPersistLock(stateKey.topicFileKey)) {
      try {
        final byte[] payload = serializeCommitState(state);
        final Map<String, ProgressIndexEntry> indexEntries =
            topicProgressIndexes.getOrDefault(stateKey.topicFileKey, Collections.emptyMap());
        final ProgressIndexEntry indexEntry = indexEntries.get(stateKey.regionIdStr);
        final File metaFile = getMetaFile(stateKey.topicFileKey);
        if (Objects.nonNull(indexEntry)
            && indexEntry.payloadLength == payload.length
            && metaFile.exists()
            && metaFile.length() >= indexEntry.payloadOffset + indexEntry.payloadLength) {
          overwriteRegionPayload(metaFile, indexEntry.payloadOffset, payload);
          return;
        }
        rewriteTopicProgressFilesUnderLock(stateKey);
      } catch (final IOException e) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_FAILED_TO_PERSIST_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_4EA71236,
            stateKey.consumerGroupId,
            stateKey.topicName,
            stateKey.regionIdStr,
            e);
      }
    }
  }

  private void overwriteRegionPayload(
      final File metaFile, final long payloadOffset, final byte[] payload) throws IOException {
    try (final RandomAccessFile randomAccessFile = new RandomAccessFile(metaFile, "rw")) {
      randomAccessFile.seek(payloadOffset);
      randomAccessFile.write(payload);
      if (SubscriptionConfig.getInstance().isSubscriptionConsensusCommitFsyncEnabled()) {
        randomAccessFile.getFD().sync();
      }
    }
  }

  private void rewriteTopicProgressFiles(final CommitStateKey stateKey) {
    synchronized (getTopicPersistLock(stateKey.topicFileKey)) {
      try {
        rewriteTopicProgressFilesUnderLock(stateKey);
      } catch (final IOException e) {
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_FAILED_TO_REWRITE_CONSENSUS_SUBSCRIPTION_PROGRESS_FOR_CONSUMERGROUPID_8B230D50,
            stateKey.consumerGroupId,
            stateKey.topicName,
            e);
      }
    }
  }

  private void rewriteTopicProgressFilesUnderLock(final CommitStateKey stateKey)
      throws IOException {
    final Map<String, ConsensusSubscriptionCommitState> topicStates =
        collectTopicStates(stateKey.topicFileKey);
    if (topicStates.isEmpty()) {
      deleteTopicProgressFiles(stateKey.topicFileKey);
      topicProgressIndexes.remove(stateKey.topicFileKey);
      return;
    }

    final ByteArrayOutputStream metaBytes = new ByteArrayOutputStream();
    final Map<String, ProgressIndexEntry> indexEntries = new LinkedHashMap<>();
    try (final DataOutputStream metaStream = new DataOutputStream(metaBytes)) {
      ReadWriteIOUtils.write(TOPIC_PROGRESS_FILE_VERSION, metaStream);
      ReadWriteIOUtils.write(topicStates.size(), metaStream);
      for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
          topicStates.entrySet()) {
        final byte[] payload = serializeCommitState(entry.getValue());
        ReadWriteIOUtils.write(entry.getKey(), metaStream);
        ReadWriteIOUtils.write(payload.length, metaStream);
        final long payloadOffset = metaBytes.size();
        metaStream.write(payload);
        final ProgressIndexEntry indexEntry = new ProgressIndexEntry(payloadOffset, payload.length);
        indexEntries.put(entry.getKey(), indexEntry);
      }
    }
    writeFileAtomically(getMetaFile(stateKey.topicFileKey), metaBytes.toByteArray());
    topicProgressIndexes.put(stateKey.topicFileKey, indexEntries);
    recoveredTopicKeys.add(stateKey.topicFileKey);
  }

  private Map<String, ConsensusSubscriptionCommitState> collectTopicStates(
      final String topicFileKey) {
    final Map<String, ConsensusSubscriptionCommitState> topicStates = new TreeMap<>();
    for (final Map.Entry<String, CommitStateKey> entry : commitStateKeys.entrySet()) {
      final CommitStateKey candidate = entry.getValue();
      if (!candidate.topicFileKey.equals(topicFileKey)) {
        continue;
      }
      final ConsensusSubscriptionCommitState state = commitStates.get(entry.getKey());
      if (Objects.nonNull(state)) {
        topicStates.put(candidate.regionIdStr, state);
      }
    }
    return topicStates;
  }

  private void writeFileAtomically(final File targetFile, final byte[] bytes) throws IOException {
    final File parent = targetFile.getParentFile();
    if (Objects.nonNull(parent) && !parent.exists()) {
      parent.mkdirs();
    }
    final File tmpFile = new File(targetFile.getAbsolutePath() + PROGRESS_TMP_FILE_SUFFIX);
    try (final FileOutputStream fos = new FileOutputStream(tmpFile)) {
      fos.write(bytes);
      fos.flush();
      if (SubscriptionConfig.getInstance().isSubscriptionConsensusCommitFsyncEnabled()) {
        fos.getFD().sync();
      }
    }
    try {
      Files.move(
          tmpFile.toPath(),
          targetFile.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      try {
        Files.move(tmpFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      } catch (final IOException fallbackException) {
        deleteFileIfExists(tmpFile);
        throw fallbackException;
      }
    } catch (final IOException e) {
      deleteFileIfExists(tmpFile);
      throw e;
    }
  }

  private ConfigNodeProgressQueryResult queryCommitProgressFromConfigNode(
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
        LOGGER.warn(
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_FAILED_TO_QUERY_COMMIT_31E47F21,
            consumerGroupId,
            topicName,
            regionId,
            resp.status);
        return ConfigNodeProgressQueryResult.unavailable();
      }
      if (resp.isSetCommittedRegionProgress()) {
        final RegionProgress committedRegionProgress =
            deserializeRegionProgress(
                ByteBuffer.wrap(resp.getCommittedRegionProgress()).asReadOnlyBuffer());
        final RegionProgress availableProgress =
            Objects.nonNull(committedRegionProgress)
                ? committedRegionProgress
                : new RegionProgress(Collections.emptyMap());
        if (!availableProgress.getWriterPositions().isEmpty()) {
          LOGGER.info(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_RECOVERED_COMMITTEDREGIONPROGRESS_F6B92C6B,
              availableProgress,
              consumerGroupId,
              topicName,
              regionId);
        }
        return ConfigNodeProgressQueryResult.available(availableProgress);
      }
      return ConfigNodeProgressQueryResult.absent();
    } catch (final ClientManagerException | TException | RuntimeException e) {
      LOGGER.warn(
          DataNodePipeMessages
              .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITMANAGER_FAILED_TO_QUERY_COMMIT_16CFDCD9,
          consumerGroupId,
          topicName,
          regionId,
          e);
      return ConfigNodeProgressQueryResult.unavailable();
    }
  }

  private static ByteBuffer serializeRegionProgress(final RegionProgress regionProgress) {
    if (Objects.isNull(regionProgress)) {
      return null;
    }
    try (final PublicBAOS baos = new PublicBAOS();
        final DataOutputStream dos = new DataOutputStream(baos)) {
      regionProgress.serialize(dos);
      return ByteBuffer.wrap(baos.getBuf(), 0, baos.size());
    } catch (final IOException e) {
      LOGGER.warn(
          DataNodePipeMessages.PIPE_LOG_FAILED_TO_SERIALIZE_COMMITTED_REGION_PROGRESS_0D8D2129,
          regionProgress,
          e);
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

  private static RegionProgress copyRegionProgress(final RegionProgress regionProgress) {
    return new RegionProgress(
        Objects.nonNull(regionProgress)
            ? new LinkedHashMap<>(regionProgress.getWriterPositions())
            : Collections.emptyMap());
  }

  @FunctionalInterface
  interface ConfigNodeProgressFetcher {

    ConfigNodeProgressQueryResult fetch(
        String consumerGroupId, String topicName, ConsensusGroupId regionId);
  }

  enum ConfigNodeProgressAvailability {
    AVAILABLE,
    ABSENT,
    UNAVAILABLE
  }

  static final class ConfigNodeProgressQueryResult {

    private static final ConfigNodeProgressQueryResult ABSENT =
        new ConfigNodeProgressQueryResult(
            ConfigNodeProgressAvailability.ABSENT, new RegionProgress(Collections.emptyMap()));

    private static final ConfigNodeProgressQueryResult UNAVAILABLE =
        new ConfigNodeProgressQueryResult(
            ConfigNodeProgressAvailability.UNAVAILABLE, new RegionProgress(Collections.emptyMap()));

    private final ConfigNodeProgressAvailability availability;

    private final RegionProgress regionProgress;

    private ConfigNodeProgressQueryResult(
        final ConfigNodeProgressAvailability availability, final RegionProgress regionProgress) {
      this.availability = availability;
      this.regionProgress = copyRegionProgress(regionProgress);
    }

    static ConfigNodeProgressQueryResult available(final RegionProgress regionProgress) {
      return new ConfigNodeProgressQueryResult(
          ConfigNodeProgressAvailability.AVAILABLE, regionProgress);
    }

    static ConfigNodeProgressQueryResult absent() {
      return ABSENT;
    }

    static ConfigNodeProgressQueryResult unavailable() {
      return UNAVAILABLE;
    }

    boolean isAvailable() {
      return availability == ConfigNodeProgressAvailability.AVAILABLE;
    }

    ConfigNodeProgressAvailability getAvailability() {
      return availability;
    }

    RegionProgress getRegionProgress() {
      return copyRegionProgress(regionProgress);
    }
  }

  static final class SetupSnapshot {

    private final String consumerGroupId;

    private final Map<String, TopicSetupSnapshot> topicSnapshots;

    private SetupSnapshot(
        final String consumerGroupId, final Map<String, TopicSetupSnapshot> topicSnapshots) {
      this.consumerGroupId = consumerGroupId;
      this.topicSnapshots = topicSnapshots;
    }
  }

  private static final class TopicSetupSnapshot {

    private final String topicFileKey;

    private final Map<String, StateSetupSnapshot> stateSnapshots;

    private TopicSetupSnapshot(
        final String topicFileKey, final Map<String, StateSetupSnapshot> stateSnapshots) {
      this.topicFileKey = topicFileKey;
      this.stateSnapshots = stateSnapshots;
    }
  }

  private static final class StateSetupSnapshot {

    private final SubscriptionConsensusProgress progress;

    private final boolean initialProgressPending;

    private final boolean newRegionProgressPending;

    private final boolean configNodeProgressAvailable;

    private final boolean tailProposalInitialized;

    private StateSetupSnapshot(
        final SubscriptionConsensusProgress progress,
        final boolean initialProgressPending,
        final boolean newRegionProgressPending,
        final boolean configNodeProgressAvailable,
        final boolean tailProposalInitialized) {
      this.progress = progress;
      this.initialProgressPending = initialProgressPending;
      this.newRegionProgressPending = newRegionProgressPending;
      this.configNodeProgressAvailable = configNodeProgressAvailable;
      this.tailProposalInitialized = tailProposalInitialized;
    }
  }

  private static final class CommitStateKey {

    private final String consumerGroupId;

    private final String topicName;

    private final String regionIdStr;

    private final String encodedStateKey;

    private final String topicFileKey;

    private CommitStateKey(
        final String consumerGroupId,
        final String topicName,
        final String regionIdStr,
        final String encodedStateKey,
        final String topicFileKey) {
      this.consumerGroupId = consumerGroupId;
      this.topicName = topicName;
      this.regionIdStr = regionIdStr;
      this.encodedStateKey = encodedStateKey;
      this.topicFileKey = topicFileKey;
    }

    private boolean sameTopic(final String consumerGroupId, final String topicName) {
      return Objects.equals(this.consumerGroupId, consumerGroupId)
          && Objects.equals(this.topicName, topicName);
    }
  }

  private static final class TopicProgressSnapshot {

    private final Map<String, ConsensusSubscriptionCommitState> states;

    private final Map<String, ProgressIndexEntry> indexEntries;

    private TopicProgressSnapshot(
        final Map<String, ConsensusSubscriptionCommitState> states,
        final Map<String, ProgressIndexEntry> indexEntries) {
      this.states = states;
      this.indexEntries = indexEntries;
    }

    private static TopicProgressSnapshot empty() {
      return new TopicProgressSnapshot(Collections.emptyMap(), Collections.emptyMap());
    }
  }

  private static final class ProgressIndexEntry {

    private final long payloadOffset;

    private final int payloadLength;

    private ProgressIndexEntry(final long payloadOffset, final int payloadLength) {
      this.payloadOffset = payloadOffset;
      this.payloadLength = payloadLength;
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
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IGNORE_MAPPING_WITHOUT_3E66A74D,
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
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_DUPLICATE_OUTSTANDING_MAPPING_B5B34891,
              slot,
              previous,
              key);
        }
        final int size = outstandingKeys.size();
        if (size > OUTSTANDING_SIZE_WARN_THRESHOLD && size % OUTSTANDING_SIZE_WARN_THRESHOLD == 1) {
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_OUTSTANDING_SIZE_EXCEEDS_1463BF02,
              size,
              OUTSTANDING_SIZE_WARN_THRESHOLD,
              getCommittedPhysicalTime(),
              getCommittedLocalSeq(),
              getCommittedWriterNodeId());
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
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_MISSING_WRITER_IDENTITY_01040357,
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
                DataNodePipeMessages
                    .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IDEMPOTENT_RE_COMMIT_FOR_30464FC4,
                key.physicalTime,
                key.writerNodeId,
                key.localSeq);
            progress.incrementPersistenceThrottleCounter();
            return CommitOperationResult.handledWithoutAdvance();
          }
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_UNKNOWN_KEY_FOR_COMMIT_5F699CFD,
              key.physicalTime,
              key.writerNodeId,
              key.localSeq);
          return CommitOperationResult.unhandled();
        }
        final ProgressKey effectiveKey = recordedKey.resolveMissingFields(writerId, writerProgress);
        final WriterId effectiveWriterId = effectiveKey.toWriterId(regionId);
        final WriterProgress before = getCommittedWriterProgressForWriter(effectiveWriterId);
        recentlyCommittedKeys.add(effectiveKey);
        stageCommittedAndAdvance(effectiveKey);
        progress.incrementPersistenceThrottleCounter();
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
            DataNodePipeMessages
                .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_MISSING_WRITER_IDENTITY_BB10A3B1,
            writerId,
            writerProgress);
        return CommitOperationResult.unhandled();
      }
      final ProgressKey incomingKey = new ProgressKey(writerId, writerProgress);

      synchronized (this) {
        if (recentlyCommittedKeys.contains(incomingKey)) {
          LOGGER.debug(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_IDEMPOTENT_DIRECT_COMMIT_B093AC01,
              incomingKey.physicalTime,
              incomingKey.writerNodeId,
              incomingKey.localSeq);
          progress.incrementPersistenceThrottleCounter();
          return CommitOperationResult.handledWithoutAdvance();
        }

        final ProgressKey outstandingKey = outstandingKeys.remove(ProgressSlot.from(incomingKey));
        if (Objects.isNull(outstandingKey)) {
          LOGGER.warn(
              DataNodePipeMessages
                  .PIPE_LOG_CONSENSUSSUBSCRIPTIONCOMMITSTATE_REJECT_DIRECT_COMMIT_WITHOUT_5B975E49,
              incomingKey.physicalTime,
              incomingKey.writerNodeId,
              incomingKey.localSeq);
          return CommitOperationResult.unhandled();
        }
        final ProgressKey effectiveKey =
            outstandingKey.resolveMissingFields(writerId, writerProgress);
        final WriterId effectiveWriterId = effectiveKey.toWriterId(regionId);
        final WriterProgress before = getCommittedWriterProgressForWriter(effectiveWriterId);
        recentlyCommittedKeys.add(effectiveKey);
        stageCommittedAndAdvance(effectiveKey);
        progress.incrementPersistenceThrottleCounter();
        syncPersistedProgress();
        return buildCommitOperationResult(
            effectiveWriterId, before, getCommittedWriterProgressForWriter(effectiveWriterId));
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
        final ProgressKey broadcastKey = new ProgressKey(writerId, writerProgress);
        final WriterId broadcastWriterId = broadcastKey.toWriterId(regionId);
        final WriterProgress currentWriterProgress =
            getCommittedWriterProgressForWriter(broadcastWriterId);
        final ProgressKey current = new ProgressKey(broadcastWriterId, currentWriterProgress);
        if (broadcastKey.compareTo(current) > 0) {
          committedWriterPositions.put(broadcastWriterId, broadcastKey.toWriterProgress());
          progress.incrementPersistenceThrottleCounter();
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
      return Objects.nonNull(writerId) && writerId.getNodeId() == key.writerNodeId;
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
      return Objects.nonNull(maxKey) ? maxKey : new ProgressKey(0L, -1, -1L);
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
    final long physicalTime;
    final int writerNodeId;
    final long localSeq;

    private ProgressSlot(final long physicalTime, final int writerNodeId, final long localSeq) {
      this.physicalTime = physicalTime;
      this.writerNodeId = writerNodeId;
      this.localSeq = localSeq;
    }

    static ProgressSlot of(final long physicalTime, final int writerNodeId, final long localSeq) {
      return new ProgressSlot(physicalTime, writerNodeId, localSeq);
    }

    static ProgressSlot from(final ProgressKey key) {
      return new ProgressSlot(key.physicalTime, key.writerNodeId, key.localSeq);
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
      return physicalTime == that.physicalTime
          && writerNodeId == that.writerNodeId
          && localSeq == that.localSeq;
    }

    @Override
    public int hashCode() {
      return Objects.hash(physicalTime, writerNodeId, localSeq);
    }

    @Override
    public String toString() {
      return "(" + physicalTime + "," + writerNodeId + "," + localSeq + ")";
    }
  }

  // ======================== ProgressKey ========================

  /**
   * Comparable key for tracking commit progress: (physicalTime, nodeId, localSeq). Physical time
   * takes priority; within the same physical time, writer node id and source searchIndex determine
   * order.
   */
  static final class ProgressKey implements Comparable<ProgressKey> {
    final long physicalTime;
    final int writerNodeId;
    final long localSeq;

    ProgressKey(final long physicalTime, final long localSeq) {
      this(physicalTime, -1, localSeq);
    }

    ProgressKey(final WriterId writerId, final WriterProgress writerProgress) {
      this(
          Objects.nonNull(writerProgress) ? writerProgress.getPhysicalTime() : 0L,
          Objects.nonNull(writerId) ? writerId.getNodeId() : -1,
          Objects.nonNull(writerProgress) ? writerProgress.getLocalSeq() : -1L);
    }

    ProgressKey(final long physicalTime, final int writerNodeId, final long localSeq) {
      this.physicalTime = physicalTime;
      this.writerNodeId = writerNodeId;
      this.localSeq = localSeq;
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
      if (effectivePhysicalTime == this.physicalTime
          && effectiveLocalSeq == this.localSeq
          && effectiveWriterNodeId == this.writerNodeId) {
        return this;
      }
      return new ProgressKey(effectivePhysicalTime, effectiveWriterNodeId, effectiveLocalSeq);
    }

    WriterId toWriterId(final String regionId) {
      return writerNodeId >= 0 ? new WriterId(regionId, writerNodeId) : null;
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
          && writerNodeId == that.writerNodeId
          && localSeq == that.localSeq;
    }

    @Override
    public int hashCode() {
      return Objects.hash(physicalTime, writerNodeId, localSeq);
    }

    @Override
    public String toString() {
      return "(" + physicalTime + "," + writerNodeId + "," + localSeq + ")";
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
