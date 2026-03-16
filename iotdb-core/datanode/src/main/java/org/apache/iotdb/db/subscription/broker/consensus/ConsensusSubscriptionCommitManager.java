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

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.subscription.config.SubscriptionConfig;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetCommitProgressResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
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
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

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
              new SubscriptionConsensusProgress(fallbackSearchIndex, 0L));
        });
  }

  /**
   * Records a dispatched event's search index for commit tracking.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID
   * @param searchIndex the WAL search index corresponding to this event
   */
  public void recordMapping(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final long searchIndex) {
    final ConsensusSubscriptionCommitState state =
        getOrCreateState(consumerGroupId, topicName, regionId);
    state.recordMapping(searchIndex);
  }

  /**
   * Handles commit (ack) for an event. Updates the progress and potentially advances the committed
   * search index.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID
   * @param searchIndex the end search index of the committed event
   * @return true if commit handled successfully
   */
  public boolean commit(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final long searchIndex) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      LOGGER.warn(
          "ConsensusSubscriptionCommitManager: Cannot commit for unknown state, "
              + "consumerGroupId={}, topicName={}, regionId={}, searchIndex={}",
          consumerGroupId,
          topicName,
          regionId,
          searchIndex);
      return false;
    }
    final boolean success = state.commit(searchIndex);
    if (success) {
      // Periodically persist progress
      persistProgressIfNeeded(key, state);
    }
    return success;
  }

  /**
   * Gets the current committed search index for a specific region's state.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID
   * @return the committed search index, or -1 if no state exists
   */
  public long getCommittedSearchIndex(
      final String consumerGroupId, final String topicName, final ConsensusGroupId regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      return -1;
    }
    return state.getCommittedSearchIndex();
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
   * Resets the commit state for a specific (consumerGroup, topic, region) triple to a new search
   * index. Used by seek operations to discard all outstanding commit tracking and restart from the
   * specified position.
   */
  public void resetState(
      final String consumerGroupId,
      final String topicName,
      final ConsensusGroupId regionId,
      final long newSearchIndex) {
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
    state.resetForSeek(newSearchIndex);
    persistProgress(key, state);
  }

  /** Persists all states. Should be called during graceful shutdown. */
  public void persistAll() {
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      persistProgress(entry.getKey(), entry.getValue());
    }
  }

  /** Collects all current committedSearchIndex values for reporting to ConfigNode. */
  public Map<String, Long> collectAllProgress(final int dataNodeId) {
    final Map<String, Long> result = new ConcurrentHashMap<>();
    final String suffix = KEY_SEPARATOR + dataNodeId;
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      result.put(entry.getKey() + suffix, entry.getValue().getCommittedSearchIndex());
    }
    return result;
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
   * Tracks commit state for a single (consumerGroup, topic, region) triple. Tracks outstanding and
   * committed search indices within one region's WAL.
   */
  public static class ConsensusSubscriptionCommitState {

    private final SubscriptionConsensusProgress progress;

    /** LRU set of recently committed search indices for idempotent re-commit detection. */
    private static final int RECENTLY_COMMITTED_CAPACITY = 1024;

    private final Set<Long> recentlyCommittedSearchIndices =
        Collections.newSetFromMap(
            new LinkedHashMap<Long, Boolean>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry<Long, Boolean> eldest) {
                return size() > RECENTLY_COMMITTED_CAPACITY;
              }
            });

    /**
     * Tracks the safe recovery position: the highest search index where all prior dispatched events
     * have been committed. Only advances contiguously — never jumps over uncommitted gaps.
     */
    private volatile long committedSearchIndex;

    /**
     * Tracks the maximum search index among all committed events (may be ahead of
     * committedSearchIndex when out-of-order commits exist). Used to update committedSearchIndex
     * once all outstanding events are committed.
     */
    private long maxCommittedSearchIndex;

    /**
     * Tracks search indices of dispatched but not-yet-committed events. Used to prevent
     * committedSearchIndex from jumping over uncommitted gaps. On commit, the frontier advances to
     * min(outstanding) - 1 (or maxCommittedSearchIndex if empty).
     *
     * <p>Since state is now per-region, searchIndex values within this set are guaranteed unique
     * (they come from a single region's monotonically increasing WAL searchIndex).
     */
    private final TreeSet<Long> outstandingSearchIndices = new TreeSet<>();

    public ConsensusSubscriptionCommitState(final SubscriptionConsensusProgress progress) {
      this.progress = progress;
      this.committedSearchIndex = progress.getSearchIndex();
      this.maxCommittedSearchIndex = progress.getSearchIndex();
    }

    public SubscriptionConsensusProgress getProgress() {
      return progress;
    }

    public long getCommittedSearchIndex() {
      return committedSearchIndex;
    }

    /** Threshold for warning about outstanding (uncommitted) search indices accumulation. */
    private static final int OUTSTANDING_SIZE_WARN_THRESHOLD = 10000;

    public void recordMapping(final long searchIndex) {
      synchronized (this) {
        outstandingSearchIndices.add(searchIndex);
        final int size = outstandingSearchIndices.size();
        if (size > OUTSTANDING_SIZE_WARN_THRESHOLD && size % OUTSTANDING_SIZE_WARN_THRESHOLD == 1) {
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: outstandingSearchIndices size ({}) exceeds "
                  + "threshold ({}), consumers may not be committing. committedSearchIndex={}, "
                  + "maxCommittedSearchIndex={}",
              size,
              OUTSTANDING_SIZE_WARN_THRESHOLD,
              committedSearchIndex,
              maxCommittedSearchIndex);
        }
      }
    }

    /**
     * Commits the specified event and advances the committed search index contiguously.
     *
     * <p>The committed search index only advances to a position where all prior dispatched events
     * have been committed. This prevents the recovery position from jumping over uncommitted gaps,
     * ensuring at-least-once delivery even after crash recovery.
     *
     * @param searchIndex the end search index of the event to commit
     * @return true if successfully committed
     */
    public boolean commit(final long searchIndex) {
      progress.incrementCommitIndex();

      synchronized (this) {
        if (!outstandingSearchIndices.remove(searchIndex)) {
          // Check if this is an idempotent re-commit
          if (recentlyCommittedSearchIndices.contains(searchIndex)) {
            LOGGER.debug(
                "ConsensusSubscriptionCommitState: idempotent re-commit for searchIndex {}",
                searchIndex);
            return true;
          }
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: unknown searchIndex {} for commit", searchIndex);
          return false;
        }
        recentlyCommittedSearchIndices.add(searchIndex);
        if (searchIndex > maxCommittedSearchIndex) {
          maxCommittedSearchIndex = searchIndex;
        }

        if (outstandingSearchIndices.isEmpty()) {
          // All dispatched events have been committed — advance to the max
          committedSearchIndex = maxCommittedSearchIndex;
        } else {
          // Advance to just below the earliest uncommitted event
          // (never go backward)
          committedSearchIndex =
              Math.max(committedSearchIndex, outstandingSearchIndices.first() - 1);
        }
        progress.setSearchIndex(committedSearchIndex);
      }

      return true;
    }

    /**
     * Resets all commit tracking state for a seek operation. Clears all outstanding mappings and
     * resets progress to the new search index position.
     */
    public void resetForSeek(final long newSearchIndex) {
      synchronized (this) {
        outstandingSearchIndices.clear();
        recentlyCommittedSearchIndices.clear();
        final long baseIndex = newSearchIndex - 1;
        committedSearchIndex = baseIndex;
        maxCommittedSearchIndex = baseIndex;
        progress.setSearchIndex(baseIndex);
      }
    }

    public void serialize(final DataOutputStream stream) throws IOException {
      progress.serialize(stream);
      stream.writeLong(committedSearchIndex);
    }

    public static ConsensusSubscriptionCommitState deserialize(final ByteBuffer buffer) {
      final SubscriptionConsensusProgress progress =
          SubscriptionConsensusProgress.deserialize(buffer);
      final ConsensusSubscriptionCommitState state = new ConsensusSubscriptionCommitState(progress);
      state.committedSearchIndex = buffer.getLong();
      state.maxCommittedSearchIndex = state.committedSearchIndex;
      return state;
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
