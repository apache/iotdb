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

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages commit state for consensus-based subscriptions.
 *
 * <p>This manager tracks which events have been committed by consumers and maps commit IDs back to
 * WAL search indices. It maintains the progress for each (consumerGroup, topic, region) triple and
 * supports persistence and recovery.
 *
 * <p>Progress is tracked <b>per-region</b> because searchIndex is region-local — each DataRegion
 * has its own independent WAL with its own searchIndex namespace. Using a single state per topic
 * would cause TreeSet deduplication bugs when different regions emit the same searchIndex value.
 *
 * <p>Key responsibilities:
 *
 * <ul>
 *   <li>Track the mapping from commitId to searchIndex
 *   <li>Handle commit/ack from consumers
 *   <li>Persist and recover progress state
 * </ul>
 */
public class ConsensusSubscriptionCommitManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConsensusSubscriptionCommitManager.class);

  private static final String PROGRESS_FILE_PREFIX = "consensus_subscription_progress_";
  private static final String PROGRESS_FILE_SUFFIX = ".dat";

  /** Key: "consumerGroupId_topicName_regionId" -> progress tracking state */
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
   * @param regionId the consensus group / data region ID string
   * @return the commit state
   */
  public ConsensusSubscriptionCommitState getOrCreateState(
      final String consumerGroupId, final String topicName, final String regionId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    return commitStates.computeIfAbsent(
        key,
        k -> {
          // Try to recover from persisted state
          final ConsensusSubscriptionCommitState recovered = tryRecover(key);
          if (recovered != null) {
            return recovered;
          }
          return new ConsensusSubscriptionCommitState(new SubscriptionConsensusProgress(0L, 0L));
        });
  }

  /**
   * Records commitId to searchIndex mapping for later commit handling.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID string
   * @param commitId the assigned commit ID
   * @param searchIndex the WAL search index corresponding to this event
   */
  public void recordCommitMapping(
      final String consumerGroupId,
      final String topicName,
      final String regionId,
      final long commitId,
      final long searchIndex) {
    final ConsensusSubscriptionCommitState state =
        getOrCreateState(consumerGroupId, topicName, regionId);
    state.recordMapping(commitId, searchIndex);
  }

  /**
   * Handles commit (ack) for an event. Updates the progress and potentially advances the committed
   * search index.
   *
   * @param consumerGroupId the consumer group ID
   * @param topicName the topic name
   * @param regionId the consensus group / data region ID string
   * @param commitId the committed event's commit ID
   * @return true if commit handled successfully
   */
  public boolean commit(
      final String consumerGroupId,
      final String topicName,
      final String regionId,
      final long commitId) {
    final String key = generateKey(consumerGroupId, topicName, regionId);
    final ConsensusSubscriptionCommitState state = commitStates.get(key);
    if (state == null) {
      LOGGER.warn(
          "ConsensusSubscriptionCommitManager: Cannot commit for unknown state, "
              + "consumerGroupId={}, topicName={}, regionId={}, commitId={}",
          consumerGroupId,
          topicName,
          regionId,
          commitId);
      return false;
    }
    final boolean success = state.commit(commitId);
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
   * @param regionId the consensus group / data region ID string
   * @return the committed search index, or -1 if no state exists
   */
  public long getCommittedSearchIndex(
      final String consumerGroupId, final String topicName, final String regionId) {
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
   * @param regionId the consensus group / data region ID string
   */
  public void removeState(
      final String consumerGroupId, final String topicName, final String regionId) {
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

  /** Persists all states. Should be called during graceful shutdown. */
  public void persistAll() {
    for (final Map.Entry<String, ConsensusSubscriptionCommitState> entry :
        commitStates.entrySet()) {
      persistProgress(entry.getKey(), entry.getValue());
    }
  }

  // ======================== Helper Methods ========================

  // Use a separator that cannot appear in consumerGroupId, topicName, or regionId
  // to prevent key collisions (e.g., "a_b" + "c" vs "a" + "b_c").
  private static final String KEY_SEPARATOR = "##";

  private String generateKey(
      final String consumerGroupId, final String topicName, final String regionId) {
    return consumerGroupId + KEY_SEPARATOR + topicName + KEY_SEPARATOR + regionId;
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

  private void persistProgressIfNeeded(
      final String key, final ConsensusSubscriptionCommitState state) {
    // Persist every 100 commits to reduce disk IO
    if (state.getProgress().getCommitIndex() % 100 == 0) {
      persistProgress(key, state);
    }
  }

  private void persistProgress(final String key, final ConsensusSubscriptionCommitState state) {
    final File file = getProgressFile(key);
    try (final FileOutputStream fos = new FileOutputStream(file);
        final DataOutputStream dos = new DataOutputStream(fos)) {
      state.serialize(dos);
      dos.flush();
    } catch (final IOException e) {
      LOGGER.warn("Failed to persist consensus subscription progress to {}", file, e);
    }
  }

  // ======================== Inner State Class ========================

  /**
   * Tracks commit state for a single (consumerGroup, topic, region) triple. Maintains the mapping
   * from commitId to searchIndex and tracks committed progress within one region's WAL.
   */
  public static class ConsensusSubscriptionCommitState {

    private final SubscriptionConsensusProgress progress;

    /**
     * Maps commitId -> searchIndex. Records which WAL search index corresponds to each committed
     * event. Entries are removed once committed.
     */
    private final Map<Long, Long> commitIdToSearchIndex = new ConcurrentHashMap<>();

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

    public void recordMapping(final long commitId, final long searchIndex) {
      synchronized (this) {
        commitIdToSearchIndex.put(commitId, searchIndex);
        outstandingSearchIndices.add(searchIndex);
        final int size = outstandingSearchIndices.size();
        if (size > OUTSTANDING_SIZE_WARN_THRESHOLD && size % OUTSTANDING_SIZE_WARN_THRESHOLD == 1) {
          LOGGER.warn(
              "ConsensusSubscriptionCommitState: outstandingSearchIndices size ({}) exceeds "
                  + "threshold ({}), consumers may not be committing. committedSearchIndex={}, "
                  + "maxCommittedSearchIndex={}, commitIdToSearchIndex size={}",
              size,
              OUTSTANDING_SIZE_WARN_THRESHOLD,
              committedSearchIndex,
              maxCommittedSearchIndex,
              commitIdToSearchIndex.size());
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
     * @param commitId the commit ID to commit
     * @return true if successfully committed
     */
    public boolean commit(final long commitId) {
      progress.incrementCommitIndex();

      // Advance committed search index contiguously (gap-aware).
      // Both remove from commitIdToSearchIndex and outstandingSearchIndices must be
      // inside the same synchronized block to prevent a race with recordMapping():
      //   recordMapping: put(commitId, si) -> add(si)
      //   commit:        remove(commitId) -> remove(si)
      // Without atomicity, commit could remove from map between put and add,
      // leaving si permanently in outstandingSearchIndices (WAL leak).
      synchronized (this) {
        final Long searchIndex = commitIdToSearchIndex.remove(commitId);
        if (searchIndex == null) {
          LOGGER.warn("ConsensusSubscriptionCommitState: unknown commitId {} for commit", commitId);
          return false;
        }
        outstandingSearchIndices.remove(searchIndex);
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
