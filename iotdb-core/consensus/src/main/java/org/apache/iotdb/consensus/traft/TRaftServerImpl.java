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

package org.apache.iotdb.consensus.traft;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.TRaftConfig;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class TRaftServerImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(TRaftServerImpl.class);
  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final long INITIAL_PARTITION_INDEX = 0;

  // ── persistent / structural state ───────────────────────────────────────────
  private final String storageDir;
  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final TreeSet<Peer> configuration = new TreeSet<>();
  private final ConcurrentHashMap<Integer, TRaftFollowerInfo> followerInfoMap =
      new ConcurrentHashMap<>();
  private final TRaftLogStore logStore;
  private final AtomicLong logicalClock = new AtomicLong(0);
  private final Random random;

  // ── volatile node state ──────────────────────────────────────────────────────
  private volatile TRaftRole role = TRaftRole.FOLLOWER;
  private volatile boolean active = true;
  private volatile boolean started = false;

  // ── term / election state ────────────────────────────────────────────────────
  private long currentTerm = 0;
  private int leaderId = -1;
  private int votedFor = -1;

  // ── partition tracking ───────────────────────────────────────────────────────
  private long historicalMaxTimestamp = Long.MIN_VALUE;
  private long maxPartitionIndex = INITIAL_PARTITION_INDEX;
  private long currentPartitionIndexCount = 0;

  /**
   * The partition index of entries currently being actively inserted by the leader (fast path).
   * Entries arriving with a partition index equal to this value are eligible for direct
   * in-memory replication to matching followers. Entries with a higher partition index are
   * written directly to disk and replicated by the per-follower {@link TRaftLogAppender}.
   */
  private long currentLeaderInsertingPartitionIndex = INITIAL_PARTITION_INDEX;

  // ── quorum commit tracking ───────────────────────────────────────────────────
  /**
   * Tracks, per log index, how many followers have acknowledged that entry. Once the ACK count
   * reaches ⌈followerCount/2⌉ the entry is considered committed and removed from the map.
   * The map becoming empty signals that all entries of the current partition have achieved
   * quorum, allowing the next partition's entries to be transmitted.
   */
  private final ConcurrentHashMap<Long, AtomicInteger> currentReplicationgIndicesToAckFollowerCount =
      new ConcurrentHashMap<>();

  // ── background appenders ─────────────────────────────────────────────────────
  /** One background appender thread per follower, keyed by follower node-id. */
  private final ConcurrentHashMap<Integer, TRaftLogAppender> appenderMap =
      new ConcurrentHashMap<>();

  private ExecutorService appenderExecutor;

  private TRaftConfig config;

  // ────────────────────────────────────────────────────────────────────────────
  // Construction
  // ────────────────────────────────────────────────────────────────────────────

  TRaftServerImpl(
      String storageDir,
      Peer thisNode,
      TreeSet<Peer> peers,
      IStateMachine stateMachine,
      TRaftConfig config)
      throws IOException {
    this.storageDir = storageDir;
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.config = config;
    this.logStore = new TRaftLogStore(storageDir);
    this.random = new Random(config.getElection().getRandomSeed() + thisNode.getNodeId());
    if (peers.isEmpty()) {
      this.configuration.addAll(loadConfigurationFromDisk());
    } else {
      this.configuration.addAll(peers);
      persistConfiguration();
    }
    if (!this.configuration.contains(thisNode)) {
      this.configuration.add(thisNode);
      persistConfiguration();
    }
    recoverFromDisk();
    electInitialLeader();
    initFollowerInfoMap();
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Lifecycle
  // ────────────────────────────────────────────────────────────────────────────

  public synchronized void start() {
    if (started) {
      return;
    }
    stateMachine.start();
    started = true;
    notifyLeaderChanged();
    if (role == TRaftRole.LEADER) {
      initAppenders();
    }
  }

  public synchronized void stop() {
    // Mark as stopped first so appender callbacks become no-ops immediately.
    started = false;
    stopAppenders();
    stateMachine.stop();
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Write path (Leader)
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Process a client write request as the leader.
   *
   * <p><b>Step 1 – Parse:</b> Build a {@link TRaftLogEntry} with timestamp and partition index
   * derived from the request.
   *
   * <p><b>Step 2 – Route by partition index:</b>
   * <ul>
   *   <li>If the entry's partition index is <em>greater than</em>
   *       {@code currentLeaderInsertingPartitionIndex}: the entry and all subsequent entries go
   *       directly to disk. The partition index pointer is advanced. A best-effort quorum-commit
   *       check is performed (if the map is already empty the pointer advances cleanly; otherwise
   *       it advances anyway and the appender will enforce ordering). All followers receive the
   *       entry in their delayed queue; disk dispatch is attempted synchronously.
   *   <li>If the entry's partition index <em>equals</em>
   *       {@code currentLeaderInsertingPartitionIndex}: each follower is inspected individually.
   *       Followers whose {@code currentFollowerReplicatingPartitionIndex} matches are sent the
   *       entry immediately (fast path) and the index is recorded in
   *       {@code currentReplicatingIndices}. Followers that are behind receive it as a delayed
   *       entry and disk dispatch is attempted. The entry is then persisted to disk.
   * </ul>
   *
   * <p><b>Step 3 – Quorum tracking:</b> Each sent entry is registered in
   * {@code currentReplicationgIndicesToAckFollowerCount}. When a follower acknowledges the entry,
   * its ACK count is incremented; once ≥ half the follower count the entry is removed from the
   * map. When the map empties the current partition is fully committed and the next partition may
   * be transmitted.
   */
  public synchronized TSStatus write(IConsensusRequest request) {
    if (!active) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT,
          String.format("Peer %s is inactive and cannot process writes", thisNode));
    }
    if (role != TRaftRole.LEADER) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT,
          String.format("Peer %s is not leader, current leader id: %s", thisNode, leaderId));
    }

    TRaftLogEntry logEntry = buildLogEntry(request);

    // Apply to local state machine immediately (leader apply-on-write model).
    TSStatus localStatus = stateMachine.write(stateMachine.deserializeRequest(request));
    if (!isSuccess(localStatus)) {
      return localStatus;
    }

    List<Peer> followers = getFollowers();
    int followerCount = followers.size();
    long entryPartitionIndex = logEntry.getPartitionIndex();

    if (entryPartitionIndex > currentLeaderInsertingPartitionIndex) {
      // ── Higher partition: write directly to disk ─────────────────────────
      // Log a debug note if there are still uncommitted entries from the old partition.
      if (!currentReplicationgIndicesToAckFollowerCount.isEmpty()) {
        LOGGER.debug(
            "Partition boundary crossed from {} to {} with {} uncommitted indices pending quorum",
            currentLeaderInsertingPartitionIndex,
            entryPartitionIndex,
            currentReplicationgIndicesToAckFollowerCount.size());
      }
      currentLeaderInsertingPartitionIndex = entryPartitionIndex;

      try {
        logStore.append(logEntry);
      } catch (IOException e) {
        LOGGER.error("Failed to append TRaft log entry {}", logEntry.getLogIndex(), e);
        return RpcUtils.getStatus(
            TSStatusCode.INTERNAL_SERVER_ERROR, "Failed to persist TRaft log entry");
      }
      logicalClock.updateAndGet(v -> Math.max(v, logEntry.getLogIndex()));
      updatePartitionIndexStat(logEntry);

      // Add to every follower's delayed queue; attempt synchronous dispatch.
      for (Peer follower : followers) {
        TRaftFollowerInfo info =
            followerInfoMap.computeIfAbsent(
                follower.getNodeId(),
                key -> new TRaftFollowerInfo(getLatestPartitionIndex(), logicalClock.get()));
        info.addDelayedIndex(logEntry.getPartitionIndex(), logEntry.getLogIndex());
        limitDelayedEntriesIfNecessary(info);
        dispatchNextDelayedEntry(follower, info);
      }

    } else {
      // ── Same partition: attempt fast-path replication per follower ────────
      if (followerCount > 0) {
        currentReplicationgIndicesToAckFollowerCount.put(
            logEntry.getLogIndex(), new AtomicInteger(0));
      }

      List<Peer> delayedFollowers = new ArrayList<>();
      for (Peer follower : followers) {
        TRaftFollowerInfo info =
            followerInfoMap.computeIfAbsent(
                follower.getNodeId(),
                key -> new TRaftFollowerInfo(getLatestPartitionIndex(), logicalClock.get() + 1));

        if (entryPartitionIndex == info.getCurrentReplicatingPartitionIndex()) {
          // Fast path: record index and send directly.
          info.addCurrentReplicatingIndex(logEntry.getLogIndex());
          boolean success = sendEntryToFollower(follower, logEntry);
          if (success) {
            info.recordMemoryReplicationSuccess();
            onFollowerAck(follower, info, logEntry);
          } else {
            onFollowerSendFailed(info, logEntry);
            delayedFollowers.add(follower);
          }
        } else if (entryPartitionIndex > info.getCurrentReplicatingPartitionIndex()) {
          // Follower is behind – defer to disk path.
          delayedFollowers.add(follower);
        }
        // entryPartitionIndex < info.getCurrentReplicatingPartitionIndex() should not happen.
      }

      // Persist to disk after in-memory replication attempts.
      try {
        logStore.append(logEntry);
      } catch (IOException e) {
        LOGGER.error("Failed to append TRaft log entry {}", logEntry.getLogIndex(), e);
        return RpcUtils.getStatus(
            TSStatusCode.INTERNAL_SERVER_ERROR, "Failed to persist TRaft log entry");
      }
      logicalClock.updateAndGet(v -> Math.max(v, logEntry.getLogIndex()));
      updatePartitionIndexStat(logEntry);

      // For delayed followers: enqueue + synchronous disk dispatch.
      for (Peer follower : delayedFollowers) {
        TRaftFollowerInfo info = followerInfoMap.get(follower.getNodeId());
        if (info == null) {
          continue;
        }
        info.addDelayedIndex(logEntry.getPartitionIndex(), logEntry.getLogIndex());
        limitDelayedEntriesIfNecessary(info);
        dispatchNextDelayedEntry(follower, info);
      }
    }

    return localStatus;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Read path
  // ────────────────────────────────────────────────────────────────────────────

  public synchronized DataSet read(IConsensusRequest request) {
    return stateMachine.read(request);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Follower RPC handler
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Receive a replicated log entry from the leader.
   *
   * <p><b>Term check:</b>
   * <ul>
   *   <li>If the leader's term is less than the follower's current term the request is rejected
   *       so the leader can discover it is stale and step down.
   *   <li>If the leader's term is greater the follower updates its own term and resets its vote.
   * </ul>
   *
   * <p><b>Partition index check (after term is accepted):</b>
   * <ul>
   *   <li>If the entry's partition index is <em>less than</em> the follower's current maximum
   *       partition index, the follower is ahead of what the leader is sending – this is an
   *       inconsistency and an error is returned.
   *   <li>If the entry's partition index is equal to or greater than the follower's maximum, the
   *       entry is accepted, persisted, and applied to the state machine.
   * </ul>
   */
  synchronized TSStatus receiveReplicatedLog(TRaftLogEntry logEntry, int newLeaderId, long term) {
    // ── Term comparison ──────────────────────────────────────────────────────
    if (term < currentTerm) {
      LOGGER.warn(
          "Rejecting replicated log from leader {} with stale term {} (currentTerm={})",
          newLeaderId,
          term,
          currentTerm);
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT,
          "Leader term " + term + " is stale; follower currentTerm=" + currentTerm);
    }
    if (term > currentTerm) {
      currentTerm = term;
      votedFor = -1;
    }
    becomeFollower(newLeaderId);

    // Idempotency: already have this log index.
    if (logStore.contains(logEntry.getLogIndex())) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }

    // ── Partition index comparison ───────────────────────────────────────────
    if (logEntry.getPartitionIndex() < maxPartitionIndex) {
      // Follower's partition is ahead of what the leader sent – inconsistency.
      LOGGER.error(
          "Partition inconsistency on follower {}: entry partitionIndex={}, follower maxPartitionIndex={}",
          thisNode,
          logEntry.getPartitionIndex(),
          maxPartitionIndex);
      return RpcUtils.getStatus(
          TSStatusCode.INTERNAL_SERVER_ERROR,
          "Partition index inconsistency: follower maxPartition="
              + maxPartitionIndex
              + " > entry partition="
              + logEntry.getPartitionIndex());
    }

    // ── Persist and apply ────────────────────────────────────────────────────
    try {
      logStore.append(logEntry);
    } catch (IOException e) {
      LOGGER.error("Failed to persist replicated TRaft log {}", logEntry.getLogIndex(), e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, "Failed to persist log");
    }

    logicalClock.updateAndGet(v -> Math.max(v, logEntry.getLogIndex()));
    updatePartitionIndexStat(logEntry);

    IConsensusRequest deserializedRequest =
        stateMachine.deserializeRequest(TRaftRequestParser.buildRequest(logEntry.getData()));
    deserializedRequest.markAsGeneratedByRemoteConsensusLeader();
    return stateMachine.write(deserializedRequest);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Election
  // ────────────────────────────────────────────────────────────────────────────

  synchronized TRaftVoteResult requestVote(TRaftVoteRequest voteRequest) {
    if (voteRequest.getTerm() < currentTerm) {
      return new TRaftVoteResult(false, currentTerm);
    }
    if (voteRequest.getTerm() > currentTerm) {
      currentTerm = voteRequest.getTerm();
      votedFor = voteRequest.getCandidateId();
      becomeFollower(-1);
      return new TRaftVoteResult(true, currentTerm);
    }
    if (votedFor != -1 && votedFor != voteRequest.getCandidateId()) {
      return new TRaftVoteResult(false, currentTerm);
    }
    int freshnessCompareResult =
        compareCandidateFreshness(
            voteRequest.getPartitionIndex(),
            voteRequest.getCurrentPartitionIndexCount(),
            maxPartitionIndex,
            currentPartitionIndexCount);
    boolean shouldVote = freshnessCompareResult > 0;
    if (!shouldVote && freshnessCompareResult == 0) {
      shouldVote = random.nextBoolean();
    }
    if (!shouldVote) {
      return new TRaftVoteResult(false, currentTerm);
    }
    votedFor = voteRequest.getCandidateId();
    return new TRaftVoteResult(true, currentTerm);
  }

  synchronized boolean campaignLeader() {
    if (!active) {
      return false;
    }
    role = TRaftRole.CANDIDATE;
    leaderId = -1;
    currentTerm++;
    votedFor = thisNode.getNodeId();
    int grantVotes = 1;
    TRaftVoteRequest voteRequest =
        new TRaftVoteRequest(
            thisNode.getNodeId(), currentTerm, maxPartitionIndex, currentPartitionIndexCount);
    for (Peer follower : getFollowers()) {
      Optional<TRaftServerImpl> followerServer = TRaftNodeRegistry.resolveServer(follower);
      if (!followerServer.isPresent()) {
        continue;
      }
      TRaftVoteResult voteResult = followerServer.get().requestVote(voteRequest);
      if (voteResult.getTerm() > currentTerm) {
        currentTerm = voteResult.getTerm();
        becomeFollower(-1);
        return false;
      }
      if (voteResult.isGranted()) {
        grantVotes++;
      }
    }
    if (grantVotes > configuration.size() / 2) {
      becomeLeader();
      return true;
    }
    becomeFollower(-1);
    return false;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Configuration management
  // ────────────────────────────────────────────────────────────────────────────

  synchronized void transferLeader(Peer newLeader) {
    if (Objects.equals(newLeader, thisNode)) {
      campaignLeader();
      return;
    }
    Optional<TRaftServerImpl> targetServer = TRaftNodeRegistry.resolveServer(newLeader);
    if (!targetServer.isPresent()) {
      return;
    }
    targetServer.get().campaignLeader();
    if (targetServer.get().isLeader()) {
      becomeFollower(newLeader.getNodeId());
    }
  }

  synchronized void resetPeerList(List<Peer> newPeers) throws IOException {
    configuration.clear();
    configuration.addAll(newPeers);
    if (!configuration.contains(thisNode)) {
      configuration.add(thisNode);
    }
    persistConfiguration();
    electInitialLeader();
    initFollowerInfoMap();
    if (started && role == TRaftRole.LEADER) {
      stopAppenders();
      initAppenders();
    }
  }

  synchronized void addPeer(Peer peer) throws IOException {
    configuration.add(peer);
    persistConfiguration();
    initFollowerInfoMap();
    if (started && role == TRaftRole.LEADER) {
      stopAppenders();
      initAppenders();
    }
  }

  synchronized void removePeer(Peer peer) throws IOException {
    configuration.remove(peer);
    persistConfiguration();
    initFollowerInfoMap();
    if (started && role == TRaftRole.LEADER) {
      stopAppenders();
      initAppenders();
    }
    if (peer.getNodeId() == leaderId) {
      electInitialLeader();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Background appender: called by TRaftLogAppender every waitingReplicationTimeMs
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Entry point for the per-follower {@link TRaftLogAppender} background thread.
   *
   * <p>Checks whether the follower has pending delayed entries and, if so, attempts to dispatch
   * the next one from disk. This provides a periodic retry mechanism for followers that were
   * offline during the original write, and ensures that entries written to disk during partition
   * transitions are eventually transmitted.
   */
  synchronized void tryReplicateDiskEntriesToFollower(Peer follower) {
    if (!started || role != TRaftRole.LEADER || !active) {
      return;
    }
    TRaftFollowerInfo info = followerInfoMap.get(follower.getNodeId());
    if (info == null || !info.hasDelayedEntries() || info.hasCurrentReplicatingIndex()) {
      return;
    }
    dispatchNextDelayedEntry(follower, info);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Accessors
  // ────────────────────────────────────────────────────────────────────────────

  synchronized Peer getLeader() {
    return configuration.stream()
        .filter(peer -> peer.getNodeId() == leaderId)
        .findFirst()
        .orElse(null);
  }

  synchronized List<Peer> getConfiguration() {
    return new ArrayList<>(configuration);
  }

  synchronized long getLogicalClock() {
    return logicalClock.get();
  }

  synchronized boolean isLeader() {
    return role == TRaftRole.LEADER;
  }

  synchronized boolean isLeaderReady() {
    return isLeader() && active;
  }

  synchronized boolean isReadOnly() {
    return stateMachine.isReadOnly();
  }

  synchronized boolean isActive() {
    return active;
  }

  synchronized void setActive(boolean active) {
    this.active = active;
  }

  synchronized long getCurrentPartitionIndexCount() {
    return currentPartitionIndexCount;
  }

  synchronized List<TRaftLogEntry> getLogEntries() {
    return logStore.getAllEntries();
  }

  synchronized TRaftFollowerInfo getFollowerInfo(int followerNodeId) {
    return followerInfoMap.get(followerNodeId);
  }

  synchronized long getLatestPartitionIndex() {
    TRaftLogEntry last = logStore.getLastEntry();
    return last == null ? INITIAL_PARTITION_INDEX : last.getPartitionIndex();
  }

  synchronized long getCurrentLeaderInsertingPartitionIndex() {
    return currentLeaderInsertingPartitionIndex;
  }

  synchronized int getPendingQuorumEntryCount() {
    return currentReplicationgIndicesToAckFollowerCount.size();
  }

  synchronized void reloadConsensusConfig(TRaftConfig config) {
    this.config = config;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: log entry construction
  // ────────────────────────────────────────────────────────────────────────────

  private TRaftLogEntry buildLogEntry(IConsensusRequest request) {
    TRaftLogEntry previous = logStore.getLastEntry();
    long timestamp =
        TRaftRequestParser.extractTimestamp(
            request, historicalMaxTimestamp == Long.MIN_VALUE ? 0 : historicalMaxTimestamp);
    long logIndex = previous == null ? 1 : previous.getLogIndex() + 1;
    long logTerm = currentTerm;
    long partitionIndex;
    long interPartitionIndex;
    long lastPartitionCount;
    if (previous == null) {
      partitionIndex = INITIAL_PARTITION_INDEX;
      interPartitionIndex = INITIAL_PARTITION_INDEX;
      lastPartitionCount = 0;
      historicalMaxTimestamp = timestamp;
    } else if (timestamp < historicalMaxTimestamp) {
      partitionIndex = previous.getPartitionIndex() + 1;
      interPartitionIndex = 0;
      lastPartitionCount = previous.getInterPartitionIndex();
    } else {
      partitionIndex = previous.getPartitionIndex();
      interPartitionIndex = previous.getInterPartitionIndex() + 1;
      lastPartitionCount = previous.getLastPartitionCount();
      historicalMaxTimestamp = Math.max(historicalMaxTimestamp, timestamp);
    }
    return new TRaftLogEntry(
        timestamp,
        partitionIndex,
        logIndex,
        logTerm,
        interPartitionIndex,
        lastPartitionCount,
        TRaftRequestParser.extractRawRequest(request));
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: follower ACK and quorum tracking
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Called when a follower successfully receives and acknowledges {@code ackEntry}.
   *
   * <ol>
   *   <li>Clears the entry from the follower's in-flight tracking.
   *   <li>Updates the quorum commit counter for this log index.
   *   <li>Chains dispatch of the next pending delayed entry for this follower.
   * </ol>
   */
  private void onFollowerAck(Peer follower, TRaftFollowerInfo info, TRaftLogEntry ackEntry) {
    info.removeCurrentReplicatingIndex(ackEntry.getLogIndex());
    info.removeDelayedIndex(ackEntry.getPartitionIndex(), ackEntry.getLogIndex());
    updateQuorumTracking(ackEntry.getLogIndex());
    dispatchNextDelayedEntry(follower, info);
  }

  /**
   * Increments the ACK counter for {@code logIndex}. When the count reaches at least half the
   * follower count the entry is considered committed and removed from the tracking map.
   */
  private void updateQuorumTracking(long logIndex) {
    AtomicInteger counter = currentReplicationgIndicesToAckFollowerCount.get(logIndex);
    if (counter == null) {
      return;
    }
    int count = counter.incrementAndGet();
    int followerCount = getFollowers().size();
    // Quorum: ACK count >= half of follower count (per the requirement).
    // Using count * 2 >= followerCount avoids floating-point and handles all sizes correctly.
    if (followerCount > 0 && count * 2 >= followerCount) {
      currentReplicationgIndicesToAckFollowerCount.remove(logIndex);
    }
  }

  private void onFollowerSendFailed(TRaftFollowerInfo info, TRaftLogEntry failedEntry) {
    info.removeCurrentReplicatingIndex(failedEntry.getLogIndex());
    info.addDelayedIndex(failedEntry.getPartitionIndex(), failedEntry.getLogIndex());
    limitDelayedEntriesIfNecessary(info);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: disk dispatch
  // ────────────────────────────────────────────────────────────────────────────

  /**
   * Attempts to dispatch the next pending delayed log entry for {@code follower} from the local
   * disk. After a successful dispatch the chain continues via {@link #onFollowerAck} so that
   * subsequent delayed entries in the same (or an advanced) partition are sent without waiting
   * for the next appender cycle.
   */
  private void dispatchNextDelayedEntry(Peer follower, TRaftFollowerInfo info) {
    if (info.hasCurrentReplicatingIndex()) {
      return;
    }

    // Advance the follower's current replicating partition if possible.
    Long firstDelayedPartition = info.getFirstDelayedPartitionIndex();
    if (firstDelayedPartition == null) {
      return;
    }
    if (firstDelayedPartition > info.getCurrentReplicatingPartitionIndex()) {
      info.setCurrentReplicatingPartitionIndex(firstDelayedPartition);
    }

    Long delayedIndex =
        info.getFirstDelayedIndexOfPartition(info.getCurrentReplicatingPartitionIndex());
    if (delayedIndex == null) {
      return;
    }
    info.setNextPartitionFirstIndex(delayedIndex);

    TRaftLogEntry diskEntry = logStore.getByIndex(delayedIndex);
    if (diskEntry == null) {
      // Entry not yet on disk (race between write path and appender); retry next cycle.
      return;
    }

    info.addCurrentReplicatingIndex(diskEntry.getLogIndex());
    boolean success = sendEntryToFollower(follower, diskEntry);
    if (success) {
      info.recordDiskReplicationSuccess();
      // ACK handling: remove from in-flight, update quorum, chain next dispatch.
      info.removeCurrentReplicatingIndex(diskEntry.getLogIndex());
      info.removeDelayedIndex(diskEntry.getPartitionIndex(), diskEntry.getLogIndex());
      updateQuorumTracking(diskEntry.getLogIndex());
      // Recurse into the chain until there is nothing left or a send fails.
      dispatchNextDelayedEntry(follower, info);
    } else {
      info.removeCurrentReplicatingIndex(diskEntry.getLogIndex());
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: RPC helpers
  // ────────────────────────────────────────────────────────────────────────────

  private boolean sendEntryToFollower(Peer follower, TRaftLogEntry entry) {
    Optional<TRaftServerImpl> followerServer = TRaftNodeRegistry.resolveServer(follower);
    if (!followerServer.isPresent()) {
      return false;
    }
    TSStatus status =
        followerServer
            .get()
            .receiveReplicatedLog(entry.copy(), thisNode.getNodeId(), currentTerm);
    return isSuccess(status);
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: misc helpers
  // ────────────────────────────────────────────────────────────────────────────

  private void limitDelayedEntriesIfNecessary(TRaftFollowerInfo info) {
    if (info.delayedEntryCount()
        <= config.getReplication().getMaxPendingRetryEntriesPerFollower()) {
      return;
    }
    LOGGER.warn(
        "Delayed TRaft entries for a follower exceed limit {}. Current count: {}",
        config.getReplication().getMaxPendingRetryEntriesPerFollower(),
        info.delayedEntryCount());
  }

  private List<Peer> getFollowers() {
    List<Peer> followers = new ArrayList<>();
    for (Peer peer : configuration) {
      if (peer.getNodeId() != thisNode.getNodeId()) {
        followers.add(peer);
      }
    }
    return followers;
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: follower info init
  // ────────────────────────────────────────────────────────────────────────────

  private void initFollowerInfoMap() {
    followerInfoMap.clear();
    long latestPartitionIndex = getLatestPartitionIndex();
    long nextLogIndex = logicalClock.get() + 1;
    for (Peer follower : getFollowers()) {
      followerInfoMap.put(
          follower.getNodeId(), new TRaftFollowerInfo(latestPartitionIndex, nextLogIndex));
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: appender lifecycle
  // ────────────────────────────────────────────────────────────────────────────

  private void initAppenders() {
    if (appenderExecutor == null || appenderExecutor.isShutdown()) {
      appenderExecutor = Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r);
            t.setName("TRaftAppender-" + thisNode.getGroupId());
            t.setDaemon(true);
            return t;
          });
    }
    long waitMs = config.getReplication().getWaitingReplicationTimeMs();
    for (Peer follower : getFollowers()) {
      TRaftLogAppender appender = new TRaftLogAppender(this, follower, waitMs);
      appenderMap.put(follower.getNodeId(), appender);
      appenderExecutor.submit(appender);
    }
  }

  private void stopAppenders() {
    // Signal all appenders to stop, then interrupt their threads via shutdownNow().
    // We do NOT block here: stopAppenders() is called while holding the server lock, and appender
    // threads also need that lock for tryReplicateDiskEntriesToFollower(). Waiting here would
    // deadlock. Instead, the guard in tryReplicateDiskEntriesToFollower() (started == false)
    // ensures appenders become no-ops as soon as they wake from their sleep, and they exit on
    // the next loop iteration once they detect stopped==true or an InterruptedException.
    appenderMap.values().forEach(TRaftLogAppender::stop);
    appenderMap.clear();
    if (appenderExecutor != null) {
      appenderExecutor.shutdownNow();
      appenderExecutor = null;
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: leader/follower transitions
  // ────────────────────────────────────────────────────────────────────────────

  private void electInitialLeader() {
    leaderId =
        configuration.stream()
            .map(Peer::getNodeId)
            .max(Integer::compareTo)
            .orElse(thisNode.getNodeId());
    if (leaderId == thisNode.getNodeId()) {
      role = TRaftRole.LEADER;
    } else {
      role = TRaftRole.FOLLOWER;
    }
  }

  private void becomeLeader() {
    role = TRaftRole.LEADER;
    leaderId = thisNode.getNodeId();
    votedFor = thisNode.getNodeId();
    currentReplicationgIndicesToAckFollowerCount.clear();
    initFollowerInfoMap();
    if (started) {
      stopAppenders();
      initAppenders();
      notifyLeaderChanged();
    }
  }

  private void becomeFollower(int newLeaderId) {
    boolean wasLeader = (role == TRaftRole.LEADER);
    role = TRaftRole.FOLLOWER;
    leaderId = newLeaderId;
    if (wasLeader) {
      stopAppenders();
    }
    if (started) {
      notifyLeaderChanged();
    }
  }

  private void notifyLeaderChanged() {
    if (leaderId != -1) {
      stateMachine.event().notifyLeaderChanged(thisNode.getGroupId(), leaderId);
    }
    if (role == TRaftRole.LEADER) {
      stateMachine.event().notifyLeaderReady();
    } else {
      stateMachine.event().notifyNotLeader();
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: recovery
  // ────────────────────────────────────────────────────────────────────────────

  private void recoverFromDisk() {
    List<TRaftLogEntry> entries = logStore.getAllEntries();
    for (TRaftLogEntry entry : entries) {
      logicalClock.updateAndGet(v -> Math.max(v, entry.getLogIndex()));
      currentTerm = Math.max(currentTerm, entry.getLogTerm());
      historicalMaxTimestamp = Math.max(historicalMaxTimestamp, entry.getTimestamp());
      updatePartitionIndexStat(entry);
    }
    // On recovery, the inserting partition pointer starts at the maximum persisted partition.
    currentLeaderInsertingPartitionIndex = maxPartitionIndex;
  }

  private void updatePartitionIndexStat(TRaftLogEntry entry) {
    if (entry.getPartitionIndex() > maxPartitionIndex) {
      maxPartitionIndex = entry.getPartitionIndex();
      currentPartitionIndexCount = 1;
      return;
    }
    if (entry.getPartitionIndex() == maxPartitionIndex) {
      currentPartitionIndexCount++;
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: configuration persistence
  // ────────────────────────────────────────────────────────────────────────────

  private TreeSet<Peer> loadConfigurationFromDisk() throws IOException {
    TreeSet<Peer> peers = new TreeSet<>();
    File file = new File(storageDir, CONFIGURATION_FILE_NAME);
    if (!file.exists()) {
      return peers;
    }
    try (BufferedReader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] split = line.split(",", 3);
        if (split.length != 3) {
          continue;
        }
        int nodeId = Integer.parseInt(split[0]);
        String ip = split[1];
        int port = Integer.parseInt(split[2]);
        peers.add(new Peer(thisNode.getGroupId(), nodeId, new TEndPoint(ip, port)));
      }
    }
    return peers;
  }

  private void persistConfiguration() throws IOException {
    File file = new File(storageDir, CONFIGURATION_FILE_NAME);
    try (BufferedWriter writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
      for (Peer peer : configuration) {
        writer.write(
            String.format(
                "%s,%s,%s",
                peer.getNodeId(), peer.getEndpoint().getIp(), peer.getEndpoint().getPort()));
        writer.newLine();
      }
    }
  }

  // ────────────────────────────────────────────────────────────────────────────
  // Private: election utilities
  // ────────────────────────────────────────────────────────────────────────────

  private int compareCandidateFreshness(
      long candidatePartitionIndex,
      long candidatePartitionCount,
      long localPartitionIndex,
      long localPartitionCount) {
    int partitionCompare = Long.compare(candidatePartitionIndex, localPartitionIndex);
    if (partitionCompare != 0) {
      return partitionCompare;
    }
    return Long.compare(candidatePartitionCount, localPartitionCount);
  }

  private boolean isSuccess(TSStatus status) {
    return status != null
        && (status.getCode() == 0
            || status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
