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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Core TRaft implementation.
 *
 * <p>Safety-critical behavior follows traditional Raft: term-based leadership, randomized
 * elections, AppendEntries log matching via {@code prevLogIndex/prevLogTerm}, majority commit, and
 * apply-after-commit. TRaft extends the log with time-partition metadata so time-series workloads
 * can preserve ordering hints across replication and snapshots, but that metadata does not replace
 * the ordinary Raft safety rules.
 */
class TRaftServerImpl {

  private static final Logger LOGGER = LoggerFactory.getLogger(TRaftServerImpl.class);
  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final String METADATA_FILE_NAME = "metadata.properties";
  private static final String SNAPSHOT_DIR_NAME = "snapshot";
  private static final long INITIAL_PARTITION_INDEX = 0;

  private final String storageDir;
  private final Peer thisNode;
  private final IStateMachine stateMachine;
  private final TreeSet<Peer> configuration = new TreeSet<>();
  private final TRaftLogStore logStore;
  private final TRaftTransport transport;
  private final AtomicLong logicalClock = new AtomicLong();
  private final Random random;
  private final ConcurrentHashMap<Integer, Long> nextIndexByFollower = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, Long> matchIndexByFollower = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Long, TSStatus> applyResultByIndex = new ConcurrentHashMap<>();
  private final AtomicBoolean snapshotInProgress = new AtomicBoolean(false);
  private final File metadataFile;
  private final File snapshotDir;

  private volatile TRaftRole role = TRaftRole.FOLLOWER;
  private volatile boolean active = true;
  private volatile boolean started = false;
  private volatile boolean leaderReady = false;

  private long currentTerm = 0;
  private int leaderId = -1;
  private int votedFor = -1;
  private long commitIndex = 0;
  private long lastApplied = 0;
  private long leaderReadyIndex = 0;

  private long snapshotLastIncludedIndex = 0;
  private long snapshotLastIncludedTerm = 0;
  private long snapshotHistoricalMaxTimestamp = Long.MIN_VALUE;
  private long snapshotPartitionIndex = INITIAL_PARTITION_INDEX;
  private long snapshotPartitionCount = 0;

  // These fields are TRaft-specific metadata used to remember how time-partition assignment
  // progressed. They help reconstruct partition state after replay or snapshot restore, but commit
  // and election safety still depend on the ordinary Raft log term/index pair.
  private long historicalMaxTimestamp = Long.MIN_VALUE;
  private long maxPartitionIndex = INITIAL_PARTITION_INDEX;
  private long currentPartitionIndexCount = 0;

  private long electionDeadlineMs;

  private ScheduledExecutorService backgroundExecutor;
  private ScheduledFuture<?> electionFuture;
  private ScheduledFuture<?> heartbeatFuture;

  private TRaftConfig config;

  TRaftServerImpl(
      String storageDir,
      Peer thisNode,
      TreeSet<Peer> peers,
      IStateMachine stateMachine,
      TRaftConfig config,
      TRaftTransport transport)
      throws IOException {
    this.storageDir = storageDir;
    this.thisNode = thisNode;
    this.stateMachine = stateMachine;
    this.config = config;
    this.transport = transport;
    this.logStore = new TRaftLogStore(storageDir);
    this.random = new Random(config.getElection().getRandomSeed() + thisNode.getNodeId());
    this.metadataFile = new File(storageDir, METADATA_FILE_NAME);
    this.snapshotDir = new File(storageDir, SNAPSHOT_DIR_NAME);
    if (peers.isEmpty()) {
      this.configuration.addAll(loadConfigurationFromDisk());
    } else {
      this.configuration.addAll(peers);
      persistConfiguration();
    }
    if (this.configuration.isEmpty()) {
      this.configuration.add(thisNode);
      persistConfiguration();
    }
    recoverFromDisk();
    resetElectionDeadlineLocked();
  }

  public synchronized void start() {
    if (started) {
      return;
    }
    started = true;
    stateMachine.start();
    loadSnapshotIfPresent();
    applyCommittedEntriesLocked();
    backgroundExecutor =
        Executors.newScheduledThreadPool(
            3,
            r -> {
              Thread thread = new Thread(r);
              thread.setName("TRaft-" + thisNode.getGroupId() + "-" + thisNode.getNodeId());
              thread.setDaemon(true);
              return thread;
            });
    electionFuture =
        backgroundExecutor.scheduleWithFixedDelay(
            this::checkElectionTimeout,
            50L,
            50L,
            TimeUnit.MILLISECONDS);
    heartbeatFuture =
        backgroundExecutor.scheduleWithFixedDelay(
            this::heartbeat,
            config.getElection().getHeartbeatIntervalMs(),
            config.getElection().getHeartbeatIntervalMs(),
            TimeUnit.MILLISECONDS);
    notifyFollowerStateLocked();
  }

  public synchronized void stop() {
    started = false;
    leaderReady = false;
    if (electionFuture != null) {
      electionFuture.cancel(true);
      electionFuture = null;
    }
    if (heartbeatFuture != null) {
      heartbeatFuture.cancel(true);
      heartbeatFuture = null;
    }
    if (backgroundExecutor != null) {
      backgroundExecutor.shutdownNow();
      backgroundExecutor = null;
    }
    stateMachine.stop();
  }

  public TSStatus write(IConsensusRequest request) {
    TRaftLogEntry logEntry;
    synchronized (this) {
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
      try {
        // The leader always persists the entry locally before waiting for quorum. State-machine
        // application happens only after the entry is committed.
        logEntry = appendDataEntryLocked(request);
      } catch (IOException e) {
        LOGGER.error("Failed to append TRaft log entry for request", e);
        return RpcUtils.getStatus(
            TSStatusCode.INTERNAL_SERVER_ERROR, "Failed to persist TRaft log entry");
      }
      advanceCommitIndexLocked();
      applyCommittedEntriesLocked();
      maybeTriggerSnapshotAsyncLocked();
    }
    if (!waitForCommit(logEntry.getLogIndex(), config.getReplication().getRequestTimeoutMs())) {
      return RpcUtils.getStatus(
          TSStatusCode.WRITE_PROCESS_REJECT,
          "Timed out waiting for TRaft entry to reach quorum");
    }
    TSStatus status = applyResultByIndex.remove(logEntry.getLogIndex());
    return status == null ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS) : status;
  }

  public synchronized DataSet read(IConsensusRequest request) {
    return stateMachine.read(request);
  }

  synchronized TRaftAppendEntriesResponse receiveAppendEntries(TRaftAppendEntriesRequest request) {
    if (!active) {
      return new TRaftAppendEntriesResponse(false, currentTerm, getLastLogIndexLocked(), getLastLogIndexLocked() + 1);
    }
    if (request.getTerm() < currentTerm) {
      return new TRaftAppendEntriesResponse(false, currentTerm, getLastLogIndexLocked(), getLastLogIndexLocked() + 1);
    }
    if (request.getTerm() > currentTerm) {
      updateTermLocked(request.getTerm(), -1);
    }
    if (role != TRaftRole.FOLLOWER || leaderId != request.getLeaderId()) {
      becomeFollowerLocked(request.getLeaderId());
    }
    resetElectionDeadlineLocked();

    // Traditional Raft log matching: the follower only accepts new entries if the leader proves
    // the term/index pair immediately before them. TRaft's partition metadata is intentionally not
    // part of this safety check.
    long expectedPrevTerm = getLogTermLocked(request.getPrevLogIndex());
    if (expectedPrevTerm != request.getPrevLogTerm()) {
      long nextIndexHint = computeNextIndexHintLocked(request.getPrevLogIndex());
      return new TRaftAppendEntriesResponse(false, currentTerm, getLastLogIndexLocked(), nextIndexHint);
    }

    try {
      appendReplicatedEntriesLocked(request.getEntries());
      if (request.getLeaderCommit() > commitIndex) {
        commitIndex = Math.min(request.getLeaderCommit(), getLastLogIndexLocked());
        persistMetadataLocked();
      }
      applyCommittedEntriesLocked();
    } catch (IOException e) {
      LOGGER.error("Failed to persist replicated TRaft log for {}", thisNode, e);
      return new TRaftAppendEntriesResponse(false, currentTerm, getLastLogIndexLocked(), getLastLogIndexLocked() + 1);
    }

    long matchIndex =
        request.getEntries().isEmpty()
            ? request.getPrevLogIndex()
            : request.getEntries().get(request.getEntries().size() - 1).getLogIndex();
    return new TRaftAppendEntriesResponse(true, currentTerm, matchIndex, matchIndex + 1);
  }

  synchronized TRaftVoteResult requestVote(TRaftVoteRequest voteRequest) {
    if (!active) {
      return new TRaftVoteResult(false, currentTerm);
    }
    if (voteRequest.getTerm() < currentTerm) {
      return new TRaftVoteResult(false, currentTerm);
    }
    if (voteRequest.getTerm() > currentTerm) {
      updateTermLocked(voteRequest.getTerm(), -1);
      // A rejected higher-term vote request should not refresh the election deadline. Otherwise a
      // stale candidate could repeatedly bump term and starve the more up-to-date follower.
      becomeFollowerLocked(-1, false);
    }
    boolean canVote = votedFor == -1 || votedFor == voteRequest.getCandidateId();
    // Election safety follows standard Raft freshness. The TRaft partition counters travel with
    // the RPC for observability and future extensions, but they do not override term/index order.
    boolean candidateUpToDate =
        compareLogFreshnessLocked(
                voteRequest.getLastLogTerm(),
                voteRequest.getLastLogIndex(),
                getLastLogTermLocked(),
                getLastLogIndexLocked())
            >= 0;
    if (!canVote || !candidateUpToDate) {
      return new TRaftVoteResult(false, currentTerm);
    }
    votedFor = voteRequest.getCandidateId();
    persistMetadataQuietlyLocked();
    resetElectionDeadlineLocked();
    return new TRaftVoteResult(true, currentTerm);
  }

  synchronized TRaftInstallSnapshotResponse receiveInstallSnapshot(
      TRaftInstallSnapshotRequest request) {
    if (!active) {
      return new TRaftInstallSnapshotResponse(false, currentTerm, snapshotLastIncludedIndex);
    }
    if (request.getTerm() < currentTerm) {
      return new TRaftInstallSnapshotResponse(false, currentTerm, snapshotLastIncludedIndex);
    }
    if (request.getTerm() > currentTerm) {
      updateTermLocked(request.getTerm(), -1);
    }
    becomeFollowerLocked(request.getLeaderId());
    resetElectionDeadlineLocked();
    try {
      replaceSnapshotLocked(request);
      if (started) {
        stateMachine.loadSnapshot(snapshotDir);
      }
      return new TRaftInstallSnapshotResponse(true, currentTerm, snapshotLastIncludedIndex);
    } catch (IOException e) {
      LOGGER.error("Failed to install TRaft snapshot for {}", thisNode, e);
      return new TRaftInstallSnapshotResponse(false, currentTerm, snapshotLastIncludedIndex);
    }
  }

  synchronized TRaftTriggerElectionResponse triggerElection() {
    if (!active || !started) {
      return new TRaftTriggerElectionResponse(false, currentTerm);
    }
    electionDeadlineMs = 0;
    if (backgroundExecutor != null) {
      backgroundExecutor.execute(this::startElection);
    }
    return new TRaftTriggerElectionResponse(true, currentTerm);
  }

  synchronized void transferLeader(Peer newLeader) {
    if (!configuration.contains(newLeader)) {
      return;
    }
    if (Objects.equals(newLeader, thisNode)) {
      if (backgroundExecutor != null) {
        backgroundExecutor.execute(this::startElection);
      }
      return;
    }
    try {
      TRaftTriggerElectionResponse response = transport.triggerElection(newLeader);
      if (response.getTerm() > currentTerm) {
        updateTermLocked(response.getTerm(), -1);
      }
      if (response.isAccepted()) {
        becomeFollowerLocked(-1);
      }
    } catch (IOException e) {
      LOGGER.warn("Failed to transfer TRaft leadership to {}", newLeader, e);
    }
  }

  synchronized void resetPeerList(List<Peer> newPeers) throws IOException {
    configuration.clear();
    configuration.addAll(newPeers);
    persistConfiguration();
    reinitializeReplicationStateLocked();
    if (!configuration.contains(thisNode) || role != TRaftRole.LEADER) {
      leaderReady = false;
    }
    stateMachine.event().notifyConfigurationChanged(currentTerm, commitIndex, new ArrayList<>(configuration));
  }

  synchronized void addPeer(Peer peer) throws IOException {
    if (role != TRaftRole.LEADER) {
      throw new IOException("Only leader can add peer in TRaft");
    }
    TreeSet<Peer> newConfiguration = new TreeSet<>(configuration);
    newConfiguration.add(peer);
    appendConfigurationChangeAndAwaitCommitLocked(newConfiguration);
    long targetIndex = getLastLogIndexLocked();
    if (!waitForFollowerCatchUp(peer, targetIndex, config.getReplication().getRequestTimeoutMs())) {
      throw new IOException("Timed out waiting for new TRaft peer to catch up");
    }
  }

  synchronized void removePeer(Peer peer) throws IOException {
    if (role != TRaftRole.LEADER) {
      throw new IOException("Only leader can remove peer in TRaft");
    }
    TreeSet<Peer> newConfiguration = new TreeSet<>(configuration);
    newConfiguration.remove(peer);
    appendConfigurationChangeAndAwaitCommitLocked(newConfiguration);
  }

  synchronized void triggerSnapshot(boolean force) throws IOException {
    if (!started || !snapshotInProgress.compareAndSet(false, true)) {
      return;
    }
    try {
      long snapshotIndex = commitIndex;
      if (!force
          && (snapshotIndex <= snapshotLastIncludedIndex
              || snapshotIndex - snapshotLastIncludedIndex
                  < config.getSnapshot().getAutoTriggerLogThreshold())) {
        return;
      }
      if (snapshotIndex <= 0) {
        return;
      }
      File tmpSnapshotDir = new File(storageDir, SNAPSHOT_DIR_NAME + ".tmp");
      deleteRecursively(tmpSnapshotDir);
      if (!tmpSnapshotDir.exists() && !tmpSnapshotDir.mkdirs()) {
        throw new IOException(String.format("Failed to create snapshot dir %s", tmpSnapshotDir));
      }
      if (!stateMachine.takeSnapshot(tmpSnapshotDir)) {
        throw new IOException("State machine refused to take TRaft snapshot");
      }
      deleteRecursively(snapshotDir);
      if (!tmpSnapshotDir.renameTo(snapshotDir)) {
        throw new IOException(
            String.format("Failed to move TRaft snapshot from %s to %s", tmpSnapshotDir, snapshotDir));
      }
      // Capture the last included term before advancing snapshotLastIncludedIndex. Once the index
      // moves forward, getLogTermLocked(snapshotIndex) would read the snapshot metadata we are in
      // the middle of updating instead of the original log term.
      long snapshotTerm = getLogTermLocked(snapshotIndex);
      snapshotLastIncludedIndex = snapshotIndex;
      snapshotLastIncludedTerm = snapshotTerm;
      snapshotHistoricalMaxTimestamp = historicalMaxTimestamp;
      snapshotPartitionIndex = maxPartitionIndex;
      snapshotPartitionCount = currentPartitionIndexCount;
      logStore.compactPrefix(snapshotLastIncludedIndex);
      recalculatePartitionStatsLocked();
      persistMetadataLocked();
    } finally {
      snapshotInProgress.set(false);
    }
  }

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
    return isLeader() && active && leaderReady;
  }

  synchronized boolean isReadOnly() {
    return stateMachine.isReadOnly();
  }

  synchronized boolean isActive() {
    return active;
  }

  synchronized void setActive(boolean active) {
    this.active = active;
    if (!active && role == TRaftRole.LEADER) {
      leaderReady = false;
      notifyFollowerStateLocked();
    }
  }

  synchronized long getCurrentPartitionIndexCount() {
    return currentPartitionIndexCount;
  }

  synchronized List<TRaftLogEntry> getLogEntries() {
    return logStore.getAllEntries();
  }

  synchronized long getCommitIndex() {
    return commitIndex;
  }

  synchronized long getCurrentTerm() {
    return currentTerm;
  }

  synchronized void reloadConsensusConfig(TRaftConfig config) {
    this.config = config;
  }

  private void checkElectionTimeout() {
    synchronized (this) {
      if (!started || !active || role == TRaftRole.LEADER) {
        return;
      }
      if (System.currentTimeMillis() < electionDeadlineMs) {
        return;
      }
      resetElectionDeadlineLocked();
    }
    startElection();
  }

  private void heartbeat() {
    List<Peer> followers;
    synchronized (this) {
      if (!started || !active || role != TRaftRole.LEADER) {
        return;
      }
      followers = getFollowersLocked();
    }
    for (Peer follower : followers) {
      replicateToPeer(follower, true);
    }
  }

  private void startElection() {
    TRaftVoteRequest voteRequest;
    List<Peer> followers;
    long electionTerm;
    int clusterSize;
    synchronized (this) {
      if (!started || !active || role == TRaftRole.LEADER) {
        return;
      }
      role = TRaftRole.CANDIDATE;
      leaderReady = false;
      leaderId = -1;
      currentTerm++;
      votedFor = thisNode.getNodeId();
      persistMetadataQuietlyLocked();
      resetElectionDeadlineLocked();
      voteRequest =
          new TRaftVoteRequest(
              thisNode.getNodeId(),
              currentTerm,
              getLastLogIndexLocked(),
              getLastLogTermLocked(),
              maxPartitionIndex,
              currentPartitionIndexCount);
      followers = getFollowersLocked();
      electionTerm = currentTerm;
      clusterSize = configuration.size();
    }
    int votes = 1;
    for (Peer follower : followers) {
      try {
        TRaftVoteResult result = transport.requestVote(follower, voteRequest);
        synchronized (this) {
          if (role != TRaftRole.CANDIDATE || currentTerm != electionTerm) {
            return;
          }
          if (result.getTerm() > currentTerm) {
            updateTermLocked(result.getTerm(), -1);
            becomeFollowerLocked(-1);
            return;
          }
          if (result.isGranted()) {
            votes++;
          }
        }
      } catch (IOException e) {
        LOGGER.debug("Failed to request TRaft vote from {}", follower, e);
      }
    }
    boolean wonElection = false;
    synchronized (this) {
      if (role == TRaftRole.CANDIDATE && currentTerm == electionTerm && votes > clusterSize / 2) {
        becomeLeaderLocked();
        wonElection = true;
      } else if (role == TRaftRole.CANDIDATE && currentTerm == electionTerm) {
        becomeFollowerLocked(-1);
      }
    }
    if (wonElection) {
      try {
        // Like traditional Raft, leader readiness is gated by a committed no-op in the new term.
        long noOpIndex = appendSpecialEntry(TRaftEntryType.NO_OP, new byte[0]);
        waitForCommit(noOpIndex, config.getReplication().getRequestTimeoutMs());
      } catch (IOException e) {
        LOGGER.error("Failed to append leader no-op entry for {}", thisNode, e);
        synchronized (this) {
          becomeFollowerLocked(-1);
        }
      }
    }
  }

  private boolean waitForCommit(long logIndex, long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (true) {
      synchronized (this) {
        if (commitIndex >= logIndex) {
          return true;
        }
        if (!started || !active || role != TRaftRole.LEADER) {
          return false;
        }
      }
      for (Peer follower : getFollowersSnapshot()) {
        replicateToPeer(follower, false);
      }
      synchronized (this) {
        if (commitIndex >= logIndex) {
          return true;
        }
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
          return false;
        }
        try {
          wait(Math.min(remaining, config.getElection().getHeartbeatIntervalMs()));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
  }

  private boolean waitForFollowerCatchUp(Peer follower, long targetIndex, long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (true) {
      synchronized (this) {
        if (!configuration.contains(follower)) {
          return false;
        }
        if (matchIndexByFollower.getOrDefault(follower.getNodeId(), 0L) >= targetIndex) {
          return true;
        }
      }
      replicateToPeer(follower, false);
      synchronized (this) {
        if (matchIndexByFollower.getOrDefault(follower.getNodeId(), 0L) >= targetIndex) {
          return true;
        }
        long remaining = deadline - System.currentTimeMillis();
        if (remaining <= 0) {
          return false;
        }
        try {
          wait(Math.min(remaining, config.getElection().getHeartbeatIntervalMs()));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
  }

  private void replicateToPeer(Peer follower, boolean forceHeartbeat) {
    TRaftInstallSnapshotRequest snapshotRequest = null;
    TRaftAppendEntriesRequest appendRequest = null;
    synchronized (this) {
      if (!started || !active || role != TRaftRole.LEADER || !configuration.contains(follower)) {
        return;
      }
      long nextIndex = nextIndexByFollower.getOrDefault(follower.getNodeId(), firstAvailableIndexLocked());
      if (nextIndex <= snapshotLastIncludedIndex) {
        // Once the follower falls behind the compacted prefix, catch-up must switch from
        // AppendEntries to InstallSnapshot.
        snapshotRequest = buildInstallSnapshotRequestLocked();
      } else {
        long prevLogIndex = nextIndex - 1;
        long prevLogTerm = getLogTermLocked(prevLogIndex);
        List<TRaftLogEntry> entries =
            logStore.getEntriesFrom(nextIndex, config.getReplication().getMaxEntriesPerAppend());
        appendRequest =
            new TRaftAppendEntriesRequest(
                thisNode.getNodeId(), currentTerm, prevLogIndex, prevLogTerm, commitIndex, entries);
      }
    }
    if (snapshotRequest != null) {
      sendSnapshotToFollower(follower, snapshotRequest);
    } else if (appendRequest != null) {
      sendAppendEntriesToFollower(follower, appendRequest);
    }
  }

  private void sendAppendEntriesToFollower(Peer follower, TRaftAppendEntriesRequest request) {
    try {
      TRaftAppendEntriesResponse response = transport.appendEntries(follower, request);
      synchronized (this) {
        if (role != TRaftRole.LEADER || request.getTerm() != currentTerm) {
          return;
        }
        if (response.getTerm() > currentTerm) {
          updateTermLocked(response.getTerm(), -1);
          becomeFollowerLocked(-1);
          return;
        }
        if (response.isSuccess()) {
          matchIndexByFollower.put(follower.getNodeId(), response.getMatchIndex());
          nextIndexByFollower.put(follower.getNodeId(), response.getMatchIndex() + 1);
          advanceCommitIndexLocked();
          applyCommittedEntriesLocked();
          maybeTriggerSnapshotAsyncLocked();
        } else {
          long nextIndex = Math.max(1L, response.getNextIndexHint());
          nextIndexByFollower.put(follower.getNodeId(), nextIndex);
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to append TRaft entry to {}", follower, e);
    }
  }

  private void sendSnapshotToFollower(Peer follower, TRaftInstallSnapshotRequest request) {
    try {
      TRaftInstallSnapshotResponse response = transport.installSnapshot(follower, request);
      synchronized (this) {
        if (role != TRaftRole.LEADER || request.getTerm() != currentTerm) {
          return;
        }
        if (response.getTerm() > currentTerm) {
          updateTermLocked(response.getTerm(), -1);
          becomeFollowerLocked(-1);
          return;
        }
        if (response.isSuccess()) {
          matchIndexByFollower.put(follower.getNodeId(), response.getLastIncludedIndex());
          nextIndexByFollower.put(follower.getNodeId(), response.getLastIncludedIndex() + 1);
        }
      }
    } catch (IOException e) {
      LOGGER.debug("Failed to install TRaft snapshot to {}", follower, e);
    }
  }

  private synchronized long appendSpecialEntry(TRaftEntryType entryType, byte[] data)
      throws IOException {
    TRaftLogEntry entry = buildLogEntryLocked(entryType, data, null);
    if (entryType == TRaftEntryType.NO_OP && role == TRaftRole.LEADER) {
      leaderReadyIndex = entry.getLogIndex();
    }
    appendLocalEntryLocked(entry);
    advanceCommitIndexLocked();
    applyCommittedEntriesLocked();
    notifyAll();
    return entry.getLogIndex();
  }

  private void appendConfigurationChangeAndAwaitCommitLocked(TreeSet<Peer> newConfiguration)
      throws IOException {
    // TRaft persists membership changes as a dedicated log entry. This is simpler than full
    // joint-consensus Raft, so callers should serialize configuration changes one at a time.
    byte[] serializedPeers = TRaftSerializationUtils.serializePeers(new ArrayList<>(newConfiguration));
    long logIndex = appendSpecialEntry(TRaftEntryType.CONFIGURATION, serializedPeers);
    if (!waitForCommit(logIndex, config.getReplication().getRequestTimeoutMs())) {
      throw new IOException("Timed out waiting for TRaft configuration change to commit");
    }
  }

  private TRaftLogEntry appendDataEntryLocked(IConsensusRequest request) throws IOException {
    TRaftLogEntry entry =
        buildLogEntryLocked(TRaftEntryType.DATA, TRaftRequestParser.extractRawRequest(request), request);
    appendLocalEntryLocked(entry);
    // No local apply here: the state machine advances only from applyCommittedEntriesLocked().
    applyCommittedEntriesLocked();
    notifyAll();
    return entry;
  }

  private void appendLocalEntryLocked(TRaftLogEntry entry) throws IOException {
    logStore.append(entry);
    logicalClock.set(Math.max(logicalClock.get(), entry.getLogIndex()));
    updatePartitionStatsForEntryLocked(entry);
    persistMetadataLocked();
  }

  private TRaftLogEntry buildLogEntryLocked(
      TRaftEntryType entryType, byte[] data, IConsensusRequest request) {
    long timestamp =
        request == null
            ? (historicalMaxTimestamp == Long.MIN_VALUE ? 0 : historicalMaxTimestamp)
            : TRaftRequestParser.extractTimestamp(
                request, historicalMaxTimestamp == Long.MIN_VALUE ? 0 : historicalMaxTimestamp);
    long logIndex = getLastLogIndexLocked() + 1;
    long partitionIndex;
    long interPartitionIndex;
    long lastPartitionCount = currentPartitionIndexCount;
    // TRaft-specific extension: out-of-order timestamps open a fresh logical partition so catch-up
    // code can preserve time-series ordering hints. Traditional Raft has no notion of partitions;
    // safety still comes entirely from the log index/term assigned below.
    if (historicalMaxTimestamp == Long.MIN_VALUE) {
      partitionIndex = INITIAL_PARTITION_INDEX;
      interPartitionIndex = 0;
    } else if (request != null && request.hasTime() && timestamp < historicalMaxTimestamp) {
      partitionIndex = maxPartitionIndex + 1;
      interPartitionIndex = 0;
    } else {
      partitionIndex = maxPartitionIndex;
      interPartitionIndex = currentPartitionIndexCount;
    }
    return new TRaftLogEntry(
        entryType,
        timestamp,
        partitionIndex,
        logIndex,
        currentTerm,
        interPartitionIndex,
        lastPartitionCount,
        data);
  }

  private void appendReplicatedEntriesLocked(List<TRaftLogEntry> entries) throws IOException {
    for (TRaftLogEntry entry : entries) {
      long localTerm = getLogTermLocked(entry.getLogIndex());
      if (localTerm == entry.getLogTerm()) {
        continue;
      }
      if (localTerm != -1) {
        // Traditional Raft conflict repair: drop the conflicting suffix and accept the leader's
        // replacement entries from the first mismatching index.
        logStore.truncateSuffix(entry.getLogIndex());
        if (commitIndex >= entry.getLogIndex()) {
          commitIndex = entry.getLogIndex() - 1;
        }
        lastApplied = Math.min(lastApplied, commitIndex);
        recalculatePartitionStatsLocked();
      }
      logStore.append(entry);
      logicalClock.set(Math.max(logicalClock.get(), entry.getLogIndex()));
      updatePartitionStatsForEntryLocked(entry);
    }
    persistMetadataLocked();
  }

  private void advanceCommitIndexLocked() {
    List<Long> matchedIndices = new ArrayList<>();
    matchedIndices.add(getLastLogIndexLocked());
    for (Peer follower : getFollowersLocked()) {
      matchedIndices.add(matchIndexByFollower.getOrDefault(follower.getNodeId(), 0L));
    }
    matchedIndices.sort(Comparator.naturalOrder());
    long majorityMatchIndex = matchedIndices.get(matchedIndices.size() / 2);
    // Standard Raft commit rule: only commit entries from the current term once a quorum matches
    // them. Older-term entries become committed indirectly when a later current-term entry commits.
    if (majorityMatchIndex > commitIndex && getLogTermLocked(majorityMatchIndex) == currentTerm) {
      commitIndex = majorityMatchIndex;
      persistMetadataQuietlyLocked();
      notifyAll();
    }
  }

  private void applyCommittedEntriesLocked() {
    while (lastApplied < commitIndex) {
      long nextToApply = lastApplied + 1;
      if (nextToApply <= snapshotLastIncludedIndex) {
        lastApplied = nextToApply;
        continue;
      }
      TRaftLogEntry entry = logStore.getByIndex(nextToApply);
      if (entry == null) {
        break;
      }
      TSStatus status = RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      if (entry.getEntryType() == TRaftEntryType.DATA) {
        IConsensusRequest deserializedRequest =
            stateMachine.deserializeRequest(TRaftRequestParser.buildRequest(entry.getData()));
        if (leaderId != thisNode.getNodeId()) {
          deserializedRequest.markAsGeneratedByRemoteConsensusLeader();
        }
        // Both leader and follower apply only after commit. This is the key safety difference from
        // the original experimental TRaft implementation, which used to apply before quorum.
        status = stateMachine.write(deserializedRequest);
        if (role == TRaftRole.LEADER && leaderId == thisNode.getNodeId()) {
          applyResultByIndex.put(nextToApply, status);
        }
      } else if (entry.getEntryType() == TRaftEntryType.CONFIGURATION) {
        applyConfigurationEntryLocked(entry);
      }
      lastApplied = nextToApply;
      if (!isSuccess(status)) {
        LOGGER.warn("TRaft apply of log index {} returned {}", nextToApply, status);
      }
      persistMetadataQuietlyLocked();
    }
    if (!leaderReady
        && role == TRaftRole.LEADER
        && leaderReadyIndex > 0
        && commitIndex >= leaderReadyIndex) {
      // Upper layers are notified only after the current-term no-op becomes committed.
      leaderReady = true;
      stateMachine.event().notifyLeaderReady();
    }
    notifyAll();
  }

  private void applyConfigurationEntryLocked(TRaftLogEntry entry) {
    List<Peer> peers = TRaftSerializationUtils.deserializePeers(entry.getData());
    configuration.clear();
    configuration.addAll(peers);
    persistConfigurationQuietlyLocked();
    reinitializeReplicationStateLocked();
    if (!configuration.contains(thisNode)) {
      leaderReady = false;
      if (role == TRaftRole.LEADER) {
        becomeFollowerLocked(-1);
      }
    }
    stateMachine.event().notifyConfigurationChanged(entry.getLogTerm(), entry.getLogIndex(), new ArrayList<>(configuration));
  }

  private void maybeTriggerSnapshotAsyncLocked() {
    if (backgroundExecutor == null || snapshotInProgress.get()) {
      return;
    }
    if (commitIndex - snapshotLastIncludedIndex < config.getSnapshot().getAutoTriggerLogThreshold()) {
      return;
    }
    backgroundExecutor.execute(
        () -> {
          try {
            triggerSnapshot(false);
          } catch (IOException e) {
            LOGGER.warn("Failed to trigger automatic TRaft snapshot", e);
          }
        });
  }

  private void becomeLeaderLocked() {
    role = TRaftRole.LEADER;
    leaderId = thisNode.getNodeId();
    leaderReady = false;
    leaderReadyIndex = 0;
    reinitializeReplicationStateLocked();
    long nextIndex = getLastLogIndexLocked() + 1;
    // Fresh leaders optimistically probe every follower from the current log tail and back off
    // through AppendEntries responses until the common prefix is found.
    for (Peer follower : getFollowersLocked()) {
      nextIndexByFollower.put(follower.getNodeId(), nextIndex);
      matchIndexByFollower.put(follower.getNodeId(), 0L);
    }
    stateMachine.event().notifyLeaderChanged(thisNode.getGroupId(), leaderId);
  }

  private void becomeFollowerLocked(int newLeaderId) {
    becomeFollowerLocked(newLeaderId, true);
  }

  private void becomeFollowerLocked(int newLeaderId, boolean resetElectionDeadline) {
    role = TRaftRole.FOLLOWER;
    leaderId = newLeaderId;
    leaderReady = false;
    leaderReadyIndex = 0;
    if (resetElectionDeadline) {
      resetElectionDeadlineLocked();
    }
    notifyFollowerStateLocked();
  }

  private void notifyFollowerStateLocked() {
    if (leaderId != -1) {
      stateMachine.event().notifyLeaderChanged(thisNode.getGroupId(), leaderId);
    }
    if (role != TRaftRole.LEADER) {
      stateMachine.event().notifyNotLeader();
    }
  }

  private void updateTermLocked(long newTerm, int newVotedFor) {
    // currentTerm and votedFor are persisted independently from log entries so a restart does not
    // lose voting history when no data entry was appended in the higher term.
    currentTerm = newTerm;
    votedFor = newVotedFor;
    leaderReady = false;
    persistMetadataQuietlyLocked();
  }

  private void reinitializeReplicationStateLocked() {
    ConcurrentHashMap<Integer, Long> existingNextIndex = new ConcurrentHashMap<>(nextIndexByFollower);
    ConcurrentHashMap<Integer, Long> existingMatchIndex = new ConcurrentHashMap<>(matchIndexByFollower);
    nextIndexByFollower.clear();
    matchIndexByFollower.clear();
    for (Peer follower : getFollowersLocked()) {
      nextIndexByFollower.put(
          follower.getNodeId(),
          existingNextIndex.getOrDefault(follower.getNodeId(), getLastLogIndexLocked() + 1));
      matchIndexByFollower.put(
          follower.getNodeId(), existingMatchIndex.getOrDefault(follower.getNodeId(), 0L));
    }
    logicalClock.set(Math.max(logicalClock.get(), getLastLogIndexLocked()));
  }

  private List<Peer> getFollowersLocked() {
    List<Peer> followers = new ArrayList<>();
    for (Peer peer : configuration) {
      if (peer.getNodeId() != thisNode.getNodeId()) {
        followers.add(peer);
      }
    }
    return followers;
  }

  private List<Peer> getFollowersSnapshot() {
    synchronized (this) {
      return getFollowersLocked();
    }
  }

  private long getLastLogIndexLocked() {
    return Math.max(snapshotLastIncludedIndex, logStore.getLastIndex());
  }

  private long getLastLogTermLocked() {
    return getLogTermLocked(getLastLogIndexLocked());
  }

  private long getLogTermLocked(long index) {
    if (index == 0) {
      return 0;
    }
    if (index == snapshotLastIncludedIndex) {
      return snapshotLastIncludedTerm;
    }
    return logStore.getTerm(index);
  }

  private long computeNextIndexHintLocked(long rejectedPrevLogIndex) {
    if (rejectedPrevLogIndex > getLastLogIndexLocked()) {
      return getLastLogIndexLocked() + 1;
    }
    if (rejectedPrevLogIndex <= snapshotLastIncludedIndex) {
      return snapshotLastIncludedIndex + 1;
    }
    return rejectedPrevLogIndex;
  }

  private int compareLogFreshnessLocked(
      long candidateLastLogTerm, long candidateLastLogIndex, long localLastLogTerm, long localLastLogIndex) {
    // Intentionally standard Raft freshness ordering: term first, then index.
    int termCompare = Long.compare(candidateLastLogTerm, localLastLogTerm);
    if (termCompare != 0) {
      return termCompare;
    }
    return Long.compare(candidateLastLogIndex, localLastLogIndex);
  }

  private long firstAvailableIndexLocked() {
    long firstIndex = logStore.getFirstIndex();
    if (firstIndex != -1) {
      return firstIndex;
    }
    return snapshotLastIncludedIndex + 1;
  }

  private TRaftInstallSnapshotRequest buildInstallSnapshotRequestLocked() {
    if (snapshotLastIncludedIndex <= 0 || !snapshotDir.exists()) {
      return null;
    }
    try {
      // Snapshot RPC carries both the state-machine snapshot and the TRaft-specific partition
      // metadata so the follower can rebuild partition assignment without replaying compacted logs.
      return new TRaftInstallSnapshotRequest(
          thisNode.getNodeId(),
          currentTerm,
          snapshotLastIncludedIndex,
          snapshotLastIncludedTerm,
          snapshotHistoricalMaxTimestamp,
          snapshotPartitionIndex,
          snapshotPartitionCount,
          TRaftSerializationUtils.serializePeers(new ArrayList<>(configuration)),
          TRaftSerializationUtils.zipDirectory(snapshotDir));
    } catch (IOException e) {
      LOGGER.warn("Failed to build TRaft install snapshot request", e);
      return null;
    }
  }

  private void replaceSnapshotLocked(TRaftInstallSnapshotRequest request) throws IOException {
    deleteRecursively(snapshotDir);
    TRaftSerializationUtils.unzipDirectory(request.getSnapshot(), snapshotDir);
    snapshotLastIncludedIndex = request.getLastIncludedIndex();
    snapshotLastIncludedTerm = request.getLastIncludedTerm();
    snapshotHistoricalMaxTimestamp = request.getHistoricalMaxTimestamp();
    snapshotPartitionIndex = request.getLastPartitionIndex();
    snapshotPartitionCount = request.getLastPartitionCount();
    commitIndex = snapshotLastIncludedIndex;
    lastApplied = snapshotLastIncludedIndex;
    if (getLastLogIndexLocked() <= snapshotLastIncludedIndex) {
      logStore.clear();
    } else {
      logStore.compactPrefix(snapshotLastIncludedIndex);
    }
    configuration.clear();
    configuration.addAll(TRaftSerializationUtils.deserializePeers(request.getPeers()));
    persistConfiguration();
    recalculatePartitionStatsLocked();
    logicalClock.set(Math.max(getLastLogIndexLocked(), snapshotLastIncludedIndex));
    persistMetadataLocked();
    stateMachine.event().notifyConfigurationChanged(currentTerm, commitIndex, new ArrayList<>(configuration));
  }

  private void loadSnapshotIfPresent() {
    if (snapshotDir.exists()) {
      stateMachine.loadSnapshot(snapshotDir);
    }
  }

  private void recoverFromDisk() throws IOException {
    // Metadata recovery restores the persisted Raft term/vote/commit boundary before replay.
    loadMetadata();
    recalculatePartitionStatsLocked();
    logicalClock.set(Math.max(getLastLogIndexLocked(), snapshotLastIncludedIndex));
    commitIndex = Math.min(commitIndex, getLastLogIndexLocked());
    lastApplied = Math.min(lastApplied, commitIndex);
    leaderId = -1;
    role = TRaftRole.FOLLOWER;
    leaderReady = false;
    reinitializeReplicationStateLocked();
  }

  private void recalculatePartitionStatsLocked() {
    historicalMaxTimestamp = snapshotHistoricalMaxTimestamp;
    maxPartitionIndex = snapshotPartitionIndex;
    currentPartitionIndexCount = snapshotPartitionCount;
    for (TRaftLogEntry entry : logStore.getAllEntries()) {
      updatePartitionStatsForEntryLocked(entry);
    }
  }

  private void updatePartitionStatsForEntryLocked(TRaftLogEntry entry) {
    historicalMaxTimestamp = Math.max(historicalMaxTimestamp, entry.getTimestamp());
    if (entry.getPartitionIndex() > maxPartitionIndex) {
      maxPartitionIndex = entry.getPartitionIndex();
      currentPartitionIndexCount = entry.getInterPartitionIndex() + 1;
      return;
    }
    if (entry.getPartitionIndex() == maxPartitionIndex) {
      currentPartitionIndexCount =
          Math.max(currentPartitionIndexCount, entry.getInterPartitionIndex() + 1);
    }
  }

  private void loadMetadata() throws IOException {
    if (!metadataFile.exists()) {
      return;
    }
    Properties properties = new Properties();
    try (InputStream inputStream = Files.newInputStream(metadataFile.toPath())) {
      properties.load(inputStream);
    }
    currentTerm = Long.parseLong(properties.getProperty("currentTerm", "0"));
    votedFor = Integer.parseInt(properties.getProperty("votedFor", "-1"));
    commitIndex = Long.parseLong(properties.getProperty("commitIndex", "0"));
    lastApplied = Long.parseLong(properties.getProperty("lastApplied", "0"));
    snapshotLastIncludedIndex =
        Long.parseLong(properties.getProperty("snapshotLastIncludedIndex", "0"));
    snapshotLastIncludedTerm = Long.parseLong(properties.getProperty("snapshotLastIncludedTerm", "0"));
    snapshotHistoricalMaxTimestamp =
        Long.parseLong(
            properties.getProperty("snapshotHistoricalMaxTimestamp", String.valueOf(Long.MIN_VALUE)));
    snapshotPartitionIndex =
        Long.parseLong(properties.getProperty("snapshotPartitionIndex", "0"));
    snapshotPartitionCount =
        Long.parseLong(properties.getProperty("snapshotPartitionCount", "0"));
  }

  private void persistMetadataLocked() throws IOException {
    Properties properties = new Properties();
    properties.setProperty("currentTerm", String.valueOf(currentTerm));
    properties.setProperty("votedFor", String.valueOf(votedFor));
    properties.setProperty("commitIndex", String.valueOf(commitIndex));
    properties.setProperty("lastApplied", String.valueOf(lastApplied));
    properties.setProperty("snapshotLastIncludedIndex", String.valueOf(snapshotLastIncludedIndex));
    properties.setProperty("snapshotLastIncludedTerm", String.valueOf(snapshotLastIncludedTerm));
    properties.setProperty(
        "snapshotHistoricalMaxTimestamp", String.valueOf(snapshotHistoricalMaxTimestamp));
    properties.setProperty("snapshotPartitionIndex", String.valueOf(snapshotPartitionIndex));
    properties.setProperty("snapshotPartitionCount", String.valueOf(snapshotPartitionCount));
    try (OutputStream outputStream = Files.newOutputStream(metadataFile.toPath())) {
      properties.store(outputStream, "TRaft metadata");
    }
  }

  private void persistMetadataQuietlyLocked() {
    try {
      persistMetadataLocked();
    } catch (IOException e) {
      LOGGER.warn("Failed to persist TRaft metadata for {}", thisNode, e);
    }
  }

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

  private void persistConfigurationQuietlyLocked() {
    try {
      persistConfiguration();
    } catch (IOException e) {
      LOGGER.warn("Failed to persist TRaft configuration for {}", thisNode, e);
    }
  }

  private void resetElectionDeadlineLocked() {
    long timeoutMinMs = config.getElection().getTimeoutMinMs();
    long timeoutMaxMs = config.getElection().getTimeoutMaxMs();
    long timeoutRange = Math.max(1, timeoutMaxMs - timeoutMinMs);
    // Randomization keeps candidates from repeatedly colliding, just like traditional Raft.
    electionDeadlineMs =
        System.currentTimeMillis() + timeoutMinMs + random.nextInt((int) timeoutRange);
  }

  private boolean isSuccess(TSStatus status) {
    return status != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private static void deleteRecursively(File file) throws IOException {
    if (file == null || !file.exists()) {
      return;
    }
    try (java.util.stream.Stream<java.nio.file.Path> walk = Files.walk(file.toPath())) {
      walk.sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }
}
