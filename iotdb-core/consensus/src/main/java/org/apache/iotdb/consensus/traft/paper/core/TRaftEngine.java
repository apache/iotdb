/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.consensus.traft.paper.core;

import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Application;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableStore;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.IdRange;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Proposal;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.ProposalSlice;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Rank;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Report;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Role;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Fixed-membership implementation of the paper's staged-proposal protocol. Every public event is
 * serialized, including durable writes. Transport must enqueue drainMessages() results; it must not
 * invoke a peer while holding this engine's monitor. A successful durable append is the protocol
 * persistence boundary. State-machine retry atomicity belongs to Application.
 */
public final class TRaftEngine {
  private java.util.function.Consumer<EntryId> commitObserver = ignored -> {};

  /**
   * A synchronous ordinary-DATA candidate. The caller owns the payload and must not change it until
   * appendDataBatch returns; each accepted Entry takes its own defensive copy. Unaccepted boundary
   * candidates are never copied or retained by the engine.
   */
  public static final class DataInput {
    private final long timestamp;
    private final boolean hasSeries;
    private final long seriesKey;
    private final byte[] payload;

    public DataInput(long timestamp, byte[] payload) {
      this(timestamp, false, 0, payload);
    }

    /** With a series key, series ordering (setSeriesOrder) can admit it beside other series. */
    public DataInput(long timestamp, boolean hasSeries, long seriesKey, byte[] payload) {
      this.timestamp = timestamp;
      this.hasSeries = hasSeries;
      this.seriesKey = seriesKey;
      this.payload = Objects.requireNonNull(payload);
    }
  }

  /** Observes newly established commit knowledge; never participates in quorum decisions. */
  public void setCommitObserver(java.util.function.Consumer<EntryId> observer) {
    commitObserver = Objects.requireNonNull(observer);
  }

  private static final int MAX_COMMIT_RECORD_IDS = 1024;
  // Uncompacted partitions below which every due compaction runs (maybeCompact).
  private static final long COMPACTION_SMALL_WINDOW = 64;
  // Above it, partitions a compaction must drop per partition it keeps.
  private static final long COMPACTION_DROP_RATIO = 4;
  private final int self;
  private final Set<Integer> members;
  // Fixed membership: the peer set never changes, so it is computed once for the hot paths.
  private final Set<Integer> peerSet;
  private final int quorum;
  private final int theta;
  private final long maxPartitionMillis;
  private final DurableStore store;
  private final Application application;
  private final boolean durableCommitRecords;
  private final boolean durableAppliedRecords;
  private final Map<EntryId, Entry> stored = new LinkedHashMap<>();
  private final InstalledLog installed = new InstalledLog();
  // The same entries by index for point lookups, which the hot paths make for every entry; the
  // ordered log serves ranges and iteration.
  private final LongMap<Entry> installedAt = new LongMap<>();
  private final LongMap<PartitionState> partitions = new LongMap<>();
  private final Map<EntryId, ProposalIndex> proposalIndex = new HashMap<>();
  private final Map<Integer, Proposal> transferCoverage = new HashMap<>();
  private final Map<Integer, LinkedHashMap<EntryId, Message>> rejectedData = new HashMap<>();
  private long commitNoticePartition = -1;
  private long commitNoticeCursor;
  private long commitNoticeCeiling;
  private long schedulingEntryVisits;
  private final Map<EntryId, Proposal> retained = new LinkedHashMap<>();
  private final Map<EntryId, Proposal> incoming = new LinkedHashMap<>();
  // A proposal waits only on identities in its own manifest, never a flattened history.
  // The same identity wakes waiters when its bytes, retained manifest, or installation arrive.
  private final LinkedHashSet<EntryId> incomingReady = new LinkedHashSet<>();
  private final Map<EntryId, Integer> incomingSources = new HashMap<>();
  private final Map<EntryId, Set<EntryId>> incomingWaiters = new HashMap<>();
  private final Map<EntryId, Set<EntryId>> incomingDependencies = new HashMap<>();
  private final Map<Integer, LinkedHashSet<EntryId>> incomingFetchWaiters = new LinkedHashMap<>();
  private final Map<EntryId, Integer> incomingFetchSources = new HashMap<>();
  private final Set<EntryId> knownCommitted = new LinkedHashSet<>();
  private final Set<EntryId> applied = new LinkedHashSet<>();
  // ACKs per identity as a bitmask over the fixed voters (memberBit): no set per entry.
  private final Map<EntryId, Integer> entryAcks = new HashMap<>();
  private final Map<EntryId, Integer> proposalAcks = new HashMap<>();
  private final Map<Integer, Integer> memberBit = new HashMap<>();
  private final int[] bitMember;
  private final Map<Integer, Report> reports = new LinkedHashMap<>();
  private final Map<Integer, List<Report>> outgoingReports = new HashMap<>();
  private final Map<Integer, ReportAssembly> partialReports = new HashMap<>();
  private int controlPageBytes = 32 * 1024;
  private final Set<Integer> votes = new HashSet<>();
  private final Set<EntryId> confirmations = new LinkedHashSet<>();
  private final Set<EntryId> requestedHistory = new HashSet<>();
  private final Map<EntryId, Integer> requestedManifests = new LinkedHashMap<>();
  private final List<Message> outbox = new ArrayList<>();
  private final List<Entry> appliedEvents = new ArrayList<>();
  private final Map<Integer, LinkedHashMap<EntryId, Message>> inFlight = new HashMap<>();
  // setResendDelay: when each in-flight payload was last queued, per peer; empty while zero.
  private long resendDelayNanos;
  private final Map<Integer, Map<EntryId, Long>> lastSent = new HashMap<>();
  private final Map<Integer, LinkedHashMap<EntryId, Message>> pendingData = new HashMap<>();
  private final Map<Integer, Set<EntryId>> requestedEntries = new HashMap<>();
  // Members a proposal needs that were not stored on arrival, with the time their fetch is due.
  // Batches to one peer travel in parallel, so an earlier batch's entries often land a moment
  // after the proposal that names them; fetching at once would duplicate them (setFetchGrace).
  private final LinkedHashMap<EntryId, DeferredFetch> deferredFetches = new LinkedHashMap<>();
  private long fetchGraceNanos;
  private final Map<Integer, Set<EntryId>> recoveryReceipts = new HashMap<>();
  private int replicationWindow = 64;
  // Set while a run of repair payloads is processed: one wake-up pass at the end of the run.
  private boolean deferIncoming;
  private long proposalPayloadBudget = TRaftCodec.maxMessageBytes();
  private boolean recoveryPlanned;
  private Proposal recoveryBase;
  private final Map<EntryId, Proposal> recoveryProposals = new LinkedHashMap<>();
  private final Map<EntryId, Entry> recoveryMetadata = new LinkedHashMap<>();
  private final Map<EntryId, Set<Integer>> recoverySources = new LinkedHashMap<>();
  private final Set<EntryId> recoveryResult = new LinkedHashSet<>();
  private final List<EntryId> recoveryOpen = new ArrayList<>();
  private long term;
  private long electionProgress;
  private int vote = -1;
  private int leaderId = -1;
  private Role role = Role.FOLLOWER;
  // The single in-flight recovery proposal of a LeaderCandidate.
  private Proposal current;
  // Pipelined boundaries (the paper's "later proposals extend earlier ones"): a Leader keeps
  // ingesting while its ordinary boundary proposals are pending. They wait here in partition order;
  // the newest opens the partition new entries go to. A quorum ACK of one confirms it and every
  // earlier one, whose results its exact prefix contains.
  private final LinkedHashMap<EntryId, Proposal> chain = new LinkedHashMap<>();
  // The same proposals by partition: chain partitions are contiguous above the installed one.
  private final Map<Long, Proposal> chainByPartition = new HashMap<>();
  // Per peer, the newest chain partition it ACKed in this term. An ACK certifies the exact prefix,
  // so this one number stands for the peer's ACK of every earlier pending proposal.
  private final Map<Integer, Long> chainAcked = new HashMap<>();
  // Per proposal being sent: peers that hold its first entry, and peers that get metadata only.
  private final Map<EntryId, Set<Integer>> firstReceipts = new HashMap<>();
  private final Map<EntryId, Set<Integer>> metadataOnlyPeers = new HashMap<>();
  // Durable entries above the installed log, by partition and for one creation term each: a
  // Leader's staged ingestion and a follower's staged ENTRYs. Installing the proposal that opens a
  // partition installs that partition's staged entries of the proposal's term.
  private final TreeMap<Long, StagedPartition> staged = new TreeMap<>();
  // The newest proposal of `chain`, or null when nothing is pending.
  private Proposal chainTip;
  // While a follower processes one proposal: whether completeIncoming ACKed it (so the receipt of
  // its carried first needs no TRANSFER_ACK), and a proposal confirmed by reference, which it does
  // not ACK again.
  private EntryId watchedAck;
  private boolean watchedAcked;
  private EntryId confirmedByReference;
  // The extension installProposal validated for the INSTALL marker it is writing (replayed at
  // once).
  private Proposal validatedExtensionOf;
  private List<Proposal> validatedExtension;
  // Highest index this Leader assigned in its term; staged entries lie above the installed log.
  private long assignedIndex;
  // Cumulative boundary proposals created by this replica (research counters for the runtime).
  private long dataProposals;
  private long noopProposals;
  private Proposal installedOpening;
  private Proposal latestRetained;
  private long committedFrontier = -1;
  private long advertisedFrontier = -1;
  // Highest partition whose local members the frontier has marked committed: never above the last
  // locally closed partition, while advertisedFrontier may run ahead of this replica's log.
  private long markedFrontier = -1;
  private long appliedFrontier = -1;
  private long timestampMax = Long.MIN_VALUE;
  // Long.MIN_VALUE is a valid timestamp; absence represents the paper's negative infinity.
  private boolean hasTimestamp;
  private long openedAt;
  private boolean failed;
  // Series ordering (opt-in, setSeriesOrder): a data entry opens a partition only if the open
  // partition already holds an entry of its series at the same or a later timestamp, or any entry
  // that could write the same point. Entries of one partition then never write the same point, so
  // every application order gives the same state. The map is leader-local and describes exactly
  // the data of partition seriesPartition; a leader that did not admit all of it opens a new one.
  private boolean seriesOrder;
  private final Map<Long, Long> seriesMax = new HashMap<>();
  private long seriesPartition = -1;
  private boolean partitionHasData;
  private long partitionDataMax;
  private boolean partitionHasKeyless;
  private long partitionKeylessMax;
  // Compaction (opt-in, setCompaction): the per-identity state of partitions <= compactedThrough
  // is dropped. The anchor is the retained opening proposal of partition compactedThrough + 1;
  // chain walks stop there. The runs record the dropped canonical identities exactly.
  private boolean compactionEnabled;
  // Highest partition every member has closed, as this replica knows it; it only increases.
  private long compactionCap = -1;
  // Leader of this term: the highest partition each peer has closed, inferred from its ACKs.
  private final Map<Integer, Long> peerClosed = new HashMap<>();
  private long compactedThrough = -1;
  private Proposal compactionAnchor;
  private final TreeMap<Long, CompactedRun> compactedRuns = new TreeMap<>();
  private long compactedEntries;
  private long compactedTimestampMax = Long.MIN_VALUE;
  private boolean compactedHasTimestamp;
  // What checkCompactionInvariants last saw: neither may decrease within one incarnation.
  private long checkedFrontier = -1;
  private long checkedAnchorIndex = -1;
  // Tests only: the fuzz harness's negative control compacts without the cluster cap.
  private boolean ignoreCompactionCap;

  public TRaftEngine(
      int self,
      Set<Integer> members,
      int theta,
      long maxPartitionMillis,
      DurableStore store,
      Application application)
      throws IOException {
    if (!members.contains(self) || members.isEmpty() || theta < 2 || maxPartitionMillis < 0) {
      throw new IllegalArgumentException("self must be a member; theta >= 2; timeout >= 0");
    }
    this.self = self;
    this.members = Collections.unmodifiableSet(new HashSet<>(members));
    if (this.members.size() > 31) {
      throw new IllegalArgumentException("TRaft supports at most 31 voters");
    }
    List<Integer> ordered = new ArrayList<>(new TreeSet<>(this.members));
    this.bitMember = new int[ordered.size()];
    for (int i = 0; i < ordered.size(); i++) {
      memberBit.put(ordered.get(i), 1 << i);
      bitMember[i] = ordered.get(i);
    }
    Set<Integer> others = new LinkedHashSet<>(new TreeSet<>(members));
    others.remove(self);
    this.peerSet = Collections.unmodifiableSet(others);
    this.quorum = members.size() / 2 + 1;
    this.theta = theta;
    this.maxPartitionMillis = maxPartitionMillis;
    this.store = store;
    this.application = application;
    this.durableCommitRecords = application.requiresDurableCommitRecords();
    this.durableAppliedRecords = application.requiresDurableAppliedRecords();
    if (!durableCommitRecords && durableAppliedRecords) {
      throw new IllegalArgumentException(
          "volatile commitment requires snapshot/replay application without durable APPLIED records");
    }
    for (DurableRecord record : store.load()) {
      replay(record);
    }
    recomputeTimestampMax();
    advanceAppliedFrontier();
  }

  public synchronized Role role() {
    return role;
  }

  /** Whether {@code id} names the installed opening proposal, which a CONFIRM heartbeat repeats. */
  public synchronized boolean isInstalledOpening(EntryId id) {
    return installedOpening != null && installedOpening.id().equals(id);
  }

  public synchronized long term() {
    return term;
  }

  /** Monotonic useful election/recovery progress; duplicate traffic does not renew it. */
  public synchronized long electionProgress() {
    return electionProgress;
  }

  public synchronized int leaderId() {
    return leaderId;
  }

  public synchronized long committedFrontier() {
    return committedFrontier;
  }

  public synchronized long currentPartition() {
    return cur();
  }

  public synchronized boolean isFailed() {
    return failed;
  }

  public synchronized boolean hasPendingProposal() {
    return current != null || !chain.isEmpty();
  }

  /**
   * Whether this replica admits new entries now. A Leader always does: pending boundary proposals
   * do not block ingestion. A LeaderCandidate admits nothing until its recovery is confirmed.
   */
  public synchronized boolean admitting() {
    return role == Role.LEADER && current == null;
  }

  /** Cumulative boundary proposals this replica created that start with a data or barrier entry. */
  public synchronized long dataProposals() {
    return dataProposals;
  }

  /** Cumulative boundary proposals this replica created that start with a no-op. */
  public synchronized long noopProposals() {
    return noopProposals;
  }

  /** Whether {@code id} names a retained ordinary proposal: its ACK need not hurry a force. */
  public synchronized boolean isOrdinaryProposal(EntryId id) {
    Proposal proposal = retained.get(id);
    return proposal != null && !proposal.recovery;
  }

  public synchronized void setReplicationWindow(int window) {
    if (window < 1) {
      throw new IllegalArgumentException("window must be positive");
    }
    for (Map<EntryId, Message> flight : inFlight.values()) {
      if (flight.size() > window) {
        throw new IllegalStateException("cannot shrink below outstanding entries");
      }
    }
    replicationWindow = window;
  }

  /** Configure before sending ordinary proposals; oversized combined frames use metadata repair. */
  public synchronized void setProposalPayloadBudget(long bytes) {
    if (bytes <= 0) {
      throw new IllegalArgumentException("proposal payload budget must be positive");
    }
    if (!chain.isEmpty()) {
      throw new IllegalStateException("cannot change proposal budget during an active proposal");
    }
    proposalPayloadBudget = Math.min(bytes, (long) TRaftCodec.maxMessageBytes());
  }

  /**
   * A transport byte-budget rejection can downgrade only this peer's carried proposal. This is not
   * a durable receipt or quorum evidence. Repair/ordinary slots with the same ID stay intact.
   */
  public synchronized void fallbackProposalPayload(int peer, EntryId id) {
    if (id == null || peer == self || !members.contains(peer)) {
      return;
    }
    Proposal proposal = retained.get(id);
    if (proposal == null) {
      return;
    }
    boolean currentProposal = chain.containsKey(id);
    boolean changed = false;
    Map<EntryId, Message> flight = inFlight.get(peer);
    Message active = flight == null ? null : flight.get(id);
    if (active != null && active.type == MessageType.PROPOSAL && active.entry != null) {
      flight.remove(id);
      changed = true;
    }
    Map<EntryId, Message> pending = pendingData.get(peer);
    Message queued = pending == null ? null : pending.get(id);
    if (queued != null && queued.type == MessageType.PROPOSAL && queued.entry != null) {
      pending.remove(id);
      changed = true;
    }
    if (currentProposal) {
      receipts(metadataOnlyPeers, id).add(peer);
      changed = true;
    }
    if (changed) {
      send(MessageType.PROPOSAL, peer, null, proposal, null, null, null, -1, null);
      pumpPending(peer);
    }
  }

  /**
   * Reject a drained carried frame whose slot was downgraded/replaced earlier in the same drain.
   */
  public synchronized boolean isProposalPayloadInFlight(int peer, EntryId id) {
    Map<EntryId, Message> flight = inFlight.get(peer);
    Message message = flight == null ? null : flight.get(id);
    return message != null
        && message.term == term
        && message.type == MessageType.PROPOSAL
        && message.entry != null;
  }

  public synchronized int inFlightEntries(int peer) {
    return inFlight.containsKey(peer) ? inFlight.get(peer).size() : 0;
  }

  public synchronized int outstandingFetches(int peer) {
    return requestedEntries.containsKey(peer) ? requestedEntries.get(peer).size() : 0;
  }

  /** Configure before recovery starts; pages include manifest slices and never carry payloads. */
  public synchronized void setControlPageBytes(int bytes) {
    if (bytes < 1024 || bytes > 32 * 1024) {
      throw new IllegalArgumentException("control pages must be 1-32 KiB");
    }
    if (!outgoingReports.isEmpty() || !partialReports.isEmpty()) {
      throw new IllegalStateException("report snapshot already fixed");
    }
    controlPageBytes = bytes;
  }

  /**
   * Allow this replica to drop the per-identity state of partitions that every member has closed,
   * once it has committed and applied them here. Off by default. Replaying COMPACT records at
   * startup does not depend on this switch: a compacted journal is always read as one.
   */
  public synchronized void setCompaction(boolean enabled) {
    compactionEnabled = enabled;
  }

  /**
   * Order data per series instead of per group; off by default. Takes effect at the next partition.
   */
  public synchronized void setSeriesOrder(boolean enabled) {
    seriesOrder = enabled;
    seriesPartition = -1;
  }

  /** The highest partition whose per-identity state is dropped, or -1. */
  public synchronized long compactedThrough() {
    return compactedThrough;
  }

  /** The opening proposal of partition compactedThrough() + 1, or null before any compaction. */
  public synchronized EntryId compactionAnchor() {
    return compactionAnchor == null ? null : compactionAnchor.id();
  }

  /**
   * Observation only: the number of keys in each per-identity and per-proposal structure, for tests
   * and the benchmark's memory accounting. Nothing in the protocol reads it.
   */
  public synchronized Map<String, Integer> stateSizes() {
    Map<String, Integer> sizes = new LinkedHashMap<>();
    sizes.put("installed", installed.size());
    sizes.put("partitions", partitions.size());
    sizes.put("stored", stored.size());
    sizes.put("retained", retained.size());
    sizes.put("proposalIndex", proposalIndex.size());
    sizes.put("knownCommitted", knownCommitted.size());
    sizes.put("applied", applied.size());
    sizes.put("entryAcks", entryAcks.size());
    sizes.put("proposalAcks", proposalAcks.size());
    sizes.put("confirmations", confirmations.size());
    sizes.put("requestedHistory", requestedHistory.size());
    sizes.put("requestedManifests", requestedManifests.size());
    sizes.put("incoming", incoming.size());
    sizes.put("inFlight", valueSizes(inFlight));
    sizes.put("pendingData", valueSizes(pendingData));
    sizes.put("rejectedData", valueSizes(rejectedData));
    sizes.put("requestedEntries", valueSizes(requestedEntries));
    sizes.put("compactedRuns", compactedRuns.size());
    return sizes;
  }

  private static int valueSizes(Map<Integer, ?> perPeer) {
    int total = 0;
    for (Object value : perPeer.values()) {
      total += value instanceof Map ? ((Map<?, ?>) value).size() : ((Collection<?>) value).size();
    }
    return total;
  }

  public synchronized Set<EntryId> committedIds() {
    Set<EntryId> ids = new LinkedHashSet<>();
    for (long part = compactedThrough + 1; part <= markedFrontier; part++) {
      PartitionState partition = partitions.get(part);
      if (partition != null) {
        ids.addAll(partition.entries.ids());
      }
    }
    ids.addAll(knownCommitted);
    return ids;
  }

  public synchronized Rank currentRank() {
    return rank();
  }

  public synchronized boolean isCommitted(EntryId id) {
    return committedIdentity(id) || compacted(id);
  }

  public synchronized boolean isApplied(EntryId id) {
    return appliedIdentity(id) || compacted(id);
  }

  /**
   * knownCommitted holds the committed identities the marked frontier does not cover: every
   * installed identity of a partition at or below it was marked committed (markClosedFrontier), and
   * a closed partition never changes its members.
   */
  private boolean committedIdentity(EntryId id) {
    if (knownCommitted.contains(id)) {
      return true;
    }
    Entry entry = installedAt.get(id.index);
    return entry != null && entry.partition <= markedFrontier && entry.id.equals(id);
  }

  /** As committedIdentity, for applied: the applied frontier passes closed, applied partitions. */
  private boolean appliedIdentity(EntryId id) {
    if (applied.contains(id)) {
      return true;
    }
    Entry entry = installedAt.get(id.index);
    return entry != null && entry.partition <= appliedFrontier && entry.id.equals(id);
  }

  /** Whether {@code ids} holds every installed identity of the marked partitions. */
  private boolean coversMarked(Set<EntryId> ids) {
    for (long part = compactedThrough + 1; part <= markedFrontier; part++) {
      PartitionState partition = partitions.get(part);
      if (partition != null && !ids.containsAll(partition.entries.ids())) {
        return false;
      }
    }
    return true;
  }

  private void raiseAppliedFrontier(long frontier) {
    for (long part = appliedFrontier + 1; part <= frontier; part++) {
      PartitionState passed = partitions.get(part);
      if (passed != null) {
        for (int i = 0; i < passed.entries.size(); i++) {
          applied.remove(passed.entries.get(i).id);
        }
      }
    }
    appliedFrontier = Math.max(appliedFrontier, frontier);
  }

  public synchronized List<Entry> installedEntries() {
    return new ArrayList<>(installed.values());
  }

  public synchronized List<Proposal> retainedProposals() {
    return new ArrayList<>(retained.values());
  }

  public synchronized List<Message> drainMessages() {
    List<Message> result = new ArrayList<>(outbox);
    outbox.clear();
    coalesceOutgoing(result);
    return result;
  }

  /**
   * One drain is one protocol round's output. Within it, an ACK of a current-term ordinary proposal
   * certifies every ancestor and a confirmation of one installs every ancestor, so per destination
   * only the newest of each survives; commit notices to one destination merge into one bounded
   * notice. Dropping the others is a subset of what message loss may do.
   */
  private void coalesceOutgoing(List<Message> messages) {
    if (messages.size() < 2) {
      return;
    }
    Map<Integer, Integer> newestAck = new HashMap<>();
    Map<Integer, Integer> newestConfirm = new HashMap<>();
    Map<Integer, Integer> commitAt = new HashMap<>();
    boolean[] drop = new boolean[messages.size()];
    boolean dropped = false;
    for (int i = 0; i < messages.size(); i++) {
      Message m = messages.get(i);
      if (m.type == MessageType.PROPOSAL_ACK || m.type == MessageType.CONFIRM) {
        Proposal named = chainProposalNamed(m);
        if (named == null) {
          continue;
        }
        Map<Integer, Integer> newest =
            m.type == MessageType.PROPOSAL_ACK ? newestAck : newestConfirm;
        Integer prior = newest.get(m.to);
        Proposal priorNamed = prior == null ? null : chainProposalNamed(messages.get(prior));
        if (priorNamed != null && isAncestor(priorNamed, named)) {
          drop[prior] = true;
          dropped = true;
          newest.put(m.to, i);
        } else if (priorNamed != null && isAncestor(named, priorNamed)) {
          drop[i] = true;
          dropped = true;
        } else {
          newest.put(m.to, i);
        }
      } else if (m.type == MessageType.COMMIT) {
        Integer prior = commitAt.get(m.to);
        Message merged = prior == null ? null : mergeCommitNotices(messages.get(prior), m);
        if (merged != null) {
          messages.set(i, merged);
          drop[prior] = true;
          dropped = true;
        }
        commitAt.put(m.to, i);
      }
    }
    if (dropped) {
      int kept = 0;
      for (int i = 0; i < messages.size(); i++) {
        if (!drop[i]) {
          messages.set(kept++, messages.get(i));
        }
      }
      messages.subList(kept, messages.size()).clear();
    }
  }

  /** The current-term ordinary proposal a PROPOSAL_ACK or CONFIRM names, or null. */
  private Proposal chainProposalNamed(Message m) {
    if (m.term != term) {
      return null;
    }
    EntryId id = m.proposal != null ? m.proposal.id() : m.reference;
    Proposal named = id == null ? null : retained.get(id);
    return named == null || named.recovery || named.first.id.term != term ? null : named;
  }

  private static Message mergeCommitNotices(Message earlier, Message later) {
    if (earlier.from != later.from
        || earlier.term != later.term
        || earlier.identities.size() + later.identities.size() > MAX_COMMIT_RECORD_IDS) {
      return null;
    }
    Set<EntryId> ids = new LinkedHashSet<>(earlier.identities);
    ids.addAll(later.identities);
    return new Message(
        MessageType.COMMIT,
        later.from,
        later.to,
        later.term,
        null,
        null,
        later.reference != null ? later.reference : earlier.reference,
        null,
        null,
        Math.max(earlier.frontier, later.frontier),
        new ArrayList<>(ids));
  }

  public synchronized List<Entry> drainApplied() {
    List<Entry> result = new ArrayList<>(appliedEvents);
    appliedEvents.clear();
    return result;
  }

  /**
   * Rebuild an application from an empty/checkpointed state using its own identity deduplication.
   */
  public synchronized void replayCommitted(Application target) throws IOException {
    // Compacted partitions are not held here; their COMMIT evidence was replayed at startup.
    for (long part = compactedThrough + 1; part <= cur(); part++) {
      List<Entry> entries = partition(part);
      boolean complete = !entries.isEmpty();
      for (Entry entry : entries) {
        if (!committedIdentity(entry.id)) {
          complete = false;
          continue;
        }
        if (entry.kind != Kind.NOOP) {
          target.apply(entry);
        }
      }
      if (!complete) {
        break;
      }
    }
  }

  /** Reconstruct only effects with a durable protocol APPLIED record. */
  public synchronized void replayApplied(Application target) throws IOException {
    for (Entry entry : installed.values()) {
      if (appliedIdentity(entry.id) && entry.kind != Kind.NOOP) {
        target.apply(entry);
      }
    }
  }

  /** Retry interrupted local application without producing replication traffic. */
  public synchronized void applyCommitted() throws IOException {
    checkHealthy();
    advanceCommitAndApply();
    maybeCompact();
  }

  public synchronized void startElection() throws IOException {
    checkHealthy();
    adoptTerm(term + 1);
    durable(new DurableRecord(RecordType.TERM_VOTE, term, self, null, null, null, -1, null));
    role = Role.CANDIDATE;
    leaderId = -1;
    votes.add(self);
    for (int peer : peers()) {
      send(MessageType.VOTE_REQUEST, peer, null, null, null, rank(), null, -1, null);
    }
    maybeWinElection();
  }

  /**
   * Admits one command at once; null only while a LeaderCandidate's recovery is unconfirmed. A
   * boundary does not wait for earlier boundaries: its proposal extends the newest pending one
   * (pipelined boundaries), and an administrative no-op boundary is followed by this command in the
   * partition the no-op opens. Returned entry identity is stable; success is reported only after
   * isApplied.
   */
  public synchronized Entry append(Kind kind, long timestamp, byte[] payload, long nowMillis)
      throws IOException {
    return append(kind, timestamp, false, 0, payload, nowMillis);
  }

  /** As append, with the series a data entry writes; the key matters only under series ordering. */
  public synchronized Entry append(
      Kind kind, long timestamp, boolean hasSeries, long seriesKey, byte[] payload, long nowMillis)
      throws IOException {
    checkHealthy();
    if (role != Role.LEADER) {
      throw new IllegalStateException("leader recovery is not complete");
    }
    if (kind == Kind.NOOP) {
      throw new IllegalArgumentException("no-op is protocol-owned");
    }
    if (current != null) {
      return null;
    }
    if (tipNonNoop() == 0) {
      // Recovery has no wall-clock dependency. Start the administrative timer
      // when this no-op-only partition first receives client data.
      openedAt = nowMillis;
    }
    if (tipBarriers() != 0
        || (tipNonNoop() != 0 && tipSize() >= theta)
        || (seriesOrder && kind == Kind.DATA && !seriesMapCurrent())) {
      // An administrative boundary: a no-op opens the next partition, which this command joins.
      propose(Kind.NOOP, 0, new byte[0], false, tipEntries(), tip(), nowMillis);
      if (seriesOrder) {
        resetSeries(tip().first.partition);
      }
    }
    boolean boundary =
        seriesOrder
            ? kind == Kind.DATA && seriesConflict(timestamp, hasSeries, seriesKey)
            : hasTimestamp && timestamp <= timestampMax;
    if (kind == Kind.BARRIER || boundary) {
      long next = tip().first.partition + 1;
      Entry first = propose(kind, timestamp, payload, false, tipEntries(), tip(), nowMillis).first;
      noteTimestamp(first);
      if (seriesOrder && kind == Kind.DATA) {
        // The new partition starts with exactly this entry.
        resetSeries(next);
        noteSeries(timestamp, hasSeries, seriesKey);
      }
      return first;
    }
    Proposal opening = tip();
    Entry entry =
        new Entry(
            new EntryId(term, nextIndex()),
            opening.first.partition,
            timestamp,
            opening.first.prevSize,
            kind,
            payload);
    if (chainTip == null) {
      installEntry(entry);
    } else {
      // A pending partition: the entry is staged here and installed with its opening.
      persistEntry(entry);
      noteTimestamp(entry);
    }
    assignedIndex = Math.max(assignedIndex, entry.id.index);
    if (seriesOrder && kind == Kind.DATA) {
      noteSeries(timestamp, hasSeries, seriesKey);
    }
    addEntryAck(entry.id, self);
    for (int peer : peers()) {
      send(MessageType.ENTRY, peer, entry, null, opening.id(), null, null, -1, null);
    }
    advanceCommitAndApply();
    return entry;
  }

  /** A Leader's staged data entries count toward the running timestamp maximum at once. */
  private void noteTimestamp(Entry entry) {
    if (entry.kind == Kind.DATA) {
      timestampMax = hasTimestamp ? Math.max(timestampMax, entry.timestamp) : entry.timestamp;
      hasTimestamp = true;
    }
  }

  /**
   * Synchronously persist the admissible DATA prefix of an already bounded caller batch with one
   * durable write. A timestamp (or series) conflict does not end the batch: the conflicting input
   * opens a new partition through a pipelined proposal fixing the partition before it, and later
   * inputs join the new partition. The batch stops before an administrative boundary (theta, a
   * barrier partition, a stale series map), which the single-entry append opens with a no-op. The
   * caller binds returned identities, in input order, before processing another event.
   */
  public synchronized List<Entry> appendDataBatch(List<DataInput> inputs, long nowMillis)
      throws IOException {
    checkHealthy();
    if (role != Role.LEADER) {
      throw new IllegalStateException("leader recovery is not complete");
    }
    if (quorum == 1 || current != null || inputs.isEmpty()) {
      return Collections.emptyList();
    }
    if (tipBarriers() != 0 || (seriesOrder && !seriesMapCurrent())) {
      return Collections.emptyList();
    }
    Proposal opening = tip();
    long part = opening.first.partition;
    boolean openingInstalled = chainTip == null;
    int count = tipSize();
    int nonNoop = tipNonNoop();
    boolean wasNoopOnly = nonNoop == 0;
    // Members of the partition before the next boundary: loaded only if a boundary occurs.
    List<EntryId> tipIds = null;
    List<EntryId> batchTipIds = new ArrayList<>();
    boolean timestampPresent = hasTimestamp;
    long maximum = timestampMax;
    // Series ordering checks each input against the newest partition plus the earlier inputs.
    Map<Long, Long> batchSeries = seriesOrder ? new HashMap<>() : null;
    boolean seriesFresh = false;
    boolean batchHasData = partitionHasData;
    long batchDataMax = partitionDataMax;
    boolean batchHasKeyless = partitionHasKeyless;
    long batchKeylessMax = partitionKeylessMax;
    long index = nextIndex();
    List<Entry> entries = new ArrayList<>();
    List<Proposal> created = new ArrayList<>();
    List<Proposal> openings = new ArrayList<>();
    List<DurableRecord> records = new ArrayList<>();
    for (DataInput input : inputs) {
      Objects.requireNonNull(input);
      if (nonNoop != 0 && count >= theta) {
        break;
      }
      boolean conflict;
      if (seriesOrder) {
        if (!input.hasSeries) {
          conflict = batchHasData && input.timestamp <= batchDataMax;
        } else {
          Long seen = batchSeries.get(input.seriesKey);
          if (seen == null && !seriesFresh) {
            seen = seriesMax.get(input.seriesKey);
          }
          conflict =
              (batchHasKeyless && input.timestamp <= batchKeylessMax)
                  || (seen != null && input.timestamp <= seen);
        }
      } else {
        conflict = timestampPresent && input.timestamp <= maximum;
      }
      Entry entry;
      if (conflict) {
        if (tipIds == null) {
          tipIds = new ArrayList<>();
          for (Entry e : tipEntries()) {
            tipIds.add(e.id);
          }
        }
        tipIds.addAll(batchTipIds);
        entry =
            new Entry(
                new EntryId(term, index++),
                part + 1,
                input.timestamp,
                tipIds.size(),
                Kind.DATA,
                input.payload);
        Proposal proposal = new Proposal(opening.id(), tipIds, entry, false);
        records.add(new DurableRecord(RecordType.ENTRY, term, -1, entry, null, null, -1, null));
        records.add(
            new DurableRecord(
                RecordType.RETAIN_PROPOSAL,
                term,
                -1,
                null,
                metadataProposal(proposal),
                null,
                -1,
                null));
        created.add(proposal);
        opening = proposal;
        part++;
        openingInstalled = false;
        tipIds = new ArrayList<>();
        batchTipIds = new ArrayList<>();
        batchTipIds.add(entry.id);
        count = 1;
        nonNoop = 1;
        if (seriesOrder) {
          // The new partition starts with exactly this entry.
          batchSeries.clear();
          seriesFresh = true;
          batchHasData = false;
          batchHasKeyless = false;
        }
      } else {
        entry =
            new Entry(
                new EntryId(term, index++),
                part,
                input.timestamp,
                opening.first.prevSize,
                Kind.DATA,
                input.payload);
        records.add(
            new DurableRecord(
                openingInstalled ? RecordType.INSTALL : RecordType.ENTRY,
                term,
                -1,
                entry,
                null,
                null,
                -1,
                null));
        batchTipIds.add(entry.id);
        count++;
        nonNoop++;
      }
      entries.add(entry);
      openings.add(opening);
      if (seriesOrder) {
        if (input.hasSeries) {
          batchSeries.merge(input.seriesKey, input.timestamp, Math::max);
        } else {
          batchKeylessMax =
              batchHasKeyless ? Math.max(batchKeylessMax, input.timestamp) : input.timestamp;
          batchHasKeyless = true;
        }
        batchDataMax = batchHasData ? Math.max(batchDataMax, input.timestamp) : input.timestamp;
        batchHasData = true;
      }
      maximum = timestampPresent ? Math.max(maximum, input.timestamp) : input.timestamp;
      timestampPresent = true;
    }
    if (entries.isEmpty()) {
      return entries;
    }
    persistInstallBatch(records);
    assignedIndex = Math.max(assignedIndex, index - 1);
    for (Proposal proposal : created) {
      Proposal retainedProposal = retained.get(proposal.id());
      chain.put(retainedProposal.id(), retainedProposal);
      chainByPartition.put(retainedProposal.first.partition, retainedProposal);
      chainTip = retainedProposal;
      dataProposals++;
    }
    for (int i = 0; i < entries.size(); i++) {
      Entry entry = entries.get(i);
      DataInput input = inputs.get(i);
      if (seriesOrder) {
        if (entry.id.equals(openings.get(i).id())) {
          resetSeries(entry.partition);
        }
        noteSeries(input.timestamp, input.hasSeries, input.seriesKey);
      }
      noteTimestamp(entry);
    }
    if (wasNoopOnly || !created.isEmpty()) {
      // The batch filled a no-op-only opening, or opened partitions: restart the timer.
      openedAt = nowMillis;
    }
    for (int i = 0; i < entries.size(); i++) {
      Entry entry = entries.get(i);
      Proposal entryOpening = openings.get(i);
      if (entry.id.equals(entryOpening.id())) {
        for (int peer : peers()) {
          sendProposal(peer, retained.get(entryOpening.id()));
        }
      } else {
        addEntryAck(entry.id, self);
        for (int peer : peers()) {
          send(MessageType.ENTRY, peer, entry, null, entryOpening.id(), null, null, -1, null);
        }
      }
    }
    // Newly generated identities have no remote ACK before this synchronized call returns.
    advanceCommitAndApply();
    return entries;
  }

  /**
   * Process a complete bounded run of same-opening ordinary ENTRY messages with one force, also
   * while repair is active: the whole run shares the opening it names, so installing it before the
   * next completeIncoming is the order in which the run had overtaken the proposal it completes. A
   * run that cannot be installed yet is staged durably in one force, so that the proposal which
   * will name these identities finds their bytes; false then selects receive for each message,
   * which reports the NACKs. Staging certifies nothing and is reported only as stored.
   */
  public synchronized boolean receiveEntries(List<Message> messages) throws IOException {
    checkHealthy();
    if (messages.size() < 2 || role != Role.FOLLOWER || current != null) {
      return false;
    }
    Message first = messages.get(0);
    if (first == null || first.from == self || !members.contains(first.from)) {
      return false;
    }
    List<Entry> entries = new ArrayList<>(messages.size());
    for (Message message : messages) {
      if (message == null
          || message.type != MessageType.ENTRY
          || message.from != first.from
          || message.to != self
          || message.term != term
          || message.entry == null
          || message.reference == null
          || !message.entry.hasPayload()
          || message.entry.kind != Kind.DATA) {
        return false;
      }
      // Followers accept legal same-partition holes/reordering. Local timestampMax is not
      // an admission test: a lower timestamp can be a previously missing ordinary entry.
      entries.add(message.entry);
    }
    if (!installableRun(messages)) {
      boolean stageable = stageableRun(messages);
      stage(entries);
      if (stageable) {
        // A pipelined partition: the staged run is durable, so it is ACKed like an installed one.
        leaderId = first.from;
        List<EntryId> rest = new ArrayList<>(entries.size() - 1);
        for (int i = 1; i < entries.size(); i++) {
          rest.add(entries.get(i).id);
        }
        send(
            MessageType.ENTRY_ACK,
            first.from,
            null,
            null,
            entries.get(0).id,
            null,
            null,
            cur(),
            rest);
      }
      completeIncoming();
      return stageable;
    }
    List<DurableRecord> records;
    try {
      records = prepareInstallBatch(entries);
    } catch (IOException invalid) {
      // Preserve the original per-message rejection/fencing behavior, with no batch effects.
      return false;
    }
    persistInstallBatch(records);
    leaderId = first.from;
    // One durable batch, one acknowledgment frame: the reference carries the first identity and
    // the identity list the rest. Each identity is certified exactly as a single ACK would be.
    List<EntryId> rest = new ArrayList<>(entries.size() - 1);
    for (int i = 1; i < entries.size(); i++) {
      rest.add(entries.get(i).id);
    }
    send(MessageType.ENTRY_ACK, first.from, null, null, entries.get(0).id, null, null, cur(), rest);
    completeIncoming();
    return true;
  }

  /** Every message stages into one partition whose pipelined opening this replica retains. */
  private boolean stageableRun(List<Message> messages) throws IOException {
    Proposal origin = retained.get(messages.get(0).reference);
    if (origin == null || !pipelinedOrigin(origin)) {
      return false;
    }
    for (Message message : messages) {
      Entry entry = message.entry;
      if (!message.reference.equals(origin.id())
          || entry.partition != origin.first.partition
          || entry.id.term != origin.first.id.term
          || !frozenAllows(entry)) {
        return false;
      }
    }
    return true;
  }

  /**
   * A retained, uninstalled ordinary opening of this term on this replica's chain: its partition
   * lies above the installed log and retaining it already required the whole prefix in between.
   */
  private boolean pipelinedOrigin(Proposal origin) {
    return role == Role.FOLLOWER
        && !origin.recovery
        && origin.first.id.term == term
        && origin.first.kind != Kind.BARRIER
        && !installedIdentity(origin.id())
        && origin.first.partition > cur()
        && installedOpening != null
        && isAncestor(installedOpening, origin);
  }

  private boolean installableRun(List<Message> messages) throws IOException {
    if (installedOpening == null || partitions.get(cur()).barriers != 0) {
      return false;
    }
    for (Message message : messages) {
      Entry entry = message.entry;
      if (!message.reference.equals(installedOpening.id())
          || entry.partition != cur()
          || entry.id.term != installedOpening.first.id.term
          || !frozenAllows(entry)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Durable staging only: no ACK, no count, no installation, no change of election rank. Unlike a
   * repaired payload, a staged identity may not be named by any proposal yet. That is safe because
   * stored bytes only serve identities that a proposal or a recovery result names, and ACKs and the
   * recovery union read installed entries and retained manifests only. Repeats are skipped.
   */
  private void stage(List<Entry> entries) throws IOException {
    List<DurableRecord> records = new ArrayList<>(entries.size());
    Set<EntryId> run = new HashSet<>();
    for (Entry entry : entries) {
      if (entry.id.term == term
          && entry.partition >= cur()
          && !stored.containsKey(entry.id)
          && run.add(entry.id)) {
        records.add(new DurableRecord(RecordType.ENTRY, term, -1, entry, null, null, -1, null));
      }
    }
    persistInstallBatch(records);
  }

  /**
   * One force and one completeIncoming for a run of requested repair payloads on a follower; a
   * leader candidate keeps the per-message path, which also drives its recovery collection.
   */
  public synchronized void receiveTransfers(List<Message> messages) throws IOException {
    checkHealthy();
    if (role == Role.FOLLOWER && messages.size() > 1) {
      List<DurableRecord> records = new ArrayList<>(messages.size());
      Set<EntryId> run = new HashSet<>();
      for (Message message : messages) {
        if (message.type == MessageType.TRANSFER_ENTRY
            && message.to == self
            && message.term == term
            && message.entry != null
            && message.entry.hasPayload()
            && message.entry.partition > compactedThrough
            && requested(message.from, message.entry.id)
            && !stored.containsKey(message.entry.id)
            && run.add(message.entry.id)) {
          records.add(
              new DurableRecord(RecordType.ENTRY, term, -1, message.entry, null, null, -1, null));
        }
      }
      persistInstallBatch(records);
      electionProgress += records.size();
      deferIncoming = true;
    }
    try {
      for (Message message : messages) {
        receive(message);
      }
    } finally {
      deferIncoming = false;
    }
    completeIncoming();
    flushDeferredFetches();
  }

  public synchronized void tick(long nowMillis) throws IOException {
    checkHealthy();
    flushDeferredFetches();
    if (role == Role.LEADER
        && current == null
        && maxPartitionMillis > 0
        && tipNonNoop() != 0
        && nowMillis - openedAt >= maxPartitionMillis) {
      propose(Kind.NOOP, 0, new byte[0], false, tipEntries(), tip(), nowMillis);
      if (seriesOrder) {
        resetSeries(tip().first.partition);
      }
    }
    maybeCompact();
    resend();
  }

  /** Retry active work and current commit information; never advertise all historical payloads. */
  public synchronized void resend() throws IOException {
    checkHealthy();
    if (resendDelayNanos == 0) {
      for (Map<EntryId, Message> flight : inFlight.values()) {
        outbox.addAll(flight.values());
      }
    } else {
      long now = System.nanoTime();
      for (Map.Entry<Integer, LinkedHashMap<EntryId, Message>> peerFlight : inFlight.entrySet()) {
        Map<EntryId, Message> flight = peerFlight.getValue();
        Map<EntryId, Long> sent =
            lastSent.computeIfAbsent(peerFlight.getKey(), ignored -> new HashMap<>());
        if (sent.size() > 2 * flight.size()) {
          // Released identities are dropped here, amortized, instead of on every release path.
          sent.keySet().retainAll(flight.keySet());
        }
        for (Map.Entry<EntryId, Message> payload : flight.entrySet()) {
          Long queued = sent.get(payload.getKey());
          if (queued == null || now - queued >= resendDelayNanos) {
            outbox.add(payload.getValue());
            sent.put(payload.getKey(), now);
          }
        }
      }
    }
    for (Map.Entry<Integer, Set<EntryId>> requests : requestedEntries.entrySet()) {
      for (EntryId id : requests.getValue()) {
        send(MessageType.FETCH_ENTRY, requests.getKey(), null, null, id, null, null, -1, null);
      }
    }
    for (Map.Entry<EntryId, Integer> request : requestedManifests.entrySet()) {
      send(
          MessageType.FETCH_PROPOSAL,
          request.getValue(),
          null,
          null,
          request.getKey(),
          null,
          null,
          -1,
          null);
    }
    if (role == Role.CANDIDATE) {
      for (int peer : peers()) {
        send(MessageType.VOTE_REQUEST, peer, null, null, null, rank(), null, -1, null);
      }
    } else if (role == Role.LEADER_CANDIDATE && current == null) {
      if (recoveryPlanned) {
        finishRecoveryCollection();
      } else {
        for (int peer : peers()) {
          requestReportPage(peer);
        }
      }
    } else if (role == Role.LEADER || role == Role.LEADER_CANDIDATE) {
      if (current != null) {
        for (int peer : peers()) {
          sendProposal(peer, current);
        }
      }
      if (role == Role.LEADER) {
        for (Proposal pending : chain.values()) {
          for (int peer : peers()) {
            // A carried frame in flight is resent above; a repair TRANSFER in the same slot is
            // not the proposal, so the metadata still goes out.
            if (!chainAckedBy(peer, pending) && !isProposalPayloadInFlight(peer, pending.id())) {
              sendProposal(peer, pending);
            }
          }
        }
        Proposal open = opening();
        if (open != null) {
          for (int peer : peers()) {
            sendConfirm(peer, open);
            retryRejectedData(peer, open);
          }
        }
        advertiseCommit();
      }
    }
    completeIncoming();
  }

  public synchronized void receive(Message message) throws IOException {
    checkHealthy();
    if (message.to != self || !members.contains(message.from) || message.from == self) {
      return;
    }
    if (message.term < term) {
      send(MessageType.NACK, message.from, null, null, null, null, null, cur(), null);
      return;
    }
    if (message.term > term) {
      adoptTerm(message.term);
    }
    if (role == Role.CANDIDATE
        && (message.type == MessageType.REPORT_REQUEST
            || message.type == MessageType.ENTRY
            || message.type == MessageType.PROPOSAL
            || message.type == MessageType.CONFIRM
            || message.type == MessageType.COMMIT)) {
      // Only this term's unique winner sends these: the election is lost, as in Raft.
      role = Role.FOLLOWER;
      votes.clear();
    }
    switch (message.type) {
      case VOTE_REQUEST:
        if ((vote == -1 || vote == message.from)
            && message.rank != null
            && message.rank.compareTo(rank()) >= 0) {
          if (vote != message.from) {
            durable(
                new DurableRecord(
                    RecordType.TERM_VOTE, term, message.from, null, null, null, -1, null));
            electionProgress++;
          }
          send(MessageType.VOTE, message.from, null, null, null, null, null, -1, null);
        }
        break;
      case VOTE:
        if (role == Role.CANDIDATE) {
          if (votes.add(message.from)) {
            electionProgress++;
          }
          maybeWinElection();
        }
        break;
      case REPORT_REQUEST:
        leaderId = message.from;
        sendReportPage(message.from, message.report);
        break;
      case REPORT:
        if (role == Role.LEADER_CANDIDATE
            && current == null
            && !recoveryPlanned
            && message.report != null) {
          receiveReportPage(message.from, message.report);
        }
        break;
      case ENTRY:
        receiveEntry(message);
        break;
      case ENTRY_ACK:
        acknowledgeEntry(message.from, message.reference, message.frontier);
        for (EntryId id : message.identities) {
          acknowledgeEntry(message.from, id, message.frontier);
        }
        if (role == Role.LEADER) {
          advanceCommitAndApply();
        }
        break;
      case PROPOSAL:
        leaderId = message.from;
        receiveProposal(message.proposal, message.from, false, message.entry);
        break;
      case CONFIRM:
        leaderId = message.from;
        if (message.proposal == null && message.reference != null) {
          // Named by reference: this replica ACKed it, so it retains it, unless compacted.
          Proposal named = retained.get(message.reference);
          if (named != null) {
            // The leader counted this replica's ACK already; installing needs no repeat.
            confirmedByReference = named.id();
            try {
              receiveProposal(named, message.from, true);
            } finally {
              confirmedByReference = null;
            }
          } else if (incoming.containsKey(message.reference)) {
            // Waiting here for members or its base: the confirmation is what it may wait for.
            receiveProposal(incoming.get(message.reference), message.from, true);
          } else if (!compacted(message.reference)) {
            // Unknown here: remember the confirmation and fetch the manifest; receiving it then
            // completes as a confirmed proposal would.
            confirmations.add(message.reference);
            if (!requestedManifests.containsKey(message.reference)) {
              requestedManifests.put(message.reference, message.from);
              send(
                  MessageType.FETCH_PROPOSAL,
                  message.from,
                  null,
                  null,
                  message.reference,
                  null,
                  null,
                  -1,
                  null);
            }
          }
          break;
        }
        receiveProposal(message.proposal, message.from, true);
        break;
      case PROPOSAL_ACK:
        Proposal acknowledged = retained.get(message.reference);
        if (acknowledged != null) {
          releaseCoveredTransfers(message.from, acknowledged);
          if (role == Role.LEADER && !acknowledged.recovery && message.frontier >= 0) {
            // The peer reports its installed open partition: it has closed every one before.
            // A pipelined proposal can be ACKed while its base is still only staged there.
            notePeerClosed(message.from, message.frontier - 1);
          }
        }
        if (current != null
            && (role == Role.LEADER || role == Role.LEADER_CANDIDATE)
            && current.id().equals(message.reference)) {
          receipts(firstReceipts, current.id()).add(message.from);
          if (!hasAck(proposalAcks, current.id(), message.from)) {
            electionProgress++;
          }
          addAck(proposalAcks, current.id(), message.from);
          maybeConfirm();
        } else if (role == Role.LEADER && chain.containsKey(message.reference)) {
          // An ACK certifies the exact prefix, so it counts for every earlier pending proposal.
          chainAcked.merge(message.from, acknowledged.first.partition, Math::max);
          maybeConfirm();
        }
        break;
      case FETCH_ENTRY:
        Entry fetched = stored.get(message.reference);
        if (fetched != null) {
          send(MessageType.TRANSFER_ENTRY, message.from, fetched, null, null, null, null, -1, null);
        }
        break;
      case TRANSFER_ENTRY:
        if (message.entry != null && message.entry.partition <= compactedThrough) {
          // A compacted partition is never persisted again; only its canonical identity is
          // receipted, so that a late repair releases the sender's slot.
          if (compacted(message.entry.id)) {
            send(
                MessageType.TRANSFER_ACK,
                message.from,
                null,
                null,
                message.entry.id,
                null,
                null,
                -1,
                null);
          }
        } else if (message.entry != null && requested(message.from, message.entry.id)) {
          Entry expected = recoveryMetadata.get(message.entry.id);
          if (expected != null && !expected.equals(message.entry)) {
            throw new IOException("recovery payload differs from report");
          }
          boolean newPayload = !stored.containsKey(message.entry.id);
          persistEntry(message.entry);
          if (newPayload) {
            electionProgress++;
          }
          requestedEntries.get(message.from).remove(message.entry.id);
          send(
              MessageType.TRANSFER_ACK,
              message.from,
              null,
              null,
              message.entry.id,
              null,
              null,
              -1,
              null);
          if (recoveryPlanned && current == null && role == Role.LEADER_CANDIDATE) {
            finishRecoveryCollection();
          } else {
            completeIncoming();
          }
        } else if (message.entry != null
            && stored.containsKey(message.entry.id)
            && stored.get(message.entry.id).equals(message.entry)) {
          // The durable receipt may have succeeded while its transport ACK was lost.
          send(
              MessageType.TRANSFER_ACK,
              message.from,
              null,
              null,
              message.entry.id,
              null,
              null,
              -1,
              null);
        }
        break;
      case TRANSFER_ACK:
        Message transferred =
            inFlight.containsKey(message.from)
                ? inFlight.get(message.from).get(message.reference)
                : null;
        if (transferred != null
            && transferred.type == MessageType.TRANSFER_ENTRY
            && role == Role.LEADER_CANDIDATE
            && current != null
            && current.recovery
            && (recoveryResult.contains(message.reference)
                || current.id().equals(message.reference))
            && recoveryReceipts
                .computeIfAbsent(message.from, ignored -> new HashSet<>())
                .add(message.reference)) {
          // Useful reverse repair, never quorum evidence. Remember receipts for
          // the term: a repeated FETCH must not renew progress for the same ID.
          electionProgress++;
        }
        if ((current != null && current.id().equals(message.reference))
            || chain.containsKey(message.reference)) {
          receipts(firstReceipts, message.reference).add(message.from);
        }
        // Payload receipt releases only a repair/carried-first slot, never an
        // ordinary ENTRY slot. It certifies no prefix and contributes no quorum ACK.
        if (transferred != null && isReceiptReleasedPayload(transferred)) {
          releaseEntry(message.from, message.reference);
        } else {
          // A delayed receipt can overtake a queued retransmission of this same
          // payload. Remove that payload intent without touching an ordinary ENTRY.
          Map<EntryId, Message> pending = pendingData.get(message.from);
          Message queued = pending == null ? null : pending.get(message.reference);
          if (queued != null && isReceiptReleasedPayload(queued)) {
            pending.remove(message.reference);
            pumpPending(message.from);
          }
        }
        retryOrdinaryAfterRepair(message.from, message.reference);
        break;
      case FETCH_PROPOSAL:
        Proposal fetchedProposal = retained.get(message.reference);
        if (fetchedProposal != null) {
          sendProposal(message.from, fetchedProposal);
        }
        break;
      case COMMIT:
        leaderId = message.from;
        learnCommit(message.frontier, message.identities);
        if (compactionEnabled && message.reference != null) {
          // The leader's cap names the opening after it; only a retained one is understood.
          Proposal capOpening = retained.get(message.reference);
          if (capOpening != null) {
            compactionCap = Math.max(compactionCap, capOpening.first.partition - 1);
          }
        }
        advanceCommitAndApply();
        break;
      case NACK:
        // The NACK can belong to an older ordinary ENTRY attempt while the
        // transport slot now contains its repair. Preserve ordinary intent even
        // if that repair's receipt is lost after this NACK releases the slot.
        releaseEntry(message.from, message.reference);
        Entry rejected = ordinaryEntryNeedingAck(message.from, message.reference);
        if (rejected != null) {
          rejectedData
              .computeIfAbsent(message.from, ignored -> new LinkedHashMap<>())
              .put(
                  rejected.id,
                  new Message(
                      MessageType.ENTRY,
                      self,
                      message.from,
                      term,
                      rejected,
                      null,
                      openingOf(rejected).id(),
                      null,
                      null,
                      -1,
                      null));
        } else if (message.reference == null && role == Role.LEADER && opening() != null) {
          // A same-term proposal NACK: the peer holds the proposal's members, possibly staged by
          // an earlier ENTRY NACK, but cannot ACK before they are installed. Staged bytes need no
          // FETCH, so no TRANSFER_ACK triggers retryOrdinaryAfterRepair; retry the peer's rejected
          // ordinary entries now instead of on the next heartbeat.
          retryRejectedData(message.from, opening());
        }
        break;
      default:
        throw new IOException("unsupported message " + message.type);
    }
  }

  /** One acknowledged identity of an ENTRY_ACK frame; commitment advances once per frame. */
  private void acknowledgeEntry(int peer, EntryId id, long frontier) {
    releaseEntry(peer, id);
    if (role == Role.LEADER && id != null && (installedIdentity(id) || stagedIdentity(id))) {
      // A staged entry's ACK counts once its partition is installed (indexInstalled).
      addEntryAck(id, peer);
      if (frontier >= 0) {
        // The peer reports its installed open partition: it has closed every one before.
        notePeerClosed(peer, frontier - 1);
      }
    }
  }

  private void adoptTerm(long nextTerm) throws IOException {
    seriesPartition = -1;
    if (nextTerm <= term) {
      return;
    }
    durable(new DurableRecord(RecordType.TERM_VOTE, nextTerm, -1, null, null, null, -1, null));
    role = Role.FOLLOWER;
    leaderId = -1;
    current = null;
    chain.clear();
    chainByPartition.clear();
    chainAcked.clear();
    chainTip = null;
    assignedIndex = 0;
    firstReceipts.clear();
    metadataOnlyPeers.clear();
    votes.clear();
    reports.clear();
    outgoingReports.clear();
    partialReports.clear();
    entryAcks.clear();
    peerClosed.clear();
    for (PartitionState partition : partitions.values()) {
      partition.commitReady = null;
    }
    transferCoverage.clear();
    rejectedData.clear();
    proposalAcks.clear();
    incoming.clear();
    incomingReady.clear();
    incomingSources.clear();
    incomingWaiters.clear();
    incomingDependencies.clear();
    incomingFetchWaiters.clear();
    incomingFetchSources.clear();
    confirmations.clear();
    requestedHistory.clear();
    requestedManifests.clear();
    inFlight.clear();
    lastSent.clear();
    pendingData.clear();
    requestedEntries.clear();
    deferredFetches.clear();
    recoveryReceipts.clear();
    recoveryPlanned = false;
    recoveryBase = null;
    recoveryProposals.clear();
    recoveryMetadata.clear();
    recoverySources.clear();
    recoveryResult.clear();
    recoveryOpen.clear();
    outbox.clear();
  }

  private void maybeWinElection() throws IOException {
    if (role != Role.CANDIDATE || votes.size() < quorum) {
      return;
    }
    role = Role.LEADER_CANDIDATE;
    electionProgress++;
    leaderId = self;
    reports.put(self, localReport());
    for (int peer : peers()) {
      requestReportPage(peer);
    }
    maybeBuildRecovery();
  }

  private static boolean retainsOpening(Report report, Proposal opening) {
    for (Proposal p : report.retained) {
      if (p.first.partition == opening.first.partition
          && p.first.id.term == opening.first.id.term) {
        return true;
      }
    }
    return false;
  }

  private void maybeBuildRecovery() throws IOException {
    if (reports.size() < quorum || current != null) {
      return;
    }
    Map<EntryId, Proposal> proposals = new LinkedHashMap<>();
    Map<EntryId, Entry> entries = new LinkedHashMap<>();
    for (Map.Entry<Integer, Report> sourceReport : reports.entrySet()) {
      Report report = sourceReport.getValue();
      for (Entry entry : report.stored) {
        if (entry.partition <= compactedThrough) {
          // Closed on every member and compacted here: no result can change or need it.
          continue;
        }
        putConsistent(entries, entry);
        recoverySources
            .computeIfAbsent(entry.id, ignored -> new LinkedHashSet<>())
            .add(sourceReport.getKey());
      }
      for (Proposal proposal : report.retained) {
        if (!compactedProposal(proposal) || isCompactionAnchor(proposal)) {
          putConsistentProposal(proposals, proposal);
        }
      }
    }
    Proposal latest = null;
    for (Proposal candidate : proposals.values()) {
      if (latest == null || later(candidate, latest)) {
        latest = candidate;
      }
    }
    List<Entry> recoveredOpen = new ArrayList<>();
    if (latest != null) {
      Set<EntryId> prefix = prefixIds(latest, proposals, new HashSet<EntryId>());
      for (EntryId id : prefix) {
        requireEntry(entries, id);
      }
      putConsistent(entries, latest.first);
      Map<EntryId, Entry> union = new LinkedHashMap<>();
      union.put(latest.id(), latest.first);
      for (Report report : reports.values()) {
        List<Entry> eligible = new ArrayList<>(report.installed);
        for (Proposal p : report.retained) {
          if (compactedProposal(p) && !isCompactionAnchor(p)) {
            continue;
          }
          eligible.add(p.first);
          if (isCompactionAnchor(p)) {
            // Its members form the compacted partition, which cannot be the open one.
            continue;
          }
          for (EntryId id : p.members) {
            eligible.add(requireEntry(entries, id));
          }
        }
        // Staged entries of the latest partition count too when the reporter retains its
        // opening: a follower ACKs a pipelined partition's entries before installing them, so a
        // committed one may be only staged. Bytes stored without that opening were never ACKed.
        if (retainsOpening(report, latest)) {
          for (Entry e : report.stored) {
            if (e.partition == latest.first.partition && e.id.term == latest.first.id.term) {
              eligible.add(e);
            }
          }
        }
        for (Entry e : eligible) {
          if (e.partition == latest.first.partition && e.id.term == latest.first.id.term) {
            putConsistent(union, e);
          }
        }
      }
      recoveredOpen.addAll(union.values());
      recoveredOpen.sort(Comparator.comparingLong(e -> e.id.index));
      Set<EntryId> result = new LinkedHashSet<>(prefix);
      result.addAll(union.keySet());
      checkClosedPrefix(result, entries);
      if (!result.containsAll(knownCommitted) || !coversMarked(result)) {
        throw new IOException("recovery omitted a known committed identity");
      }
      recoveryResult.addAll(result);
    } else if (!installed.isEmpty()) {
      throw new IOException("nonempty recovered history has no opening proposal");
    }
    recoveryBase = latest;
    recoveryProposals.putAll(proposals);
    recoveryMetadata.putAll(entries);
    for (Entry entry : recoveredOpen) {
      recoveryOpen.add(entry.id);
    }
    recoveryPlanned = true;
    electionProgress++;
    finishRecoveryCollection();
  }

  /** Fixed metadata membership precedes incremental payload synchronization. */
  private void finishRecoveryCollection() throws IOException {
    boolean complete = true;
    for (EntryId id : recoveryResult) {
      if (stored.containsKey(id)) {
        if (!stored.get(id).equals(recoveryMetadata.get(id))) {
          throw new IOException("local recovery identity differs from report");
        }
        continue;
      }
      complete = false;
      Set<Integer> sources = recoverySources.get(id);
      if (sources == null || sources.isEmpty()) {
        throw new IOException("recovery entry has no durable source");
      }
      for (int source : sources) {
        if (source != self) {
          requestEntry(source, id);
          break;
        }
      }
    }
    if (!complete) {
      return;
    }
    if (recoveryBase != null) {
      // Only a complete durable result can become a retained proposal.
      retainChain(recoveryBase, recoveryProposals, stored, new HashSet<EntryId>());
    }
    List<Entry> open = new ArrayList<>();
    for (EntryId id : recoveryOpen) {
      open.add(requireEntry(stored, id));
    }
    propose(Kind.NOOP, 0, new byte[0], true, open, recoveryBase, 0);
    electionProgress++;
  }

  private Proposal propose(
      Kind kind,
      long timestamp,
      byte[] payload,
      boolean recovery,
      List<Entry> predecessor,
      Proposal base,
      long nowMillis)
      throws IOException {
    long part = base == null ? 0 : base.first.partition + 1;
    // The opening entry's index exceeds its complete prefix by construction.
    long maxIndex = base == null ? 0 : base.first.id.index;
    for (Entry e : predecessor) {
      maxIndex = Math.max(maxIndex, e.id.index);
    }
    if (!recovery) {
      // An ordinary opening extends the Leader's ingestion log, staged entries included.
      maxIndex = Math.max(maxIndex, nextIndex() - 1);
    }
    // During service every installed entry is retained; recovery may drop an unchosen suffix.
    Entry first =
        new Entry(
            new EntryId(term, maxIndex + 1), part, timestamp, predecessor.size(), kind, payload);
    List<EntryId> ids = new ArrayList<>();
    for (Entry e : predecessor) {
      ids.add(e.id);
    }
    Proposal proposal = new Proposal(base == null ? null : base.id(), ids, first, recovery);
    if (!recovery) {
      persistFirstAndRetain(proposal, first);
    } else {
      persistEntry(first);
      validateProposal(proposal, retained, stored);
      retain(proposal);
    }
    proposal = retained.get(proposal.id());
    if (recovery) {
      current = proposal;
    } else {
      chain.put(proposal.id(), proposal);
      chainByPartition.put(proposal.first.partition, proposal);
      chainTip = proposal;
      assignedIndex = Math.max(assignedIndex, first.id.index);
      if (kind == Kind.NOOP) {
        noopProposals++;
      } else {
        dataProposals++;
      }
    }
    firstReceipts.remove(proposal.id());
    metadataOnlyPeers.remove(proposal.id());
    openedAt = nowMillis;
    if (recovery) {
      addAck(proposalAcks, proposal.id(), self);
    }
    for (int peer : peers()) {
      sendProposal(peer, proposal);
    }
    maybeConfirm();
    return proposal;
  }

  private void receiveEntry(Message message) throws IOException {
    Entry entry = message.entry;
    if (entry == null || message.reference == null) {
      return;
    }
    leaderId = message.from;
    Proposal origin = retained.get(message.reference);
    if (origin != null
        && pipelinedOrigin(origin)
        && entry.partition == origin.first.partition
        && entry.id.term == origin.first.id.term
        && entry.kind == Kind.DATA
        && entry.hasPayload()
        && frozenAllows(entry)) {
      // A partition whose opening this replica retains but has not installed: the entry is
      // staged durably and ACKed; the confirmation that installs its opening installs it too.
      persistEntry(entry);
      send(MessageType.ENTRY_ACK, message.from, null, null, entry.id, null, null, cur(), null);
      completeIncoming();
      return;
    }
    if (origin == null
        || !installedIdentity(origin.id())
        || entry.partition != cur()
        || entry.id.term != origin.first.id.term
        || !origin.id().equals(installedOpening.id())) {
      send(MessageType.NACK, message.from, null, null, entry.id, null, null, cur(), null);
      if (entry.hasPayload() && entry.kind == Kind.DATA && role == Role.FOLLOWER) {
        stage(Collections.singletonList(entry));
        completeIncoming();
      }
      return;
    }
    if (entry.kind != Kind.DATA || partitions.get(cur()).barriers != 0 || !frozenAllows(entry)) {
      send(MessageType.NACK, message.from, null, null, entry.id, null, null, cur(), null);
      return;
    }
    installEntry(entry);
    send(MessageType.ENTRY_ACK, message.from, null, null, entry.id, null, null, cur(), null);
    completeIncoming();
  }

  private void receiveProposal(Proposal proposal, int source, boolean confirmed)
      throws IOException {
    receiveProposal(proposal, source, confirmed, null);
  }

  private void receiveProposal(Proposal proposal, int source, boolean confirmed, Entry carried)
      throws IOException {
    if (carried != null
        && (proposal == null || !carried.hasPayload() || !proposal.first.equals(carried))) {
      throw new IOException("carried proposal first differs from its immutable metadata");
    }
    if (proposal == null) {
      return;
    }
    if (carried != null) {
      proposal = new Proposal(proposal.base, proposal.members, carried, proposal.recovery);
    }
    requestedManifests.remove(proposal.id());
    Proposal old = retained.get(proposal.id());
    if (old != null && !old.equals(proposal)) {
      throw new IOException("proposal identity changed");
    }
    if (confirmed
        && carried == null
        && old != null
        && installedOpening != null
        && installedOpening.id().equals(old.id())
        && old.first.id.term == term
        && !(old.recovery && requestedHistory.contains(old.id()))) {
      // A repeated confirmation of the installed opening (the heartbeat): nothing to install,
      // and the answer is the ACK completeIncoming would send, without rescanning the manifest.
      send(MessageType.PROPOSAL_ACK, source, null, null, old.id(), null, null, cur(), null);
      advanceCommitAndApply();
      return;
    }
    Proposal pending = incoming.get(proposal.id());
    if (pending != null && !pending.equals(proposal)) {
      throw new IOException("pending proposal changed");
    }
    if (compactedProposal(proposal)) {
      // The anchor or older, or a branch that cannot extend it: already installed or dead here,
      // so nothing is retained, acknowledged or fetched. A durable carried first is receipted.
      if (carried != null && heldOrCompacted(carried)) {
        send(MessageType.TRANSFER_ACK, source, null, null, carried.id, null, null, -1, null);
      }
      return;
    }
    if (confirmed) {
      confirmations.add(proposal.id());
    }
    if (proposal.first.hasPayload()) {
      if (carried != null
          && !stored.containsKey(proposal.id())
          && !proposal.recovery
          && proposal.first.id.term == term
          && normalPredecessorComplete(proposal)) {
        // The complete predecessor is installed and durable. Publish neither
        // local record until the ENTRY + RETAIN batch has fully persisted.
        persistFirstAndRetain(proposal, carried);
      } else {
        persistEntry(proposal.first);
      }
    }
    Proposal kept = retained.get(proposal.id());
    if (kept != null) {
      // The retained object, equal to this one and validated when retained.
      proposal = kept;
    } else if (stored.containsKey(proposal.id())) {
      proposal = residentProposal(proposal);
    }
    incoming.put(proposal.id(), proposal);
    incomingSources.put(proposal.id(), source);
    enqueueIncoming(proposal.id());
    watchedAck = proposal.id();
    watchedAcked = false;
    try {
      completeIncoming();
    } finally {
      watchedAck = null;
    }
    if (carried != null && !watchedAcked) {
      // Even when the prefix is missing, receipt frees w=1 for its repair. Repeat
      // this ACK for duplicate carried-first messages whose first is already durable.
      // A PROPOSAL_ACK sent for it now releases that slot as a prefix certificate.
      send(MessageType.TRANSFER_ACK, source, null, null, carried.id, null, null, -1, null);
    }
  }

  /** Entries of a newly retained pipelined partition staged before it arrived: ACK them now. */
  private void ackStagedOf(Proposal opening, int source) throws IOException {
    StagedPartition partition = staged.get(opening.first.partition);
    if (partition == null || partition.term != opening.first.id.term) {
      return;
    }
    List<EntryId> ids = new ArrayList<>();
    for (Entry entry : partition.entries.values()) {
      if (!entry.id.equals(opening.id()) && entry.kind == Kind.DATA && frozenAllows(entry)) {
        ids.add(entry.id);
      }
    }
    if (!ids.isEmpty()) {
      EntryId head = ids.remove(0);
      send(MessageType.ENTRY_ACK, source, null, null, head, null, null, cur(), ids);
    }
  }

  private void enqueueIncoming(EntryId id) {
    if (incoming.containsKey(id)) {
      incomingReady.add(id);
    }
  }

  private void awaitIncomingIdentity(EntryId proposal, EntryId dependency) {
    incomingDependencies.computeIfAbsent(proposal, ignored -> new HashSet<>()).add(dependency);
    incomingWaiters.computeIfAbsent(dependency, ignored -> new LinkedHashSet<>()).add(proposal);
  }

  private void wakeIncomingIdentity(EntryId identity) {
    Set<EntryId> waiting = incomingWaiters.remove(identity);
    if (waiting != null) {
      for (EntryId proposal : waiting) {
        // A wake-up rescans the whole manifest; a partial arrival cannot complete it. The
        // proposal's own identity means "installed by a descendant", which wakes it separately.
        Set<EntryId> dependencies = incomingDependencies.get(proposal);
        if (dependencies != null) {
          dependencies.remove(identity);
          if (dependencies.size() > (dependencies.contains(proposal) ? 1 : 0)) {
            continue;
          }
        }
        enqueueIncoming(proposal);
      }
    }
  }

  private void clearIncomingWaiters(EntryId id) {
    Set<EntryId> dependencies = incomingDependencies.remove(id);
    if (dependencies != null) {
      for (EntryId dependency : dependencies) {
        Set<EntryId> waiters = incomingWaiters.get(dependency);
        if (waiters != null) {
          waiters.remove(id);
          if (waiters.isEmpty()) {
            incomingWaiters.remove(dependency);
          }
        }
      }
    }
    Integer source = incomingFetchSources.remove(id);
    if (source != null) {
      incomingFetchWaiters.get(source).remove(id);
    }
  }

  private void awaitIncomingFetchCapacity(int source, EntryId proposal) {
    incomingFetchSources.put(proposal, source);
    incomingFetchWaiters.computeIfAbsent(source, ignored -> new LinkedHashSet<>()).add(proposal);
  }

  private boolean enqueueAvailableFetchWaiter() {
    // Fixed membership bounds this outer scan. Wake only work that can fill a free fetch slot.
    for (Map.Entry<Integer, LinkedHashSet<EntryId>> source : incomingFetchWaiters.entrySet()) {
      Set<EntryId> requests = requestedEntries.get(source.getKey());
      // Refill in bulk: each woken proposal rescans its manifest to find unrequested identities.
      if (requests != null && requests.size() > replicationWindow / 2) {
        continue;
      }
      Iterator<EntryId> waiting = source.getValue().iterator();
      while (waiting.hasNext()) {
        EntryId id = waiting.next();
        waiting.remove();
        incomingFetchSources.remove(id);
        if (incoming.containsKey(id)) {
          enqueueIncoming(id);
          return true;
        }
      }
    }
    return false;
  }

  private void completeIncoming() throws IOException {
    if (deferIncoming) {
      return;
    }
    while (!incomingReady.isEmpty() || enqueueAvailableFetchWaiter()) {
      Iterator<EntryId> ready = incomingReady.iterator();
      EntryId identity = ready.next();
      ready.remove();
      Proposal proposal = incoming.get(identity);
      if (proposal == null) {
        continue;
      }
      schedulingEntryVisits++;
      int source = incomingSources.get(identity);
      clearIncomingWaiters(identity);
      if (proposal.base != null && !retained.containsKey(proposal.base)) {
        if (requestedHistory.add(proposal.base)) {
          // A normal proposal already waiting on installation may now serve as history.
          enqueueIncoming(proposal.base);
        }
        awaitIncomingIdentity(identity, proposal.base);
        if (!incoming.containsKey(proposal.base)
            && !requestedManifests.containsKey(proposal.base)) {
          requestedManifests.put(proposal.base, source);
          send(MessageType.FETCH_PROPOSAL, source, null, null, proposal.base, null, null, -1, null);
        }
        continue;
      }
      // Members first, then the proposal's own first, as fetch windows expect.
      boolean complete = true;
      for (EntryId id : proposal.members) {
        complete &= awaitStored(identity, source, id);
      }
      complete &= awaitStored(identity, source, proposal.id());
      if (!complete) {
        continue;
      }
      if (retained.get(proposal.id()) != proposal) {
        // A retained proposal object was validated when it was retained.
        validateProposal(proposal, retained, stored);
      }
      boolean currentTermProposal = proposal.first.id.term == term;
      if (currentTermProposal
          && !proposal.recovery
          && !requestedHistory.contains(proposal.id())
          && !confirmations.contains(proposal.id())
          && !normalPredecessorComplete(proposal)) {
        // Staged bytes are insufficient: wake when the missing predecessor identities are
        // installed. A descendant confirmation can also install this proposal's own first.
        awaitIncomingIdentity(identity, identity);
        for (EntryId id : proposal.members) {
          if (!installedIdentity(id)) {
            awaitIncomingIdentity(identity, id);
          }
        }
        send(MessageType.NACK, source, null, null, null, null, null, cur(), null);
        continue;
      }
      proposal = residentProposal(proposal);
      retain(proposal);
      incoming.remove(identity);
      incomingSources.remove(identity);
      clearIncomingWaiters(identity);
      // A proposal fetched as history for a later one is ACKed too once it would pass the
      // predecessor check itself: a pipelined Leader may still be waiting for its quorum.
      if (currentTermProposal
          && !proposal.id().equals(confirmedByReference)
          && (!requestedHistory.contains(proposal.id())
              || (!proposal.recovery && normalPredecessorComplete(proposal)))) {
        send(MessageType.PROPOSAL_ACK, source, null, null, proposal.id(), null, null, cur(), null);
        if (proposal.id().equals(watchedAck)) {
          watchedAcked = true;
        }
        if (pipelinedOrigin(proposal)) {
          ackStagedOf(proposal, source);
        }
      }
      if (confirmations.contains(proposal.id())) {
        installProposal(proposal);
      }
    }
    advanceCommitAndApply();
  }

  /** Whether {@code id}'s bytes are stored; if not, waits for them and asks {@code source}. */
  private boolean awaitStored(EntryId identity, int source, EntryId id) {
    if (stored.containsKey(id)) {
      return true;
    }
    awaitIncomingIdentity(identity, id);
    if (fetchGraceNanos > 0 && !requested(source, id)) {
      DeferredFetch pending = deferredFetches.get(id);
      long now = System.nanoTime();
      if (pending == null) {
        deferredFetches.put(id, new DeferredFetch(source, identity, now + fetchGraceNanos));
        return false;
      }
      if (now - pending.due < 0) {
        return false;
      }
      deferredFetches.remove(id);
    }
    requestEntry(source, id);
    if (!requested(source, id)) {
      awaitIncomingFetchCapacity(source, identity);
    }
    return false;
  }

  private static final class DeferredFetch {
    final int source;
    final EntryId proposal;
    final long due;

    DeferredFetch(int source, EntryId proposal, long due) {
      this.source = source;
      this.proposal = proposal;
      this.due = due;
    }
  }

  /**
   * Followers wait this long for a missing proposal member before fetching it; zero fetches at
   * once. Set before traffic: the deferred queue assumes one grace for its order.
   */
  public synchronized void setFetchGrace(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("negative fetch grace");
    }
    fetchGraceNanos = TimeUnit.MILLISECONDS.toNanos(millis);
  }

  /** Fetches whose grace has passed with the entry still missing. */
  private void flushDeferredFetches() {
    if (deferredFetches.isEmpty()) {
      return;
    }
    long now = System.nanoTime();
    Iterator<Map.Entry<EntryId, DeferredFetch>> pending = deferredFetches.entrySet().iterator();
    while (pending.hasNext()) {
      Map.Entry<EntryId, DeferredFetch> next = pending.next();
      if (now - next.getValue().due < 0) {
        break; // inserted in due order
      }
      pending.remove();
      EntryId id = next.getKey();
      DeferredFetch fetch = next.getValue();
      if (stored.containsKey(id) || belowAnchor(id) || !incoming.containsKey(fetch.proposal)) {
        continue;
      }
      requestEntry(fetch.source, id);
      if (!requested(fetch.source, id)) {
        awaitIncomingFetchCapacity(fetch.source, fetch.proposal);
      }
    }
  }

  private boolean normalPredecessorComplete(Proposal proposal) throws IOException {
    if (installedIdentity(proposal.id())) {
      return true;
    }
    if (proposal.first.partition == cur() + 1) {
      return installedOpening != null
          && installedOpening.id().equals(proposal.base)
          && partitions.get(cur()).entries.size() == proposal.first.prevSize
          && sameIdentities(partitions.get(cur()).entries, proposal.members);
    }
    // Pipelined: the base is this term's retained, uninstalled ordinary opening on this replica's
    // chain, so its result is durable here; every member must be stored too (the carried-first
    // fast path asks before completeIncoming has fetched them), and the installed open partition
    // must lie inside the chain's first closure.
    Proposal base = proposal.base == null ? null : retained.get(proposal.base);
    if (base == null || proposal.first.partition <= cur() + 1 || !pipelinedOrigin(base)) {
      return false;
    }
    for (EntryId id : proposal.members) {
      if (!stored.containsKey(id)) {
        return false;
      }
    }
    return openPartitionClosedBy(proposal);
  }

  /**
   * The chain's first closure below {@code proposal} holds every installed open entry. The proposal
   * itself is not retained (indexed) yet, so the walk starts at its retained base.
   */
  private boolean openPartitionClosedBy(Proposal proposal) {
    Proposal base = proposal.base == null ? null : retained.get(proposal.base);
    if (base == null) {
      return false;
    }
    Proposal closure = base.first.partition == cur() + 1 ? base : ancestorAt(base, cur() + 1);
    if (closure == null
        || installedOpening == null
        || !installedOpening.id().equals(closure.base)) {
      return false;
    }
    Collection<EntryId> closureMembers = proposalIndex.get(closure.id()).members();
    PartitionEntries open = partitions.get(cur()).entries;
    for (int i = 0; i < open.size(); i++) {
      schedulingEntryVisits++;
      if (!closureMembers.contains(open.get(i).id)) {
        return false;
      }
    }
    return true;
  }

  private void maybeConfirm() throws IOException {
    if (current == null) {
      maybeConfirmChain();
      return;
    }
    if (ackCount(proposalAcks, current.id()) < quorum) {
      return;
    }
    Proposal confirmed = current;
    electionProgress++;
    installProposal(confirmed);
    // A confirmed proposal collects no further ACKs: a late one no longer names current.
    List<Integer> ackers = ackers(proposalAcks.remove(confirmed.id()));
    Collection<EntryId> certified =
        confirmed.recovery ? resultIds(confirmed, retained) : confirmed.members;
    for (EntryId id : certified) {
      for (int acker : ackers) {
        addEntryAck(id, acker);
      }
    }
    for (int acker : ackers) {
      addEntryAck(confirmed.id(), acker);
    }
    current = null;
    firstReceipts.remove(confirmed.id());
    metadataOnlyPeers.remove(confirmed.id());
    for (int peer : peers()) {
      send(MessageType.CONFIRM, peer, null, confirmed, null, null, null, -1, null);
    }
    advanceCommitAndApply();
    if (role == Role.LEADER_CANDIDATE && confirmed.recovery && committedIdentity(confirmed.id())) {
      role = Role.LEADER;
      leaderId = self;
      recomputeTimestampMax();
      seriesPartition = -1;
      reports.clear();
      recoveryPlanned = false;
      recoveryBase = null;
      recoveryMetadata.clear();
      recoveryProposals.clear();
      recoverySources.clear();
      recoveryResult.clear();
      recoveryOpen.clear();
    }
  }

  /**
   * Confirms the newest pending proposal with quorum ACKs, and with it every earlier one: its exact
   * prefix contains their results. One INSTALL marker installs the whole chain up to it plus the
   * staged entries of its partition; one CONFIRM tells the followers.
   */
  private void maybeConfirmChain() throws IOException {
    if (role != Role.LEADER || chain.isEmpty()) {
      return;
    }
    // This Leader's own copy counts: a remote ACK is processed only once the Leader's force covers
    // what it had written when the ACK arrived, which includes every proposal the ACK names.
    long through = Math.min(quorumChainAck(), chainTip.first.partition);
    Proposal newest = chainByPartition.get(through);
    if (newest == null) {
      return;
    }
    electionProgress++;
    installProposal(newest);
    List<Proposal> confirmed = new ArrayList<>();
    Iterator<Proposal> pending = chain.values().iterator();
    while (pending.hasNext()) {
      Proposal next = pending.next();
      pending.remove();
      confirmed.add(next);
      if (next.id().equals(newest.id())) {
        break;
      }
    }
    if (chain.isEmpty()) {
      chainTip = null;
    }
    for (Proposal proposal : confirmed) {
      // A confirmed proposal collects no further ACKs; its ACKs certify its result.
      chainByPartition.remove(proposal.first.partition);
      List<Integer> ackers = new ArrayList<>();
      ackers.add(self);
      for (int peer : peers()) {
        if (chainAckedBy(peer, proposal)) {
          ackers.add(peer);
        }
      }
      firstReceipts.remove(proposal.id());
      metadataOnlyPeers.remove(proposal.id());
      // ACKs are idempotent: no set is needed to visit the members and then the first once.
      for (EntryId id : proposal.members) {
        for (int acker : ackers) {
          addEntryAck(id, acker);
        }
      }
      for (int acker : ackers) {
        addEntryAck(proposal.id(), acker);
      }
    }
    for (int peer : peers()) {
      sendConfirm(peer, newest);
    }
    advanceCommitAndApply();
  }

  /**
   * An ordinary proposal is confirmed by reference: a peer that retains it (every peer whose ACK
   * certified it, and usually the others) installs it from there, and one that does not fetches it
   * (FETCH_PROPOSAL), so a long partition's manifest is not re-encoded for every confirmation and
   * heartbeat. A recovery proposal goes in full.
   */
  private void sendConfirm(int peer, Proposal proposal) {
    if (!proposal.recovery) {
      send(MessageType.CONFIRM, peer, null, null, proposal.id(), null, null, -1, null);
    } else {
      send(MessageType.CONFIRM, peer, null, proposal, null, null, null, -1, null);
    }
  }

  /** The newest chain partition that quorum - 1 peers have ACKed (this Leader holds them all). */
  private long quorumChainAck() {
    int needed = quorum - 1;
    if (needed <= 0) {
      return Long.MAX_VALUE;
    }
    long[] acked = new long[peers().size()];
    int i = 0;
    for (int peer : peers()) {
      acked[i++] = chainAcked.getOrDefault(peer, -1L);
    }
    Arrays.sort(acked);
    return acked[acked.length - needed];
  }

  private boolean chainAckedBy(int peer, Proposal pending) {
    return chainAcked.getOrDefault(peer, -1L) >= pending.first.partition;
  }

  private void advanceCommitAndApply() throws IOException {
    if (role == Role.LEADER || role == Role.LEADER_CANDIDATE) {
      Set<EntryId> newly = new LinkedHashSet<>();
      long previous = committedFrontier;
      long frontier = committedFrontier;
      while (true) {
        PartitionState partition = partitions.get(frontier + 1);
        if (partition == null || partition.entries.isEmpty()) {
          break;
        }
        if (partition.commitReady != null) {
          for (Entry entry : partition.commitReady.values()) {
            schedulingEntryVisits++;
            newly.add(entry.id);
          }
        }
        // Ready identities commit independently. Only crossing a closed partition
        // waits for all its identities; a hole never delays its ready siblings.
        if (partition.uncommitted == size(partition.commitReady) && frontier + 1 < cur()) {
          frontier++;
        } else {
          break;
        }
      }
      if (!newly.isEmpty() || frontier != committedFrontier) {
        learnCommit(frontier, newly);
        advertiseCommitDelta(newly);
      }
      // A closed partition the frontier passed is committed in full: its identities need no
      // further quorum evidence, and retries only look at the open partition.
      for (long part = previous + 1; part <= committedFrontier; part++) {
        PartitionState passed = partitions.get(part);
        if (passed != null) {
          for (int i = 0; i < passed.entries.size(); i++) {
            entryAcks.remove(passed.entries.get(i).id);
          }
        }
      }
    }
    while (true) {
      PartitionState partition = partitions.get(appliedFrontier + 1);
      if (partition == null || partition.entries.isEmpty()) {
        break;
      }
      while (partition.applyReady != null) {
        Entry entry = partition.applyReady.get(partition.applyReady.firstKey());
        schedulingEntryVisits++;
        if (entry.kind != Kind.NOOP) {
          application.apply(entry);
        }
        DurableRecord appliedRecord =
            new DurableRecord(RecordType.APPLIED, term, -1, null, null, entry.id, -1, null);
        if (durableAppliedRecords) {
          durable(appliedRecord);
        } else {
          replayVolatile(appliedRecord);
        }
        appliedEvents.add(entry);
      }
      if (partition.unapplied != 0 || appliedFrontier + 1 >= cur()) {
        break;
      }
      raiseAppliedFrontier(appliedFrontier + 1);
    }
  }

  private void learnCommit(long frontier, Collection<EntryId> ids) throws IOException {
    Set<EntryId> additions = new LinkedHashSet<>(ids);
    additions.removeIf(this::committedIdentity);
    if (compactionAnchor != null) {
      // A late notice of a compacted identity: committed identities are canonical and unique per
      // index, and every one below the anchor's index is compacted here.
      long anchorIndex = compactionAnchor.first.id.index;
      additions.removeIf(id -> id.index < anchorIndex);
    }
    if (frontier <= advertisedFrontier && additions.isEmpty()) {
      return;
    }
    long targetFrontier = Math.max(frontier, advertisedFrontier);
    Iterator<EntryId> remaining = additions.iterator();
    do {
      List<EntryId> batch = new ArrayList<>(MAX_COMMIT_RECORD_IDS);
      while (remaining.hasNext() && batch.size() < MAX_COMMIT_RECORD_IDS) {
        batch.add(remaining.next());
      }
      // Each identity already has quorum/leader evidence. With durable commit
      // metadata, publish the frontier only in the final durable batch. The
      // snapshot/replay mode forgets this knowledge on restart and reacquires
      // evidence before applying; it never infers commitment from stored data.
      DurableRecord committed =
          new DurableRecord(
              RecordType.COMMIT,
              term,
              -1,
              null,
              null,
              null,
              remaining.hasNext() ? advertisedFrontier : targetFrontier,
              batch);
      if (durableCommitRecords) {
        durable(committed);
      } else {
        replayVolatile(committed);
      }
      for (EntryId id : batch) {
        commitObserver.accept(id);
      }
    } while (remaining.hasNext());
  }

  private void advertiseCommitDelta(Collection<EntryId> ids) {
    List<EntryId> page = new ArrayList<>(MAX_COMMIT_RECORD_IDS);
    for (EntryId id : ids) {
      Entry entry = stored.get(id);
      if (entry != null && entry.partition > committedFrontier) {
        page.add(id);
        if (page.size() == MAX_COMMIT_RECORD_IDS) {
          sendCommitPage(page);
          page.clear();
        }
      }
    }
    if (!page.isEmpty() || ids.isEmpty() || committedFrontier >= 0) {
      sendCommitPage(page);
    }
  }

  private void sendCommitPage(Collection<EntryId> page) {
    EntryId cap = compactionCapOpening();
    for (int peer : peers()) {
      send(MessageType.COMMIT, peer, null, null, cap, null, null, committedFrontier, page);
    }
  }

  /**
   * The leader advertises its cap in the otherwise unused reference of COMMIT, as the installed
   * opening of the partition after it; null while compaction is off or nothing is closed.
   */
  private EntryId compactionCapOpening() {
    if (!compactionEnabled || compactionCap < 0 || installedOpening == null) {
      return null;
    }
    Proposal opening = ancestorAt(installedOpening, compactionCap + 1);
    return opening == null ? null : opening.id();
  }

  private void advertiseCommit() {
    long part = committedFrontier + 1;
    if (commitNoticePartition != part) {
      commitNoticePartition = part;
      commitNoticeCursor = 0;
      commitNoticeCeiling = 0;
    }
    PartitionState partition = partitions.get(part);
    List<EntryId> page = new ArrayList<>(MAX_COMMIT_RECORD_IDS);
    if (partition != null && partition.committed != null) {
      if (commitNoticeCeiling == 0) {
        commitNoticeCeiling = partition.committed.lastKey();
      }
      // Replay a bounded cyclic page even without new commits. A lost delta,
      // restart, or notification arriving before payload cannot strand an ID.
      for (Map.Entry<Long, EntryId> entry :
          partition
              .committed
              .subMap(commitNoticeCursor, false, commitNoticeCeiling, true)
              .entrySet()) {
        schedulingEntryVisits++;
        page.add(entry.getValue());
        commitNoticeCursor = entry.getKey();
        if (page.size() == MAX_COMMIT_RECORD_IDS) {
          break;
        }
      }
      Long next = partition.committed.higherKey(commitNoticeCursor);
      if (next == null || next > commitNoticeCeiling) {
        commitNoticeCursor = 0;
        commitNoticeCeiling = 0;
      }
    }
    sendCommitPage(page);
  }

  private void persistEntry(Entry entry) throws IOException {
    if (!entry.hasPayload()) {
      throw new IOException("cannot persist metadata as payload");
    }
    Entry old = stored.get(entry.id);
    if (old != null) {
      if (!old.equals(entry)) {
        throw new IOException("entry identity changed " + entry.id);
      }
      return;
    }
    durable(new DurableRecord(RecordType.ENTRY, term, -1, entry, null, null, -1, null));
  }

  private void installEntry(Entry entry) throws IOException {
    if (!entry.hasPayload()) {
      throw new IOException("cannot install metadata as data");
    }
    Entry durableEntry = stored.get(entry.id);
    if (durableEntry != null && !durableEntry.equals(entry)) {
      throw new IOException("entry identity changed");
    }
    Entry previous = installedAt.get(entry.id.index);
    if (previous != null) {
      if (!previous.equals(entry)) {
        throw new IOException("installed index collision");
      }
      return;
    }
    durable(new DurableRecord(RecordType.INSTALL, term, -1, entry, null, null, -1, null));
  }

  /** Validate installed-index and immutable-identity conflicts including earlier batch members. */
  private List<DurableRecord> prepareInstallBatch(List<Entry> entries) throws IOException {
    Map<EntryId, Entry> batchIdentities = new HashMap<>();
    Map<Long, Entry> batchIndexes = new HashMap<>();
    List<DurableRecord> records = new ArrayList<>();
    for (Entry entry : entries) {
      if (!entry.hasPayload()) {
        throw new IOException("cannot install metadata as data");
      }
      Entry durableEntry = stored.get(entry.id);
      Entry batchEntry = batchIdentities.get(entry.id);
      if ((durableEntry != null && !durableEntry.equals(entry))
          || (batchEntry != null && !batchEntry.equals(entry))) {
        throw new IOException("entry identity changed in INSTALL batch");
      }
      Entry previous = installedAt.get(entry.id.index);
      Entry priorBatchIndex = batchIndexes.get(entry.id.index);
      if ((previous != null && !previous.equals(entry))
          || (priorBatchIndex != null && !priorBatchIndex.equals(entry))) {
        throw new IOException("installed index collision in INSTALL batch");
      }
      if (previous == null && priorBatchIndex == null) {
        records.add(new DurableRecord(RecordType.INSTALL, term, -1, entry, null, null, -1, null));
      }
      batchIdentities.put(entry.id, entry);
      batchIndexes.put(entry.id.index, entry);
    }
    return records;
  }

  private void persistInstallBatch(List<DurableRecord> records) throws IOException {
    if (records.isEmpty()) {
      return;
    }
    try {
      store.appendBatch(records);
      for (DurableRecord record : records) {
        replay(record);
      }
    } catch (IOException | RuntimeException failure) {
      // A valid durable prefix can survive failure, but this incarnation publishes no ACK.
      failed = true;
      outbox.clear();
      throw failure;
    }
  }

  private void persistFirstAndRetain(Proposal proposal, Entry first) throws IOException {
    if (!first.hasPayload() || !proposal.first.equals(first)) {
      throw new IOException("proposal first candidate must match full immutable payload");
    }
    validateProposal(proposal, retained, stored, first);
    if (stored.containsKey(first.id)) {
      retain(proposal);
      return;
    }
    // The old two-frame journal layout is retained. In particular, a crash after
    // only ENTRY is valid staged data, not a durable pending proposal.
    List<DurableRecord> records = new ArrayList<>(2);
    records.add(new DurableRecord(RecordType.ENTRY, term, -1, first, null, null, -1, null));
    records.add(
        new DurableRecord(
            RecordType.RETAIN_PROPOSAL,
            term,
            -1,
            null,
            metadataProposal(proposal),
            null,
            -1,
            null));
    try {
      store.appendBatch(records);
      for (DurableRecord record : records) {
        replay(record);
      }
    } catch (IOException | RuntimeException failure) {
      failed = true;
      outbox.clear();
      throw failure;
    }
  }

  private void retain(Proposal proposal) throws IOException {
    proposal = residentProposal(proposal);
    Proposal old = retained.get(proposal.id());
    if (old != null) {
      if (!old.equals(proposal)) {
        throw new IOException("immutable proposal changed");
      }
      return;
    }
    durable(
        new DurableRecord(
            RecordType.RETAIN_PROPOSAL,
            term,
            -1,
            null,
            metadataProposal(proposal),
            null,
            -1,
            null));
  }

  private void installProposal(Proposal proposal) throws IOException {
    if (installedOpening != null && isAncestor(proposal, installedOpening)) {
      // Exact retained ancestry, not merely a matching partition number/index.
      // Its immutable result was already installed with the descendant opening.
      return;
    }
    List<Proposal> extension = installedExtension(proposal);
    if (extension != null) {
      // Exact ancestry preserves the closed prefix. Validate the complete extension before
      // persisting its one INSTALL marker; replay publishes only the new identities, reusing
      // this validation (nothing changes between the two).
      validatedExtensionOf = proposal;
      validatedExtension = extension;
      try {
        durableDeferred(
            new DurableRecord(RecordType.INSTALL, term, -1, null, null, proposal.id(), -1, null));
      } finally {
        validatedExtensionOf = null;
        validatedExtension = null;
      }
      return;
    }
    Set<EntryId> result = resultIds(proposal, retained);
    if (proposal.first.partition < cur()) {
      if (!identities(installed.values()).containsAll(result)) {
        throw new IOException("late confirmation changes closed prefix");
      }
      return;
    }
    checkClosedPrefix(result, stored);
    if (installedIdentity(proposal.id()) && identities(installed.values()).containsAll(result)) {
      return;
    }
    durableDeferred(
        new DurableRecord(RecordType.INSTALL, term, -1, null, null, proposal.id(), -1, null));
  }

  /**
   * Return only the new segments, newest first, when the installed prefix can remain untouched.
   * Null deliberately selects the existing full reconciliation path for a branch/replaced suffix.
   */
  private List<Proposal> installedExtension(Proposal proposal) throws IOException {
    if (installedOpening == null || !isAncestor(installedOpening, proposal)) {
      return null;
    }
    List<Proposal> extension = new ArrayList<>();
    Proposal cursor = proposal;
    while (!cursor.id().equals(installedOpening.id())) {
      schedulingEntryVisits++;
      extension.add(cursor);
      cursor = retained.get(cursor.base);
      if (cursor == null) {
        throw new IOException("confirmed extension missing retained ancestor");
      }
    }
    if (extension.isEmpty()) {
      return extension;
    }
    Proposal firstClosure = extension.get(extension.size() - 1);
    Collection<EntryId> closureMembers = proposalIndex.get(firstClosure.id()).members();
    PartitionState open = partitions.get(cur());
    for (int i = 0; i < open.entries.size(); i++) {
      schedulingEntryVisits++;
      if (!closureMembers.contains(open.entries.get(i).id)) {
        return null;
      }
    }
    // Retained immutable ancestry already proves the older closed prefix. Check every proposed
    // addition before force/replay so no partial in-memory installation precedes a rejected ID.
    for (Proposal segment : extension) {
      for (EntryId id : segment.members) {
        validateExtensionEntry(requireEntry(stored, id));
      }
      validateExtensionEntry(requireEntry(stored, segment.id()));
    }
    return extension;
  }

  private void validateExtensionEntry(Entry entry) throws IOException {
    schedulingEntryVisits++;
    Entry old = installedAt.get(entry.id.index);
    if (old != null && !old.equals(entry)) {
      throw new IOException("confirmed extension changed an installed index");
    }
  }

  private void installExtensionEntry(Entry entry) {
    if (installedAt.containsKey(entry.id.index)) {
      return;
    }
    installed.put(entry);
    installedAt.put(entry.id.index, entry);
    indexInstalled(entry);
    if (entry.kind == Kind.DATA) {
      timestampMax = Math.max(timestampMax, entry.timestamp);
      hasTimestamp = true;
    }
  }

  private void retainChain(
      Proposal proposal,
      Map<EntryId, Proposal> proposals,
      Map<EntryId, Entry> entries,
      Set<EntryId> visited)
      throws IOException {
    List<Proposal> chain = new ArrayList<>();
    Proposal cursor = proposal;
    while (cursor != null) {
      if (reachesCompactionAnchor(cursor)) {
        // Retained here already; everything below it is compacted.
        break;
      }
      if (!visited.add(cursor.id())) {
        throw new IOException("cyclic proposal chain");
      }
      chain.add(cursor);
      schedulingEntryVisits++;
      if (cursor.base == null) {
        break;
      }
      cursor = proposals.get(cursor.base);
      if (cursor == null) {
        throw new IOException("missing proposal base");
      }
    }
    for (int i = chain.size() - 1; i >= 0; i--) {
      Proposal p = chain.get(i);
      for (EntryId id : p.members) {
        persistEntry(requireEntry(entries, id));
      }
      persistEntry(requireEntry(entries, p.id()));
      validateProposal(p, proposals, entries);
      retain(p);
    }
  }

  private void validateProposal(
      Proposal proposal, Map<EntryId, Proposal> proposals, Map<EntryId, Entry> entries)
      throws IOException {
    validateProposal(proposal, proposals, entries, null);
  }

  private void validateProposal(
      Proposal proposal,
      Map<EntryId, Proposal> proposals,
      Map<EntryId, Entry> entries,
      Entry firstCandidate)
      throws IOException {
    if (proposal.first.prevSize != proposal.members.size()) {
      throw new IOException("invalid predecessor count");
    }
    if (proposal.base == null) {
      if (proposal.first.partition != 0 || !proposal.members.isEmpty()) {
        throw new IOException("invalid initial proposal");
      }
    } else {
      Proposal base = proposals.get(proposal.base);
      if (base == null || proposal.first.partition != base.first.partition + 1) {
        throw new IOException("invalid proposal ancestry");
      }
      // One pass over a possibly long manifest: every member lies in the base's partition and
      // term, the base is among them, and their indices are distinct (ascending, as every replica
      // writes them, or else checked through a set).
      boolean baseIncluded = false;
      boolean ascending = true;
      long previous = Long.MIN_VALUE;
      for (EntryId id : proposal.members) {
        Entry entry = requireEntry(entries, id);
        if (entry.partition != base.first.partition
            || entry.id.term != base.first.id.term
            || id.index < base.first.id.index
            || id.index >= proposal.first.id.index) {
          throw new IOException("incompatible open partition union");
        }
        baseIncluded |= id.equals(base.id());
        ascending &= id.index > previous;
        previous = id.index;
      }
      if (!baseIncluded) {
        throw new IOException("invalid proposal ancestry");
      }
      if (!ascending) {
        Set<Long> indices = new HashSet<>();
        for (EntryId id : proposal.members) {
          if (!indices.add(id.index)) {
            throw new IOException("incompatible open partition union");
          }
        }
      }
    }
    Entry first = entries.get(proposal.id());
    if (first == null) {
      first = firstCandidate;
    }
    if (first == null || !proposal.first.equals(first)) {
      throw new IOException("proposal successor differs from durable payload/candidate");
    }
  }

  private Set<EntryId> prefixIds(
      Proposal proposal, Map<EntryId, Proposal> proposals, Set<EntryId> visited)
      throws IOException {
    List<Proposal> chain = new ArrayList<>();
    Proposal cursor = proposal;
    while (cursor != null) {
      if (reachesCompactionAnchor(cursor)) {
        // The anchor's first is a member of its child; its own members are compacted.
        break;
      }
      if (!visited.add(cursor.id())) {
        throw new IOException("proposal ancestry cycle");
      }
      chain.add(cursor);
      schedulingEntryVisits++;
      if (cursor.base == null) {
        break;
      }
      cursor = proposals.get(cursor.base);
      if (cursor == null) {
        throw new IOException("missing proposal ancestry");
      }
    }
    Set<EntryId> result = new LinkedHashSet<>();
    for (int i = chain.size() - 1; i >= 0; i--) {
      schedulingEntryVisits += chain.get(i).members.size();
      result.addAll(chain.get(i).members);
    }
    return result;
  }

  private Set<EntryId> resultIds(Proposal proposal, Map<EntryId, Proposal> proposals)
      throws IOException {
    Set<EntryId> result = prefixIds(proposal, proposals, new HashSet<EntryId>());
    result.add(proposal.id());
    return result;
  }

  private void checkClosedPrefix(Set<EntryId> result, Map<EntryId, Entry> entries)
      throws IOException {
    for (Entry entry : installed.values()) {
      if (entry.partition < cur() && !result.contains(entry.id)) {
        throw new IOException("recovery omitted closed entry " + entry.id);
      }
    }
    for (EntryId id : result) {
      Entry incomingEntry = requireEntry(entries, id);
      if (incomingEntry.partition < cur()) {
        Entry local = installedAt.get(id.index);
        if (local == null || !local.equals(incomingEntry)) {
          throw new IOException("recovery changes closed partition");
        }
      }
    }
  }

  private boolean frozenAllows(Entry entry) throws IOException {
    return latestRetained == null
        || entry.partition >= latestRetained.first.partition
        || covers(latestRetained, entry.id);
  }

  private void durable(DurableRecord record) throws IOException {
    try {
      store.append(record);
      replay(record);
    } catch (IOException | RuntimeException failure) {
      failed = true;
      outbox.clear();
      throw failure;
    }
  }

  /**
   * An installation marker of a majority-confirmed proposal. Every ACK-bearing fact it depends on
   * (the retained proposal, its payloads, votes) was already forced, so the marker itself may be
   * forced together with the next forced record. A crash before that force only forgets local
   * confirmation knowledge; recovery re-inherits the retained proposal from a report quorum.
   */
  private void durableDeferred(DurableRecord record) throws IOException {
    try {
      store.appendDeferred(record);
      replay(record);
    } catch (IOException | RuntimeException failure) {
      failed = true;
      outbox.clear();
      throw failure;
    }
  }

  private void replayVolatile(DurableRecord record) throws IOException {
    try {
      replay(record);
    } catch (IOException | RuntimeException failure) {
      failed = true;
      outbox.clear();
      throw failure;
    }
  }

  private void replay(DurableRecord record) throws IOException {
    // Records of compacted partitions after the COMPACT that covers them are ignored, as their
    // runtime counterparts are: nothing below the anchor is rebuilt.
    switch (record.type) {
      case TERM_VOTE:
        term = record.term;
        vote = record.vote;
        break;
      case ENTRY:
        if (record.entry.partition <= compactedThrough) {
          break;
        }
        Entry durableEntry = store.residentEntry(record.entry);
        putConsistent(stored, durableEntry);
        noteStaged(stored.get(durableEntry.id));
        releaseFetchSlots(record.entry.id);
        wakeIncomingIdentity(record.entry.id);
        break;
      case RETAIN_PROPOSAL:
        if (compactedProposal(record.proposal)) {
          break;
        }
        Proposal resident = residentProposal(record.proposal);
        putConsistentProposal(retained, resident);
        indexProposal(resident);
        wakeIncomingIdentity(resident.id());
        if (latestRetained == null || later(resident, latestRetained)) {
          latestRetained = resident;
        }
        break;
      case INSTALL:
        if (record.entry != null) {
          if (record.entry.partition <= compactedThrough) {
            break;
          }
          Entry residentEntry = store.residentEntry(record.entry);
          putConsistent(stored, residentEntry);
          releaseFetchSlots(residentEntry.id);
          Entry old = installed.put(residentEntry);
          installedAt.put(residentEntry.id.index, residentEntry);
          if (old != null && !old.equals(residentEntry)) {
            throw new IOException("installed index reused");
          }
          indexInstalled(residentEntry);
          if (residentEntry.kind == Kind.DATA) {
            timestampMax = Math.max(timestampMax, residentEntry.timestamp);
            hasTimestamp = true;
          }
        } else {
          Proposal proposal = retained.get(record.reference);
          if (proposal == null && belowAnchor(record.reference)) {
            break;
          }
          if (proposal == null) {
            throw new IOException("installed unknown proposal");
          }
          if (compactedProposal(proposal)) {
            // The anchor itself: installed before it became the anchor.
            break;
          }
          List<Proposal> extension =
              validatedExtensionOf != null && validatedExtensionOf.id().equals(proposal.id())
                  ? validatedExtension
                  : installedExtension(proposal);
          if (extension != null) {
            for (int i = extension.size() - 1; i >= 0; i--) {
              Proposal segment = extension.get(i);
              for (EntryId id : segment.members) {
                installExtensionEntry(requireEntry(stored, id));
              }
              installExtensionEntry(requireEntry(stored, segment.id()));
            }
            // Entries staged in the partition this opening opens become installed with it.
            installStagedOf(proposal);
            installedOpening = proposal;
            markClosedFrontier();
            break;
          }
          TreeMap<Long, Entry> next = new TreeMap<>();
          for (EntryId id : resultIds(proposal, retained)) {
            Entry e = requireEntry(stored, id);
            next.put(id.index, e);
          }
          for (Entry e : installed.values()) {
            if (e.partition == proposal.first.partition && e.id.term == proposal.first.id.term) {
              Entry previous = next.put(e.id.index, e);
              if (previous != null && !previous.equals(e)) {
                throw new IOException("confirmation changed existing entry");
              }
            }
          }
          installed.clear();
          for (Entry e : next.values()) {
            installed.put(e);
          }
          installedAt.clear();
          installedAt.putAll(next);
          partitions.clear();
          for (Entry e : next.values()) {
            indexInstalled(e);
          }
          installStagedOf(proposal);
          installedOpening = proposal;
          recomputeTimestampMax();
          markClosedFrontier();
        }
        break;
      case COMMIT:
        advertisedFrontier = Math.max(advertisedFrontier, record.frontier);
        raiseCommittedFrontier(record.frontier);
        for (EntryId id : record.identities) {
          if (!belowAnchor(id)) {
            markCommitted(id);
          }
        }
        markClosedFrontier();
        break;
      case APPLIED:
        if (belowAnchor(record.reference)) {
          break;
        }
        if (!appliedIdentity(record.reference)
            && applied.add(record.reference)
            && installedIdentity(record.reference)) {
          Entry entry = installedAt.get(record.reference.index);
          PartitionState partition = partitions.get(entry.partition);
          partition.unapplied--;
          partition.applyReady = removed(partition.applyReady, entry.id.index);
        }
        break;
      case COMPACT:
        replayCompaction(record.frontier, record.reference);
        staged.headMap(record.frontier, true).clear();
        break;
      default:
        throw new IOException("unknown journal record");
    }
  }

  /**
   * The frontier certifies a local partition only once it is closed here. A majority-confirmed
   * opening closed it with exactly the leader's members, while an open partition can still hold a
   * suffix that a recovery dropped: its local membership proves nothing. Runs again after each
   * installation, so a frontier that overtook the closing proposal takes effect when that arrives.
   */
  private void markClosedFrontier() {
    long limit = Math.min(advertisedFrontier, cur() - 1);
    long marked = markedFrontier;
    for (long part = markedFrontier + 1; part <= limit; part++) {
      PartitionState partition = partitions.get(part);
      if (partition != null) {
        for (int i = 0; i < partition.entries.size(); i++) {
          schedulingEntryVisits++;
          markCommitted(partition.entries.get(i).id);
        }
      }
    }
    markedFrontier = Math.max(markedFrontier, limit);
    // committedIdentity answers for the newly marked partitions now.
    for (long part = marked + 1; part <= markedFrontier; part++) {
      PartitionState partition = partitions.get(part);
      if (partition != null) {
        for (int i = 0; i < partition.entries.size(); i++) {
          knownCommitted.remove(partition.entries.get(i).id);
        }
      }
    }
  }

  private void notePeerClosed(int peer, long partition) {
    if (!compactionEnabled || partition < 0) {
      return;
    }
    Long known = peerClosed.get(peer);
    if (known == null || known < partition) {
      peerClosed.put(peer, partition);
    }
    refreshCompactionCap();
  }

  /** Leader: the cap is the least closed partition over all members; an unheard peer is -1. */
  private void refreshCompactionCap() {
    if (role != Role.LEADER) {
      return;
    }
    long cap = cur() - 1;
    for (int peer : peers()) {
      cap = Math.min(cap, peerClosed.getOrDefault(peer, -1L));
    }
    compactionCap = Math.max(compactionCap, cap);
  }

  /**
   * Compact as far as every member has closed and this replica has closed, committed and applied.
   * Runs between top-level events only, as FOLLOWER or LEADER, never while a recovery is pending. A
   * lagging or unreachable peer freezes the cap, and with it compaction.
   */
  private void maybeCompact() throws IOException {
    if (!compactionEnabled
        || (role != Role.FOLLOWER && role != Role.LEADER)
        || installedOpening == null) {
      return;
    }
    refreshCompactionCap();
    long cap = ignoreCompactionCap ? Long.MAX_VALUE : compactionCap;
    long target = Math.min(Math.min(cap, cur() - 1), Math.min(markedFrontier, appliedFrontier));
    // A compaction scans the whole uncompacted state (retained proposals, identity sets), not only
    // what it drops. While many partitions stay uncompacted (the cap lags and every partition holds
    // one entry), run it only once it drops at least four times as many as it keeps, so the scans
    // of kept state add a quarter to the removal work per dropped partition; the state then stays
    // within five times the lag.
    long kept = (chainTip != null ? chainTip.first.partition : cur()) - target;
    if (target > compactedThrough
        && (kept <= COMPACTION_SMALL_WINDOW
            || target - compactedThrough >= COMPACTION_DROP_RATIO * kept)) {
      compact(target);
    }
  }

  /** Tests only: a negative control that compacts as if every member had closed everything. */
  synchronized void ignoreCompactionCapForTesting() {
    ignoreCompactionCap = true;
  }

  /**
   * Tests only: compact through {@code frontier} without consulting the cluster cap. The caller
   * guarantees that every member has closed these partitions; the local conditions still hold.
   */
  synchronized void compactThrough(long frontier) throws IOException {
    checkHealthy();
    if ((role != Role.FOLLOWER && role != Role.LEADER)
        || installedOpening == null
        || frontier <= compactedThrough
        || frontier > Math.min(cur() - 1, Math.min(markedFrontier, appliedFrontier))) {
      throw new IllegalStateException("cannot compact through partition " + frontier);
    }
    compact(frontier);
  }

  private void compact(long frontier) throws IOException {
    Proposal anchor = ancestorAt(installedOpening, frontier + 1);
    if (anchor == null) {
      throw new IOException("no installed opening proposal at partition " + (frontier + 1));
    }
    // Deferred like an INSTALL marker: the anchor's own marker precedes it in the journal, so a
    // surviving COMPACT implies a surviving installation, and losing it means full replay.
    durableDeferred(
        new DurableRecord(RecordType.COMPACT, term, -1, null, null, anchor.id(), frontier, null));
  }

  /**
   * A COMPACT record is commit evidence for the partitions it covers: they were closed, committed
   * and applied here before it was written. Replay validates the anchor (fail closed), raises the
   * frontiers, applies what the journal does not show as applied and drops the per-identity state,
   * at runtime and at startup alike. At runtime nothing is left to apply.
   */
  private void replayCompaction(long frontier, EntryId reference) throws IOException {
    if (frontier <= compactedThrough) {
      return;
    }
    Proposal anchor = reference == null ? null : retained.get(reference);
    if (anchor == null
        || anchor.first.partition != frontier + 1
        || !installedIdentity(anchor.id())
        || installedOpening == null
        || !(installedOpening.id().equals(anchor.id()) || isAncestor(anchor, installedOpening))) {
      throw new IOException(
          "compaction anchor " + reference + " is not the installed opening of " + (frontier + 1));
    }
    advertisedFrontier = Math.max(advertisedFrontier, frontier);
    raiseCommittedFrontier(frontier);
    markClosedFrontier();
    // After a restart without APPLIED records (the snapshot/replay contract) the application
    // rebuilds these partitions here, in partition order and before they are dropped, since no
    // leader notice can certify them again; it deduplicates its snapshot effects by identity.
    // Durable APPLIED records leave nothing to do. Re-application is not reported by
    // drainApplied: only new effects are.
    for (long part = appliedFrontier + 1; part <= frontier; part++) {
      PartitionState partition = partitions.get(part);
      if (partition == null) {
        continue;
      }
      while (partition.applyReady != null) {
        Entry entry = partition.applyReady.get(partition.applyReady.firstKey());
        if (entry.kind != Kind.NOOP) {
          application.apply(entry);
        }
        replay(new DurableRecord(RecordType.APPLIED, term, -1, null, null, entry.id, -1, null));
      }
      if (partition.unapplied != 0) {
        throw new IOException("compaction record covers an uncommitted entry in partition " + part);
      }
    }
    raiseAppliedFrontier(frontier);
    dropCompacted(frontier, anchor);
  }

  /**
   * Drop every structure's state of partitions <= frontier. Installed entries below the anchor's
   * index are exactly those partitions; committed and applied identities are canonical, so any left
   * below that index are stale too. Proposals other than the anchor and its descendants are history
   * or dead branches. Nothing here may need a member's compacted data again: the cap guarantees
   * that every member durably holds it.
   */
  private void dropCompacted(long frontier, Proposal anchor) {
    long anchorIndex = anchor.first.id.index;
    while (!installed.isEmpty() && installed.first().id.index < anchorIndex) {
      Entry entry = installed.removeFirst();
      recordCompacted(entry);
      stored.remove(entry.id);
      installedAt.remove(entry.id.index);
    }
    partitions.removeKeysIf(part -> part <= frontier);
    // One sequential pass each also drops these entries' ids, without a random lookup per entry.
    knownCommitted.removeIf(id -> id.index < anchorIndex);
    applied.removeIf(id -> id.index < anchorIndex);
    entryAcks.keySet().removeIf(id -> id.index < anchorIndex);
    deferredFetches.keySet().removeIf(id -> id.index < anchorIndex);
    if (stored.size() > installed.size()) {
      // Staged or repaired bytes that no installed history names; usually none.
      stored.values().removeIf(entry -> entry.partition <= frontier);
    }
    // Retained proposals that are neither the anchor nor its descendants, in one pass: in partition
    // order, a proposal descends from the anchor exactly when its base (one partition below) is
    // the anchor or a descendant; a missing base ends the line, as a missing index pointer did.
    List<EntryId> obsolete = new ArrayList<>();
    List<Proposal> above = new ArrayList<>();
    for (Proposal proposal : retained.values()) {
      if (proposal.id().equals(anchor.id())) {
        continue;
      }
      if (proposal.first.partition <= anchor.first.partition) {
        obsolete.add(proposal.id());
      } else {
        above.add(proposal);
      }
    }
    above.sort(Comparator.comparingLong(proposal -> proposal.first.partition));
    Set<EntryId> descendants = new HashSet<>();
    descendants.add(anchor.id());
    for (Proposal proposal : above) {
      if (proposal.base != null && descendants.contains(proposal.base)) {
        descendants.add(proposal.id());
      } else {
        obsolete.add(proposal.id());
      }
    }
    for (EntryId id : obsolete) {
      retained.remove(id);
      ProposalIndex dropped = proposalIndex.remove(id);
      if (dropped != null) {
        dropped.dropped = true;
        // Live descendants may still point here; nothing further back stays reachable.
        dropped.parent = null;
        dropped.jump = null;
      }
    }
    compactedThrough = frontier;
    compactionAnchor = anchor;
    if (latestRetained != null && !retained.containsKey(latestRetained.id())) {
      latestRetained = null;
      for (Proposal proposal : retained.values()) {
        if (latestRetained == null || later(proposal, latestRetained)) {
          latestRetained = proposal;
        }
      }
    }
    // Per-proposal bookkeeping of the compacted era: canonical proposals above the anchor have
    // larger indices, and none at or below it can be installed or fetched again.
    confirmations.removeIf(id -> id.index <= anchorIndex);
    requestedHistory.removeIf(id -> id.index <= anchorIndex);
    requestedManifests.keySet().removeIf(id -> id.index <= anchorIndex);
    List<EntryId> stale = new ArrayList<>();
    for (Proposal proposal : incoming.values()) {
      if (compactedProposal(proposal)) {
        stale.add(proposal.id());
      }
    }
    for (EntryId id : stale) {
      incoming.remove(id);
      incomingSources.remove(id);
      incomingReady.remove(id);
      clearIncomingWaiters(id);
    }
    transferCoverage.values().removeIf(proposal -> !retained.containsKey(proposal.id()));
    for (Set<EntryId> requests : requestedEntries.values()) {
      requests.removeIf(id -> id.index < anchorIndex);
    }
    // Every member durably holds these partitions: nothing about them is resent.
    purgeTransport(inFlight, frontier);
    purgeTransport(pendingData, frontier);
    purgeTransport(rejectedData, frontier);
    for (int peer : peers()) {
      pumpPending(peer);
    }
  }

  private static void purgeTransport(
      Map<Integer, LinkedHashMap<EntryId, Message>> transport, long frontier) {
    for (LinkedHashMap<EntryId, Message> messages : transport.values()) {
      messages.values().removeIf(m -> m.entry != null && m.entry.partition <= frontier);
    }
  }

  /** Only for canonical identities (committed, applied) and proposal bookkeeping. */
  private boolean belowAnchor(EntryId id) {
    return compactionAnchor != null && id.index < compactionAnchor.first.id.index;
  }

  private boolean isCompactionAnchor(Proposal proposal) {
    return compactionAnchor != null && compactionAnchor.id().equals(proposal.id());
  }

  /**
   * Chain walks stop at the compaction anchor. Every result extends the partitions closed on all
   * members, so a chain that reaches the anchor's partition through another proposal diverges from
   * that closed prefix: fail closed.
   */
  private boolean reachesCompactionAnchor(Proposal cursor) throws IOException {
    if (compactionAnchor == null || cursor.first.partition > compactionAnchor.first.partition) {
      return false;
    }
    if (!isCompactionAnchor(cursor)) {
      throw new IOException("chain diverges below the compaction anchor at " + cursor.id());
    }
    return true;
  }

  /** The bytes of this identity are durable here, or it is a compacted canonical identity. */
  private boolean heldOrCompacted(Entry entry) {
    Entry held = stored.get(entry.id);
    return held != null ? held.equals(entry) : compacted(entry.id);
  }

  /** A proposal that can no longer extend this replica: at or below the anchor's partition. */
  private boolean compactedProposal(Proposal proposal) {
    return compactionAnchor != null
        && (proposal.first.partition <= compactionAnchor.first.partition
            || (proposal.first.partition == compactionAnchor.first.partition + 1
                && !compactionAnchor.id().equals(proposal.base)));
  }

  private void recordCompacted(Entry entry) {
    Map.Entry<Long, CompactedRun> last = compactedRuns.lastEntry();
    if (last != null
        && last.getValue().term == entry.id.term
        && last.getValue().end == entry.id.index) {
      last.getValue().end++;
    } else {
      compactedRuns.put(entry.id.index, new CompactedRun(entry.id.term, entry.id.index + 1));
    }
    compactedEntries++;
    if (entry.kind == Kind.DATA) {
      compactedTimestampMax = Math.max(compactedTimestampMax, entry.timestamp);
      compactedHasTimestamp = true;
    }
  }

  /** Exactly the dropped canonical identities: committed and applied here. */
  private boolean compacted(EntryId id) {
    Map.Entry<Long, CompactedRun> run = compactedRuns.floorEntry(id.index);
    return run != null && id.index < run.getValue().end && run.getValue().term == id.term;
  }

  /** Consecutive compacted identities of one term: indices [start, end). */
  private static final class CompactedRun {
    final long term;
    long end;

    CompactedRun(long term, long end) {
      this.term = term;
      this.end = end;
    }
  }

  private Rank rank() {
    return new Rank(
        installed.isEmpty() ? 0 : installed.last().id.term,
        cur(),
        partitions.containsKey(cur()) ? partitions.get(cur()).entries.size() : 0);
  }

  private Proposal residentProposal(Proposal proposal) {
    Entry first = stored.get(proposal.id());
    if (first == null) {
      first = store.residentEntry(proposal.first);
    }
    return new Proposal(proposal.base, proposal.members, first, proposal.recovery);
  }

  private Report localReport() {
    List<Entry> installedMetadata = new ArrayList<>();
    List<Entry> storedMetadata = new ArrayList<>();
    List<Proposal> proposalMetadata = new ArrayList<>();
    for (Entry entry : installed.values()) {
      installedMetadata.add(Entry.metadata(entry));
    }
    for (Entry entry : stored.values()) {
      storedMetadata.add(Entry.metadata(entry));
    }
    for (Proposal proposal : retained.values()) {
      proposalMetadata.add(metadataProposal(proposal));
    }
    return new Report(installedMetadata, storedMetadata, proposalMetadata);
  }

  private static final class ReportPageBuilder {
    final List<Entry> installed = new ArrayList<>();
    final List<Entry> stored = new ArrayList<>();
    final List<ProposalSlice> slices = new ArrayList<>();
    // Includes message fields, UTF generation, and the four collection counts.
    int bytes = 512;
  }

  private List<Report> snapshotReportPages() throws IOException {
    Report snapshot = localReport();
    List<ReportPageBuilder> builders = new ArrayList<>();
    ReportPageBuilder page = new ReportPageBuilder();
    builders.add(page);
    for (int category = 0; category < 2; category++) {
      for (Entry entry : category == 0 ? snapshot.installed : snapshot.stored) {
        if (page.bytes + TRaftCodec.REPORT_ENTRY_BOUND > controlPageBytes) {
          page = new ReportPageBuilder();
          builders.add(page);
        }
        (category == 0 ? page.installed : page.stored).add(entry);
        page.bytes += TRaftCodec.REPORT_ENTRY_BOUND;
      }
    }
    for (Proposal proposal : snapshot.retained) {
      // A range costs one member whatever its length (TRaftCodec), so it is never split.
      boolean range = proposal.members instanceof IdRange;
      int offset = 0;
      do {
        if (page.bytes
                + TRaftCodec.REPORT_SLICE_BOUND
                + (proposal.members.isEmpty() ? 0 : TRaftCodec.REPORT_MEMBER_BOUND)
            > controlPageBytes) {
          page = new ReportPageBuilder();
          builders.add(page);
        }
        int count =
            range
                ? proposal.members.size()
                : Math.min(
                    proposal.members.size() - offset,
                    (controlPageBytes - page.bytes - TRaftCodec.REPORT_SLICE_BOUND)
                        / TRaftCodec.REPORT_MEMBER_BOUND);
        page.slices.add(
            new ProposalSlice(
                proposal.base,
                proposal.first,
                proposal.recovery,
                offset,
                proposal.members.size(),
                proposal.members.subList(offset, offset + count)));
        page.bytes +=
            TRaftCodec.REPORT_SLICE_BOUND
                + TRaftCodec.REPORT_MEMBER_BOUND * (range ? Math.min(count, 1) : count);
        offset += count;
      } while (offset < proposal.members.size());
    }
    String generation = UUID.randomUUID().toString();
    List<Report> pages = new ArrayList<>(builders.size());
    for (int i = 0; i < builders.size(); i++) {
      ReportPageBuilder builder = builders.get(i);
      Report report =
          new Report(
              builder.installed,
              builder.stored,
              Collections.<Proposal>emptyList(),
              generation,
              i,
              builders.size(),
              builder.slices);
      Message message =
          new Message(
              MessageType.REPORT, self, self, term, null, null, null, null, report, -1, null);
      if (TRaftCodec.encodedMessageUpperBound(message) > controlPageBytes) {
        throw new IOException("report page exceeded its metadata budget");
      }
      pages.add(report);
    }
    return pages;
  }

  private void sendReportPage(int peer, Report request) throws IOException {
    List<Report> pages = outgoingReports.get(peer);
    if (pages == null) {
      pages = snapshotReportPages();
      outgoingReports.put(peer, pages);
    }
    int index = 0;
    if (request != null
        && pages.get(0).generation.equals(request.generation)
        && request.totalPages == pages.size()
        && request.pageIndex >= 0
        && request.pageIndex < pages.size()) {
      index = request.pageIndex;
    }
    send(MessageType.REPORT, peer, null, null, null, null, pages.get(index), -1, null);
  }

  private void requestReportPage(int peer) {
    if (reports.containsKey(peer)) {
      return;
    }
    ReportAssembly assembly = partialReports.get(peer);
    Report request =
        assembly == null
            ? null
            : new Report(
                Collections.<Entry>emptyList(),
                Collections.<Entry>emptyList(),
                Collections.<Proposal>emptyList(),
                assembly.generation,
                assembly.nextMissing,
                assembly.totalPages,
                Collections.<ProposalSlice>emptyList());
    send(MessageType.REPORT_REQUEST, peer, null, null, null, null, request, -1, null);
  }

  private void receiveReportPage(int peer, Report page) throws IOException {
    if (reports.containsKey(peer)
        || page.generation.isEmpty()
        || page.pageIndex < 0
        || page.totalPages < 1
        || page.pageIndex >= page.totalPages
        || !page.retained.isEmpty()) {
      return;
    }
    ReportAssembly assembly = partialReports.get(peer);
    if (assembly == null) {
      assembly = new ReportAssembly(page);
      partialReports.put(peer, assembly);
    }
    // A report is one immutable snapshot: never splice pages across reporter restarts.
    if (!assembly.generation.equals(page.generation) || assembly.totalPages != page.totalPages) {
      return;
    }
    if (assembly.pages.putIfAbsent(page.pageIndex, page) == null) {
      electionProgress++;
      while (assembly.pages.containsKey(assembly.nextMissing)) {
        assembly.nextMissing++;
      }
      if (assembly.pages.size() == assembly.totalPages) {
        reports.put(peer, assembleReport(assembly));
        partialReports.remove(peer);
        maybeBuildRecovery();
        return;
      }
    }
    requestReportPage(peer);
  }

  private static final class ReportAssembly {
    final String generation;
    final int totalPages;
    final TreeMap<Integer, Report> pages = new TreeMap<>();
    int nextMissing;

    ReportAssembly(Report first) {
      generation = first.generation;
      totalPages = first.totalPages;
    }
  }

  private static final class ManifestAssembly {
    final ProposalSlice header;
    final TreeMap<Integer, ProposalSlice> slices = new TreeMap<>();

    ManifestAssembly(ProposalSlice header) {
      this.header = header;
    }

    void add(ProposalSlice slice) throws IOException {
      if (!Objects.equals(header.base, slice.base)
          || !header.first.equals(slice.first)
          || header.recovery != slice.recovery
          || header.totalMembers != slice.totalMembers
          || slices.putIfAbsent(slice.offset, slice) != null) {
        throw new IOException("inconsistent or overlapping report manifest slice");
      }
    }

    Proposal complete() throws IOException {
      List<EntryId> ids = new ArrayList<>();
      for (ProposalSlice slice : slices.values()) {
        if (slice.offset != ids.size()
            || slice.totalMembers < 0
            || slice.members.size() > slice.totalMembers - slice.offset) {
          throw new IOException("incomplete or overlapping report manifest");
        }
        ids.addAll(slice.members);
      }
      if (ids.size() != header.totalMembers) {
        throw new IOException("incomplete report manifest");
      }
      return new Proposal(header.base, ids, header.first, header.recovery);
    }
  }

  private static Report assembleReport(ReportAssembly assembly) throws IOException {
    Map<EntryId, Entry> installed = new LinkedHashMap<>();
    Map<EntryId, Entry> stored = new LinkedHashMap<>();
    Map<EntryId, ManifestAssembly> proposals = new LinkedHashMap<>();
    for (Report page : assembly.pages.values()) {
      for (Entry entry : page.installed) {
        putConsistent(installed, entry);
      }
      for (Entry entry : page.stored) {
        putConsistent(stored, entry);
      }
      for (ProposalSlice slice : page.proposalSlices) {
        ManifestAssembly manifest = proposals.get(slice.first.id);
        if (manifest == null) {
          manifest = new ManifestAssembly(slice);
          proposals.put(slice.first.id, manifest);
        }
        manifest.add(slice);
      }
    }
    List<Proposal> complete = new ArrayList<>();
    for (ManifestAssembly manifest : proposals.values()) {
      complete.add(manifest.complete());
    }
    return new Report(installed.values(), stored.values(), complete);
  }

  /**
   * Whether the series map describes this Leader's newest partition (reset if it holds no data).
   */
  private boolean seriesMapCurrent() {
    long newest = tip().first.partition;
    if (seriesPartition == newest) {
      return true;
    }
    if (tipNonNoop() != 0) {
      return false;
    }
    resetSeries(newest);
    return true;
  }

  private void resetSeries(long partition) {
    seriesMax.clear();
    seriesPartition = partition;
    partitionHasData = false;
    partitionHasKeyless = false;
  }

  /**
   * Whether a data entry must open a partition under series ordering: it may write a point that an
   * entry of the open partition writes. A keyless entry may write any point at its timestamp.
   */
  private boolean seriesConflict(long timestamp, boolean hasSeries, long seriesKey) {
    if (!hasSeries) {
      return partitionHasData && timestamp <= partitionDataMax;
    }
    if (partitionHasKeyless && timestamp <= partitionKeylessMax) {
      return true;
    }
    Long max = seriesMax.get(seriesKey);
    return max != null && timestamp <= max;
  }

  private void noteSeries(long timestamp, boolean hasSeries, long seriesKey) {
    if (hasSeries) {
      seriesMax.merge(seriesKey, timestamp, Math::max);
    } else {
      partitionKeylessMax =
          partitionHasKeyless ? Math.max(partitionKeylessMax, timestamp) : timestamp;
      partitionHasKeyless = true;
    }
    partitionDataMax = partitionHasData ? Math.max(partitionDataMax, timestamp) : timestamp;
    partitionHasData = true;
  }

  private long cur() {
    return installed.isEmpty() ? -1 : installed.last().partition;
  }

  /** A Leader's staged entries lie above the installed log: never reuse their indices. */
  private long nextIndex() {
    long installedNext = installed.isEmpty() ? 1 : installed.last().id.index + 1;
    return role == Role.LEADER ? Math.max(installedNext, assignedIndex + 1) : installedNext;
  }

  private static Set<Integer> receipts(Map<EntryId, Set<Integer>> map, EntryId id) {
    return map.computeIfAbsent(id, ignored -> new HashSet<>());
  }

  /**
   * Entries durably held for a partition this replica has not installed yet. Only the highest
   * creation term counts: a later Leader that reuses a partition index proves the earlier term's
   * partition was not adopted.
   */
  private static final class StagedPartition {
    final long term;
    // Sized for the one-entry partitions of disordered input.
    final LinkedHashMap<EntryId, Entry> entries = new LinkedHashMap<>(4);
    int nonNoop;
    int barriers;

    StagedPartition(long term) {
      this.term = term;
    }
  }

  private void noteStaged(Entry entry) {
    if (entry.partition <= cur() || installedIdentity(entry.id)) {
      return;
    }
    StagedPartition partition = staged.get(entry.partition);
    if (partition == null || partition.term < entry.id.term) {
      partition = new StagedPartition(entry.id.term);
      staged.put(entry.partition, partition);
    } else if (partition.term > entry.id.term) {
      return;
    }
    if (partition.entries.putIfAbsent(entry.id, entry) == null) {
      if (entry.kind != Kind.NOOP) {
        partition.nonNoop++;
      }
      if (entry.kind == Kind.BARRIER) {
        partition.barriers++;
      }
    }
  }

  /**
   * Installs the staged entries of the partition {@code opening} opens that carry its creation
   * term, then drops staging at and below that partition: anything left there was not adopted.
   */
  private void installStagedOf(Proposal opening) {
    StagedPartition partition = staged.get(opening.first.partition);
    if (partition != null && partition.term == opening.first.id.term) {
      for (Entry entry : new ArrayList<>(partition.entries.values())) {
        schedulingEntryVisits++;
        installExtensionEntry(entry);
      }
    }
    staged.headMap(opening.first.partition, true).clear();
  }

  /** The opening proposal of this Leader's newest partition: pending, else the installed one. */
  private Proposal tip() {
    return chainTip != null ? chainTip : installedOpening;
  }

  /** Entries of this Leader's newest partition in index order: installed open, or staged. */
  private List<Entry> tipEntries() {
    if (chainTip == null) {
      return partition(cur());
    }
    StagedPartition partition = staged.get(chainTip.first.partition);
    if (partition == null || partition.term != chainTip.first.id.term) {
      throw new IllegalStateException("pending partition lost its staged entries");
    }
    schedulingEntryVisits += partition.entries.size();
    return new ArrayList<>(partition.entries.values());
  }

  private int tipSize() {
    if (chainTip == null) {
      return partitions.get(cur()).entries.size();
    }
    return staged.get(chainTip.first.partition).entries.size();
  }

  private int tipNonNoop() {
    return chainTip == null
        ? partitions.get(cur()).nonNoop
        : staged.get(chainTip.first.partition).nonNoop;
  }

  private int tipBarriers() {
    return chainTip == null
        ? partitions.get(cur()).barriers
        : staged.get(chainTip.first.partition).barriers;
  }

  /** Whether {@code id} is one of this Leader's own staged entries of a pending partition. */
  private boolean stagedIdentity(EntryId id) {
    Entry entry = stored.get(id);
    if (entry == null || entry.partition <= cur()) {
      return false;
    }
    StagedPartition partition = staged.get(entry.partition);
    return partition != null && partition.entries.containsKey(id);
  }

  private Proposal opening() {
    return installedOpening;
  }

  private List<Entry> partition(long part) {
    PartitionState partition = partitions.get(part);
    if (partition == null) {
      return new ArrayList<>();
    }
    schedulingEntryVisits += partition.entries.size();
    return new ArrayList<>(partition.entries.values());
  }

  /**
   * The installed log in index order, one entry per index. Installation appends as a rule; an
   * earlier index is inserted in place, and compaction drops a prefix.
   */
  private static final class InstalledLog {
    private Entry[] entries = new Entry[16];
    private int start;
    private int end;

    int size() {
      return end - start;
    }

    boolean isEmpty() {
      return end == start;
    }

    /** The lowest index's entry; the log is not empty. */
    Entry first() {
      return entries[start];
    }

    /** The highest index's entry; the log is not empty. */
    Entry last() {
      return entries[end - 1];
    }

    Entry removeFirst() {
      Entry entry = entries[start];
      entries[start++] = null;
      if (start == end) {
        start = end = 0;
      }
      return entry;
    }

    /** Installs {@code entry} at its index and returns the entry it replaced there, if any. */
    Entry put(Entry entry) {
      long index = entry.id.index;
      int at = end;
      if (end > start && entries[end - 1].id.index >= index) {
        at = search(index);
        if (at >= 0) {
          Entry old = entries[at];
          entries[at] = entry;
          return old;
        }
        at = -at - 1;
      }
      if (end == entries.length) {
        int live = end - start;
        Entry[] target = live * 2 > entries.length ? new Entry[2 * entries.length] : entries;
        System.arraycopy(entries, start, target, 0, live);
        if (target == entries) {
          java.util.Arrays.fill(entries, live, end, null);
        }
        entries = target;
        at -= start;
        start = 0;
        end = live;
      }
      System.arraycopy(entries, at, entries, at + 1, end - at);
      entries[at] = entry;
      end++;
      return null;
    }

    /** The slot of {@code index}, else -(insertion slot) - 1. */
    private int search(long index) {
      int low = start;
      int high = end - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        long found = entries[mid].id.index;
        if (found < index) {
          low = mid + 1;
        } else if (found > index) {
          high = mid - 1;
        } else {
          return mid;
        }
      }
      return -low - 1;
    }

    void clear() {
      entries = new Entry[16];
      start = end = 0;
    }

    /** A view in index order; the log does not change while a caller iterates it. */
    List<Entry> values() {
      return new AbstractList<Entry>() {
        @Override
        public Entry get(int i) {
          if (i < 0 || i >= end - start) {
            throw new IndexOutOfBoundsException("entry " + i + " of " + (end - start));
          }
          return entries[start + i];
        }

        @Override
        public int size() {
          return end - start;
        }
      };
    }

    /** Whether {@code map} holds exactly these entries by index. */
    boolean sameAs(LongMap<Entry> map) {
      if (map.size() != size()) {
        return false;
      }
      for (int i = start; i < end; i++) {
        if (!entries[i].equals(map.get(entries[i].id.index))) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * The installed entries of one partition, in index order: the installed log holds one entry per
   * index. They arrive in index order as a rule; an earlier one is inserted in place.
   */
  private static final class PartitionEntries {
    private Entry[] entries = new Entry[2];
    private int size;

    /** False when this identity is present already. */
    boolean add(Entry entry) {
      int at = size;
      if (size > 0 && entries[size - 1].id.index >= entry.id.index) {
        at = search(entry.id.index);
        if (at >= 0) {
          if (!entries[at].id.equals(entry.id)) {
            throw new IllegalStateException("partition index reused by " + entry.id);
          }
          return false;
        }
        at = -at - 1;
      }
      if (size == entries.length) {
        entries = Arrays.copyOf(entries, 2 * size);
      }
      System.arraycopy(entries, at, entries, at + 1, size - at);
      entries[at] = entry;
      size++;
      return true;
    }

    /** The position of {@code index}, else -(insertion point) - 1. */
    private int search(long index) {
      int low = 0;
      int high = size - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        long found = entries[mid].id.index;
        if (found < index) {
          low = mid + 1;
        } else if (found > index) {
          high = mid - 1;
        } else {
          return mid;
        }
      }
      return -low - 1;
    }

    boolean contains(EntryId id) {
      int at = search(id.index);
      return at >= 0 && entries[at].id.equals(id);
    }

    /** The entry at position {@code i} in index order. */
    Entry get(int i) {
      if (i < 0 || i >= size) {
        throw new IndexOutOfBoundsException("entry " + i + " of " + size);
      }
      return entries[i];
    }

    int size() {
      return size;
    }

    boolean isEmpty() {
      return size == 0;
    }

    /** A view in index order; this partition does not change while a caller iterates it. */
    List<Entry> values() {
      return new AbstractList<Entry>() {
        @Override
        public Entry get(int i) {
          return PartitionEntries.this.get(i);
        }

        @Override
        public int size() {
          return size;
        }
      };
    }

    /** As values, the identities. */
    List<EntryId> ids() {
      return new AbstractList<EntryId>() {
        @Override
        public EntryId get(int i) {
          return PartitionEntries.this.get(i).id;
        }

        @Override
        public int size() {
          return size;
        }
      };
    }
  }

  private boolean installedIdentity(EntryId id) {
    Entry entry = installedAt.get(id.index);
    return entry != null && entry.id.equals(id);
  }

  private Set<Integer> peers() {
    return peerSet;
  }

  private void sendProposal(int peer, Proposal proposal) {
    Entry carried =
        role == Role.LEADER
                && chain.containsKey(proposal.id())
                && !chainAckedBy(peer, proposal)
                && !receipts(firstReceipts, proposal.id()).contains(peer)
                && !receipts(metadataOnlyPeers, proposal.id()).contains(peer)
            ? proposal.first
            : null;
    send(MessageType.PROPOSAL, peer, carried, proposal, null, null, null, -1, null);
  }

  private void send(
      MessageType type,
      int peer,
      Entry entry,
      Proposal proposal,
      EntryId reference,
      Rank rank,
      Report report,
      long frontier,
      Collection<EntryId> ids) {
    Entry wireEntry = entry == null ? null : stored.getOrDefault(entry.id, entry);
    Message message =
        new Message(
            type,
            self,
            peer,
            term,
            wireEntry,
            proposal == null ? null : metadataProposal(proposal),
            reference,
            rank,
            report,
            frontier,
            ids);
    if (type == MessageType.PROPOSAL
        && wireEntry != null
        && TRaftCodec.encodedMessageUpperBound(message) > proposalPayloadBudget) {
      // Metadata fits the existing control bound; first is fetched independently.
      // The discarded combined message has not read any lazy payload or occupied a slot.
      wireEntry = null;
      message =
          new Message(
              type,
              self,
              peer,
              term,
              null,
              metadataProposal(proposal),
              reference,
              rank,
              report,
              frontier,
              ids);
    }
    if (type == MessageType.ENTRY
        || type == MessageType.TRANSFER_ENTRY
        || (type == MessageType.PROPOSAL && wireEntry != null)) {
      LinkedHashMap<EntryId, Message> flight =
          inFlight.computeIfAbsent(peer, ignored -> new LinkedHashMap<>());
      if (!flight.containsKey(entry.id) && flight.size() >= replicationWindow) {
        pendingData.computeIfAbsent(peer, ignored -> new LinkedHashMap<>()).put(entry.id, message);
        return;
      }
      flight.put(entry.id, message);
      noteSent(peer, entry.id);
    }
    outbox.add(message);
  }

  private static Proposal metadataProposal(Proposal proposal) {
    return new Proposal(
        proposal.base, proposal.members, Entry.metadata(proposal.first), proposal.recovery);
  }

  private boolean requested(int peer, EntryId id) {
    return requestedEntries.containsKey(peer) && requestedEntries.get(peer).contains(id);
  }

  private void requestEntry(int peer, EntryId id) {
    Set<EntryId> requests =
        requestedEntries.computeIfAbsent(peer, ignored -> new LinkedHashSet<>());
    if (requests.contains(id) || requests.size() >= replicationWindow) {
      return;
    }
    requests.add(id);
    send(MessageType.FETCH_ENTRY, peer, null, null, id, null, null, -1, null);
  }

  /**
   * Bytes stored by any route end their fetch. Otherwise the slot stays taken until a TRANSFER that
   * may never come, because the source drops one covered by this replica's own proposal ACK. A late
   * TRANSFER is still acknowledged, as a duplicate.
   */
  private void releaseFetchSlots(EntryId id) {
    for (Set<EntryId> requests : requestedEntries.values()) {
      requests.remove(id);
    }
    deferredFetches.remove(id);
  }

  private static boolean isReceiptReleasedPayload(Message message) {
    return message.type == MessageType.TRANSFER_ENTRY
        || (message.type == MessageType.PROPOSAL && message.entry != null);
  }

  private void releaseEntry(int peer, EntryId id) {
    if (id == null) {
      return;
    }
    LinkedHashMap<EntryId, Message> flight =
        inFlight.computeIfAbsent(peer, ignored -> new LinkedHashMap<>());
    flight.remove(id);
    Map<EntryId, Message> rejected = rejectedData.get(peer);
    if (rejected != null) {
      rejected.remove(id);
    }
    LinkedHashMap<EntryId, Message> pending = pendingData.get(peer);
    if (pending != null) {
      pending.remove(id);
    }
    pumpPending(peer);
  }

  private void pumpPending(int peer) {
    LinkedHashMap<EntryId, Message> flight =
        inFlight.computeIfAbsent(peer, ignored -> new LinkedHashMap<>());
    LinkedHashMap<EntryId, Message> pending = pendingData.get(peer);
    if (pending == null) {
      return;
    }
    Proposal covered = transferCoverage.get(peer);
    while (flight.size() < replicationWindow && !pending.isEmpty()) {
      Map.Entry<EntryId, Message> next = pending.entrySet().iterator().next();
      EntryId nextId = next.getKey();
      Message message = next.getValue();
      pending.remove(nextId);
      if (covered != null && covers(covered, nextId)) {
        continue;
      }
      flight.put(nextId, message);
      noteSent(peer, nextId);
      outbox.add(message);
    }
  }

  private void noteSent(int peer, EntryId id) {
    if (resendDelayNanos > 0) {
      lastSent.computeIfAbsent(peer, ignored -> new HashMap<>()).put(id, System.nanoTime());
    }
  }

  /**
   * Retransmission delay for in-flight payloads. Zero (the default) requeues every in-flight
   * payload on every resend. A positive delay requeues one only once it was last queued that long
   * ago: a receiver whose acknowledgments wait for a buffered force answers later than one tick,
   * and requeueing its whole window every tick would multiply the replication traffic.
   */
  public synchronized void setResendDelay(long millis) {
    if (millis < 0) {
      throw new IllegalArgumentException("resend delay must not be negative");
    }
    resendDelayNanos = TimeUnit.MILLISECONDS.toNanos(millis);
    lastSent.clear();
  }

  /** Resend the peer's NACKed ordinary entries of the open partition that it has not ACKed. */
  private void retryRejectedData(int peer, Proposal open) {
    LinkedHashMap<EntryId, Message> rejected = rejectedData.remove(peer);
    if (rejected != null) {
      for (Message retry : rejected.values()) {
        Proposal entryOpening = openingOf(retry.entry);
        if (entryOpening != null && !hasAck(entryAcks, retry.entry.id, peer)) {
          send(MessageType.ENTRY, peer, retry.entry, null, entryOpening.id(), null, null, -1, null);
        }
      }
    }
  }

  /** The opening of an entry's partition on this Leader: installed open, or a pending one. */
  private Proposal openingOf(Entry entry) {
    if (entry.partition == cur()) {
      return installedOpening;
    }
    return chainByPartition.get(entry.partition);
  }

  private Entry ordinaryEntryNeedingAck(int peer, EntryId id) {
    if (role != Role.LEADER || id == null || hasAck(entryAcks, id, peer)) {
      return null;
    }
    Entry entry;
    if (installedIdentity(id)) {
      entry = installedAt.get(id.index);
    } else if (stagedIdentity(id)) {
      entry = stored.get(id);
    } else {
      return null;
    }
    Proposal entryOpening = openingOf(entry);
    if (entryOpening == null || entry.kind != Kind.DATA || entryOpening.id().equals(id)) {
      return null;
    }
    return entry;
  }

  private void retryOrdinaryAfterRepair(int peer, EntryId id) {
    Entry entry = ordinaryEntryNeedingAck(peer, id);
    if (entry == null) {
      return;
    }
    Map<EntryId, Message> flight = inFlight.get(peer);
    if (flight != null && flight.containsKey(id) && flight.get(id).type == MessageType.ENTRY) {
      return;
    }
    // FETCH may have replaced the same ID's ordinary transmission with repair.
    // Repair only stores bytes; an ordinary successor still requires those bytes
    // installed in its predecessor. Restore this intent even if an older ENTRY's
    // NACK already removed the repair slot, and never count repair as quorum ACK.
    send(MessageType.ENTRY, peer, entry, null, openingOf(entry).id(), null, null, -1, null);
  }

  private void releaseCoveredTransfers(int peer, Proposal acknowledged) {
    Proposal previous = transferCoverage.get(peer);
    if (previous == null || isAncestor(previous, acknowledged)) {
      transferCoverage.put(peer, acknowledged);
    }
    Map<EntryId, Message> flight = inFlight.get(peer);
    if (flight != null) {
      // Only the bounded transport window is inspected, in send order: what a prefix certificate
      // covers is the window's head up to the first identity it does not cover (an entry of its
      // own or a later partition), so the scan stops there instead of visiting the whole window
      // on every ACK. A repair slot behind that point is released by its own receipt.
      Iterator<EntryId> ids = flight.keySet().iterator();
      while (ids.hasNext()) {
        schedulingEntryVisits++;
        if (!covers(acknowledged, ids.next())) {
          break;
        }
        ids.remove();
      }
    }
    pumpPending(peer);
  }

  private void checkHealthy() throws IOException {
    if (failed) {
      throw new IOException("engine fenced after durable-store failure");
    }
  }

  private void recomputeTimestampMax() {
    // Compacted DATA still counts: the partition recurrence compares with the whole history.
    timestampMax = compactedTimestampMax;
    hasTimestamp = compactedHasTimestamp;
    for (Entry entry : installed.values()) {
      if (entry.kind == Kind.DATA) {
        timestampMax = Math.max(timestampMax, entry.timestamp);
        hasTimestamp = true;
      }
    }
    if (role == Role.LEADER) {
      for (StagedPartition partition : staged.values()) {
        if (partition.term != term) {
          continue;
        }
        for (Entry entry : partition.entries.values()) {
          if (entry.kind == Kind.DATA) {
            timestampMax = Math.max(timestampMax, entry.timestamp);
            hasTimestamp = true;
          }
        }
      }
    }
  }

  private void advanceAppliedFrontier() {
    while (appliedFrontier + 1 < cur()) {
      PartitionState partition = partitions.get(appliedFrontier + 1);
      if (partition == null || partition.entries.isEmpty() || partition.unapplied != 0) {
        return;
      }
      raiseAppliedFrontier(appliedFrontier + 1);
    }
  }

  /** Deterministic work counter for scheduler complexity regressions; no wall-clock timing. */
  synchronized long schedulingEntryVisits() {
    return schedulingEntryVisits;
  }

  synchronized int pendingIncomingWork() {
    int fetchWaiters = 0;
    for (Set<EntryId> waiters : incomingFetchWaiters.values()) {
      fetchWaiters += waiters.size();
    }
    return incoming.size()
        + incomingReady.size()
        + incomingSources.size()
        + incomingWaiters.size()
        + incomingDependencies.size()
        + incomingFetchSources.size()
        + fetchWaiters;
  }

  /**
   * Tests only: fail if any structure still holds compacted state, the anchor left the installed
   * chain, the frontier or the anchor moved back, or the runs miscount the dropped identities.
   */
  synchronized void checkCompactionInvariants() {
    if ((compactionAnchor == null) != (compactedThrough < 0)) {
      throw new IllegalStateException("compaction frontier and anchor disagree");
    }
    long anchorIndex = compactionAnchor == null ? -1 : compactionAnchor.first.id.index;
    require(compactedThrough >= checkedFrontier, "frontier moved back");
    require(anchorIndex >= checkedAnchorIndex, "anchor moved back");
    checkedFrontier = compactedThrough;
    checkedAnchorIndex = anchorIndex;
    if (compactionAnchor == null) {
      require(compactedRuns.isEmpty() && compactedEntries == 0, "runs without a frontier");
      return;
    }
    long frontier = compactedThrough;
    Proposal anchor = compactionAnchor;
    require(
        retained.containsKey(anchor.id()) && anchor.first.partition == frontier + 1,
        "anchor is not the retained opening of the next partition");
    require(installedIdentity(anchor.id()), "anchor is not installed");
    require(
        installedOpening != null
            && (installedOpening.id().equals(anchor.id()) || isAncestor(anchor, installedOpening)),
        "installed opening does not descend from the anchor");
    require(
        frontier < cur() && frontier <= markedFrontier && frontier <= appliedFrontier,
        "frontier beyond what is closed, committed and applied here");
    for (Entry entry : installed.values()) {
      require(entry.partition > frontier && entry.id.index >= anchorIndex, "installed " + entry.id);
    }
    for (long part : partitions.keys()) {
      require(part > frontier, "partition state " + part);
    }
    for (Entry entry : stored.values()) {
      require(entry.partition > frontier, "stored " + entry.id);
    }
    for (Proposal proposal : retained.values()) {
      require(
          proposal.id().equals(anchor.id())
              || (proposal.first.partition > anchor.first.partition
                  && isAncestor(anchor, proposal)),
          "retained " + proposal.id() + " is neither the anchor nor its descendant");
    }
    require(proposalIndex.keySet().equals(retained.keySet()), "proposal index differs");
    require(installed.sameAs(installedAt), "installed point index differs");
    requireAbove(knownCommitted, anchorIndex - 1, "committed identity");
    requireAbove(applied, anchorIndex - 1, "applied identity");
    requireAbove(entryAcks.keySet(), anchorIndex - 1, "entry ACK");
    requireAbove(deferredFetches.keySet(), anchorIndex - 1, "deferred fetch");
    requireAbove(confirmations, anchorIndex, "confirmation");
    requireAbove(requestedHistory, anchorIndex, "history request");
    requireAbove(requestedManifests.keySet(), anchorIndex, "manifest request");
    for (Proposal proposal : incoming.values()) {
      require(!compactedProposal(proposal), "incoming " + proposal.id());
    }
    for (Proposal proposal : transferCoverage.values()) {
      require(retained.containsKey(proposal.id()), "transfer coverage " + proposal.id());
    }
    for (Set<EntryId> requests : requestedEntries.values()) {
      requireAbove(requests, anchorIndex - 1, "fetch");
    }
    requireTransportAbove(inFlight, frontier);
    requireTransportAbove(pendingData, frontier);
    requireTransportAbove(rejectedData, frontier);
    long previousEnd = Long.MIN_VALUE;
    long covered = 0;
    for (Map.Entry<Long, CompactedRun> run : compactedRuns.entrySet()) {
      require(
          run.getKey() >= previousEnd
              && run.getValue().end > run.getKey()
              && run.getValue().end <= anchorIndex,
          "compacted runs overlap or reach the anchor");
      covered += run.getValue().end - run.getKey();
      previousEnd = run.getValue().end;
    }
    require(covered == compactedEntries, "compacted runs miscount the dropped identities");
    require(
        !compactedHasTimestamp || (hasTimestamp && timestampMax >= compactedTimestampMax),
        "timestamp maximum lost the compacted partitions");
  }

  private static void require(boolean condition, String violation) {
    if (!condition) {
      throw new IllegalStateException("compaction invariant: " + violation);
    }
  }

  private static void requireAbove(Collection<EntryId> ids, long index, String what) {
    for (EntryId id : ids) {
      require(id.index > index, what + " " + id + " is compacted");
    }
  }

  private static void requireTransportAbove(
      Map<Integer, LinkedHashMap<EntryId, Message>> transport, long frontier) {
    for (LinkedHashMap<EntryId, Message> messages : transport.values()) {
      for (Message message : messages.values()) {
        if (message.entry != null) {
          require(
              message.entry.partition > frontier,
              "transport " + message.type + " of " + message.entry.id);
        }
      }
    }
  }

  private static final class PartitionState {
    final PartitionEntries entries = new PartitionEntries();
    // By index; null while empty. Committed identities are kept only while the commit frontier is
    // below the partition: only the commit notice of the partition above it reads them.
    TreeMap<Long, Entry> commitReady;
    TreeMap<Long, Entry> applyReady;
    TreeMap<Long, EntryId> committed;
    int nonNoop;
    int barriers;
    int uncommitted;
    int unapplied;
  }

  private static <V> TreeMap<Long, V> added(TreeMap<Long, V> map, long index, V value) {
    TreeMap<Long, V> target = map == null ? new TreeMap<>() : map;
    target.put(index, value);
    return target;
  }

  private static <V> TreeMap<Long, V> removed(TreeMap<Long, V> map, long index) {
    if (map == null) {
      return null;
    }
    map.remove(index);
    return map.isEmpty() ? null : map;
  }

  private static int size(Map<?, ?> map) {
    return map == null ? 0 : map.size();
  }

  private void raiseCommittedFrontier(long frontier) {
    for (long part = committedFrontier + 1; part <= frontier; part++) {
      PartitionState passed = partitions.get(part);
      if (passed != null) {
        passed.committed = null;
      }
    }
    committedFrontier = Math.max(committedFrontier, frontier);
  }

  private void indexInstalled(Entry entry) {
    schedulingEntryVisits++;
    wakeIncomingIdentity(entry.id);
    enqueueIncoming(entry.id);
    PartitionState partition = partitions.get(entry.partition);
    if (partition == null) {
      partition = new PartitionState();
      partitions.put(entry.partition, partition);
    }
    if (!partition.entries.add(entry)) {
      return;
    }
    if (entry.kind != Kind.NOOP) {
      partition.nonNoop++;
    }
    if (entry.kind == Kind.BARRIER) {
      partition.barriers++;
    }
    // Identity evidence can precede installation. Frontier evidence is markClosedFrontier's.
    if (committedIdentity(entry.id)) {
      if (entry.partition > committedFrontier) {
        partition.committed = added(partition.committed, entry.id.index, entry.id);
      }
      if (!appliedIdentity(entry.id)) {
        partition.applyReady = added(partition.applyReady, entry.id.index, entry);
      }
    } else {
      partition.uncommitted++;
      if (ackCount(entryAcks, entry.id) >= quorum) {
        partition.commitReady = added(partition.commitReady, entry.id.index, entry);
      }
    }
    if (!appliedIdentity(entry.id)) {
      partition.unapplied++;
    }
  }

  private void markCommitted(EntryId id) {
    Entry entry = installedAt.get(id.index);
    boolean installedHere = entry != null && entry.id.equals(id);
    if (installedHere) {
      if (entry.partition <= markedFrontier) {
        // Marked already (committedIdentity).
        return;
      }
      // Keep the entry's own identity object: one fewer object per committed entry.
      id = entry.id;
    }
    if (!knownCommitted.add(id) || !installedHere) {
      return;
    }
    PartitionState partition = partitions.get(entry.partition);
    partition.uncommitted--;
    partition.commitReady = removed(partition.commitReady, id.index);
    if (entry.partition > committedFrontier) {
      partition.committed = added(partition.committed, id.index, id);
    }
    if (!appliedIdentity(id)) {
      partition.applyReady = added(partition.applyReady, id.index, entry);
    }
  }

  private void addEntryAck(EntryId id, int peer) {
    Entry installedEntry = installedAt.get(id.index);
    if (installedEntry != null
        && installedEntry.id.equals(id)
        && installedEntry.partition <= committedFrontier
        && committedIdentity(id)) {
      // Committed inside a partition the frontier passed: never recreate dropped evidence.
      return;
    }
    addAck(entryAcks, id, peer);
    if (!committedIdentity(id) && installedIdentity(id) && ackCount(entryAcks, id) >= quorum) {
      Entry entry = installedAt.get(id.index);
      PartitionState partition = partitions.get(entry.partition);
      partition.commitReady = added(partition.commitReady, id.index, entry);
    }
  }

  private static final class ProposalIndex {
    final Proposal proposal;
    final long partition;
    private Collection<EntryId> members;
    // The base's node and one jump to an older ancestor, chosen by the skew-binary rule (Myers, "An
    // applicative random-access stack", 1983): a node is built from its parent and its parent's
    // jump alone, and a walk takes O(log distance) steps. A table of 2^k-th parents cost one cold
    // read per level for every new proposal. Direct references, so building and walking them costs
    // no hash lookups; a compacted node is marked dropped, keeps no pointers, and ends every walk
    // that reaches it, as a missing entry of the index did.
    ProposalIndex parent;
    ProposalIndex jump;
    long jumpPartition;
    boolean dropped;

    ProposalIndex(Proposal proposal) {
      this.proposal = proposal;
      this.partition = proposal.first.partition;
    }

    /**
     * The manifest with constant-time contains: a range as it is, a list as a set built on first
     * use (most proposals are never asked).
     */
    Collection<EntryId> members() {
      if (members == null) {
        members =
            proposal.members instanceof IdRange
                ? proposal.members
                : new HashSet<>(proposal.members);
      }
      return members;
    }
  }

  private void indexProposal(Proposal proposal) throws IOException {
    if (proposalIndex.containsKey(proposal.id())) {
      return;
    }
    ProposalIndex index = new ProposalIndex(proposal);
    if (proposal.base != null) {
      ProposalIndex parent = proposalIndex.get(proposal.base);
      if (parent == null) {
        throw new IOException("retained proposal missing indexed ancestry");
      }
      index.parent = parent;
      // Jump twice as far as the parent's jump when the parent's jump and its own span equally,
      // else to the parent. A dropped jump keeps no pointers, so jumps restart above the anchor.
      ProposalIndex skip = parent.jump;
      if (skip != null
          && skip.jump != null
          && parent.partition - skip.partition == skip.partition - skip.jumpPartition) {
        index.jump = skip.jump;
        index.jumpPartition = skip.jumpPartition;
      } else {
        index.jump = parent;
        index.jumpPartition = parent.partition;
      }
    }
    proposalIndex.put(proposal.id(), index);
  }

  private Proposal ancestorAt(Proposal proposal, long partition) {
    long distance = proposal.first.partition - partition;
    if (distance < 0
        || (compactionAnchor != null && partition < compactionAnchor.first.partition)) {
      // Nothing below the compaction anchor is retained.
      return null;
    }
    if (distance == 0) {
      return proposal;
    }
    ProposalIndex cursor = proposalIndex.get(proposal.id());
    if (cursor == null) {
      return null;
    }
    while (cursor.partition > partition) {
      schedulingEntryVisits++;
      // A jump that stays at or above the target, else one partition down. Every node between a
      // live proposal and the anchor is live, so only a walk from dropped history meets one.
      ProposalIndex next =
          cursor.jump != null && cursor.jumpPartition >= partition ? cursor.jump : cursor.parent;
      if (next == null || next.dropped) {
        return null;
      }
      cursor = next;
    }
    return cursor.partition == partition ? cursor.proposal : null;
  }

  private boolean isAncestor(Proposal ancestor, Proposal descendant) {
    Proposal found = ancestorAt(descendant, ancestor.first.partition);
    return found != null && found.id().equals(ancestor.id());
  }

  private boolean covers(Proposal proposal, EntryId id) {
    if (proposal.id().equals(id)) {
      return true;
    }
    Entry entry = stored.get(id);
    if (entry == null || entry.partition >= proposal.first.partition) {
      return false;
    }
    Proposal segment = ancestorAt(proposal, entry.partition + 1);
    return segment != null && proposalIndex.get(segment.id()).members().contains(id);
  }

  /** As sameIdentities, for the entries of a partition, which hold each identity once. */
  private static boolean sameIdentities(PartitionEntries entries, List<EntryId> list) {
    if (entries.size() != list.size()) {
      return false;
    }
    long previous = Long.MIN_VALUE;
    boolean ascending = true;
    for (EntryId id : list) {
      ascending &= id.index > previous;
      previous = id.index;
      if (!entries.contains(id)) {
        return false;
      }
    }
    // A repeated identity in the list would leave one of the partition's out.
    return ascending || new HashSet<>(list).size() == list.size();
  }

  /** Set equality of {@code set} and {@code list} without copying a long manifest into a set. */
  private static boolean sameIdentities(Set<EntryId> set, List<EntryId> list) {
    if (set.size() != list.size()) {
      return false;
    }
    long previous = Long.MIN_VALUE;
    for (EntryId id : list) {
      if (id.index <= previous) {
        // Not ascending: duplicates are possible, so compare as sets.
        return set.equals(new HashSet<>(list));
      }
      previous = id.index;
      if (!set.contains(id)) {
        return false;
      }
    }
    return true;
  }

  private static boolean later(Proposal a, Proposal b) {
    return a.first.id.term > b.first.id.term
        || (a.first.id.term == b.first.id.term && a.first.partition > b.first.partition);
  }

  private static Set<EntryId> identities(Collection<Entry> entries) {
    Set<EntryId> result = new LinkedHashSet<>();
    for (Entry entry : entries) {
      result.add(entry.id);
    }
    return result;
  }

  private static Entry requireEntry(Map<EntryId, Entry> entries, EntryId id) throws IOException {
    Entry entry = entries.get(id);
    if (entry == null) {
      throw new IOException("missing durable entry " + id);
    }
    return entry;
  }

  private static void putConsistent(Map<EntryId, Entry> map, Entry entry) throws IOException {
    Entry previous = map.get(entry.id);
    if (previous != null && !previous.equals(entry)) {
      throw new IOException("conflicting immutable identity");
    }
    map.put(entry.id, entry);
  }

  private static void putConsistentProposal(Map<EntryId, Proposal> map, Proposal proposal)
      throws IOException {
    Proposal previous = map.get(proposal.id());
    if (previous != null && !previous.equals(proposal)) {
      throw new IOException("conflicting immutable proposal");
    }
    map.put(proposal.id(), proposal);
  }

  private void addAck(Map<EntryId, Integer> acks, EntryId id, int peer) {
    Integer bit = memberBit.get(peer);
    if (bit != null) {
      acks.merge(id, bit, (a, b) -> a | b);
    }
  }

  private static int ackCount(Map<EntryId, Integer> acks, EntryId id) {
    Integer mask = acks.get(id);
    return mask == null ? 0 : Integer.bitCount(mask);
  }

  private boolean hasAck(Map<EntryId, Integer> acks, EntryId id, int peer) {
    Integer mask = acks.get(id);
    Integer bit = memberBit.get(peer);
    return mask != null && bit != null && (mask & bit) != 0;
  }

  /** The voters a bitmask of ACKs names. */
  private List<Integer> ackers(Integer mask) {
    List<Integer> result = new ArrayList<>();
    if (mask != null) {
      for (int i = 0; i < bitMember.length; i++) {
        if ((mask & (1 << i)) != 0) {
          result.add(bitMember[i]);
        }
      }
    }
    return result;
  }
}
