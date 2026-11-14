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

package org.apache.iotdb.consensus.traft.paper.runtime;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.StateMachineApplyResult;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.raft.server.GroupExecutor;
import org.apache.iotdb.consensus.raft.storage.AtomicFiles;
import org.apache.iotdb.consensus.traft.paper.core.TRaftCodec;
import org.apache.iotdb.consensus.traft.paper.core.TRaftEngine;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Application;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Role;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperBatch;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Serial per-group driver. Network workers never call the engine while holding a peer lock. */
public final class TRaftGroup implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TRaftGroup.class);
  // Bounds encoded journal bytes, not total heap: fixed ordinary INSTALL fields/frame fit here.
  private static final long BATCH_RECORD_OVERHEAD = 256;
  private static final int SNAPSHOT_FORMAT_2 = -2;
  // Research counters of protocol messages this node sends, by type ("traft.sent_nack", ...).
  private static final LongAdder[] SENT_COUNTERS = new LongAdder[MessageType.values().length];

  static {
    for (MessageType type : MessageType.values()) {
      SENT_COUNTERS[type.ordinal()] =
          ResearchMetrics.counter("traft.sent_" + type.name().toLowerCase(Locale.ROOT));
    }
  }

  // Held research counters and latency recorders of the per-entry and per-message paths.
  private static final LongAdder RECEIVED_BYTES =
      ResearchMetrics.counter("traft.protocol_received_bytes");
  private static final LongAdder RECEIVED_MESSAGES =
      ResearchMetrics.counter("traft.protocol_received_messages");
  private static final LongAdder ENQUEUED_BYTES =
      ResearchMetrics.counter("traft.protocol_enqueued_bytes");
  private static final LongAdder ENQUEUED_MESSAGES =
      ResearchMetrics.counter("traft.protocol_enqueued_messages");
  private static final LongAdder PAYLOAD_BYTES_OFFERED =
      ResearchMetrics.counter("traft.replication_payload_bytes_offered");
  private static final LongAdder ENTRIES_OFFERED =
      ResearchMetrics.counter("traft.replication_entries_offered");
  private static final LongAdder COMMITTED_ENTRIES =
      ResearchMetrics.counter("traft.committed_entries");
  private static final LongAdder APPLIED_ENTRIES = ResearchMetrics.counter("traft.applied_entries");
  private static final ResearchMetrics.LatencyRecorder LEADER_COMMIT =
      ResearchMetrics.latencyRecorder("traft.leader_commit");
  private static final ResearchMetrics.LatencyRecorder LEADER_APPLY =
      ResearchMetrics.latencyRecorder("traft.leader_apply");
  private static final ResearchMetrics.LatencyRecorder LAT_SEND =
      ResearchMetrics.latencyRecorder("traft.lat_send");
  private static final ResearchMetrics.LatencyRecorder SUBMIT_WAIT =
      ResearchMetrics.latencyRecorder("traft.leader_submit_wait");
  private static final ResearchMetrics.LatencyRecorder ADMISSION_WAIT =
      ResearchMetrics.latencyRecorder("traft.leader_admission_wait");
  private static final ResearchMetrics.LatencyRecorder LAT_ACK_PROCESS =
      ResearchMetrics.latencyRecorder("traft.lat_ack_process");
  private static final ResearchMetrics.LatencyRecorder LAT_FOLLOWER_RECEIVE =
      ResearchMetrics.latencyRecorder("traft.lat_follower_receive");
  private static final ResearchMetrics.LatencyRecorder LAT_ACK_RETURN =
      ResearchMetrics.latencyRecorder("traft.lat_ack_return");
  private static final ResearchMetrics.LatencyRecorder LAT_FOLLOWER_HOLD =
      ResearchMetrics.latencyRecorder("traft.lat_follower_hold");
  private static final ResearchMetrics.LatencyRecorder TASK_RECEIVE =
      ResearchMetrics.latencyRecorder("traft.task_receive");
  private static final ResearchMetrics.LatencyRecorder TASK_DRIVE =
      ResearchMetrics.latencyRecorder("traft.task_drive");
  private static final ResearchMetrics.LatencyRecorder TASK_APPLY_WRITE =
      ResearchMetrics.latencyRecorder("traft.task_apply_write");

  private final int self;
  private final ConsensusGroupId groupId;
  private final File directory;
  private final Map<Integer, Peer> peers = new TreeMap<>();
  private final IStateMachine machine;
  private final RaftConfig config;
  private final TRaftRpc rpc;
  // Where RPC responses (replies carried back from a peer) are fed into this node.
  private final Consumer<List<ByteBuffer>> responses;
  private final GroupExecutor executor;
  private final ExecutorService network;
  private final TRaftFileStore store;
  private final TRaftEngine engine;
  private final ScheduledFuture<?> ticker;
  private final Random random = new Random();
  // Keyed by a forwarded request's token, or by a fresh object for a local request.
  private final Map<Object, PendingWrite> waiting = new HashMap<>();
  private final ArrayDeque<Command> admissions = new ArrayDeque<>();
  // Identities this leader admitted, to their waiters: applying one needs neither its payload
  // nor its envelope (TRaftGroup.apply). Cleared with the waiters.
  private final Map<EntryId, PendingWrite> admittedHere = new HashMap<>();
  // Applied identities at or above appliedBelow. Every committed identity below it is applied:
  // the engine compacted it (it applies before it compacts), or a snapshot recorded the bound.
  private final Set<EntryId> appliedIds = new HashSet<>();
  private long appliedBelow;
  private final Map<Integer, Channel> channels = new HashMap<>();
  private final LinkedHashMap<String, TSStatus> results =
      new LinkedHashMap<String, TSStatus>(256, .75f, true) {
        protected boolean removeEldestEntry(Map.Entry<String, TSStatus> e) {
          return size() > 10000;
        }
      };
  private volatile Role role = Role.FOLLOWER;
  private volatile int leader = -1;
  private volatile long term;
  private volatile Throwable failure;
  private volatile boolean closed;
  private long electionDeadline;
  private long lastHeartbeat;
  private long pendingBytes;
  private long appliedCount;
  // Accessed only by this group's executor; no timer or batching delay is introduced.
  private boolean driveQueued;
  private final Map<EntryId, Long> admissionStarts = new HashMap<>();
  // Research timing only: when a leader's entry first left in a frame, until its first ACK arrives.
  private final Map<EntryId, Long> entrySentAt = new HashMap<>();
  // A one-voter append can commit and apply before append returns its new identity.
  private boolean observingAppend;
  private Command activeAdmission;
  // Research timing only: since when queued client writes wait for a pending proposal, or -1.
  private long admissionBlockedSince = -1;
  // Admitted read fences: their commit notices are flushed immediately, never deferred.
  private final Set<EntryId> barrierIdentities = new HashSet<>();
  private boolean urgentCommitNotice;
  private final Map<EntryId, Long> appendCommitCompletions = new HashMap<>();
  private final Map<EntryId, Long> appendApplyCompletions = new HashMap<>();
  private final ResearchMetrics.AdmissionTracker admissionMetrics =
      new ResearchMetrics.AdmissionTracker("traft");
  // The entries-in-flight window handed to the engine; reported by status() so that a test or
  // benchmark can check that the configured limits reached the protocol.
  private final int replicationWindow;

  private static final class PendingWrite {
    final CompletableFuture<TSStatus> response;
    final long term;
    // Null for a local request, which has no retry to answer.
    final String token;
    final Object key;
    final int length;
    EntryId identity;
    IConsensusRequest liveRequest;

    PendingWrite(
        CompletableFuture<TSStatus> response,
        long term,
        String token,
        Object key,
        int length,
        IConsensusRequest liveRequest) {
      this.response = response;
      this.term = term;
      this.token = token;
      this.key = key;
      this.length = length;
      this.liveRequest = liveRequest;
    }
  }

  private static final class Command {
    final String token;
    final Kind kind;
    final long timestamp;
    final boolean hasSeries;
    final long seriesKey;
    final byte[] bytes;
    final PendingWrite pending;
    // Research timing only: when write() was called and when the group executor queued it.
    final long calledNanos;
    final long queuedNanos;

    Command(
        String token,
        Kind kind,
        long timestamp,
        boolean hasSeries,
        long seriesKey,
        byte[] bytes,
        PendingWrite pending,
        long calledNanos) {
      this.token = token;
      this.kind = kind;
      this.timestamp = timestamp;
      this.hasSeries = hasSeries;
      this.seriesKey = seriesKey;
      this.bytes = bytes;
      this.pending = pending;
      this.calledNanos = calledNanos;
      this.queuedNanos = System.nanoTime();
    }
  }

  // Series ordering: data of different series shares a partition (TRaftEngine.setSeriesOrder).
  private volatile boolean seriesOrder;
  // Pipelined journal (TRaftFileStore.setPipelined): the executor that runs journal forces, set on
  // the group executor. Null forces on the group thread inside each append.
  private volatile Executor journal;
  // Frames and replies produced after a write that no force has covered yet, in production order.
  private final ArrayDeque<Effect> effects = new ArrayDeque<>();
  // Guarded by effects: received ACKs waiting until this node's own copies of what they acknowledge
  // are durable (awaitLocalDurable). Released by position, not behind earlier effects.
  private final PriorityQueue<Effect> ackWaits =
      new PriorityQueue<>(Comparator.comparingLong(effect -> effect.position));
  // Guarded by effects: a force loop is scheduled or running / is running released effects.
  private boolean forcing;
  private boolean releasing;
  // Pipelined journal only, set on the group executor: frames that report no durable state of this
  // node leave before its force (setReplicateBeforeForce), and forces wait for the store's buffered
  // policy, replies then leaving in their own frames instead of RPC responses (setForcePolicy).
  private volatile boolean replicateBeforeForce;
  private volatile boolean bufferedForce;

  // Batched application (setApplyBatch): entries the engine applies during one protocol task are
  // staged here and handed to the state machine together when the task ends, in apply order.
  private boolean applyBatch;
  // When set, staged runs are written to the state machine here, in order and off this group's
  // executor (as Raft's apply pool does), and reads and snapshots queue behind them. The engine
  // and appliedIds count an entry as applied once staged; the machine catches up in this queue.
  private volatile GroupExecutor applier;
  // Commands one drive admits before yielding the executor (setAdmissionRound).
  private int admissionRound;
  private final List<StagedApply> staged = new ArrayList<>();

  private static final class StagedApply {
    final Entry entry;
    final String token;
    final PendingWrite pending;
    // Null for an empty payload, which completes without a state machine write.
    final IConsensusRequest request;
    final int length;

    StagedApply(
        Entry entry, String token, PendingWrite pending, IConsensusRequest request, int length) {
      this.entry = entry;
      this.token = token;
      this.pending = pending;
      this.request = request;
      this.length = length;
    }
  }

  private static final class Effect {
    final long position;
    final Runnable action;
    final Consumer<Throwable> abort;

    Effect(long position, Runnable action, Consumer<Throwable> abort) {
      this.position = position;
      this.action = action;
      this.abort = abort;
    }
  }

  public TRaftGroup(
      int self,
      ConsensusGroupId id,
      File dir,
      List<Peer> membership,
      IStateMachine machine,
      RaftConfig config,
      int theta,
      long partitionMillis,
      TRaftRpc rpc,
      Consumer<List<ByteBuffer>> responses,
      ExecutorService cores,
      ExecutorService network,
      ScheduledExecutorService timers)
      throws IOException {
    this(
        self,
        id,
        dir,
        membership,
        machine,
        config,
        theta,
        partitionMillis,
        true,
        rpc,
        responses,
        cores,
        network,
        timers);
  }

  /**
   * @param compaction let the engine drop the per-identity state of partitions every member has
   *     closed (traft_compaction); a compacted journal is replayed as one either way
   */
  public TRaftGroup(
      int self,
      ConsensusGroupId id,
      File dir,
      List<Peer> membership,
      IStateMachine machine,
      RaftConfig config,
      int theta,
      long partitionMillis,
      boolean compaction,
      TRaftRpc rpc,
      Consumer<List<ByteBuffer>> responses,
      ExecutorService cores,
      ExecutorService network,
      ScheduledExecutorService timers)
      throws IOException {
    this.self = self;
    this.groupId = id;
    this.directory = dir;
    this.machine = machine;
    this.config = config;
    this.admissionRound = config.replication.maxEntriesPerAppend;
    this.rpc = rpc;
    this.responses = responses != null ? responses : replies -> receive(replies, false);
    this.network = network;
    this.executor = new GroupExecutor(id + "-traft-paper", cores);
    for (Peer peer : membership) {
      peers.put(peer.getNodeId(), peer);
    }
    store = new TRaftFileStore(dir, config.log.cacheBytes);
    try {
      machine.start();
      loadSnapshot();
      engine =
          new TRaftEngine(
              self,
              peers.keySet(),
              theta,
              partitionMillis,
              store,
              new Application() {
                @Override
                public void apply(Entry entry) throws IOException {
                  TRaftGroup.this.apply(entry);
                }

                @Override
                public boolean requiresDurableAppliedRecords() {
                  // Snapshot effects are deduplicated while newly confirmed commands replay.
                  return false;
                }

                @Override
                public boolean requiresDurableCommitRecords() {
                  // All acknowledged data/proposals remain durable. After restart, an active
                  // leader notification or a fresh recovery quorum re-establishes commitment.
                  return false;
                }
              });
      engine.setCommitObserver(
          identity -> {
            long completed = System.nanoTime();
            if (barrierIdentities.remove(identity)) {
              urgentCommitNotice = true;
            }
            COMMITTED_ENTRIES.increment();
            entrySentAt.remove(identity);
            Long start = admissionStarts.get(identity);
            if (start != null) {
              LEADER_COMMIT.record(completed - start);
            } else if (observingAppend) {
              appendCommitCompletions.put(identity, completed);
            }
          });
      replicationWindow =
          Math.multiplyExact(
              config.replication.maxInflightBatches, config.replication.maxEntriesPerAppend);
      engine.setReplicationWindow(replicationWindow);
      engine.setProposalPayloadBudget(config.replication.maxInflightBytes);
      engine.setCompaction(compaction);
      // Construction may have re-applied compacted partitions: they lie below the anchor.
      refreshAppliedBelow();
      engine.replayCommitted(this::apply);
      engine.applyCommitted();
      publish();
      resetElection();
      ticker =
          ScheduledExecutorUtil.unsafelyScheduleWithFixedDelay(
              timers,
              () ->
                  submit(
                      () -> {
                        tick();
                        return null;
                      }),
              10,
              10,
              TimeUnit.MILLISECONDS);
    } catch (IOException | RuntimeException e) {
      try {
        machine.stop();
      } catch (RuntimeException suppressed) {
        e.addSuppressed(suppressed);
      }
      try {
        store.close();
      } catch (IOException suppressed) {
        e.addSuppressed(suppressed);
      }
      executor.close();
      throw e;
    }
  }

  public int leaderId() {
    return leader;
  }

  public long term() {
    return term;
  }

  public boolean ready() {
    return !closed && failure == null && role == Role.LEADER;
  }

  public List<Peer> peers() {
    return new ArrayList<>(peers.values());
  }

  public Peer leaderPeer() {
    return peers.get(leader);
  }

  /** A read-only snapshot of one group, taken on its executor; see {@link #status()}. */
  public static final class Status {
    public final Role role;
    public final long term;
    public final int leader;

    /** Entries this node applied to its state machine since start (data and barriers, no NOOPs). */
    public final long applied;

    /** Client writes accepted by this node (queued or admitted) that have not been answered yet. */
    public final int pendingWrites;

    /** Client commands still waiting for admission (a subset of {@link #pendingWrites}). */
    public final int queuedAdmissions;

    public final long committedFrontier;
    public final long currentPartition;

    /** Entries in flight per follower, as configured: maxInflightBatches x maxEntriesPerAppend. */
    public final int replicationWindow;

    /** The highest partition whose per-identity state this replica dropped, or -1. */
    public final long compactedThrough;

    /** Sizes of the engine's per-identity structures plus {@code appliedIds}; observation only. */
    public final Map<String, Integer> stateSizes;

    Status(
        Role role,
        long term,
        int leader,
        long applied,
        int pendingWrites,
        int queuedAdmissions,
        long committedFrontier,
        long currentPartition,
        int replicationWindow,
        long compactedThrough,
        Map<String, Integer> stateSizes) {
      this.role = role;
      this.term = term;
      this.leader = leader;
      this.applied = applied;
      this.pendingWrites = pendingWrites;
      this.queuedAdmissions = queuedAdmissions;
      this.committedFrontier = committedFrontier;
      this.currentPartition = currentPartition;
      this.replicationWindow = replicationWindow;
      this.compactedThrough = compactedThrough;
      this.stateSizes = stateSizes;
    }
  }

  /**
   * Observation only: a consistent snapshot taken between two protocol steps on the group executor.
   * Nothing in the protocol reads it; tests and the in-JVM benchmark use it to wait for quiescence
   * (no pending client writes, applied counts settled) and to check configured limits. Fails like
   * {@link #submit} when the group is closed or fenced.
   */
  public CompletableFuture<Status> status() {
    return submit(
        () -> {
          Map<String, Integer> sizes = engine.stateSizes();
          sizes.put("appliedIds", appliedIds.size());
          return new Status(
              engine.role(),
              engine.term(),
              engine.leaderId(),
              appliedCount,
              waiting.size(),
              admissions.size(),
              engine.committedFrontier(),
              engine.currentPartition(),
              replicationWindow,
              engine.compactedThrough(),
              sizes);
        });
  }

  public <T> CompletableFuture<T> submit(Callable<T> operation) {
    CompletableFuture<T> result = new CompletableFuture<>();
    if (closed || failure != null) {
      result.completeExceptionally(new IOException("TRaft group unavailable", failure));
      return result;
    }
    executor.execute(
        () -> {
          try {
            if (closed || failure != null) {
              throw new IOException("TRaft group unavailable", failure);
            }
            T value;
            try {
              value = operation.call();
            } finally {
              flushAppliesOrFail();
            }
            result.complete(value);
          } catch (Throwable e) {
            result.completeExceptionally(e);
          }
        });
    return result;
  }

  private void resetElection() {
    long min = config.election.electionTimeoutMinMs;
    long spread = Math.max(1, config.election.electionTimeoutMaxMs - min);
    electionDeadline =
        System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(min + (long) (random.nextDouble() * spread));
  }

  private void tick() {
    try {
      if (closed || failure != null) {
        return;
      }
      long now = System.nanoTime();
      if (engine.role() != Role.LEADER && now >= electionDeadline) {
        engine.startElection();
        resetElection();
      }
      if (now - lastHeartbeat
          >= TimeUnit.MILLISECONDS.toNanos(config.election.heartbeatIntervalMs)) {
        engine.tick(TimeUnit.NANOSECONDS.toMillis(now));
        lastHeartbeat = now;
      }
      // Commit notices deferred for piggybacking leave within one ticker period when idle, and
      // a deferred installation marker is forced within the same period.
      for (Channel channel : channels.values()) {
        channel.flushDeferred();
      }
      if (journal == null) {
        store.forcePending();
      } else if (store.hasPendingForce()) {
        // A pipelined journal forces a lone deferred marker through the force loop.
        afterDurable(() -> {}, error -> {});
      }
      scheduleDrive();
    } catch (Throwable e) {
      fail(e);
    }
  }

  public CompletableFuture<Void> receive(List<ByteBuffer> messages) {
    return receive(messages, false).thenApply(ignored -> null);
  }

  /**
   * Processes one delivered batch on the group executor. With {@code replyAllowed}, control replies
   * addressed to the batch sender are returned instead of queued on that sender's channel, so the
   * transport carries them in the RPC response: one round trip then holds a request and its
   * acknowledgments. Frames that arrived as such a response pass {@code false}.
   */
  public CompletableFuture<List<ByteBuffer>> receive(
      List<ByteBuffer> messages, boolean replyAllowed) {
    long arrived = System.nanoTime();
    CompletableFuture<List<ByteBuffer>> answer = new CompletableFuture<>();
    // Decoding (with each entry's digest) runs on the calling network thread, off the serial group
    // executor; a malformed frame still fails the group there, as before.
    List<Message> received = new ArrayList<>(messages.size());
    Throwable malformed = null;
    try {
      for (ByteBuffer bytes : messages) {
        byte[] array = new byte[bytes.remaining()];
        bytes.duplicate().get(array);
        received.add(TRaftCodec.decodeMessage(array));
        RECEIVED_BYTES.add(array.length);
        RECEIVED_MESSAGES.increment();
      }
    } catch (Throwable e) {
      malformed = e;
    }
    Throwable decodeFailure = malformed;
    submit(
            () -> {
              long taskStarted = System.nanoTime();
              try {
                if (decodeFailure != null) {
                  throw decodeFailure;
                }
                List<Message> decoded = received;
                int sender = -1;
                boolean singleSender = true;
                boolean durableAcks = false;
                boolean proposalAcks = false;
                for (Message m : decoded) {
                  if (sender == -1) {
                    sender = m.from;
                  } else if (sender != m.from) {
                    singleSender = false;
                  }
                  durableAcks |=
                      m.type == MessageType.ENTRY_ACK || m.type == MessageType.PROPOSAL_ACK;
                  proposalAcks |= m.type == MessageType.PROPOSAL_ACK;
                }
                long needed = replicateBeforeForce && durableAcks ? acknowledgedEnd(decoded) : -1;
                if (needed > store.forcedEnd()) {
                  // These ACKs may complete a quorum that counts this node, whose data left before
                  // its force: process them once this node's own copies of the acknowledged
                  // entries and proposals are durable (for a recovery proposal, everything
                  // written so far).
                  ResearchMetrics.add("traft.deferred_ack_batches", 1);
                  if (proposalAcks && bufferedForce && engine.role() != Role.LEADER) {
                    // A recovery proposal waiting for its quorum: never buffer that. Pipelined
                    // ordinary proposals block no admission and follow the force policy.
                    store.requestForce();
                  }
                  awaitLocalDurable(
                      needed,
                      () ->
                          submit(
                              () -> {
                                receiveDecoded(decoded, -1, arrived);
                                return null;
                              }));
                  answer.complete(Collections.emptyList());
                  return null;
                }
                int replyTo =
                    replyAllowed && singleSender && sender != self && !bufferedForce ? sender : -1;
                List<ByteBuffer> replies = receiveDecoded(decoded, replyTo, arrived);
                if (bufferedForce) {
                  // The replies travel in their own frames once forced (driveInternal); answering
                  // now keeps a buffered force from holding the sender's delivery slot and thread.
                  answer.complete(replies);
                } else {
                  // The replies acknowledge records this round wrote; they leave once those are
                  // durable.
                  long repliesAt = System.nanoTime();
                  afterDurable(
                      () -> {
                        if (!replies.isEmpty()) {
                          LAT_FOLLOWER_HOLD.record(System.nanoTime() - repliesAt);
                        }
                        answer.complete(replies);
                      },
                      answer::completeExceptionally);
                }
                return null;
              } catch (Throwable e) {
                fail(e);
                throw new IOException("TRaft receive failed", e);
              } finally {
                // Research timing only: one delivered batch's protocol work on the executor.
                TASK_RECEIVE.record(System.nanoTime() - taskStarted);
              }
            })
        .whenComplete(
            (ignored, error) -> {
              if (error != null) {
                answer.completeExceptionally(error);
              }
            });
    return answer;
  }

  /**
   * The smallest forced journal end that covers this node's copy of every entry the ENTRY_ACKs in
   * {@code messages} acknowledge and of every ordinary proposal their PROPOSAL_ACKs acknowledge (an
   * ACK certifies the exact prefix: forces cover journal prefixes, so the force covering this
   * proposal's RETAIN record covers every earlier one); the whole written journal if one of them
   * has no local record or acknowledges a recovery proposal.
   */
  private long acknowledgedEnd(List<Message> messages) {
    long needed = 0;
    for (Message m : messages) {
      if (m.type == MessageType.PROPOSAL_ACK) {
        if (m.reference == null || !engine.isOrdinaryProposal(m.reference)) {
          return store.writtenEnd();
        }
        long end = store.retainEnd(m.reference);
        if (end < 0) {
          return store.writtenEnd();
        }
        needed = Math.max(needed, end);
        continue;
      }
      if (m.type != MessageType.ENTRY_ACK) {
        continue;
      }
      List<EntryId> ids = new ArrayList<>(m.identities.size() + 1);
      if (m.reference != null) {
        ids.add(m.reference);
      }
      ids.addAll(m.identities);
      for (EntryId id : ids) {
        long end = store.payloadEnd(id);
        if (end < 0) {
          return store.writtenEnd();
        }
        needed = Math.max(needed, end);
      }
    }
    return needed;
  }

  /** Processes one decoded delivery on the group executor and drains the resulting round. */
  private List<ByteBuffer> receiveDecoded(List<Message> decoded, int replyTo, long arrived)
      throws IOException {
    try {
      // Research timing only: how long the first ACK of each entry took to come back after the
      // entry first left, how long ACK processing waited after arrival (executor queue and the
      // wait for this node's own durable copy), and how long a follower took from receiving data
      // to producing its ACK frames.
      boolean carriesEntries = false;
      boolean carriesAcks = false;
      for (Message m : decoded) {
        if (m.type == MessageType.ENTRY) {
          carriesEntries = true;
        } else if (m.type == MessageType.ENTRY_ACK) {
          carriesAcks = true;
          noteAckReturn(m.reference, arrived);
          for (EntryId id : m.identities) {
            noteAckReturn(id, arrived);
          }
        }
      }
      if (carriesAcks) {
        LAT_ACK_PROCESS.record(System.nanoTime() - arrived);
      }
      List<Message> entries = new ArrayList<>();
      List<Message> transfers = new ArrayList<>();
      long entryBytes = 0;
      for (Message m : decoded) {
        long size = m.entry == null ? 0 : m.entry.payloadSize() + BATCH_RECORD_OVERHEAD;
        boolean ordinary =
            m.type == MessageType.ENTRY
                && m.entry != null
                && m.entry.kind == Kind.DATA
                && m.entry.hasPayload();
        if (!entries.isEmpty()
            && (!ordinary
                || entries.size() >= config.replication.maxEntriesPerAppend
                || size > batchByteLimit() - entryBytes
                || !sameEntryRun(entries.get(0), m))) {
          receiveEntryRun(entries);
          entries.clear();
          entryBytes = 0;
        }
        if (m.type != MessageType.TRANSFER_ENTRY && !transfers.isEmpty()) {
          receiveTransferRun(transfers);
          transfers.clear();
        }
        if (ordinary && size <= batchByteLimit()) {
          entries.add(m);
          entryBytes += size;
        } else if (m.type == MessageType.TRANSFER_ENTRY) {
          transfers.add(m);
        } else {
          receiveMessage(m);
        }
      }
      receiveEntryRun(entries);
      receiveTransferRun(transfers);
      List<ByteBuffer> replies = driveInternal(replyTo);
      if (carriesEntries) {
        LAT_FOLLOWER_RECEIVE.record(System.nanoTime() - arrived);
      }
      return replies;
    } catch (Throwable e) {
      fail(e);
      throw new IOException("TRaft receive failed", e);
    }
  }

  private void noteAckReturn(EntryId id, long arrived) {
    Long sent = id == null ? null : entrySentAt.remove(id);
    if (sent != null) {
      LAT_ACK_RETURN.record(arrived - sent);
    }
  }

  /**
   * Messages that certify this node's durable records (or carry its vote or recovery report). They
   * leave only after the force that covers the writes made before them, whatever the force policy.
   */
  static boolean reportsDurableState(MessageType type) {
    switch (type) {
      case ENTRY_ACK:
      case PROPOSAL_ACK:
      case TRANSFER_ACK:
      case NACK:
      case VOTE:
      case REPORT:
        return true;
      default:
        return false;
    }
  }

  private static boolean sameEntryRun(Message first, Message next) {
    return first.from == next.from
        && first.to == next.to
        && first.term == next.term
        && Objects.equals(first.reference, next.reference);
  }

  private void receiveEntryRun(List<Message> entries) throws IOException {
    if (entries.size() > 1 && engine.receiveEntries(entries)) {
      // The fast path accepts only one current-term leader's fully durable ordinary run.
      resetElection();
    } else {
      for (Message entry : entries) {
        receiveMessage(entry);
      }
    }
  }

  private void receiveTransferRun(List<Message> transfers) throws IOException {
    if (transfers.size() < 2) {
      for (Message transfer : transfers) {
        receiveMessage(transfer);
      }
      return;
    }
    long progress = engine.electionProgress();
    Message first = transfers.get(0);
    engine.receiveTransfers(transfers);
    if (engine.electionProgress() != progress
        || (first.term == engine.term() && first.from == engine.leaderId())) {
      resetElection();
    }
  }

  private void receiveMessage(Message message) throws IOException {
    long progress = engine.electionProgress();
    engine.receive(message);
    if (engine.electionProgress() != progress
        || (message.term == engine.term() && message.from == engine.leaderId())) {
      resetElection();
    }
  }

  /** Order data per series (true) or per group as in the paper's original rule (false). */
  public void setSeriesOrder(boolean enabled) {
    seriesOrder = enabled;
    // The engine is synchronized; the switch applies from its next partition.
    engine.setSeriesOrder(enabled);
  }

  /**
   * Pipelines this group's synchronous journal: appends only write, forces run on {@code
   * forceExecutor}, and frames and RPC replies wait for the force that covers the writes made
   * before them (TRaftFileStore.setPipelined). Commitment always needs an ACK from another member,
   * which can only answer a frame sent after the leader's own force, so client completions need no
   * hold of their own. A one-voter group commits inside its own append and keeps forcing inline.
   */
  public void pipelineJournal(Executor forceExecutor) {
    if (peers.size() < 2) {
      return;
    }
    // Switch between two protocol rounds, never inside one.
    submit(
        () -> {
          store.setPipelined(true);
          journal = forceExecutor;
          return null;
        });
  }

  /**
   * For a pipelined journal: data, proposals and notices leave as soon as a protocol round has
   * written them, so the leader forces in parallel with its followers, while ACKs, votes and
   * reports still wait for the force covering them (reportsDurableState). The leader processes a
   * remote ENTRY_ACK or PROPOSAL_ACK only once its own force covers what it had written when the
   * ACK arrived, so every quorum still holds the leader's durable copy. No effect without a
   * pipelined journal, whose appends force before they return.
   */
  public void setReplicateBeforeForce(boolean enabled) {
    submit(
        () -> {
          replicateBeforeForce = enabled && journal != null;
          return null;
        });
  }

  /**
   * For a pipelined journal: forces start only when the store's buffered policy says they are due
   * (TRaftFileStore.setForcePolicy). With a positive interval, replies leave in their own frames
   * after the force instead of in RPC responses, so a buffered force never holds the sender's
   * delivery slot or network thread. Every acknowledgment still waits for its force.
   */
  public void setForcePolicy(long intervalMillis, long dataRecords) {
    submit(
        () -> {
          if (journal != null) {
            store.setForcePolicy(intervalMillis, dataRecords);
            bufferedForce = intervalMillis > 0;
            // Data ACKs now arrive up to one interval late; retransmit only after that.
            engine.setResendDelay(
                bufferedForce ? intervalMillis + 2 * config.election.heartbeatIntervalMs : 0);
          }
          return null;
        });
  }

  /**
   * Runs an externally visible effect once every journal write made before it is durable: at once
   * without a pipelined journal or when nothing is pending, otherwise from the force loop, always
   * in production order. Called on the group executor only.
   */
  private void afterDurable(Runnable action, Consumer<Throwable> abort) {
    Executor forceExecutor = journal;
    if (forceExecutor == null) {
      action.run();
      return;
    }
    long forced = store.forcedEnd();
    long written = store.writtenEnd();
    synchronized (effects) {
      if (!effects.isEmpty() || releasing || written > forced) {
        effects.addLast(new Effect(written, action, abort));
        if (!forcing) {
          forcing = true;
          forceExecutor.execute(this::forceLoop);
        }
        return;
      }
    }
    action.run();
  }

  /**
   * Runs {@code action} once a force covers journal position {@code position}, without waiting for
   * effects queued before it. Called on the group executor only.
   */
  private void awaitLocalDurable(long position, Runnable action) {
    Executor forceExecutor = journal;
    if (forceExecutor == null || position <= store.forcedEnd()) {
      action.run();
      return;
    }
    synchronized (effects) {
      ackWaits.add(new Effect(position, action, error -> {}));
      if (!forcing) {
        forcing = true;
        forceExecutor.execute(this::forceLoop);
      }
    }
  }

  /** Forces the journal while effects wait, releasing each once a force covers its position. */
  private void forceLoop() {
    while (true) {
      synchronized (effects) {
        if (effects.isEmpty() && ackWaits.isEmpty()) {
          forcing = false;
          return;
        }
      }
      long forced;
      try {
        store.awaitForceDue();
        forced = store.forceWritten();
      } catch (Throwable error) {
        abortEffects(error, true);
        if (!closed) {
          submit(
              () -> {
                fail(error);
                return null;
              });
        }
        return;
      }
      List<Effect> ready = new ArrayList<>();
      synchronized (effects) {
        while (!effects.isEmpty() && effects.peekFirst().position <= forced) {
          ready.add(effects.pollFirst());
        }
        while (!ackWaits.isEmpty() && ackWaits.peek().position <= forced) {
          ready.add(ackWaits.poll());
        }
        releasing = true;
      }
      try {
        for (Effect effect : ready) {
          effect.action.run();
        }
      } catch (Throwable error) {
        LOG.error("TRaft group {} failed to release durable effects", groupId, error);
        submit(
            () -> {
              fail(error);
              return null;
            });
      } finally {
        synchronized (effects) {
          releasing = false;
        }
      }
    }
  }

  /** Drops held effects; only the force loop itself may end the loop. */
  private void abortEffects(Throwable error, boolean endLoop) {
    List<Effect> dropped;
    synchronized (effects) {
      dropped = new ArrayList<>(effects);
      effects.clear();
      dropped.addAll(ackWaits);
      ackWaits.clear();
      if (endLoop) {
        forcing = false;
      }
    }
    for (Effect effect : dropped) {
      try {
        effect.abort.accept(error);
      } catch (Throwable ignored) {
        // The group is stopping; a frame reservation or reply future no longer matters.
      }
    }
  }

  /**
   * Batched application: a multi-voter group stages what the engine applies during one protocol
   * task and writes it with one IStateMachine.writeBatch call when the task ends, so per-call state
   * machine work is shared. Nothing outside the group executor observes the state machine in
   * between: reads and snapshots run as their own tasks. A one-voter group, whose append can apply
   * before returning its identity, and engine construction apply at once.
   */
  /**
   * Admits at most {@code commands} per drive, then yields the executor to queued deliveries and
   * writes before the next drive (0: the transport batch, raft.max_entries_per_append).
   */
  /** Followers wait this long for a missing proposal member before fetching it (TRaftEngine). */
  public void setFetchGrace(long millis) {
    submit(
        () -> {
          engine.setFetchGrace(millis);
          return null;
        });
  }

  public void setAdmissionRound(int commands) {
    submit(
        () -> {
          admissionRound =
              commands > 0
                  ? Math.min(commands, config.replication.maxEntriesPerAppend)
                  : config.replication.maxEntriesPerAppend;
          return null;
        });
  }

  public void setApplyBatch(boolean enabled) {
    if (peers.size() < 2) {
      return;
    }
    submit(
        () -> {
          applyBatch = enabled;
          return null;
        });
  }

  /**
   * Writes staged runs on a serial executor over {@code pool} instead of this group's executor;
   * only batched application (multi-voter groups) uses it. Configure before serving.
   */
  public void setApplyExecutor(ExecutorService pool) {
    submit(
        () -> {
          if (applier == null && applyBatch && pool != null) {
            applier = new GroupExecutor(groupId + "-traft-apply", pool);
          }
          return null;
        });
  }

  /** Journal durability (TRaftFileStore.setAsyncForceInterval); negative forces every append. */
  public void setJournalAsyncForceInterval(long intervalMillis) {
    store.setAsyncForceInterval(intervalMillis);
  }

  private long batchByteLimit() {
    return Math.min(config.replication.maxBytesPerAppend, config.replication.maxInflightBytes);
  }

  public CompletableFuture<TSStatus> write(
      String token, Kind kind, long timestamp, byte[] payload) {
    return write(token, kind, timestamp, payload, null);
  }

  /** The optional local request is a bounded optimization, never commit or completion evidence. */
  public CompletableFuture<TSStatus> write(
      String token, Kind kind, long timestamp, byte[] payload, IConsensusRequest liveRequest) {
    return write(token, kind, timestamp, false, 0, payload, liveRequest);
  }

  /**
   * As write, with the series a data write touches; keyless writes may touch any series. A null
   * token marks a local request: its entry carries no token and its waiter is bound to the entry
   * identity at admission. A forwarded request keeps its token in the entry, so a retry, also at a
   * later leader, is answered from the recorded result.
   */
  public CompletableFuture<TSStatus> write(
      String token,
      Kind kind,
      long timestamp,
      boolean hasSeries,
      long seriesKey,
      byte[] payload,
      IConsensusRequest liveRequest) {
    long calledNanos = System.nanoTime();
    CompletableFuture<TSStatus> answer = new CompletableFuture<>();
    boolean invalidToken = token != null && (token.isEmpty() || token.length() > 128);
    Object key = token == null ? new Object() : token;
    // Built on the calling thread, off the serial group executor; an invalid token is still
    // rejected there.
    byte[] enveloped;
    try {
      enveloped = invalidToken ? null : envelope(token, payload);
    } catch (IOException e) {
      answer.completeExceptionally(e);
      return answer;
    }
    answer.whenComplete(
        (status, error) -> {
          if (answer.isCancelled()) {
            // Cancellation abandons only the wait/fast path, never an admitted journal entry.
            submit(
                () -> {
                  PendingWrite pending = waiting.get(key);
                  if (pending != null && pending.response == answer) {
                    pending.liveRequest = null;
                  }
                  return null;
                });
          }
        });
    submit(
            () -> {
              if (!ready()
                  || engine.role() != Role.LEADER
                  || engine.leaderId() != self
                  || engine.term() != term) {
                throw new IOException("TRaft leader is not ready");
              }
              if (invalidToken) {
                throw new IOException("Invalid TRaft request identity");
              }
              if (payload.length > TRaftFileStore.MAX_CLIENT_PAYLOAD
                  || payload.length + 4096L > config.replication.maxInflightBytes) {
                throw new IOException("TRaft request exceeds journal or transport payload limit");
              }
              TSStatus prior = token == null ? null : results.get(token);
              if (prior != null) {
                answer.complete(prior);
                return null;
              }
              PendingWrite existing = waiting.get(key);
              if (existing != null) {
                existing.response.whenComplete(
                    (r, e) -> {
                      if (e == null) answer.complete(r);
                      else answer.completeExceptionally(e);
                    });
                return null;
              }
              if (waiting.size() >= config.replication.maxUncommittedEntries
                  || pendingBytes + payload.length > config.replication.maxUncommittedBytes) {
                throw new IOException("TRaft admission budget exhausted");
              }
              PendingWrite pending =
                  new PendingWrite(
                      answer,
                      engine.term(),
                      token,
                      key,
                      payload.length,
                      answer.isCancelled() ? null : liveRequest);
              waiting.put(key, pending);
              pendingBytes += payload.length;
              admissions.addLast(
                  new Command(
                      token,
                      kind,
                      timestamp,
                      hasSeries,
                      seriesKey,
                      enveloped,
                      pending,
                      calledNanos));
              scheduleDrive();
              return null;
            })
        .whenComplete(
            (r, e) -> {
              if (e != null) {
                answer.completeExceptionally(e);
              }
            });
    return answer;
  }

  private void scheduleDrive() {
    if (driveQueued || closed || failure != null) {
      return;
    }
    driveQueued = true;
    // The shared executor is kept alive until group.close() finishes. GroupExecutor's existing
    // rejection semantics are unchanged: scheduling here always originates in its running task.
    executor.execute(
        () -> {
          driveQueued = false;
          if (closed || failure != null) {
            return;
          }
          long started = System.nanoTime();
          try {
            drive();
          } catch (Throwable error) {
            fail(error);
          } finally {
            flushAppliesOrFail();
            // Research timing only: how long one drive holds the group's executor.
            TASK_DRIVE.record(System.nanoTime() - started);
          }
        });
  }

  private void drive() throws IOException {
    try {
      driveInternal(-1);
    } catch (Throwable e) {
      fail(e);
      throw new IOException("TRaft protocol event failed", e);
    }
  }

  /**
   * One protocol round: admit queued client commands, drain the engine and dispatch frames. Control
   * messages addressed to {@code replyTo} are returned for the pending RPC response.
   */
  private List<ByteBuffer> driveInternal(int replyTo) throws IOException {
    if (role == Role.LEADER && (engine.role() != Role.LEADER || engine.term() != term)) {
      IOException lost = new IOException("TRaft leadership changed");
      for (PendingWrite pending : waiting.values()) {
        pending.liveRequest = null;
        pending.response.completeExceptionally(lost);
      }
      waiting.clear();
      admittedHere.clear();
      admissions.clear();
      admissionStarts.clear();
      entrySentAt.clear();
      barrierIdentities.clear();
      pendingBytes = 0;
      admissionBlockedSince = -1;
    }
    if (engine.role() == Role.LEADER) {
      noteAdmissionUnblocked();
      int admitted = 0;
      long dataProposals = engine.dataProposals();
      long noopProposals = engine.noopProposals();
      // Pending boundary proposals do not stop admission (pipelined boundaries); only a
      // LeaderCandidate's unconfirmed recovery does.
      while (!admissions.isEmpty() && engine.admitting() && admitted < admissionRound) {
        int batched = appendOrdinaryBatch(admissionRound - admitted);
        if (batched != 0) {
          admitted += batched;
          continue;
        }
        Command c = admissions.peekFirst();
        long admissionStart = System.nanoTime();
        observingAppend = true;
        activeAdmission = c;
        try {
          Entry entry =
              engine.append(
                  c.kind,
                  c.timestamp,
                  c.hasSeries,
                  c.seriesKey,
                  c.bytes,
                  TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
          if (entry == null) {
            break;
          }
          bind(c.pending, entry.id);
          admittedHere.put(entry.id, c.pending);
          if (c.kind == Kind.BARRIER && !engine.isCommitted(entry.id)) {
            // A one-voter group commits inside append, before this point; its observer call
            // has already passed and there is no peer to notify, so nothing is tracked.
            barrierIdentities.add(entry.id);
          }
          Long committed = appendCommitCompletions.get(entry.id);
          if (committed != null) {
            LEADER_COMMIT.record(committed - admissionStart);
          }
          Long applied = appendApplyCompletions.get(entry.id);
          if (applied != null) {
            LEADER_APPLY.record(applied - admissionStart);
          }
          if (!engine.isApplied(entry.id)) {
            admissionStarts.put(entry.id, admissionStart);
          }
          admissionMetrics.accepted(c.kind != Kind.DATA, c.timestamp);
          noteAdmissionWaits(c, admissionStart);
          admissions.removeFirst();
          admitted++;
        } finally {
          // Only the returned client entry belongs to this admission. Recovery/no-op or older
          // entries observed during the call must not be attributed to its start time.
          observingAppend = false;
          activeAdmission = null;
          appendCommitCompletions.clear();
          appendApplyCompletions.clear();
        }
      }
      // Boundaries opened by this round: data (a barrier or an order conflict, in a batch or
      // alone) and no-op (theta, the partition timer, a barrier's successor, a new leader's map).
      ResearchMetrics.add("traft.data_boundaries", engine.dataProposals() - dataProposals);
      ResearchMetrics.add("traft.noop_boundaries", engine.noopProposals() - noopProposals);
      if (admissionBlockedSince < 0 && !admissions.isEmpty() && !engine.admitting()) {
        admissionBlockedSince = System.nanoTime();
      }
    }
    Map<EntryId, byte[]> roundPayloads = new HashMap<>();
    long roundPayloadBytes = 0;
    List<Message> outgoing = engine.drainMessages();
    coalesceControlMessages(outgoing);
    // Repair requests, confirmations and durable ACKs get capacity before data retries.
    // A carried empty NOOP still occupies a data window slot. Keep it in stable data order
    // after its ordinary predecessors so followers can persist ENTRY+RETAIN in one batch.
    outgoing.sort(Comparator.comparingInt(m -> m.entry != null && m.entry.hasPayload() ? 1 : 0));
    Map<Channel, List<Frame>> batches = new LinkedHashMap<>();
    Map<Channel, List<Frame>> immediate = new LinkedHashMap<>();
    long framesAt = System.nanoTime();
    boolean ackFrames = false;
    // A held frame that elections or recovery wait for (a vote, a report, a recovery proposal's
    // ACK, a repair receipt, which is a candidate's recovery progress, and a candidate's NACK)
    // must not wait for a buffered force. Pipelined ordinary proposals block no admission, so a
    // follower's ordinary proposal ACKs and NACKs follow the force policy like its data ACKs. The
    // ACK that answers each CONFIRM heartbeat for the installed opening proposal is a repetition
    // the leader does not wait for.
    boolean urgent = false;
    boolean follower = engine.role() == Role.FOLLOWER;
    List<ByteBuffer> replies = new ArrayList<>();
    try {
      for (Message m : outgoing) {
        Peer peer = peers.get(m.to);
        if (peer != null && m.to != self) {
          SENT_COUNTERS[m.type.ordinal()].increment();
          if (m.to == replyTo && (m.entry == null || !m.entry.hasPayload())) {
            // A control reply rides the pending RPC response. It carries no payload, so no
            // transport window slot, byte reservation or duplicate check is involved.
            byte[] encoded = TRaftCodec.encodeMessage(m, roundPayloads);
            replies.add(ByteBuffer.wrap(encoded));
            ENQUEUED_BYTES.add(encoded.length);
            ENQUEUED_MESSAGES.increment();
            continue;
          }
          long newPayloadBytes =
              m.entry != null && m.entry.hasPayload() && !roundPayloads.containsKey(m.entry.id)
                  ? m.entry.payloadSize()
                  : 0;
          Channel channel =
              channels.computeIfAbsent(
                  m.to,
                  ignored ->
                      new Channel(
                          peer.getEndpoint(),
                          config.replication,
                          network,
                          () -> closed,
                          (endpoint, batch) ->
                              rpc.deliver(
                                  endpoint,
                                  new TPaperBatch(groupId.convertToTConsensusGroupId(), batch)),
                          responses));
          String dataKey = dataKey(m);
          long reservation = TRaftCodec.encodedMessageUpperBound(m);
          if (!reserveFrame(
              channel,
              engine,
              m,
              reservation,
              dataKey,
              newPayloadBytes <= config.replication.maxInflightBytes - roundPayloadBytes)) {
            continue;
          }
          byte[] encoded;
          try {
            encoded = TRaftCodec.encodeMessage(m, roundPayloads);
            if (encoded.length > reservation) {
              throw new IOException("TRaft frame exceeded its byte reservation");
            }
          } catch (IOException | RuntimeException e) {
            channel.cancel(reservation, dataKey);
            throw e;
          }
          roundPayloadBytes += newPayloadBytes;
          // Only the encoded frame stays live until the event batch is dispatched.
          channel.release(reservation - encoded.length);
          boolean held = !replicateBeforeForce || reportsDurableState(m.type);
          urgent |=
              held
                  && m.type != MessageType.ENTRY_ACK
                  && m.type != MessageType.ENTRY
                  && !(follower && m.type == MessageType.NACK)
                  && !(m.type == MessageType.PROPOSAL_ACK
                      && (engine.isInstalledOpening(m.reference)
                          || engine.isOrdinaryProposal(m.reference)));
          if (m.type == MessageType.ENTRY && m.entry != null && role == Role.LEADER) {
            Long admitted = admissionStarts.get(m.entry.id);
            if (admitted != null && entrySentAt.putIfAbsent(m.entry.id, framesAt) == null) {
              LAT_SEND.record(framesAt - admitted);
            }
          }
          ackFrames |= m.type == MessageType.ENTRY_ACK;
          (held ? batches : immediate)
              .computeIfAbsent(channel, ignored -> new ArrayList<>())
              .add(
                  new Frame(
                      encoded,
                      dataKey,
                      encoded.length,
                      // Boundary confirmations and commit notices propagate in replication
                      // messages and heartbeats; alone they wait for the next frame or the ticker.
                      m.type == MessageType.COMMIT || m.type == MessageType.CONFIRM));
          if (m.entry != null && m.entry.hasPayload()) {
            PAYLOAD_BYTES_OFFERED.add(m.entry.payloadSize());
            ENTRIES_OFFERED.increment();
          }
          ENQUEUED_BYTES.add(encoded.length);
          ENQUEUED_MESSAGES.increment();
        }
      }
    } catch (IOException | RuntimeException failure) {
      // Nothing from this drain has been dispatched yet. Release every successful reservation.
      for (Map.Entry<Channel, List<Frame>> batch : batches.entrySet()) {
        batch.getKey().cancelReserved(batch.getValue());
      }
      for (Map.Entry<Channel, List<Frame>> batch : immediate.entrySet()) {
        batch.getKey().cancelReserved(batch.getValue());
      }
      throw failure;
    }
    // Data, proposals and notices report nothing this node stored: they leave before its force.
    // Commitment still waits for the leader's durable copy, since receive processes a remote
    // ENTRY_ACK or PROPOSAL_ACK only once this node's force covers its earlier writes.
    for (Map.Entry<Channel, List<Frame>> batch : immediate.entrySet()) {
      batch.getKey().offerReserved(batch.getValue(), !urgentCommitNotice);
    }
    if (!batches.isEmpty()) {
      if (urgent && bufferedForce) {
        store.requestForce();
      }
      boolean allowDeferral = !urgentCommitNotice;
      boolean timedAcks = ackFrames;
      afterDurable(
          () -> {
            if (timedAcks) {
              LAT_FOLLOWER_HOLD.record(System.nanoTime() - framesAt);
            }
            for (Map.Entry<Channel, List<Frame>> batch : batches.entrySet()) {
              // Queue the complete peer batch under one lock before any frame can be dispatched. A
              // batch holding only commit notices and boundary confirmations waits for the next
              // data frame or ticker flush, unless a read fence has just committed; both are
              // protocol state that the heartbeat repeats, and held frames leave ahead of the frame
              // that releases them.
              batch.getKey().offerReserved(batch.getValue(), allowDeferral);
            }
          },
          error -> {
            for (Map.Entry<Channel, List<Frame>> batch : batches.entrySet()) {
              batch.getKey().cancelReserved(batch.getValue());
            }
          });
    }
    urgentCommitNotice = false;
    if (replicateBeforeForce && store.writtenEnd() > store.forcedEnd()) {
      // No frame waits for this round's writes, so start their force now: it then runs while the
      // followers persist the same data, instead of after their ACKs arrive.
      afterDurable(() -> {}, error -> {});
    }
    engine.drainApplied();
    publish();
    if (!admissions.isEmpty() && engine.admitting()) {
      scheduleDrive();
    }
    return replies;
  }

  /**
   * Research timing only: how long an admitted command waited for the group executor after write()
   * and then in the admission queue (pending proposals, the per-round admission limit).
   */
  private static void noteAdmissionWaits(Command command, long admissionStart) {
    SUBMIT_WAIT.record(command.queuedNanos - command.calledNanos);
    ADMISSION_WAIT.record(admissionStart - command.queuedNanos);
  }

  /** Research timing only: ends an interval in which queued writes waited for a proposal. */
  private void noteAdmissionUnblocked() {
    if (admissionBlockedSince >= 0 && engine.admitting()) {
      ResearchMetrics.add(
          "traft.admission_blocked_nanos", System.nanoTime() - admissionBlockedSince);
      ResearchMetrics.add("traft.admission_blocked_count", 1);
      admissionBlockedSince = -1;
    }
  }

  private int appendOrdinaryBatch(int remaining) throws IOException {
    // A one-voter append can apply synchronously. Keep its exact activeAdmission binding path.
    if (peers.size() == 1 || remaining < 2 || admissions.size() < 2) {
      return 0;
    }
    List<Command> commands = new ArrayList<>();
    List<TRaftEngine.DataInput> inputs = new ArrayList<>();
    long bytes = 0;
    for (Command command : admissions) {
      long size = command.bytes.length + BATCH_RECORD_OVERHEAD;
      if (command.kind != Kind.DATA
          || inputs.size() >= remaining
          || size > batchByteLimit() - bytes) {
        // An order conflict inside the batch opens a pipelined partition in the engine.
        break;
      }
      commands.add(command);
      inputs.add(
          new TRaftEngine.DataInput(
              command.timestamp, command.hasSeries, command.seriesKey, command.bytes));
      bytes += size;
    }
    if (inputs.size() < 2) {
      return 0;
    }
    long started = System.nanoTime();
    List<Entry> accepted = engine.appendDataBatch(inputs, TimeUnit.NANOSECONDS.toMillis(started));
    if (accepted.size() > commands.size()) {
      throw new IOException("TRaft accepted more batch entries than submitted requests");
    }
    // All entries are durable, but self ACK alone cannot commit them. The group executor
    // prevents remote ACK processing until every returned identity has its waiter and timing.
    for (int i = 0; i < accepted.size(); i++) {
      Entry entry = accepted.get(i);
      Command command = commands.get(i);
      bind(command.pending, entry.id);
      admittedHere.put(entry.id, command.pending);
      admissionStarts.put(entry.id, started);
      admissionMetrics.accepted(false, command.timestamp);
      noteAdmissionWaits(command, started);
      admissions.removeFirst();
    }
    return accepted.size();
  }

  static boolean reserveFrame(
      Channel channel,
      TRaftEngine engine,
      Message message,
      long bytes,
      String dataKey,
      boolean payloadBudgetAvailable) {
    if (message.type == MessageType.PROPOSAL
        && message.entry != null
        && !engine.isProposalPayloadInFlight(message.to, message.entry.id)) {
      // Another copy from this drain may already have fallen back, or an ACK/repair replaced it.
      return false;
    }
    Channel.Reservation result = channel.tryReserve(bytes, dataKey, payloadBudgetAvailable);
    if (result == Channel.Reservation.BYTE_LIMIT
        && message.type == MessageType.PROPOSAL
        && message.entry != null) {
      // Control traffic may prevent a nearly full-sized combined frame from ever fitting.
      // Queue metadata repair for the next drive; duplicate active frames are not a budget loss.
      engine.fallbackProposalPayload(message.to, message.entry.id);
    }
    return result == Channel.Reservation.ACCEPTED;
  }

  static String dataKey(Message message) {
    return message.entry != null
            && (message.type == MessageType.ENTRY
                || message.type == MessageType.TRANSFER_ENTRY
                || message.type == MessageType.PROPOSAL)
        ? message.term + "/" + message.type + "/" + message.entry.id
        : null;
  }

  /** Only remove identical immutable controls within this durable engine drain, never retries. */
  static void coalesceControlMessages(List<Message> messages) {
    Set<ControlKey> seen = new HashSet<>();
    Iterator<Message> iterator = messages.iterator();
    while (iterator.hasNext()) {
      Message message = iterator.next();
      if (message.entry != null
          || message.rank != null
          || message.report != null
          || message.frontier != -1
          || !message.identities.isEmpty()) {
        continue;
      }
      boolean acknowledgement =
          (message.type == MessageType.ENTRY_ACK
                  || message.type == MessageType.PROPOSAL_ACK
                  || message.type == MessageType.TRANSFER_ACK)
              && message.reference != null
              && message.proposal == null;
      boolean proposal =
          (message.type == MessageType.PROPOSAL || message.type == MessageType.CONFIRM)
              && message.proposal != null
              && message.reference == null;
      if ((acknowledgement || proposal) && !seen.add(new ControlKey(message))) {
        iterator.remove();
      }
    }
  }

  private static final class ControlKey {
    final Message message;

    ControlKey(Message message) {
      this.message = message;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof ControlKey)) {
        return false;
      }
      Message that = ((ControlKey) other).message;
      return message.type == that.type
          && message.from == that.from
          && message.to == that.to
          && message.term == that.term
          && Objects.equals(message.reference, that.reference)
          && Objects.equals(message.proposal, that.proposal);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          message.type,
          message.from,
          message.to,
          message.term,
          message.reference,
          message.proposal);
    }
  }

  /** Applied identities below the engine's compaction anchor collapse into appliedBelow. */
  private void refreshAppliedBelow() {
    EntryId anchor = engine.compactionAnchor();
    if (anchor != null && anchor.index > appliedBelow) {
      long below = anchor.index;
      appliedBelow = below;
      appliedIds.removeIf(id -> id.index < below);
    }
  }

  private void publish() {
    refreshAppliedBelow();
    Role old = role;
    int oldLeader = leader;
    role = engine.role();
    leader = engine.leaderId();
    term = engine.term();
    if (old != Role.LEADER && role == Role.LEADER) {
      ResearchMetrics.add("traft.leader_terms", 1);
    }
    try {
      if (leader >= 0 && oldLeader != leader) {
        machine.event().notifyLeaderChanged(groupId, leader);
      }
      if (old != Role.LEADER && role == Role.LEADER) {
        machine.event().notifyLeaderReady();
      }
      if (old == Role.LEADER && role != Role.LEADER) {
        machine.event().notifyNotLeader();
      }
    } catch (ClassCastException ignored) {
      /* Optional EventApi. */
    }
  }

  private void apply(Entry entry) throws IOException {
    if (entry.kind == Kind.NOOP || entry.id.index < appliedBelow || appliedIds.contains(entry.id)) {
      return;
    }
    PendingWrite own = admittedHere.remove(entry.id);
    if (own != null
        && entry.id.equals(own.identity)
        && own.liveRequest != null
        && !own.response.isCancelled()
        && own.term == engine.term()
        && engine.role() == Role.LEADER
        && engine.leaderId() == self) {
      // Admitted here and still served here: the waiter holds the token, the length and the live
      // request, so the payload is neither read back nor parsed (the general path below would
      // find the same waiter by the envelope's token and take the same live request).
      IConsensusRequest request =
          own.length == 0 ? null : machine.deserializeRequest(own.liveRequest);
      stageOrComplete(entry, own.token, own, request, own.length);
      return;
    }
    // Payload arrays are immutable (Entry.payloadView): the request below wraps them read-only.
    byte[] encoded = entry.payloadView();
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded));
    int header = in.read();
    String token;
    if (header == TOKEN_REQUEST) {
      token = in.readUTF();
    } else if (header == LOCAL_REQUEST) {
      token = null;
    } else {
      throw new IOException("Unknown TRaft request encoding");
    }
    int length = in.available();
    PendingWrite pending = token != null ? waiting.get(token) : own;
    if (pending == null
        && token == null
        && activeAdmission != null
        && activeAdmission.token == null
        && engine != null
        && entry.id.term == engine.term()
        && Arrays.equals(activeAdmission.bytes, encoded)) {
      // Every other local client entry of this term was bound when its append returned.
      pending = activeAdmission.pending;
    }
    if (pending != null
        && pending.identity == null
        && activeAdmission != null
        && activeAdmission.pending == pending
        && Objects.equals(activeAdmission.token, token)
        && Arrays.equals(activeAdmission.bytes, encoded)) {
      // A one-voter append can apply synchronously before returning its identity. Match the
      // exact currently admitted command; an unrelated recovered entry cannot bind the waiter.
      bind(pending, entry.id);
    }
    boolean local =
        pending != null
            && entry.id.equals(pending.identity)
            && pending.liveRequest != null
            && !pending.response.isCancelled()
            && pending.term == engine.term()
            && engine.role() == Role.LEADER
            && engine.leaderId() == self;
    IConsensusRequest request = null;
    if (length != 0) {
      IConsensusRequest raw;
      if (local) {
        raw = pending.liveRequest;
      } else {
        raw =
            new ByteBufferConsensusRequest(
                ByteBuffer.wrap(encoded, encoded.length - length, length)
                    .slice()
                    .asReadOnlyBuffer());
      }
      request = machine.deserializeRequest(raw);
      // No engine yet: the engine's constructor is replaying a COMPACT record, and this node is
      // not serving as leader.
      if (engine == null
          || entry.id.term != engine.term()
          || engine.role() != Role.LEADER
          || engine.leaderId() != self) {
        request.markAsGeneratedByRemoteConsensusLeader();
      }
    }
    stageOrComplete(entry, token, pending, request, length);
  }

  private void stageOrComplete(
      Entry entry, String token, PendingWrite pending, IConsensusRequest request, int length)
      throws IOException {
    if (applyBatch && engine != null) {
      // Counted as applied at once, so the engine's later calls in this task skip it.
      appliedIds.add(entry.id);
      staged.add(new StagedApply(entry, token, pending, request, length));
      return;
    }
    complete(
        entry,
        token,
        pending,
        length,
        request == null ? emptyStatus(entry) : machine.write(request));
  }

  private static TSStatus emptyStatus(Entry entry) {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        .setMessage(entry.id.term + ":" + entry.id.index);
  }

  /**
   * Writes the staged entries in apply order with one IStateMachine.writeBatch call and completes
   * them in that order. The first unsuccessful status stops the group, as a failed single write
   * does; entries before it complete normally.
   */
  private void flushApplies() throws IOException {
    if (staged.isEmpty()) {
      return;
    }
    List<StagedApply> run = new ArrayList<>(staged);
    staged.clear();
    GroupExecutor queue = applier;
    if (queue != null) {
      queue.execute(() -> applyRun(run));
      return;
    }
    long writing = System.nanoTime();
    List<TSStatus> statuses = writeRun(run);
    long completed = System.nanoTime();
    // Research timing only: a synchronous state machine write on the group's executor.
    TASK_APPLY_WRITE.record(completed - writing);
    for (int i = 0; i < run.size(); i++) {
      StagedApply s = run.get(i);
      complete(s.entry, s.token, s.pending, s.length, statuses.get(i), completed);
    }
  }

  /** One state machine call for a staged run; its statuses in run order (empty entries succeed). */
  private List<TSStatus> writeRun(List<StagedApply> run) throws IOException {
    List<IConsensusRequest> requests = new ArrayList<>(run.size());
    for (StagedApply s : run) {
      if (s.request != null) {
        requests.add(s.request);
      }
    }
    List<TSStatus> statuses =
        requests.isEmpty() ? Collections.emptyList() : machine.writeBatch(requests);
    if (statuses.size() != requests.size()) {
      throw new IOException(
          "State machine returned " + statuses.size() + " results for " + requests.size());
    }
    List<TSStatus> ordered = new ArrayList<>(run.size());
    int next = 0;
    for (StagedApply s : run) {
      ordered.add(s.request == null ? emptyStatus(s.entry) : statuses.get(next++));
    }
    return ordered;
  }

  /**
   * On the applier: writes one run, answers its clients at once, then hands this group's own
   * bookkeeping (results, waiters, budget, metrics) to its executor. The first unsuccessful status
   * stops the group there, as on the executor; entries before it complete normally.
   */
  private void applyRun(List<StagedApply> run) {
    if (closed || failure != null) {
      return;
    }
    List<TSStatus> statuses;
    try {
      statuses = writeRun(run);
    } catch (Throwable error) {
      failFromApplier(error);
      return;
    }
    long completed = System.nanoTime();
    int done = 0;
    Throwable stop = null;
    for (; done < run.size(); done++) {
      StagedApply s = run.get(done);
      TSStatus status = statuses.get(done);
      try {
        StateMachineApplyResult.requireCompleted(status);
      } catch (Throwable error) {
        stop = error;
        break;
      }
      if (s.pending != null && s.entry.id.equals(s.pending.identity)) {
        s.pending.response.complete(status);
      }
    }
    int applied = done;
    Throwable failed = stop;
    executor.execute(
        () -> {
          if (closed) {
            return;
          }
          try {
            for (int i = 0; i < applied; i++) {
              StagedApply s = run.get(i);
              complete(s.entry, s.token, s.pending, s.length, statuses.get(i), completed);
            }
            if (failed != null) {
              fail(failed);
            }
          } catch (Throwable error) {
            fail(error);
          }
        });
  }

  private void failFromApplier(Throwable error) {
    executor.execute(
        () -> {
          if (!closed) {
            fail(error);
          }
        });
  }

  private void flushAppliesOrFail() {
    if (staged.isEmpty()) {
      return;
    }
    if (closed || failure != null) {
      staged.clear();
      return;
    }
    try {
      flushApplies();
    } catch (Throwable error) {
      fail(error);
    }
  }

  private void complete(
      Entry entry, String token, PendingWrite pending, int length, TSStatus status)
      throws IOException {
    complete(entry, token, pending, length, status, System.nanoTime());
  }

  private void complete(
      Entry entry, String token, PendingWrite pending, int length, TSStatus status, long completed)
      throws IOException {
    StateMachineApplyResult.requireCompleted(status);
    if (engine != null && entry.id.index >= appliedBelow) {
      // During construction only compacted partitions are applied; they end below the anchor
      // that refreshAppliedBelow reads right after, so the set never holds them. A staged entry
      // whose partition compacted before its flush stays below the anchor too.
      appliedIds.add(entry.id);
    }
    appliedCount++;
    APPLIED_ENTRIES.increment();
    Long admitted = admissionStarts.remove(entry.id);
    if (admitted != null) {
      LEADER_APPLY.record(completed - admitted);
    } else if (observingAppend) {
      appendApplyCompletions.put(entry.id, completed);
    }
    if (token != null) {
      results.put(token, status);
    }
    if (pending != null && entry.id.equals(pending.identity)) {
      waiting.remove(pending.key);
      pendingBytes = Math.max(0, pendingBytes - length);
      pending.liveRequest = null;
      pending.response.complete(status);
    }
  }

  private static void bind(PendingWrite pending, EntryId identity) throws IOException {
    if (pending.identity != null && !pending.identity.equals(identity)) {
      throw new IOException("TRaft pending request changed its immutable entry identity");
    }
    pending.identity = identity;
  }

  // Request envelope: a header byte, a forwarded request's token (DataOutput.writeUTF), then the
  // serialized request.
  private static final int LOCAL_REQUEST = 2;
  private static final int TOKEN_REQUEST = 3;

  private static byte[] envelope(String token, byte[] payload) throws IOException {
    if (token == null) {
      byte[] out = new byte[1 + payload.length];
      out[0] = LOCAL_REQUEST;
      System.arraycopy(payload, 0, out, 1, payload.length);
      return out;
    }
    // Sized up front (modified UTF-8 takes at most three bytes per char): no regrowth copies.
    ByteArrayOutputStream bytes =
        new ByteArrayOutputStream(3 + 3 * token.length() + payload.length);
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeByte(TOKEN_REQUEST);
    out.writeUTF(token);
    out.write(payload);
    return bytes.toByteArray();
  }

  public CompletableFuture<Void> campaign() {
    return submit(
        () -> {
          try {
            engine.startElection();
            resetElection();
            drive();
            return null;
          } catch (Throwable e) {
            fail(e);
            throw new IOException("TRaft campaign failed", e);
          }
        });
  }

  /**
   * Whether this replica applied a committed identity, such as a read fence. Only committed
   * identities may be asked: below appliedBelow the answer comes from the index alone.
   */
  public CompletableFuture<Boolean> hasApplied(EntryId identity) {
    return submit(() -> identity.index < appliedBelow || appliedIds.contains(identity));
  }

  public CompletableFuture<DataSet> read(IConsensusRequest request) {
    return afterApplied(() -> machine.read(request));
  }

  public CompletableFuture<Void> takeSnapshot() {
    return submit(
            () -> {
              // Under asynchronous durability the journal records of applied entries may be
              // unforced; a durable snapshot must not name identities that the journal could
              // still lose.
              store.forceNow();
              long below = appliedBelow;
              Set<EntryId> ids = new TreeSet<>(appliedIds);
              return (Callable<Void>)
                  () -> {
                    writeSnapshot(below, ids);
                    return null;
                  };
            })
        .thenCompose(this::afterStagedRuns);
  }

  /**
   * Runs {@code operation} once every run staged before this call reached the state machine: on the
   * applier behind them when there is one, else on the group executor.
   */
  private <T> CompletableFuture<T> afterApplied(Callable<T> operation) {
    return submit(() -> operation).thenCompose(this::afterStagedRuns);
  }

  private <T> CompletableFuture<T> afterStagedRuns(Callable<T> operation) {
    CompletableFuture<T> result = new CompletableFuture<>();
    Runnable run =
        () -> {
          try {
            result.complete(operation.call());
          } catch (Throwable e) {
            result.completeExceptionally(e);
          }
        };
    GroupExecutor queue = applier;
    if (queue == null) {
      // Synchronous application: the submitted task already ran after every earlier one.
      run.run();
    } else {
      queue.execute(run);
    }
    return result;
  }

  private void writeSnapshot(long below, Set<EntryId> ids) throws IOException {
    String name = "snapshot-" + UUID.randomUUID();
    File path = new File(directory, name);
    AtomicFiles.createDirectories(path);
    if (!machine.takeSnapshot(path)) {
      throw new IOException("DataRegion snapshot failed");
    }
    AtomicFiles.forceTree(path);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    out.writeUTF(name);
    // Format 2: a negative marker, then appliedBelow, then the identities at or above it.
    // A format-1 pointer starts with the identity count and stays readable.
    out.writeInt(SNAPSHOT_FORMAT_2);
    out.writeLong(below);
    out.writeInt(ids.size());
    for (EntryId id : ids) {
      out.writeLong(id.term);
      out.writeLong(id.index);
    }
    AtomicFiles.write(new File(directory, "application-snapshot"), bytes.toByteArray());
  }

  private void loadSnapshot() throws IOException {
    File pointer = new File(directory, "application-snapshot");
    byte[] bytes = AtomicFiles.read(pointer);
    if (bytes == null) {
      if (pointer.exists()) {
        throw new IOException("Corrupt TRaft snapshot pointer");
      }
      return;
    }
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
    String name = in.readUTF();
    if (!name.matches("snapshot-[a-f0-9-]+")) {
      throw new IOException("Invalid snapshot path");
    }
    File path = new File(directory, name);
    if (!path.isDirectory()) {
      throw new IOException("Missing TRaft application snapshot");
    }
    int count = in.readInt();
    if (count == SNAPSHOT_FORMAT_2) {
      appliedBelow = in.readLong();
      if (appliedBelow < 0) {
        throw new IOException("Invalid snapshot applied bound");
      }
      count = in.readInt();
    }
    if (count < 0 || count > bytes.length / 16) {
      throw new IOException("Invalid snapshot identity count");
    }
    for (int i = 0; i < count; i++) {
      appliedIds.add(new EntryId(in.readLong(), in.readLong()));
    }
    if (in.available() != 0) {
      throw new IOException("Trailing snapshot metadata");
    }
    machine.loadSnapshot(path);
  }

  private void fail(Throwable error) {
    if (failure == null) {
      LOG.error("TRaft group {} stopped after a protocol/storage failure", groupId, error);
    }
    failure = error;
    for (PendingWrite pending : waiting.values()) {
      pending.liveRequest = null;
      pending.response.completeExceptionally(error);
    }
    waiting.clear();
    admittedHere.clear();
    admissions.clear();
    admissionStarts.clear();
    barrierIdentities.clear();
    pendingBytes = 0;
    staged.clear();
    abortEffects(error, false);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    ticker.cancel(false);
    CompletableFuture<Void> done = new CompletableFuture<>();
    executor.execute(
        () -> {
          GroupExecutor queue = applier;
          if (queue != null) {
            // Reads and snapshots queued before the close finish before the machine stops; runs
            // not yet written are skipped, and their clients fail below.
            CompletableFuture<Void> drained = new CompletableFuture<>();
            queue.execute(() -> drained.complete(null));
            try {
              drained.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
              LOG.warn("TRaft group {} closes before its applier drained", groupId, e);
            }
            queue.close();
          }
          IOException stopped = new IOException("TRaft group closed");
          for (PendingWrite pending : waiting.values()) {
            pending.liveRequest = null;
            pending.response.completeExceptionally(stopped);
          }
          waiting.clear();
          admittedHere.clear();
          admissions.clear();
          admissionStarts.clear();
          pendingBytes = 0;
          try {
            machine.stop();
          } catch (Throwable e) {
            done.completeExceptionally(e);
          } finally {
            try {
              store.close();
            } catch (IOException e) {
              done.completeExceptionally(e);
            }
            abortEffects(stopped, false);
            executor.close();
            done.complete(null);
          }
        });
    try {
      done.get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    } catch (Exception e) {
      throw new IOException("Cannot close TRaft group", e);
    }
  }

  static final class Frame {
    final byte[] bytes;
    final String dataKey;
    final long reservation;
    // Commit notices may wait for the next data frame; data and other controls never do.
    final boolean deferrable;

    Frame(byte[] bytes, String dataKey, long reservation) {
      this(bytes, dataKey, reservation, false);
    }

    Frame(byte[] bytes, String dataKey, long reservation, boolean deferrable) {
      this.bytes = bytes;
      this.dataKey = dataKey;
      this.reservation = reservation;
      this.deferrable = deferrable;
    }
  }

  interface BatchSender {
    /** Delivers one batch and returns the receiver's replies to this node (never null). */
    List<ByteBuffer> send(TEndPoint endpoint, List<ByteBuffer> messages) throws Exception;
  }

  /** Bounded batches and in-flight windows; failed delivery is retried by the protocol. */
  static final class Channel {
    final TEndPoint endpoint;
    final RaftConfig.Replication limits;
    final Executor network;
    final BooleanSupplier closed;
    final BatchSender sender;
    final Consumer<List<ByteBuffer>> replies;
    final ArrayDeque<Frame> queue = new ArrayDeque<>();
    // Commit-notice-only batches waiting to share a dispatch with later frames.
    final ArrayDeque<Frame> deferred = new ArrayDeque<>();
    final Set<String> outstandingData = new HashSet<>();
    int active;
    long reservedBytes;

    Channel(
        TEndPoint endpoint,
        RaftConfig.Replication limits,
        Executor network,
        BooleanSupplier closed,
        BatchSender sender) {
      this(endpoint, limits, network, closed, sender, ignored -> {});
    }

    Channel(
        TEndPoint endpoint,
        RaftConfig.Replication limits,
        Executor network,
        BooleanSupplier closed,
        BatchSender sender,
        Consumer<List<ByteBuffer>> replies) {
      this.endpoint = endpoint;
      this.limits = limits;
      this.network = network;
      this.closed = closed;
      this.sender = sender;
      this.replies = replies;
    }

    enum Reservation {
      ACCEPTED,
      DUPLICATE,
      BYTE_LIMIT,
      CLOSED
    }

    synchronized boolean reserve(long bytes, String dataKey) {
      return tryReserve(bytes, dataKey, true) == Reservation.ACCEPTED;
    }

    synchronized Reservation tryReserve(
        long bytes, String dataKey, boolean payloadBudgetAvailable) {
      if (closed.getAsBoolean()) {
        return Reservation.CLOSED;
      }
      if (dataKey != null && outstandingData.contains(dataKey)) {
        return Reservation.DUPLICATE;
      }
      if (bytes < 0 || bytes > limits.maxInflightBytes - reservedBytes || !payloadBudgetAvailable) {
        return Reservation.BYTE_LIMIT;
      }
      reservedBytes += bytes;
      if (dataKey != null) {
        outstandingData.add(dataKey);
      }
      return Reservation.ACCEPTED;
    }

    synchronized void release(long bytes) {
      reservedBytes -= bytes;
    }

    synchronized void cancel(long bytes, String dataKey) {
      release(bytes);
      if (dataKey != null) {
        outstandingData.remove(dataKey);
      }
    }

    synchronized void cancelReserved(List<Frame> frames) {
      for (Frame frame : frames) {
        cancel(frame.reservation, frame.dataKey);
      }
    }

    synchronized void offerReserved(List<Frame> frames) {
      offerReserved(frames, false);
    }

    /**
     * With {@code allowDeferral}, a batch made only of deferrable frames is held back until the
     * next non-deferrable batch or an explicit flush, so idle commit notices do not cost a round
     * trip of their own. Held frames keep their order ahead of the batch that releases them.
     */
    synchronized void offerReserved(List<Frame> frames, boolean allowDeferral) {
      if (closed.getAsBoolean()) {
        cancelReserved(frames);
        return;
      }
      boolean onlyDeferrable = allowDeferral && !frames.isEmpty();
      for (Frame frame : frames) {
        release(frame.reservation - frame.bytes.length);
        if (!frame.deferrable) {
          onlyDeferrable = false;
        }
      }
      if (onlyDeferrable) {
        deferred.addAll(frames);
        return;
      }
      queue.addAll(deferred);
      deferred.clear();
      queue.addAll(frames);
      dispatch();
    }

    synchronized void flushDeferred() {
      if (deferred.isEmpty()) {
        return;
      }
      if (closed.getAsBoolean()) {
        cancelReserved(new ArrayList<>(deferred));
        deferred.clear();
        return;
      }
      queue.addAll(deferred);
      deferred.clear();
      dispatch();
    }

    synchronized int deferredFrames() {
      return deferred.size();
    }

    private synchronized void dispatch() {
      while (!closed.getAsBoolean() && active < limits.maxInflightBatches && !queue.isEmpty()) {
        List<ByteBuffer> batch = new ArrayList<>();
        List<String> dataKeys = new ArrayList<>();
        long bytes = 0;
        while (!queue.isEmpty()
            && batch.size() < limits.maxEntriesPerAppend
            && (bytes == 0 || bytes + queue.peekFirst().bytes.length <= limits.maxBytesPerAppend)) {
          Frame frame = queue.removeFirst();
          bytes += frame.bytes.length;
          batch.add(ByteBuffer.wrap(frame.bytes));
          if (frame.dataKey != null) {
            dataKeys.add(frame.dataKey);
          }
        }
        active++;
        final long activeBytes = bytes;
        Runnable delivery =
            () -> {
              try {
                ResearchMetrics.add("traft.replication_rpc_attempts", 1);
                ResearchMetrics.add("traft.protocol_frame_bytes_offered", activeBytes);
                List<ByteBuffer> answer = sender.send(endpoint, batch);
                if (answer != null && !answer.isEmpty()) {
                  replies.accept(answer);
                }
              } catch (Exception e) {
                LOG.debug("TRaft delivery to {} deferred: {}", endpoint, e.toString());
              } finally {
                synchronized (Channel.this) {
                  active--;
                  release(activeBytes);
                  outstandingData.removeAll(dataKeys);
                  dispatch();
                }
              }
            };
        try {
          network.execute(delivery);
        } catch (RejectedExecutionException rejected) {
          active--;
          release(activeBytes);
          outstandingData.removeAll(dataKeys);
          LOG.debug("TRaft delivery to {} was not scheduled: {}", endpoint, rejected.toString());
          return;
        }
      }
    }
  }
}
