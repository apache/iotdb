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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.RequestSemantics;
import org.apache.iotdb.consensus.common.request.RequestSemanticsProvider;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine;
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine.StringRequest;
import org.apache.iotdb.consensus.raft.storage.AtomicFiles;
import org.apache.iotdb.consensus.traft.TRaftConsensus;
import org.apache.iotdb.consensus.traft.paper.core.TRaftCodec;
import org.apache.iotdb.consensus.traft.paper.core.TRaftEngine;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperBatch;
import org.apache.iotdb.consensus.traft.paper.thrift.TPaperControl;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Real Thrift sockets, fsync journal and empty-machine restart; no in-JVM transport registry. */
public class TRaftRuntimeTest {
  private static final ConsensusGroupId GID = new DataRegionId(431);
  private final List<IConsensus> nodes = new ArrayList<>();
  private final List<RecordingStateMachine> machines = new ArrayList<>();
  private final List<File> directories = new ArrayList<>();
  private final List<Peer> peers = new ArrayList<>();
  private final Map<String, String> properties = new HashMap<>();
  private boolean controlledTransport;
  private final FaultSettings[] faults = {
    new FaultSettings(), new FaultSettings(), new FaultSettings()
  };

  private static final class TrackingStateMachine extends RecordingStateMachine {
    final AtomicInteger liveDecoded = new AtomicInteger();
    final AtomicInteger bytesDecoded = new AtomicInteger();
    volatile IConsensusRequest lastLive;

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      if (request instanceof ByteBufferConsensusRequest) {
        bytesDecoded.incrementAndGet();
      } else {
        liveDecoded.incrementAndGet();
        lastLive = request;
      }
      return super.deserializeRequest(request);
    }
  }

  private static final class FaultSettings {
    volatile boolean dropAll;
    volatile int dropFromNode = -1;
    volatile int dropParity = -1;
    volatile long transferDelayMillis;
    volatile boolean dropTransfers;
    volatile boolean dropCommits;
    volatile boolean dropEntryAcks;
    volatile Long awaitNormalProposalTimestamp;
    volatile FaultSettings receiptDropTarget;
    volatile int dropTransferReceiptFrom = -1;
    final AtomicInteger transfers = new AtomicInteger();
    final Map<Long, EntryId> proposalFirstIds = new ConcurrentHashMap<>();
    final Map<EntryId, AtomicInteger> payloadProposals = new ConcurrentHashMap<>();
    final Set<EntryId> fetchedEntries = ConcurrentHashMap.newKeySet();
    final Set<EntryId> dropTransferReceipts = ConcurrentHashMap.newKeySet();
    final Set<EntryId> droppedTransferReceipts = ConcurrentHashMap.newKeySet();
    final Map<Integer, Long> votes = new ConcurrentHashMap<>();
    final Queue<String> events = new ConcurrentLinkedQueue<>();
  }

  /** The socket and framing remain real; only selected decoded messages are delayed/dropped. */
  private static final class FaultyConsensus extends TRaftConsensus {
    private final FaultSettings faults;

    FaultyConsensus(ConsensusConfig config, IStateMachine.Registry registry, FaultSettings faults) {
      super(config, registry);
      this.faults = faults;
    }

    @Override
    protected CompletableFuture<List<ByteBuffer>> accept(TPaperBatch batch, boolean replyAllowed)
        throws ConsensusException {
      List<ByteBuffer> keep = new ArrayList<>();
      for (ByteBuffer buffer : batch.getMessages()) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        final Message message;
        try {
          message = TRaftCodec.decodeMessage(bytes);
        } catch (java.io.IOException e) {
          throw new ConsensusException(e);
        }
        if (faults.dropAll || message.from == faults.dropFromNode) {
          continue;
        }
        Long awaitedTimestamp = faults.awaitNormalProposalTimestamp;
        if (awaitedTimestamp != null) {
          if (message.type != MessageType.PROPOSAL
              || message.proposal == null
              || message.proposal.recovery
              || message.proposal.first.kind != Kind.DATA
              || message.proposal.first.timestamp != awaitedTimestamp) {
            continue;
          }
          // The delayed replica first sees this normal successor, with its older prefix missing.
          if (faults.receiptDropTarget != null) {
            faults.receiptDropTarget.dropTransferReceipts.add(message.proposal.id());
          }
          faults.awaitNormalProposalTimestamp = null;
        }
        if (message.type == MessageType.PROPOSAL && message.proposal != null) {
          if (message.proposal.first.kind == Kind.DATA) {
            faults.proposalFirstIds.put(message.proposal.first.timestamp, message.proposal.id());
          }
          if (message.entry != null && message.entry.hasPayload()) {
            faults
                .payloadProposals
                .computeIfAbsent(message.entry.id, ignored -> new AtomicInteger())
                .incrementAndGet();
          }
        }
        if (message.type == MessageType.FETCH_ENTRY && message.reference != null) {
          faults.fetchedEntries.add(message.reference);
        }
        if (message.type == MessageType.TRANSFER_ACK && message.reference != null) {
          if (message.from == faults.dropTransferReceiptFrom
              && faults.dropTransferReceipts.remove(message.reference)) {
            faults.droppedTransferReceipts.add(message.reference);
            continue;
          }
        }
        if (faults.transferDelayMillis > 0 || faults.dropTransfers) {
          faults.events.add(
              System.nanoTime() / 1_000_000
                  + " "
                  + message.type
                  + " term="
                  + message.term
                  + " id="
                  + (message.entry == null ? message.reference : message.entry.id));
        }
        if (message.type == MessageType.VOTE_REQUEST) {
          faults.votes.merge(message.from, message.term, Math::max);
        }
        if ((faults.dropCommits && message.type == MessageType.COMMIT)
            || (faults.dropEntryAcks && message.type == MessageType.ENTRY_ACK)) {
          continue;
        }
        if (message.type == MessageType.ENTRY
            && message.entry != null
            && message.entry.id.index % 2 == faults.dropParity) {
          continue;
        }
        if (message.type == MessageType.TRANSFER_ENTRY) {
          faults.transfers.incrementAndGet();
          if (faults.dropTransfers) {
            continue;
          }
          try {
            Thread.sleep(faults.transferDelayMillis);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConsensusException(e);
          }
        }
        keep.add(buffer);
      }
      if (keep.isEmpty()) {
        return CompletableFuture.completedFuture(new ArrayList<>());
      }
      return super.accept(new TPaperBatch(batch.getGroupId(), keep), replyAllowed);
    }
  }

  public static final class DataRequest extends StringRequest implements RequestSemanticsProvider {
    private final long timestamp;

    DataRequest(String value, long timestamp) {
      super(value);
      this.timestamp = timestamp;
    }

    @Override
    public RequestSemantics getRequestSemantics() {
      return RequestSemantics.data(timestamp);
    }
  }

  public static final class BarrierRequest extends StringRequest
      implements RequestSemanticsProvider {
    BarrierRequest(String value) {
      super(value);
    }

    @Override
    public RequestSemantics getRequestSemantics() {
      return RequestSemantics.barrier();
    }
  }

  private void property(String name, String value) {
    if (!properties.containsKey(name)) {
      properties.put(name, System.getProperty(name));
    }
    System.setProperty(name, value);
  }

  @Before
  public void setUp() throws Exception {
    // Keep durable progress below the deliberately short election deadlines in this fixture.
    // Production retains its 1000 ms default group-force window.
    property("traft.journal_flush_interval_ms", "10");
    property("raft.election_timeout_min_ms", "400");
    property("raft.election_timeout_max_ms", "800");
    property("raft.heartbeat_interval_ms", "60");
    property("raft.write_timeout_ms", "2500");
    property("raft.read_index_timeout_ms", "2500");
    property("raft.rpc_timeout_ms", "3000");
    property("raft.core_threads", "2");
    property("raft.log_cache_bytes", "8");
    property("raft.read_consistency", "READ_INDEX");
    property("traft.partition_max_entries", "4");
    property("traft.partition_max_ms", "100");
    // These scenarios exercise the paper's group-wide timestamp rule (boundaries on lower stamps).
    property("traft.order_scope", "GLOBAL");
    Set<Integer> allocated = new HashSet<>();
    for (int i = 0; i < 3; i++) {
      int port;
      do {
        try (ServerSocket socket = new ServerSocket(0)) {
          port = socket.getLocalPort();
        }
      } while (!allocated.add(port));
      peers.add(new Peer(GID, i + 1, new TEndPoint("127.0.0.1", port)));
      directories.add(Files.createTempDirectory("traft-paper-node-" + i).toFile());
      machines.add(new TrackingStateMachine());
      nodes.add(newServer(i));
    }
    for (IConsensus node : nodes) {
      node.start();
    }
    for (IConsensus node : nodes) {
      node.createLocalPeer(GID, peers);
    }
  }

  private IConsensus newServer(int i) {
    ConsensusConfig config =
        ConsensusConfig.newBuilder()
            .setThisNodeId(peers.get(i).getNodeId())
            .setThisNode(peers.get(i).getEndpoint())
            .setStorageDir(directories.get(i).getAbsolutePath())
            .setConsensusGroupType(TConsensusGroupType.DataRegion)
            .build();
    if (controlledTransport) {
      return new FaultyConsensus(config, gid -> machines.get(i), faults[i]);
    }
    return ConsensusFactory.getConsensusImpl(
            TRaftConsensus.CLASS_NAME, config, gid -> machines.get(i))
        .orElseThrow(() -> new IllegalStateException("TRaft factory unavailable"));
  }

  private void stop(int i) throws Exception {
    IConsensus old = nodes.set(i, null);
    if (old != null) {
      old.stop();
    }
  }

  private void restart(int i) throws Exception {
    machines.set(i, new TrackingStateMachine());
    nodes.set(i, newServer(i));
    nodes.get(i).start();
  }

  @After
  public void tearDown() throws Exception {
    Exception failure = null;
    for (int i = 0; i < nodes.size(); i++) {
      try {
        stop(i);
      } catch (Exception e) {
        failure = e;
      }
    }
    for (File directory : directories) {
      org.apache.iotdb.commons.utils.FileUtils.deleteFileOrDirectory(directory);
    }
    for (Map.Entry<String, String> p : properties.entrySet()) {
      if (p.getValue() == null) {
        System.clearProperty(p.getKey());
      } else {
        System.setProperty(p.getKey(), p.getValue());
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  private static void await(String description, BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out: " + description);
      }
      Thread.sleep(20);
    }
  }

  private int leader() {
    for (int i = 0; i < nodes.size(); i++) {
      if (nodes.get(i) != null && nodes.get(i).isLeaderReady(GID)) {
        return i;
      }
    }
    return -1;
  }

  private int awaitLeader() throws Exception {
    await("ready leader", () -> leader() >= 0);
    return leader();
  }

  private void write(int node, String value, long timestamp) throws Exception {
    TSStatus response = nodes.get(node).write(GID, new DataRequest(value, timestamp));
    assertEquals(
        response.toString(), TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getCode());
  }

  private void barrier(int node, String value) throws Exception {
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        nodes.get(node).write(GID, new BarrierRequest(value)).getCode());
  }

  private void assertContents(List<String> expected) throws Exception {
    await(
        "all replicas applied " + expected,
        () -> {
          for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i) != null
                && machines.get(i).snapshotApplied().size() < expected.size()) {
              return false;
            }
          }
          return true;
        });
    for (int i = 0; i < nodes.size(); i++) {
      if (nodes.get(i) != null) {
        List<String> actual = machines.get(i).snapshotApplied();
        assertEquals("No duplicate apply on replica " + i, expected.size(), actual.size());
        assertEquals(new HashSet<>(expected), new HashSet<>(actual));
      }
    }
  }

  private List<String> read(int node) throws Exception {
    return ((RecordingStateMachine.AppliedView)
            nodes.get(node).read(GID, new StringRequest("read")))
        .applied;
  }

  @Test
  public void typedDataBarrierForwardingAndFollowerReadUseRealRpc() throws Exception {
    int lead = awaitLeader();
    List<String> expected = Arrays.asList("high", "low", "equal", "barrier", "after", "forwarded");
    write(lead, "high", 100);
    write(lead, "low", -4);
    write(lead, "equal", -4);
    barrier(lead, "barrier");
    write(lead, "after", Long.MAX_VALUE);
    int follower = (lead + 1) % 3;
    await("follower knows ready leader", () -> nodes.get(follower).getLeader(GID) != null);
    write(follower, "forwarded", 7);
    assertContents(expected);
    assertEquals(new HashSet<>(expected), new HashSet<>(read(follower)));
    for (RecordingStateMachine machine : machines) {
      List<String> values = machine.snapshotApplied();
      assertTrue(values.indexOf("barrier") > values.indexOf("equal"));
      assertTrue(values.indexOf("after") > values.indexOf("barrier"));
    }
    File journalDirectory = new File(nodes.get(lead).getRegionDirFromConsensusGroupId(GID));
    stop(lead);
    boolean negativeData = false, barrierSeen = false;
    try (TRaftFileStore store = new TRaftFileStore(journalDirectory)) {
      for (DurableRecord record : store.load()) {
        Entry entry =
            record.entry != null
                ? record.entry
                : record.proposal != null ? record.proposal.first : null;
        if (entry != null) {
          negativeData |= entry.kind == Kind.DATA && entry.timestamp == -4;
          barrierSeen |= entry.kind == Kind.BARRIER;
        }
      }
    }
    assertTrue("Typed DATA timestamp must survive wire encoding and fsync", negativeData);
    assertTrue("Typed BARRIER must survive wire encoding and fsync", barrierSeen);
  }

  @Test
  public void normalBoundaryCarriesItsFirstPayloadWithoutFetchingIt() throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    controlledTransport = true;
    property("traft.partition_max_entries", "10000");
    property("traft.partition_max_ms", "0");
    property("raft.log_cache_bytes", "0");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int lead = awaitLeader();
    write(lead, "before-piggyback", 100);
    write(lead, "piggyback-first", -10);
    assertContents(Arrays.asList("before-piggyback", "piggyback-first"));
    EntryId first = null;
    for (int i = 0; i < nodes.size(); i++) {
      if (i == lead) {
        continue;
      }
      final int follower = i;
      await(
          "normal first payload delivered to follower " + i,
          () -> faults[follower].proposalFirstIds.containsKey(-10L));
      EntryId observed = faults[i].proposalFirstIds.get(-10L);
      assertTrue(
          "The normal proposal must carry its first payload",
          faults[i].payloadProposals.containsKey(observed));
      if (first == null) {
        first = observed;
      } else {
        assertEquals(first, observed);
      }
    }
    assertNotNull(first);
    assertFalse(
        "The first payload must not need a separate FETCH round trip",
        faults[lead].fetchedEntries.contains(first));
  }

  @Test
  public void oneEntryWindowRepairsMissingPrefixAfterLosingFirstPayloadReceipt() throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    controlledTransport = true;
    property("traft.partition_max_entries", "10000");
    property("traft.partition_max_ms", "0");
    property("raft.max_inflight_batches", "1");
    property("raft.max_entries_per_append", "1");
    property("raft.log_cache_bytes", "0");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int original = awaitLeader();
    int delayed = (original + 1) % nodes.size();
    stop(delayed);
    write(original, "prefix-a", 100);
    write(original, "prefix-b", 101);
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    // Re-election starts the complete leader with fresh transport windows. The third replica
    // retains its older durable state; it must repair that state through real Thrift messages.
    for (int i = 0; i < nodes.size(); i++) {
      if (i != delayed) {
        restart(i);
      }
    }
    int lead = awaitLeader();
    long term = nodes.get(lead).getLogicalClock(GID);
    faults[delayed].awaitNormalProposalTimestamp = -20L;
    faults[delayed].receiptDropTarget = faults[lead];
    faults[lead].dropTransferReceiptFrom = peers.get(delayed).getNodeId();
    restart(delayed);
    write(lead, "first-after-missing-prefix", -20);
    assertContents(Arrays.asList("prefix-a", "prefix-b", "first-after-missing-prefix"));
    EntryId first = faults[delayed].proposalFirstIds.get(-20L);
    assertNotNull(first);
    assertTrue(
        "The selected first durable receipt must actually be lost",
        faults[lead].droppedTransferReceipts.contains(first));
    assertNotNull(faults[delayed].payloadProposals.get(first));
    assertTrue(
        "A duplicate first payload must repair the lost receipt",
        faults[delayed].payloadProposals.get(first).get() >= 2);
    assertTrue(
        "Prefix repair must fetch older data through the same one-entry window",
        faults[delayed].transfers.get() >= 2);
    assertFalse(
        "Even after receipt loss, the already-durable first needs no FETCH",
        faults[lead].fetchedEntries.contains(first));
    assertEquals(
        "Repair must not depend on another election", term, nodes.get(lead).getLogicalClock(GID));
  }

  @Test
  public void leaderFailureRecoversAndReplaysIntoEmptyRestartedMachine() throws Exception {
    int old = awaitLeader();
    write(old, "before-1", 30);
    write(old, "before-2", 10);
    barrier(old, "before-fence");
    List<String> expected = new ArrayList<>(Arrays.asList("before-1", "before-2", "before-fence"));
    assertContents(expected);
    stop(old);
    int elected = awaitLeader();
    assertNotEquals(old, elected);
    write(elected, "after-1", 20);
    barrier(elected, "after-fence");
    expected.add("after-1");
    expected.add("after-fence");
    restart(old);
    assertContents(expected);
    assertEquals(new HashSet<>(expected), new HashSet<>(read(old)));
    assertEquals(
        "Restart must rebuild from journal without relying on the old object",
        0,
        machines.get(old).snapshotsLoaded.get());
  }

  private void assertPayloadJournalWithoutProgressRecords() throws Exception {
    for (File directory : directories) {
      File journal = new File(directory, GID.getType().getValue() + "_" + GID.getId());
      int installed = 0;
      try (TRaftFileStore store = new TRaftFileStore(journal)) {
        for (DurableRecord record : store.load()) {
          assertNotEquals(
              "Runtime application progress is reconstructed after confirmation",
              RecordType.APPLIED,
              record.type);
          assertNotEquals(
              "Runtime commit knowledge is re-established from the leader/quorum",
              RecordType.COMMIT,
              record.type);
          if (record.type == RecordType.INSTALL) {
            installed++;
          }
        }
      }
      assertTrue("Acknowledged commands must still have durable installed data", installed > 0);
    }
  }

  @Test
  public void repeatedFullRestartReconfirmsWithoutCommitOrAppliedRecords() throws Exception {
    int lead = awaitLeader();
    List<String> expected = Arrays.asList("plain-high", "plain-low", "plain-fence", "plain-after");
    write(lead, "plain-high", 20);
    write(lead, "plain-low", -3);
    barrier(lead, "plain-fence");
    write(lead, "plain-after", 30);
    assertContents(expected);
    for (int round = 0; round < 2; round++) {
      for (int i = 0; i < nodes.size(); i++) {
        stop(i);
      }
      assertPayloadJournalWithoutProgressRecords();
      for (int i = 0; i < nodes.size(); i++) {
        restart(i);
      }
      awaitLeader();
      assertContents(expected);
      for (RecordingStateMachine machine : machines) {
        assertEquals(0, machine.snapshotsLoaded.get());
      }
      assertEquals(new HashSet<>(expected), new HashSet<>(read(leader())));
    }
  }

  @Test
  public void cacheZeroUsesTheExactLocalRequestAndRemoteAndRestartUseBytes() throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    property("raft.log_cache_bytes", "0");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int lead = awaitLeader();
    TrackingStateMachine leaderMachine = (TrackingStateMachine) machines.get(lead);
    DataRequest original = new DataRequest("local-cache-zero", 10);
    assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        nodes.get(lead).write(GID, original).getCode());
    assertContents(Collections.singletonList(original.value));
    assertSame(
        "The original object must reach deserializeRequest", original, leaderMachine.lastLive);
    assertEquals(1, leaderMachine.liveDecoded.get());
    assertEquals(
        "Local apply must not deserialize the durable bytes", 0, leaderMachine.bytesDecoded.get());
    for (int i = 0; i < nodes.size(); i++) {
      if (i != lead) {
        assertEquals(1, ((TrackingStateMachine) machines.get(i)).bytesDecoded.get());
        assertEquals(0, ((TrackingStateMachine) machines.get(i)).liveDecoded.get());
        assertEquals(1, machines.get(i).remoteApplied.get());
      }
    }
    int follower = (lead + 1) % 3;
    await("forwarder learns leader", () -> nodes.get(follower).getLeader(GID) != null);
    write(follower, "forwarded-cache-zero", 11);
    List<String> expected = Arrays.asList(original.value, "forwarded-cache-zero");
    assertContents(expected);
    assertEquals(1, leaderMachine.liveDecoded.get());
    assertEquals(1, leaderMachine.bytesDecoded.get());
    assertEquals(
        "Forwarding/fallback must not change leader-side application semantics",
        0,
        leaderMachine.remoteApplied.get());
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    assertPayloadJournalWithoutProgressRecords();
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    awaitLeader();
    assertContents(expected);
    for (RecordingStateMachine machine : machines) {
      assertEquals(
          "No live object survives restart", 0, ((TrackingStateMachine) machine).liveDecoded.get());
      assertEquals(2, ((TrackingStateMachine) machine).bytesDecoded.get());
    }
  }

  @Test
  public void asyncJournalCommitsWithoutPerWriteForcesAndRecoversAfterRestart() throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    property("traft.journal_mode", "ASYNC");
    property("traft.journal_async_fsync_ms", "60000");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int lead = awaitLeader();
    List<String> expected = new ArrayList<>();
    long forces = ResearchMetrics.value("traft.log_force_count");
    for (int i = 0; i < 20; i++) {
      expected.add("async-" + i);
      write(lead, "async-" + i, 1000 + i);
    }
    assertContents(expected);
    assertTrue(
        "Only votes and the timed flush force an asynchronous journal",
        ResearchMetrics.value("traft.log_force_count") - forces < expected.size());
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    lead = awaitLeader();
    write(lead, "after-restart", 2000);
    expected.add("after-restart");
    assertContents(expected);
  }

  @Test
  public void timedOutLocalRequestStillAppliesFromBytesAfterDurableAcksArrive() throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    controlledTransport = true;
    property("traft.partition_max_ms", "0");
    property("traft.partition_max_entries", "10000");
    property("raft.log_cache_bytes", "0");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int lead = awaitLeader();
    TrackingStateMachine machine = (TrackingStateMachine) machines.get(lead);
    faults[lead].dropEntryAcks = true;
    try {
      write(lead, "timed-out-local", 17);
      fail("No entry ACK reaches the leader before timeout");
    } catch (ConsensusException expected) {
      assertTrue(machine.snapshotApplied().isEmpty());
    }
    faults[lead].dropEntryAcks = false;
    assertContents(Collections.singletonList("timed-out-local"));
    assertTrue(nodes.get(lead).isLeaderReady(GID));
    assertEquals("Cancellation releases the live optimization", 0, machine.liveDecoded.get());
    assertEquals(
        "The accepted log entry still applies after the caller gives up",
        1,
        machine.bytesDecoded.get());
    assertEquals(
        "A current leader remains local even without a live request",
        0,
        machine.remoteApplied.get());
  }

  @Test
  public void lostCommitNotificationsAndFullRestartStillPreserveAcknowledgedWrite()
      throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
      faults[i].dropCommits = true;
    }
    controlledTransport = true;
    property("traft.partition_max_ms", "0");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int lead = awaitLeader();
    write(lead, "ack-before-notification", 51);
    assertEquals(
        Collections.singletonList("ack-before-notification"), machines.get(lead).snapshotApplied());
    for (int i = 0; i < nodes.size(); i++) {
      if (i != lead) {
        assertTrue(
            "Followers have no commit notification", machines.get(i).snapshotApplied().isEmpty());
      }
      stop(i);
      faults[i].dropCommits = false;
    }
    assertPayloadJournalWithoutProgressRecords();
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    awaitLeader();
    assertContents(Collections.singletonList("ack-before-notification"));
  }

  @Test
  public void restartWithoutQuorumWaitsForNewCommitEvidenceBeforeServingReads() throws Exception {
    int previousLeader = awaitLeader();
    write(previousLeader, "known-before-restart", 8);
    assertContents(Collections.singletonList("known-before-restart"));
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    assertPayloadJournalWithoutProgressRecords();
    restart(previousLeader);
    assertFalse(nodes.get(previousLeader).isLeaderReady(GID));
    assertTrue(
        "INSTALL alone must not be treated as commit",
        machines.get(previousLeader).snapshotApplied().isEmpty());
    try {
      read(previousLeader);
      fail("A restarted minority must not serve a linearizable read from unconfirmed history");
    } catch (ConsensusException expected) {
      assertFalse(nodes.get(previousLeader).isLeaderReady(GID));
    }
    for (int i = 0; i < nodes.size(); i++) {
      if (i != previousLeader) {
        restart(i);
      }
    }
    awaitLeader();
    assertContents(Collections.singletonList("known-before-restart"));
  }

  private long legacyProgressRecords(int node) throws Exception {
    File journal = new File(directories.get(node), GID.getType().getValue() + "_" + GID.getId());
    long count = 0;
    try (TRaftFileStore store = new TRaftFileStore(journal)) {
      for (DurableRecord record : store.load()) {
        if (record.type == RecordType.COMMIT || record.type == RecordType.APPLIED) {
          count++;
        }
      }
    }
    return count;
  }

  @Test
  public void legacyCommitAndAppliedJournalStillRebuildsBeforeNewQuorum() throws Exception {
    int lead = awaitLeader();
    write(lead, "legacy-data", 19);
    barrier(lead, "legacy-barrier");
    List<String> expected = new ArrayList<>(Arrays.asList("legacy-data", "legacy-barrier"));
    assertContents(expected);
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    long[] legacyCounts = new long[nodes.size()];
    for (int i = 0; i < nodes.size(); i++) {
      File journal = new File(directories.get(i), GID.getType().getValue() + "_" + GID.getId());
      try (TRaftFileStore store = new TRaftFileStore(journal)) {
        TRaftEngine recovered =
            new TRaftEngine(
                i + 1, new HashSet<>(Arrays.asList(1, 2, 3)), 4, 100, store, entry -> {});
        List<EntryId> installed = new ArrayList<>();
        for (Entry entry : recovered.installedEntries()) {
          installed.add(entry.id);
        }
        store.append(
            new DurableRecord(
                RecordType.COMMIT, recovered.term(), -1, null, null, null, -1, installed));
        for (EntryId identity : installed) {
          store.append(
              new DurableRecord(
                  RecordType.APPLIED, recovered.term(), -1, null, null, identity, -1, null));
        }
      }
      legacyCounts[i] = legacyProgressRecords(i);
    }
    restart(0);
    assertEquals(
        "Legacy durable commit knowledge remains replayable",
        new HashSet<>(expected),
        new HashSet<>(machines.get(0).snapshotApplied()));
    assertEquals(expected.size(), machines.get(0).snapshotApplied().size());
    assertFalse(nodes.get(0).isLeaderReady(GID));
    for (int i = 1; i < nodes.size(); i++) {
      restart(i);
    }
    lead = awaitLeader();
    write(lead, "new-runtime-data", 20);
    expected.add("new-runtime-data");
    assertContents(expected);
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
      assertEquals(
          "New runtime must not append progress records",
          legacyCounts[i],
          legacyProgressRecords(i));
    }
  }

  @Test
  public void fullRestartRestoresSnapshotAndSuffixWithoutDuplicateApply() throws Exception {
    int lead = awaitLeader();
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      write(lead, "snapshot-" + i, 20 - i);
      expected.add("snapshot-" + i);
    }
    barrier(lead, "snapshot-fence");
    expected.add("snapshot-fence");
    assertContents(expected);
    for (IConsensus node : nodes) {
      node.triggerSnapshot(GID, true);
    }
    for (int i = 0; i < 4; i++) {
      write(lead, "suffix-" + i, i - 5);
      expected.add("suffix-" + i);
    }
    barrier(lead, "suffix-fence");
    expected.add("suffix-fence");
    assertContents(expected);
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    assertPayloadJournalWithoutProgressRecords();
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    awaitLeader();
    assertContents(expected);
    for (RecordingStateMachine machine : machines) {
      assertEquals(1, machine.snapshotsLoaded.get());
    }
    assertEquals(new HashSet<>(expected), new HashSet<>(read(leader())));
  }

  private static final class RecoveryScenario {
    final int candidate;
    final int source;
    final long term;
    final long startNanos;
    final List<String> expected;

    RecoveryScenario(int candidate, int source, long term, long startNanos, List<String> expected) {
      this.candidate = candidate;
      this.source = source;
      this.term = term;
      this.startNanos = startNanos;
      this.expected = expected;
    }
  }

  private RecoveryScenario beginSparseRecovery(boolean stall) throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    controlledTransport = true;
    property("traft.partition_max_entries", "10000");
    property("traft.partition_max_ms", "0");
    // A large initial window allows disjoint losses without one loss blocking later entries.
    property("raft.max_inflight_batches", "8");
    property("raft.max_entries_per_append", "256");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int original = awaitLeader();
    int candidate = (original + 1) % 3;
    int source = (original + 2) % 3;
    faults[candidate].dropParity = 0;
    faults[source].dropParity = 1;
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      String value = "sparse-" + i;
      write(original, value, 10000 + i);
      expected.add(value);
    }
    await(
        "two equally ranked sparse replicas",
        () ->
            machines.get(candidate).applied.size() == 20
                && machines.get(source).applied.size() == 20);
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
      faults[i].dropParity = -1;
      faults[i].votes.clear();
    }
    // Exactly one transferred entry can be outstanding. Each side needs twenty 30ms
    // transfers, exceeding the candidate's timer while making continuous durable progress.
    property("raft.max_inflight_batches", "1");
    property("raft.max_entries_per_append", "1");
    property("raft.election_timeout_min_ms", "200");
    property("raft.election_timeout_max_ms", "300");
    property("raft.heartbeat_interval_ms", "40");
    faults[candidate].transferDelayMillis = 30;
    faults[source].transferDelayMillis = 30;
    faults[candidate].dropTransfers = stall;
    restart(candidate);
    // Keep the source available without racing the designated candidate's test election.
    property("raft.election_timeout_min_ms", "2000");
    property("raft.election_timeout_max_ms", "3000");
    restart(source);
    long start = System.nanoTime();
    ((TRaftConsensus) nodes.get(candidate))
        .control(new TPaperControl(GID.convertToTConsensusGroupId(), "CAMPAIGN"));
    long term = nodes.get(candidate).getLogicalClock(GID);
    return new RecoveryScenario(candidate, source, term, start, expected);
  }

  @Test
  public void progressingRecoveryLongerThanElectionTimeoutKeepsItsTerm() throws Exception {
    RecoveryScenario recovery = beginSparseRecovery(false);
    try {
      await(
          "candidate recovers the remote sparse half",
          () -> nodes.get(recovery.candidate).isLeaderReady(GID));
    } catch (AssertionError failure) {
      throw new AssertionError(
          failure.getMessage() + ": " + faults[recovery.candidate].events, failure);
    }
    assertTrue(
        "Recovery must span the configured 300ms maximum election timeout",
        System.nanoTime() - recovery.startNanos > TimeUnit.MILLISECONDS.toNanos(300));
    assertTrue(faults[recovery.candidate].transfers.get() >= 20);
    assertTrue(
        "The source must repair its own sparse half",
        faults[recovery.source].transfers.get() >= 20);
    assertEquals(
        "Valid recovery progress must not trigger a fresh election: "
            + faults[recovery.candidate].events,
        recovery.term,
        nodes.get(recovery.candidate).getLogicalClock(GID));
    assertContents(recovery.expected);
  }

  @Test
  public void stalledRecoveryEventuallyCampaignsAgain() throws Exception {
    RecoveryScenario recovery = beginSparseRecovery(true);
    await("recovery transfer attempted", () -> faults[recovery.candidate].transfers.get() > 0);
    int candidateId = peers.get(recovery.candidate).getNodeId();
    await(
        "a new election request after recovery stalls",
        () -> faults[recovery.source].votes.getOrDefault(candidateId, -1L) > recovery.term);
    assertFalse(
        "Missing recovery payload must prevent client service",
        nodes.get(recovery.candidate).isLeaderReady(GID));
  }

  @Test
  public void oversizeAdmissionIsRejectedWithoutFencingTheHealthyGroup() throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    property("raft.max_inflight_bytes", "65536");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int lead = awaitLeader();
    char[] oversized = new char[65536];
    Arrays.fill(oversized, 'x');
    try {
      write(lead, new String(oversized), 100);
      fail("A payload above the transport budget must be rejected before journaling");
    } catch (ConsensusException expected) {
      /* Expected admission failure, not a storage failure. */
    }
    assertTrue(
        "Rejected client input must not fence the leader", nodes.get(lead).isLeaderReady(GID));
    write(lead, "after-rejection", 101);
    assertContents(Arrays.asList("after-rejection"));
    assertEquals(Arrays.asList("after-rejection"), read((lead + 1) % 3));
  }

  @Test
  public void minorityCannotAcknowledgeWriteOrServeLinearizableRead() throws Exception {
    int lead = awaitLeader();
    write(lead, "committed", 1);
    assertContents(Arrays.asList("committed"));
    for (int i = 0; i < nodes.size(); i++) {
      if (i != lead) {
        stop(i);
      }
    }
    try {
      write(lead, "minority-only", 2);
      fail("A single voter must not acknowledge a new entry");
    } catch (ConsensusException expected) {
      /* no quorum */
    }
    assertFalse(machines.get(lead).snapshotApplied().contains("minority-only"));
    try {
      read(lead);
      fail("A single voter must not complete a fresh read fence");
    } catch (ConsensusException expected) {
      /* no quorum */
    }
  }

  /** Restarts with fault hooks and one long partition; returns a leader holding "committed". */
  private int isolateLeaderAfterACommittedWrite() throws Exception {
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    controlledTransport = true;
    property("traft.partition_max_entries", "10000");
    property("traft.partition_max_ms", "0");
    property("raft.write_timeout_ms", "20000");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    int old = awaitLeader();
    write(old, "committed", 1);
    assertContents(Collections.singletonList("committed"));
    faults[old].dropAll = true;
    for (int i = 0; i < nodes.size(); i++) {
      if (i != old) {
        faults[i].dropFromNode = peers.get(old).getNodeId();
      }
    }
    return old;
  }

  private int awaitLeaderOtherThan(int old) throws Exception {
    int a = (old + 1) % nodes.size();
    int b = (old + 2) % nodes.size();
    await(
        "a leader among the others",
        () -> nodes.get(a).isLeaderReady(GID) || nodes.get(b).isLeaderReady(GID));
    return nodes.get(a).isLeaderReady(GID) ? a : b;
  }

  @Test
  public void deposedLeaderMustNotAcknowledgeAWriteTheRecoveredHistoryExcluded() throws Exception {
    final int old = isolateLeaderAfterACommittedWrite();
    final IConsensus deposed = nodes.get(old);
    CompletableFuture<TSStatus> client = new CompletableFuture<>();
    Thread writer =
        new Thread(
            () -> {
              try {
                client.complete(deposed.write(GID, new DataRequest("excluded", 2)));
              } catch (Throwable e) {
                client.completeExceptionally(e);
              }
            });
    writer.start();
    int current = awaitLeaderOtherThan(old);
    write(current, "after", 3);
    // The new leader reaches the old one before the old one's stale frames reach anybody.
    faults[old].dropAll = false;
    boolean acknowledged;
    try {
      acknowledged =
          client.get(30, TimeUnit.SECONDS).getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    } catch (java.util.concurrent.ExecutionException expected) {
      acknowledged = false;
    }
    writer.join();
    for (FaultSettings fault : faults) {
      fault.dropFromNode = -1;
    }
    await("old leader caught up", () -> machines.get(old).snapshotApplied().contains("after"));
    assertFalse(
        "A client was told that a write the recovered history excluded succeeded", acknowledged);
    assertFalse(machines.get(old).snapshotApplied().contains("excluded"));
    assertFalse(read(current).contains("excluded"));
  }

  @Test
  public void restartedOldLeaderMustNotApplyAnEntryTheRecoveredHistoryExcluded() throws Exception {
    final int old = isolateLeaderAfterACommittedWrite();
    final IConsensus deposed = nodes.get(old);
    long journal = journalBytes(old);
    Thread writer =
        new Thread(
            () -> {
              try {
                deposed.write(GID, new DataRequest("excluded", 2));
              } catch (Throwable expected) {
                // The node stops under this client.
              }
            });
    writer.start();
    await("the write reached the old leader's journal", () -> journalBytes(old) > journal);
    int current = awaitLeaderOtherThan(old);
    write(current, "after", 3);
    stop(old);
    writer.join();
    for (FaultSettings fault : faults) {
      fault.dropAll = false;
      fault.dropFromNode = -1;
    }
    restart(old);
    await("old leader caught up", () -> machines.get(old).snapshotApplied().contains("after"));
    assertFalse(machines.get(old).snapshotApplied().contains("excluded"));
    assertFalse(read(current).contains("excluded"));
  }

  private File groupDirectory(int node) {
    return new File(directories.get(node), GID.getType().getValue() + "_" + GID.getId());
  }

  @SuppressWarnings("unchecked")
  private TRaftGroup groupOf(int node) throws Exception {
    Field groups = TRaftConsensus.class.getDeclaredField("groups");
    groups.setAccessible(true);
    return ((Map<ConsensusGroupId, TRaftGroup>) groups.get(nodes.get(node))).get(GID);
  }

  private TRaftGroup.Status status(int node) throws Exception {
    return ((TRaftConsensus) nodes.get(node)).groupStatus(GID);
  }

  /** A read fence through the leader: an empty BARRIER whose identity the reply names. */
  private EntryId fence(int leader) throws Exception {
    TSStatus reply =
        ((TRaftConsensus) nodes.get(leader))
            .control(new TPaperControl(GID.convertToTConsensusGroupId(), "FENCE"));
    String[] identity = reply.getMessage().split(":");
    return new EntryId(Long.parseLong(identity[0]), Long.parseLong(identity[1]));
  }

  private void awaitCompactedBeyond(long partitions) throws Exception {
    await(
        "every replica compacted beyond " + partitions + " partitions",
        () -> {
          try {
            for (int i = 0; i < nodes.size(); i++) {
              TRaftGroup.Status s = status(i);
              if (s.compactedThrough < partitions || s.compactedThrough < s.currentPartition - 3) {
                return false;
              }
            }
            return true;
          } catch (Exception e) {
            return false;
          }
        });
  }

  @Test
  public void compactionBoundsStateAndARestartRebuildsTheCompactedPrefixOnce() throws Exception {
    int lead = awaitLeader();
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      write(lead, "compact-" + i, 1_000 + i);
      expected.add("compact-" + i);
    }
    EntryId oldFence = fence(lead);
    for (int i = 30; i < 60; i++) {
      write(lead, "compact-" + i, 1_000 + i);
      expected.add("compact-" + i);
    }
    assertContents(expected);
    awaitCompactedBeyond(12);
    for (int i = 0; i < nodes.size(); i++) {
      TRaftGroup.Status s = status(i);
      assertTrue("installed " + s.stateSizes, s.stateSizes.get("installed") < 40);
      assertTrue("appliedIds " + s.stateSizes, s.stateSizes.get("appliedIds") < 40);
      assertTrue("a compacted fence stays applied", groupOf(i).hasApplied(oldFence).get());
    }
    // A follower restarts with an empty machine: its COMPACT records re-apply the compacted
    // prefix during construction, the leader certifies the suffix, nothing applies twice.
    int follower = (lead + 1) % nodes.size();
    long compacted = status(follower).compactedThrough;
    stop(follower);
    restart(follower);
    assertTrue(status(follower).compactedThrough >= compacted);
    assertContents(expected);
    assertTrue(groupOf(follower).hasApplied(oldFence).get());
    assertTrue(
        "the rebuilt prefix was applied as remote-leader work",
        machines.get(follower).remoteApplied.get() >= expected.size());
    for (int i = 60; i < 75; i++) {
      write(lead, "compact-" + i, 1_000 + i);
      expected.add("compact-" + i);
    }
    assertContents(expected);
  }

  @Test
  public void snapshotPointerKeepsTheAppliedBoundAndTheOldFormatStillLoads() throws Exception {
    int lead = awaitLeader();
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 45; i++) {
      write(lead, "snap-" + i, 500 + i);
      expected.add("snap-" + i);
    }
    assertContents(expected);
    awaitCompactedBeyond(8);
    for (IConsensus node : nodes) {
      node.triggerSnapshot(GID, true);
    }
    for (int i = 0; i < nodes.size(); i++) {
      byte[] pointer = AtomicFiles.read(new File(groupDirectory(i), "application-snapshot"));
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(pointer));
      in.readUTF();
      assertEquals("format 2 marker", -2, in.readInt());
      assertTrue("the applied bound covers the compacted prefix", in.readLong() > 20);
      assertTrue("only the uncompacted identities are listed", in.readInt() < 30);
    }
    for (int i = 45; i < 50; i++) {
      write(lead, "snap-" + i, 500 + i);
      expected.add("snap-" + i);
    }
    assertContents(expected);
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    awaitLeader();
    assertContents(expected);
    for (RecordingStateMachine machine : machines) {
      assertEquals(1, machine.snapshotsLoaded.get());
    }
    // A pointer written before format 2 (no bound, every applied identity) still loads.
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
    }
    property("traft.compaction", "false");
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    lead = awaitLeader();
    assertContents(expected);
    write(lead, "snap-old-format", 9_000);
    expected.add("snap-old-format");
    assertContents(expected);
    for (IConsensus node : nodes) {
      node.triggerSnapshot(GID, true);
    }
    for (int i = 0; i < nodes.size(); i++) {
      stop(i);
      File file = new File(groupDirectory(i), "application-snapshot");
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(AtomicFiles.read(file)));
      String name = in.readUTF();
      assertEquals(-2, in.readInt());
      long below = in.readLong();
      int count = in.readInt();
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytes);
      out.writeUTF(name);
      out.writeInt(count);
      for (int k = 0; k < count; k++) {
        out.writeLong(in.readLong());
        out.writeLong(in.readLong());
      }
      if (below == 0) {
        // Rewritten in the old format; with a nonzero bound it would under-report and is kept.
        AtomicFiles.write(file, bytes.toByteArray());
      }
    }
    for (int i = 0; i < nodes.size(); i++) {
      restart(i);
    }
    awaitLeader();
    assertContents(expected);
  }

  private long journalBytes(int node) {
    try (java.util.stream.Stream<java.nio.file.Path> files =
        Files.walk(directories.get(node).toPath())) {
      return files
          .filter(path -> path.getFileName().toString().equals("paper-journal-v1"))
          .mapToLong(path -> path.toFile().length())
          .sum();
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
