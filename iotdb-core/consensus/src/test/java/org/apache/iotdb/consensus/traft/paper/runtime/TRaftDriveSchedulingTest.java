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
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.research.ResearchMetrics;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.raft.server.RecordingStateMachine;
import org.apache.iotdb.consensus.traft.paper.core.TRaftCodec;
import org.apache.iotdb.consensus.traft.paper.core.TRaftEngine;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableStore;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Role;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Real group/journal, manually advanced executors, and deterministic in-memory peer delivery. */
public class TRaftDriveSchedulingTest {
  private static final DataRegionId ID = new DataRegionId(851);

  private static final class ManualExecutor extends AbstractExecutorService {
    final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    boolean shutdown;
    boolean inline;

    @Override
    public void execute(Runnable command) {
      if (shutdown) {
        throw new RejectedExecutionException();
      }
      if (inline) {
        command.run();
      } else {
        tasks.add(command);
      }
    }

    void runAll() {
      int budget = 1000;
      Runnable task;
      while ((task = tasks.poll()) != null) {
        assertTrue("Scheduled work must quiesce without timer progress", budget-- > 0);
        task.run();
      }
    }

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      List<Runnable> remaining = new ArrayList<>(tasks);
      tasks.clear();
      return remaining;
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown && tasks.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return isTerminated();
    }
  }

  private static final class DormantFuture extends FutureTask<Void>
      implements ScheduledFuture<Void> {
    DormantFuture() {
      super(() -> null);
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return Long.MAX_VALUE;
    }

    @Override
    public int compareTo(Delayed other) {
      return 0;
    }
  }

  private static final class ManualTimers extends ScheduledThreadPoolExecutor {
    ManualTimers() {
      super(1);
    }

    @Override
    @SuppressWarnings("unsafeThreadSchedule")
    public ScheduledFuture<?> scheduleWithFixedDelay(
        Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return new DormantFuture();
    }
  }

  private static final class Machine extends RecordingStateMachine {
    Runnable afterWrite = () -> {};
    final List<Integer> batchSizes = new ArrayList<>();
    boolean rejectWrites;
    int stopped;
    int liveDecodes;
    int byteDecodes;
    final List<List<String>> snapshots = new ArrayList<>();

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      if (request instanceof RecordingStateMachine.StringRequest) {
        liveDecodes++;
      } else {
        byteDecodes++;
      }
      return super.deserializeRequest(request);
    }

    @Override
    public TSStatus write(IConsensusRequest request) {
      if (rejectWrites) {
        return new TSStatus(TSStatusCode.WRITE_PROCESS_ERROR.getStatusCode());
      }
      TSStatus result = super.write(request);
      afterWrite.run();
      return result;
    }

    @Override
    public List<TSStatus> writeBatch(List<IConsensusRequest> requests) {
      batchSizes.add(requests.size());
      return super.writeBatch(requests);
    }

    @Override
    public boolean takeSnapshot(File directory) {
      snapshots.add(snapshotApplied());
      return super.takeSnapshot(directory);
    }

    @Override
    public void stop() {
      stopped++;
    }
  }

  private static final class Fixture implements AutoCloseable {
    final ManualExecutor cores = new ManualExecutor();
    final ManualExecutor network = new ManualExecutor();
    final ManualTimers timers = new ManualTimers();
    final Machine machine = new Machine();
    final Map<Integer, TRaftEngine> others = new LinkedHashMap<>();
    final List<List<Message>> batches = new ArrayList<>();
    final List<Message> held = new ArrayList<>();
    final Map<Integer, List<List<DurableRecord>>> durableCalls = new LinkedHashMap<>();
    final List<CompletableFuture<?>> probes = new ArrayList<>();
    final RaftConfig config = new RaftConfig();
    final File directory;
    final TRaftGroup group;
    final TRaftEngine engine;
    final TRaftFileStore store;
    final Map<Integer, TRaftGroup.Channel> channels;
    boolean holdResponses;
    // While set, frames from the group's node to its peers are lost.
    boolean partitioned;

    @SuppressWarnings("unchecked")
    Fixture(int members, int batchSize) throws Exception {
      this(members, batchSize, 1000);
    }

    @SuppressWarnings("unchecked")
    Fixture(int members, int batchSize, int theta) throws Exception {
      config.replication.maxEntriesPerAppend = batchSize;
      config.log.cacheBytes = 0;
      List<Peer> peers = new ArrayList<>();
      Set<Integer> memberIds = new HashSet<>();
      for (int id = 1; id <= members; id++) {
        memberIds.add(id);
        peers.add(new Peer(ID, id, new TEndPoint("127.0.0.1", 18000 + id)));
      }
      directory = Files.createTempDirectory("traft-drive-schedule-").toFile();
      group =
          new TRaftGroup(
              1, ID, directory, peers, machine, config, theta, 0, null, null, cores, network,
              timers);
      Field engineField = TRaftGroup.class.getDeclaredField("engine");
      engineField.setAccessible(true);
      engine = (TRaftEngine) engineField.get(group);
      Field storeField = TRaftGroup.class.getDeclaredField("store");
      storeField.setAccessible(true);
      store = (TRaftFileStore) storeField.get(group);
      Field channelField = TRaftGroup.class.getDeclaredField("channels");
      channelField.setAccessible(true);
      channels = (Map<Integer, TRaftGroup.Channel>) channelField.get(group);
      for (Peer peer : peers) {
        int id = peer.getNodeId();
        if (id == 1) {
          continue;
        }
        final List<DurableRecord> records = new ArrayList<>();
        final List<List<DurableRecord>> calls = new ArrayList<>();
        durableCalls.put(id, calls);
        DurableStore store =
            new DurableStore() {
              @Override
              public List<DurableRecord> load() {
                return new ArrayList<>(records);
              }

              @Override
              public void append(DurableRecord record) {
                records.add(record);
                calls.add(Collections.singletonList(record));
              }

              @Override
              public void appendBatch(List<DurableRecord> batch) {
                records.addAll(batch);
                calls.add(new ArrayList<>(batch));
              }
            };
        TRaftEngine remote = new TRaftEngine(id, memberIds, theta, 0, store, entry -> {});
        others.put(id, remote);
        channels.put(
            id,
            new TRaftGroup.Channel(
                peer.getEndpoint(),
                config.replication,
                network,
                () -> false,
                (endpoint, frames) -> {
                  if (partitioned) {
                    return Collections.emptyList();
                  }
                  List<Message> messages = new ArrayList<>();
                  for (ByteBuffer frame : frames) {
                    byte[] bytes = new byte[frame.remaining()];
                    frame.duplicate().get(bytes);
                    Message message = TRaftCodec.decodeMessage(bytes);
                    messages.add(message);
                    remote.receive(message);
                  }
                  batches.add(messages);
                  for (Message response : remote.drainMessages()) {
                    if (holdResponses) {
                      held.add(response);
                    } else {
                      receive(response);
                    }
                  }
                  return Collections.emptyList();
                }));
      }
      CompletableFuture<Void> campaign = group.campaign();
      pump();
      campaign.get(1, TimeUnit.SECONDS);
      assertTrue(group.ready());
      batches.clear();
      for (List<List<DurableRecord>> calls : durableCalls.values()) {
        calls.clear();
      }
    }

    CompletableFuture<Void> receive(Message message) throws Exception {
      return receive(Collections.singletonList(message));
    }

    CompletableFuture<Void> receive(List<Message> messages) throws Exception {
      return group.receive(frames(messages));
    }

    /** Delivers as an RPC request whose response may carry the replies. */
    CompletableFuture<List<ByteBuffer>> receiveWithReplies(List<Message> messages)
        throws Exception {
      return group.receive(frames(messages), true);
    }

    static List<ByteBuffer> frames(List<Message> messages) throws Exception {
      List<ByteBuffer> frames = new ArrayList<>();
      for (Message message : messages) {
        frames.add(ByteBuffer.wrap(TRaftCodec.encodeMessage(message)));
      }
      return frames;
    }

    static List<Message> decode(List<ByteBuffer> frames) throws Exception {
      List<Message> messages = new ArrayList<>();
      for (ByteBuffer frame : frames) {
        byte[] bytes = new byte[frame.remaining()];
        frame.duplicate().get(bytes);
        messages.add(TRaftCodec.decodeMessage(bytes));
      }
      return messages;
    }

    /** Commit notices held back for piggybacking; the real runtime flushes them on its ticker. */
    int flushDeferredNotices() {
      int flushed = 0;
      for (TRaftGroup.Channel channel : channels.values()) {
        flushed += channel.deferredFrames();
        channel.flushDeferred();
      }
      return flushed;
    }

    void electPeer(int peer) throws Exception {
      TRaftEngine candidate = others.get(peer);
      candidate.startElection();
      Deque<Message> messages = new ArrayDeque<>(candidate.drainMessages());
      int budget = 1000;
      while (!messages.isEmpty()) {
        assertTrue("Peer election must finish", budget-- > 0);
        Message message = messages.removeFirst();
        TRaftEngine receiver = message.to == 1 ? engine : others.get(message.to);
        receiver.receive(message);
        messages.addAll(receiver.drainMessages());
      }
      assertEquals(Role.LEADER, candidate.role());
      assertEquals(Role.FOLLOWER, engine.role());
      CompletableFuture<Void> published = group.receive(Collections.emptyList());
      pump();
      published.get(1, TimeUnit.SECONDS);
      batches.clear();
    }

    long peerTimestamp = 10;

    List<Message> peerEntries(int count) throws Exception {
      TRaftEngine leader = others.get(2);
      for (int i = 0; i < count; i++) {
        long timestamp = peerTimestamp++;
        String value = "peer-" + timestamp;
        assertNotNull(leader.append(Kind.DATA, timestamp, framed(value), 0));
      }
      List<Message> messages = new ArrayList<>();
      for (Message message : leader.drainMessages()) {
        if (message.to == 1 && message.type == MessageType.ENTRY) {
          messages.add(message);
        }
      }
      assertEquals(count, messages.size());
      return messages;
    }

    CompletableFuture<TSStatus> write(String token, long timestamp) {
      RecordingStateMachine.StringRequest request = new RecordingStateMachine.StringRequest(token);
      return group.write(
          token, Kind.DATA, timestamp, token.getBytes(StandardCharsets.UTF_8), request);
    }

    void probe(Callable<Void> assertion) {
      probes.add(group.submit(assertion));
    }

    void checkProbes() throws Exception {
      for (CompletableFuture<?> probe : probes) {
        probe.get(1, TimeUnit.SECONDS);
      }
    }

    void pump() throws Exception {
      int budget = 1000;
      while (!cores.tasks.isEmpty() || !network.tasks.isEmpty()) {
        assertTrue("Protocol delivery must quiesce", budget-- > 0);
        cores.runAll();
        network.runAll();
        if (cores.tasks.isEmpty() && network.tasks.isEmpty()) {
          flushDeferredNotices();
        }
      }
    }

    void electWithoutPublishing() throws Exception {
      engine.startElection();
      Deque<Message> messages = new ArrayDeque<>(engine.drainMessages());
      int budget = 1000;
      while (!messages.isEmpty()) {
        assertTrue(budget-- > 0);
        Message message = messages.removeFirst();
        TRaftEngine receiver = message.to == 1 ? engine : others.get(message.to);
        receiver.receive(message);
        messages.addAll(receiver.drainMessages());
      }
      assertEquals(Role.LEADER, engine.role());
    }

    @Override
    public void close() throws Exception {
      cores.runAll();
      cores.inline = true;
      try {
        group.close();
      } finally {
        timers.shutdownNow();
        network.shutdownNow();
        cores.shutdownNow();
        FileUtils.deleteFileOrDirectory(directory);
      }
    }
  }

  private static byte[] framed(String token) throws Exception {
    byte[] payload = token.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(bytes);
    // TRaftGroup's envelope of a request that keeps its token.
    output.writeByte(3);
    output.writeUTF(token);
    output.write(payload);
    return bytes.toByteArray();
  }

  private static long forces() {
    return ResearchMetrics.value("traft.log_force_count");
  }

  @Test
  public void ordinaryAdmissionsShareOneForceAndBindLiveRequestsBeforeRemoteAcks()
      throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      long before = forces();
      long forceNanos = ResearchMetrics.value("traft.log_force_nanos");
      long admitted = ResearchMetrics.value("traft.admitted_data");
      long applied = ResearchMetrics.value("traft.leader_apply.count");
      long applyNanos = ResearchMetrics.value("traft.leader_apply.sum_nanos");
      CompletableFuture<TSStatus> first = f.write("first", 10);
      CompletableFuture<TSStatus> cancelled = f.write("cancelled", 20);
      CompletableFuture<TSStatus> third = f.write("third", 30);
      cancelled.cancel(false);
      f.holdResponses = true;
      assertEquals(before, forces());
      assertFalse(first.isDone());
      assertFalse(third.isDone());
      f.cores.runAll();
      assertEquals("Three ordinary durable INSTALL frames share one force", before + 1, forces());
      long batchForceNanos = ResearchMetrics.value("traft.log_force_nanos") - forceNanos;
      assertEquals(admitted + 3, ResearchMetrics.value("traft.admitted_data"));
      assertTrue(f.machine.applied.isEmpty());
      assertFalse(first.isDone());
      assertFalse(third.isDone());
      f.network.runAll();
      assertFalse(
          "Remote durability without delivery of its ACK is not client success", first.isDone());
      f.holdResponses = false;
      for (Message response : f.held) {
        f.receive(response);
      }
      f.held.clear();
      f.pump();
      first.get(1, TimeUnit.SECONDS);
      third.get(1, TimeUnit.SECONDS);
      assertTrue(cancelled.isCancelled());
      assertEquals(Arrays.asList("first", "cancelled", "third"), f.machine.snapshotApplied());
      assertEquals(
          "Both uncancelled identities keep their local request", 2, f.machine.liveDecodes);
      assertEquals(
          "Cancellation drops the fast path, not the journal entry", 1, f.machine.byteDecodes);
      assertEquals(applied + 3, ResearchMetrics.value("traft.leader_apply.count"));
      assertTrue(
          "Every request's admission latency includes the shared force",
          ResearchMetrics.value("traft.leader_apply.sum_nanos") - applyNanos
              >= 3 * batchForceNanos);
    }
  }

  @Test
  public void ordinaryAdmissionBatchesRespectCountAndByteLimits() throws Exception {
    for (boolean byteBound : Arrays.asList(false, true)) {
      try (Fixture f = new Fixture(3, byteBound ? 8 : 2)) {
        if (byteBound) {
          // Each framed payload plus the conservative record allowance is 296 bytes.
          f.config.replication.maxBytesPerAppend = 600;
        }
        long before = forces();
        for (int i = 0; i < 5; i++) {
          f.write("bounded-value-" + i, i);
        }
        f.cores.runAll();
        assertEquals("Five entries must form 2 + 2 + 1 durable batches", before + 3, forces());
        assertTrue(f.machine.applied.isEmpty());
        f.pump();
        assertEquals(5, f.machine.applied.size());
      }
    }
  }

  @Test
  public void payloadAboveBatchBudgetStillUsesTheOriginalSingleEntryPath() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      f.config.replication.maxBytesPerAppend = 600;
      String payload = new String(new char[1024]).replace('\0', 'x');
      long before = forces();
      CompletableFuture<TSStatus> large =
          f.group.write(
              "large",
              Kind.DATA,
              10,
              payload.getBytes(StandardCharsets.UTF_8),
              new RecordingStateMachine.StringRequest(payload));
      CompletableFuture<TSStatus> first = f.write("first", 20);
      CompletableFuture<TSStatus> second = f.write("second", 30);
      f.cores.runAll();
      assertEquals("One large single append followed by one bounded pair", before + 2, forces());
      f.pump();
      for (CompletableFuture<TSStatus> write : Arrays.asList(large, first, second)) {
        write.get(1, TimeUnit.SECONDS);
      }
      assertEquals(Arrays.asList(payload, "first", "second"), f.machine.snapshotApplied());
    }
  }

  @Test
  public void ordinaryBatchOpensATimestampBoundaryInlineAndCountsDisorderOnce() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      long admitted = ResearchMetrics.value("traft.admitted_data");
      long disordered = ResearchMetrics.value("traft.admitted_disordered");
      List<CompletableFuture<TSStatus>> writes =
          Arrays.asList(
              f.write("ten", 10), f.write("twenty", 20), f.write("five", 5), f.write("thirty", 30));
      f.cores.runAll();
      assertTrue(f.engine.hasPendingProposal());
      assertEquals(
          "The request after a boundary joins the pending partition",
          admitted + 4,
          ResearchMetrics.value("traft.admitted_data"));
      assertEquals(disordered + 1, ResearchMetrics.value("traft.admitted_disordered"));
      assertEquals(
          2, f.engine.installedEntries().stream().filter(e -> e.kind == Kind.DATA).count());
      f.pump();
      for (CompletableFuture<TSStatus> write : writes) {
        write.get(1, TimeUnit.SECONDS);
      }
      assertEquals(Arrays.asList("ten", "twenty", "five", "thirty"), f.machine.snapshotApplied());
      assertEquals(admitted + 4, ResearchMetrics.value("traft.admitted_data"));
      assertEquals(disordered + 1, ResearchMetrics.value("traft.admitted_disordered"));
    }
  }

  @Test
  public void ordinaryBatchDoesNotAbsorbBarrierOrItsSuccessor() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      CompletableFuture<TSStatus> first = f.write("first", 10);
      CompletableFuture<TSStatus> second = f.write("second", 20);
      CompletableFuture<TSStatus> barrier =
          f.group.write(
              "barrier",
              Kind.BARRIER,
              0,
              "barrier".getBytes(StandardCharsets.UTF_8),
              new RecordingStateMachine.StringRequest("barrier"));
      CompletableFuture<TSStatus> after = f.write("after", 30);
      f.cores.runAll();
      assertTrue(f.engine.hasPendingProposal());
      assertFalse(barrier.isDone());
      assertFalse(after.isDone());
      assertEquals(
          2, f.engine.installedEntries().stream().filter(e -> e.kind == Kind.DATA).count());
      f.pump();
      for (CompletableFuture<TSStatus> write : Arrays.asList(first, second, barrier, after)) {
        write.get(1, TimeUnit.SECONDS);
      }
      assertEquals(
          Arrays.asList("first", "second", "barrier", "after"), f.machine.snapshotApplied());
    }
  }

  @Test
  public void receiverForcesOrdinaryRpcRunBeforeReturningAndSendingAcks() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      f.electPeer(2);
      List<Message> entries = f.peerEntries(3);
      long before = forces();
      CompletableFuture<Void> receive = f.receive(entries);
      assertEquals(before, forces());
      assertFalse(receive.isDone());
      assertTrue(f.network.tasks.isEmpty());
      f.cores.runAll();
      receive.get(1, TimeUnit.SECONDS);
      assertEquals(before + 1, forces());
      assertTrue("No commitment was received with these entries", f.machine.applied.isEmpty());
      f.network.runAll();
      assertEquals(1, f.batches.size());
      List<Message> acks =
          f.batches.get(0).stream()
              .filter(m -> m.type == MessageType.ENTRY_ACK)
              .collect(java.util.stream.Collectors.toList());
      assertEquals("One durable run is acknowledged by one frame", 1, acks.size());
      Set<EntryId> acknowledged = new HashSet<>(acks.get(0).identities);
      acknowledged.add(acks.get(0).reference);
      Set<EntryId> expected = new HashSet<>();
      for (Message entry : entries) {
        expected.add(entry.entry.id);
      }
      assertEquals(expected, acknowledged);
    }
  }

  @Test
  public void rpcRequestRepliesRideTheResponseInsteadOfAChannelRoundTrip() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      f.electPeer(2);
      List<Message> entries = f.peerEntries(3);
      long before = forces();
      CompletableFuture<List<ByteBuffer>> receive = f.receiveWithReplies(entries);
      f.cores.runAll();
      List<Message> replies = Fixture.decode(receive.get(1, TimeUnit.SECONDS));
      assertEquals("The run is durable before any reply exists", before + 1, forces());
      assertEquals(1, replies.size());
      assertEquals(MessageType.ENTRY_ACK, replies.get(0).type);
      assertEquals(2, replies.get(0).to);
      Set<EntryId> acknowledged = new HashSet<>(replies.get(0).identities);
      acknowledged.add(replies.get(0).reference);
      assertEquals(3, acknowledged.size());
      f.network.runAll();
      assertTrue(
          "Replies carried by the response never use the sender's channel", f.batches.isEmpty());
      // A response-delivered batch (replyAllowed=false) still routes replies through channels.
      CompletableFuture<List<ByteBuffer>> viaResponse =
          f.group.receive(Fixture.frames(f.peerEntries(2)), false);
      f.cores.runAll();
      assertTrue(viaResponse.get(1, TimeUnit.SECONDS).isEmpty());
      f.network.runAll();
      assertEquals(1, f.batches.size());
    }
  }

  @Test
  public void pipelinedJournalReleasesFramesAndRepliesOnlyAfterTheirForce() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      ManualExecutor journal = new ManualExecutor();
      f.group.pipelineJournal(journal);
      f.cores.runAll();
      long before = forces();
      CompletableFuture<TSStatus> first = f.write("first", 10);
      CompletableFuture<TSStatus> second = f.write("second", 20);
      f.cores.runAll();
      assertEquals("The protocol round only writes", before, forces());
      assertTrue(f.store.hasPendingForce());
      assertTrue("No frame is queued for delivery before the force", f.network.tasks.isEmpty());
      assertEquals("One force loop is scheduled", 1, journal.tasks.size());
      CompletableFuture<TSStatus> third = f.write("third", 30);
      f.cores.runAll();
      assertEquals("The waiting loop covers the later round too", 1, journal.tasks.size());
      journal.runAll();
      assertEquals("One force covers both rounds", before + 1, forces());
      assertFalse(f.store.hasPendingForce());
      pumpPipelined(f, journal);
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), first.get(1, TimeUnit.SECONDS).getCode());
      second.get(1, TimeUnit.SECONDS);
      third.get(1, TimeUnit.SECONDS);
      assertEquals(Arrays.asList("first", "second", "third"), f.machine.snapshotApplied());

      // As a follower, the acknowledgment carried by the RPC response waits for the force too.
      // The election helper pumps only the group and network executors; deferred INSTALL markers
      // of the last round still await a force, so let the journal run inline meanwhile.
      journal.inline = true;
      f.electPeer(2);
      journal.inline = false;
      pumpPipelined(f, journal);
      long received = forces();
      // Newer than the entries above, so the peer leader's run stays in its open partition.
      f.peerTimestamp = 100;
      CompletableFuture<List<ByteBuffer>> receive = f.receiveWithReplies(f.peerEntries(3));
      f.cores.runAll();
      assertEquals(received, forces());
      assertFalse("No ACK leaves before the run is durable", receive.isDone());
      journal.runAll();
      assertEquals(received + 1, forces());
      List<Message> replies = Fixture.decode(receive.get(1, TimeUnit.SECONDS));
      assertEquals(1, replies.size());
      assertEquals(MessageType.ENTRY_ACK, replies.get(0).type);
    }
  }

  @Test
  public void replicationBeforeForceSendsDataFirstAndCommitsOnlyAfterTheLeaderForce()
      throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      ManualExecutor journal = new ManualExecutor();
      f.group.pipelineJournal(journal);
      f.group.setReplicateBeforeForce(true);
      f.cores.runAll();
      long before = forces();
      CompletableFuture<TSStatus> write = f.write("early", 10);
      f.cores.runAll();
      assertEquals("The protocol round only writes", before, forces());
      assertTrue(f.store.hasPendingForce());
      assertFalse("Data leaves before the leader's force", f.network.tasks.isEmpty());
      assertEquals("The leader's force starts at once", 1, journal.tasks.size());
      // Both followers persist and acknowledge; their ACKs reach the group as replies.
      f.network.runAll();
      f.cores.runAll();
      assertFalse("Follower ACKs alone never commit before the leader's force", write.isDone());
      assertTrue(f.machine.snapshotApplied().isEmpty());
      journal.runAll();
      assertEquals("One force covers the write", before + 1, forces());
      pumpPipelined(f, journal);
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), write.get(1, TimeUnit.SECONDS).getCode());
      assertEquals(Collections.singletonList("early"), f.machine.snapshotApplied());
    }
  }

  @Test
  public void ackForAnAlreadyDurableEntryCommitsWithoutWaitingForNewerWrites() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      ManualExecutor journal = new ManualExecutor();
      f.group.pipelineJournal(journal);
      f.group.setReplicateBeforeForce(true);
      f.cores.runAll();
      f.holdResponses = true;
      CompletableFuture<TSStatus> first = f.write("first", 10);
      f.cores.runAll();
      f.network.runAll();
      assertFalse("The followers acknowledged the first write", f.held.isEmpty());
      journal.runAll();
      assertFalse(f.store.hasPendingForce());
      CompletableFuture<TSStatus> second = f.write("second", 20);
      f.cores.runAll();
      assertTrue("The second write is not durable yet", f.store.hasPendingForce());
      f.holdResponses = false;
      for (Message ack : new ArrayList<>(f.held)) {
        f.receive(ack);
      }
      f.held.clear();
      f.cores.runAll();
      assertEquals(
          "Its own copy is durable, so the first write commits at once",
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          first.get(1, TimeUnit.SECONDS).getCode());
      assertFalse(second.isDone());
      pumpPipelined(f, journal);
      second.get(1, TimeUnit.SECONDS);
      assertEquals(Arrays.asList("first", "second"), f.machine.snapshotApplied());
    }
  }

  @Test
  public void anApplierWritesRunsOffTheGroupExecutorAndReadsAndSnapshotsQueueBehindThem()
      throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      ManualExecutor appliers = new ManualExecutor();
      f.group.setApplyBatch(true);
      f.group.setApplyExecutor(appliers);
      f.cores.runAll();
      CompletableFuture<TSStatus> first = f.write("first", 10);
      CompletableFuture<TSStatus> second = f.write("second", 20);
      f.pump();
      // Committed and staged by the group; the state machine has not been written yet.
      assertTrue(f.machine.snapshotApplied().isEmpty());
      assertFalse(first.isDone());
      CompletableFuture<org.apache.iotdb.consensus.common.DataSet> read =
          f.group.read(new RecordingStateMachine.StringRequest("read"));
      CompletableFuture<Void> snapshot = f.group.takeSnapshot();
      f.cores.runAll();
      assertFalse("A read waits for the runs staged before it", read.isDone());
      assertFalse(snapshot.isDone());
      appliers.runAll();
      assertEquals(Arrays.asList("first", "second"), f.machine.snapshotApplied());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), first.get(1, TimeUnit.SECONDS).getCode());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), second.get(1, TimeUnit.SECONDS).getCode());
      assertEquals(
          Arrays.asList("first", "second"),
          ((RecordingStateMachine.AppliedView) read.get(1, TimeUnit.SECONDS)).applied);
      snapshot.get(1, TimeUnit.SECONDS);
      assertEquals(
          "The snapshot holds what its pointer names",
          Collections.singletonList(Arrays.asList("first", "second")),
          f.machine.snapshots);
      f.cores.runAll();
      CompletableFuture<TSStatus> third = f.write("third", 30);
      f.pump();
      appliers.runAll();
      third.get(1, TimeUnit.SECONDS);
      assertEquals(Arrays.asList("first", "second", "third"), f.machine.snapshotApplied());
    }
  }

  @Test
  public void proposalAckForADurableBoundaryConfirmsWithoutWaitingForNewerWrites()
      throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      ManualExecutor journal = new ManualExecutor();
      f.group.pipelineJournal(journal);
      f.group.setReplicateBeforeForce(true);
      f.cores.runAll();
      CompletableFuture<TSStatus> first = f.write("first", 10);
      pumpPipelined(f, journal);
      first.get(1, TimeUnit.SECONDS);
      f.holdResponses = true;
      CompletableFuture<TSStatus> boundary = f.write("boundary", 5);
      f.cores.runAll();
      assertTrue(f.engine.hasPendingProposal());
      f.network.runAll();
      assertTrue(
          "The followers acknowledged the boundary",
          f.held.stream().anyMatch(m -> m.type == MessageType.PROPOSAL_ACK));
      journal.runAll();
      assertFalse(f.store.hasPendingForce());
      // A later write joins the pending partition and is not durable yet.
      CompletableFuture<TSStatus> later = f.write("later", 20);
      f.cores.runAll();
      assertTrue(f.store.hasPendingForce());
      f.holdResponses = false;
      for (Message ack : new ArrayList<>(f.held)) {
        f.receive(ack);
      }
      f.held.clear();
      f.cores.runAll();
      assertFalse(
          "Its own copy of the boundary is durable, so the proposal is confirmed at once",
          f.engine.hasPendingProposal());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), boundary.get(1, TimeUnit.SECONDS).getCode());
      assertFalse(later.isDone());
      pumpPipelined(f, journal);
      later.get(1, TimeUnit.SECONDS);
      assertEquals(Arrays.asList("first", "boundary", "later"), f.machine.snapshotApplied());
    }
  }

  @Test
  public void bufferedForceAnswersTheTransportAtOnceAndAcknowledgesAfterItsForce()
      throws Exception {
    java.util.concurrent.ExecutorService journal =
        java.util.concurrent.Executors.newSingleThreadExecutor();
    try (Fixture f = new Fixture(3, 8)) {
      f.group.pipelineJournal(journal);
      f.cores.runAll();
      f.electPeer(2);
      f.group.setForcePolicy(60_000, 3);
      f.cores.runAll();
      f.peerTimestamp = 100;
      CompletableFuture<List<ByteBuffer>> first = f.receiveWithReplies(f.peerEntries(2));
      f.cores.runAll();
      assertTrue("A buffered force never holds the transport response", first.isDone());
      assertTrue(Fixture.decode(first.get(1, TimeUnit.SECONDS)).isEmpty());
      Thread.sleep(200);
      f.cores.runAll();
      f.network.runAll();
      assertTrue("Two of three entries are not due yet", f.batches.isEmpty());
      assertTrue(f.store.hasPendingForce());
      f.receiveWithReplies(f.peerEntries(1));
      f.cores.runAll();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (f.store.hasPendingForce() || f.network.tasks.isEmpty()) {
        assertTrue("The third entry makes the force due", System.nanoTime() < deadline);
        Thread.sleep(5);
      }
      f.network.runAll();
      Set<EntryId> acknowledged = new HashSet<>();
      for (List<Message> batch : f.batches) {
        for (Message message : batch) {
          if (message.type == MessageType.ENTRY_ACK) {
            acknowledged.add(message.reference);
            acknowledged.addAll(message.identities);
          }
        }
      }
      assertEquals("One force acknowledges all three entries", 3, acknowledged.size());
    } finally {
      journal.shutdownNow();
    }
  }

  private static void pumpPipelined(Fixture f, ManualExecutor journal) throws Exception {
    int budget = 1000;
    while (!f.cores.tasks.isEmpty() || !f.network.tasks.isEmpty() || !journal.tasks.isEmpty()) {
      assertTrue("Pipelined delivery must quiesce", budget-- > 0);
      f.cores.runAll();
      journal.runAll();
      f.network.runAll();
      if (f.cores.tasks.isEmpty() && f.network.tasks.isEmpty() && journal.tasks.isEmpty()) {
        f.flushDeferredNotices();
      }
    }
  }

  @Test
  public void batchedApplicationWritesATasksCommittedRunWithOneCallInOrder() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      f.group.setApplyBatch(true);
      f.cores.runAll();
      CompletableFuture<TSStatus> first = f.write("first", 10);
      CompletableFuture<TSStatus> second = f.write("second", 20);
      CompletableFuture<TSStatus> third = f.write("third", 30);
      // The fixture's peers acknowledge entry by entry; hand all their ACKs to one leader task,
      // as one ENTRY_ACK frame for the whole run does in the real transport.
      f.holdResponses = true;
      f.cores.runAll();
      f.network.runAll();
      f.holdResponses = false;
      assertFalse(first.isDone());
      List<Message> acks = new ArrayList<>(f.held);
      f.held.clear();
      f.receive(acks);
      f.pump();
      first.get(1, TimeUnit.SECONDS);
      second.get(1, TimeUnit.SECONDS);
      third.get(1, TimeUnit.SECONDS);
      assertEquals(Arrays.asList("first", "second", "third"), f.machine.snapshotApplied());
      assertEquals(
          "The ACK that commits all three applies them in one state machine call",
          Collections.singletonList(3),
          f.machine.batchSizes);
    }
  }

  @Test
  public void receiverOrdinaryBatchesRespectCountAndByteLimits() throws Exception {
    for (boolean byteBound : Arrays.asList(false, true)) {
      try (Fixture f = new Fixture(3, byteBound ? 8 : 2)) {
        f.electPeer(2);
        if (byteBound) {
          f.config.replication.maxBytesPerAppend = 600;
        }
        List<Message> entries = f.peerEntries(5);
        long before = forces();
        CompletableFuture<Void> receive = f.receive(entries);
        f.cores.runAll();
        receive.get(1, TimeUnit.SECONDS);
        assertEquals(
            "Inbound five-entry RPC must force bounded runs of 2 + 2 + 1", before + 3, forces());
        assertTrue(f.machine.applied.isEmpty());
      }
    }
  }

  @Test
  public void receiverCutsAtControlAndKeepsStaleTermFromRenewingElection() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      f.electPeer(2);
      List<Message> entries = f.peerEntries(4);
      Message stale =
          new Message(
              MessageType.NACK, 2, 1, f.engine.term() - 1, null, null, null, null, null, -1, null);
      List<Message> mixed =
          Arrays.asList(entries.get(0), entries.get(1), stale, entries.get(2), entries.get(3));
      long before = forces();
      CompletableFuture<Void> received = f.receive(mixed);
      f.cores.runAll();
      received.get(1, TimeUnit.SECONDS);
      assertEquals("The control frame separates two durable ordinary runs", before + 2, forces());
      Field deadline = TRaftGroup.class.getDeclaredField("electionDeadline");
      deadline.setAccessible(true);
      deadline.setLong(f.group, 123L);
      CompletableFuture<Void> staleReceived = f.receive(stale);
      f.cores.runAll();
      staleReceived.get(1, TimeUnit.SECONDS);
      assertEquals(
          "Stale traffic must retain the original election-reset rule",
          123L,
          deadline.getLong(f.group));
    }
  }

  @Test
  public void failedOrdinaryBatchDoesNotSendDataOrCompleteAnyWaiter() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      f.store.close();
      CompletableFuture<TSStatus> first = f.write("first", 10);
      CompletableFuture<TSStatus> second = f.write("second", 20);
      f.cores.runAll();
      assertTrue(first.isCompletedExceptionally());
      assertTrue(second.isCompletedExceptionally());
      assertFalse(f.group.ready());
      assertTrue(f.machine.applied.isEmpty());
      assertTrue(f.network.tasks.isEmpty());
    }
  }

  @Test
  public void receiverStorageFailureReturnsNoSuccessfulDeliveryOrEntryAck() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      f.electPeer(2);
      List<Message> entries = f.peerEntries(2);
      f.store.close();
      CompletableFuture<Void> receive = f.receive(entries);
      f.cores.runAll();
      assertTrue(receive.isCompletedExceptionally());
      assertTrue(f.machine.applied.isEmpty());
      assertTrue(f.network.tasks.isEmpty());
      assertTrue(f.engine.drainMessages().isEmpty());
    }
  }

  @Test
  public void queuedWritesWaitForApplyAndYieldBetweenBoundedAdmissionBatches() throws Exception {
    try (Fixture f = new Fixture(1, 2)) {
      List<CompletableFuture<TSStatus>> writes = new ArrayList<>();
      List<Integer> betweenBatches = new ArrayList<>();
      f.machine.afterWrite =
          () ->
              f.probe(
                  () -> {
                    betweenBatches.add(f.machine.applied.size());
                    return null;
                  });
      for (int i = 0; i < 5; i++) {
        writes.add(f.write("value-" + i, i));
      }
      f.probe(
          () -> {
            assertTrue(
                "Admission itself does not apply or acknowledge writes",
                f.machine.applied.isEmpty());
            for (CompletableFuture<TSStatus> write : writes) {
              assertFalse(write.isDone());
            }
            return null;
          });
      f.cores.runAll();
      f.checkProbes();
      assertEquals(Arrays.asList(2, 2, 4, 4, 5), betweenBatches);
      for (CompletableFuture<TSStatus> write : writes) {
        assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), write.get(1, TimeUnit.SECONDS).getCode());
      }
    }
  }

  @Test
  public void queuedAckEventsShareOneOutboundBatchAfterDurableApply() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      CompletableFuture<TSStatus> first = f.write("first", 10);
      CompletableFuture<TSStatus> second = f.write("second", 20);
      f.holdResponses = true;
      f.cores.runAll();
      assertFalse(first.isDone());
      assertFalse(second.isDone());
      f.network.runAll();
      assertEquals("Queued client events share one payload RPC per peer", 2, f.batches.size());
      for (List<Message> batch : f.batches) {
        assertEquals(2, batch.stream().filter(m -> m.type == MessageType.ENTRY).count());
      }
      f.batches.clear();
      List<CompletableFuture<Void>> receives = new ArrayList<>();
      for (Message ack : f.held) {
        if (ack.from == 2 && ack.type == MessageType.ENTRY_ACK) {
          receives.add(f.receive(ack));
        }
      }
      assertEquals(2, receives.size());
      f.probe(
          () -> {
            assertTrue(first.isDone());
            assertTrue(second.isDone());
            assertEquals(Arrays.asList("first", "second"), f.machine.snapshotApplied());
            assertTrue(
                "ACK processing is durable before the shared outbound drive",
                f.network.tasks.isEmpty());
            return null;
          });
      f.cores.runAll();
      f.checkProbes();
      for (CompletableFuture<Void> receive : receives) {
        receive.get(1, TimeUnit.SECONDS);
      }
      f.network.runAll();
      assertTrue(
          "Commit notices alone never cost a round trip; they wait for data or the ticker",
          f.batches.isEmpty());
      assertEquals(4, f.flushDeferredNotices());
      f.network.runAll();
      assertEquals(2, f.batches.size());
      for (List<Message> batch : f.batches) {
        assertEquals(
            "Both independent commit notifications share one RPC",
            2,
            batch.stream().filter(m -> m.type == MessageType.COMMIT).count());
      }
    }
  }

  @Test
  public void emptyBoundaryFirstPreservesDataOrderAndBatchesRetainBeforeAck() throws Exception {
    try (Fixture f = new Fixture(3, 8, 2)) {
      // The recovery NOOP plus first DATA fills theta=2. The next admission creates a
      // zero-byte NOOP proposal in the same drain as the preceding ordinary DATA.
      f.holdResponses = true;
      CompletableFuture<TSStatus> first = f.write("first", 10);
      CompletableFuture<TSStatus> second = f.write("second", 20);
      f.cores.runAll();
      assertTrue(f.engine.hasPendingProposal());
      assertFalse(first.isDone());
      assertFalse(second.isDone());
      f.network.runAll();
      assertEquals(2, f.batches.size());
      assertFalse("Durable remote storage alone must not complete a client", first.isDone());
      assertFalse(second.isDone());

      List<String> trace = new ArrayList<>();
      boolean dataBeforeBoundary = true;
      for (List<Message> batch : f.batches) {
        List<Message> data = new ArrayList<>();
        List<String> order = new ArrayList<>();
        for (Message message : batch) {
          if (message.entry != null && message.entry.hasPayload()) {
            data.add(message);
            order.add(message.type + ":" + message.entry.kind);
          }
        }
        trace.add("peer=" + batch.get(0).to + " messages=" + order);
        assertEquals(trace.toString(), 3, data.size());
        dataBeforeBoundary &= data.get(0).type == MessageType.ENTRY;
        dataBeforeBoundary &= data.get(0).entry.kind == Kind.DATA;
        dataBeforeBoundary &= data.get(1).type == MessageType.PROPOSAL;
        dataBeforeBoundary &= data.get(1).entry.kind == Kind.NOOP;
        // The second write joins the no-op's partition at once.
        dataBeforeBoundary &= data.get(2).type == MessageType.ENTRY;
        dataBeforeBoundary &= data.get(2).entry.kind == Kind.DATA;
        if (data.get(1).entry.kind == Kind.NOOP) {
          assertEquals(0, data.get(1).entry.payloadSize());
        }
      }
      int batchedFirstAndRetain = 0;
      for (Map.Entry<Integer, List<List<DurableRecord>>> peer : f.durableCalls.entrySet()) {
        for (List<DurableRecord> call : peer.getValue()) {
          List<RecordType> types = new ArrayList<>();
          for (DurableRecord record : call) {
            types.add(record.type);
          }
          trace.add("peer=" + peer.getKey() + " durable=" + types);
          if (types.equals(Arrays.asList(RecordType.ENTRY, RecordType.RETAIN_PROPOSAL))) {
            batchedFirstAndRetain++;
          }
        }
      }
      int unnecessaryRepair = 0;
      for (Message response : f.held) {
        if (response.type == MessageType.FETCH_ENTRY
            || response.type == MessageType.FETCH_PROPOSAL
            || response.type == MessageType.NACK) {
          unnecessaryRepair++;
        }
      }
      assertTrue(
          "Stable DATA/empty-NOOP order must permit one ENTRY+RETAIN batch per follower; "
              + "batchCalls="
              + batchedFirstAndRetain
              + ", repairMessages="
              + unnecessaryRepair
              + ", trace="
              + trace,
          dataBeforeBoundary && batchedFirstAndRetain == 2 && unnecessaryRepair == 0);

      f.holdResponses = false;
      for (Message response : f.held) {
        f.receive(response);
      }
      f.held.clear();
      f.pump();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), first.get(1, TimeUnit.SECONDS).getCode());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), second.get(1, TimeUnit.SECONDS).getCode());
      assertEquals(Arrays.asList("first", "second"), f.machine.snapshotApplied());
      for (TRaftEngine remote : f.others.values()) {
        for (Entry entry : f.engine.installedEntries()) {
          assertTrue(
              "Every installed identity eventually applies on every peer",
              remote.isApplied(entry.id));
        }
      }
    }
  }

  @Test
  public void pendingBoundaryDoesNotContinuouslyRescheduleItsBlockedAdmission() throws Exception {
    try (Fixture f = new Fixture(3, 2)) {
      CompletableFuture<TSStatus> high = f.write("high", 10);
      f.pump();
      high.get(1, TimeUnit.SECONDS);
      CompletableFuture<TSStatus> boundary = f.write("boundary", 0);
      CompletableFuture<TSStatus> after = f.write("after", 20);
      f.cores.runAll();
      assertTrue(f.engine.hasPendingProposal());
      assertTrue(f.cores.tasks.isEmpty());
      assertFalse(boundary.isDone());
      assertFalse(after.isDone());
      f.pump();
      boundary.get(1, TimeUnit.SECONDS);
      after.get(1, TimeUnit.SECONDS);
      assertEquals(Arrays.asList("high", "boundary", "after"), f.machine.snapshotApplied());
    }
  }

  @Test
  public void cancellationDoesNotRetractAdmissionAndSnapshotObservesActualApplyOrder()
      throws Exception {
    try (Fixture f = new Fixture(1, 2)) {
      CompletableFuture<TSStatus> cancelled = f.write("cancelled", 10);
      CompletableFuture<TSStatus> kept = f.write("kept", 20);
      cancelled.cancel(false);
      CompletableFuture<Void> beforeDrive = f.group.takeSnapshot();
      f.cores.runAll();
      beforeDrive.get(1, TimeUnit.SECONDS);
      assertTrue(cancelled.isCancelled());
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), kept.get(1, TimeUnit.SECONDS).getCode());
      assertEquals(Collections.singletonList(Collections.emptyList()), f.machine.snapshots);
      assertEquals(Arrays.asList("cancelled", "kept"), f.machine.snapshotApplied());
      CompletableFuture<Void> afterApply = f.group.takeSnapshot();
      f.cores.runAll();
      afterApply.get(1, TimeUnit.SECONDS);
      assertEquals(Arrays.asList("cancelled", "kept"), f.machine.snapshots.get(1));
    }
  }

  @Test
  public void actualStepdownRejectsQueuedWriteBeforeCachedRoleIsPublished() throws Exception {
    try (Fixture f = new Fixture(3, 2)) {
      CompletableFuture<TSStatus> old = f.write("old", 10);
      f.receive(
          new Message(
              MessageType.NACK, 2, 1, f.group.term() + 1, null, null, null, null, null, -1, null));
      CompletableFuture<TSStatus> afterStepdown = f.write("after-stepdown", 20);
      f.probe(
          () -> {
            assertFalse("The event that observed the stepdown also published it", f.group.ready());
            assertEquals(Role.FOLLOWER, f.engine.role());
            assertTrue(afterStepdown.isCompletedExceptionally());
            return null;
          });
      f.cores.runAll();
      f.checkProbes();
      assertTrue(old.isCompletedExceptionally());
      assertFalse(f.group.ready());
      assertTrue(f.machine.applied.isEmpty());
    }
  }

  @Test
  public void higherTermLocalLeaderMustPublishBeforeAcceptingNewAdmissions() throws Exception {
    try (Fixture f = new Fixture(3, 2)) {
      CompletableFuture<TSStatus> old = f.write("old-term", 10);
      f.probe(
          () -> {
            f.electWithoutPublishing();
            assertTrue(f.engine.term() > f.group.term());
            return null;
          });
      CompletableFuture<TSStatus> premature = f.write("unpublished-term", 20);
      f.probe(
          () -> {
            assertEquals(Role.LEADER, f.engine.role());
            assertTrue(f.group.ready());
            assertTrue(premature.isCompletedExceptionally());
            return null;
          });
      f.cores.runAll();
      f.checkProbes();
      assertTrue(old.isCompletedExceptionally());
      assertEquals(f.engine.term(), f.group.term());
      CompletableFuture<TSStatus> ready = f.write("published-term", 30);
      f.pump();
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), ready.get(1, TimeUnit.SECONDS).getCode());
      assertEquals(Collections.singletonList("published-term"), f.machine.snapshotApplied());
    }
  }

  @Test
  public void scheduledApplyFailureFencesEveryWaiterWithoutClientSuccess() throws Exception {
    try (Fixture f = new Fixture(1, 2)) {
      f.machine.rejectWrites = true;
      CompletableFuture<TSStatus> first = f.write("failure", 10);
      CompletableFuture<TSStatus> second = f.write("never-applied", 20);
      f.cores.runAll();
      assertTrue(first.isCompletedExceptionally());
      assertTrue(second.isCompletedExceptionally());
      assertFalse(f.group.ready());
      assertTrue(f.machine.applied.isEmpty());
      assertTrue(f.cores.tasks.isEmpty());
    }
  }

  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean()) {
      assertTrue("Lifecycle event did not arrive", System.nanoTime() < deadline);
      Thread.sleep(1);
    }
  }

  @Test
  public void closeDiscardsQueuedDriveAndFailsItsAcceptedWaiter() throws Exception {
    try (Fixture f = new Fixture(1, 2)) {
      CompletableFuture<TSStatus> write = f.write("must-not-apply-after-close", 10);
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      f.group.submit(
          () -> {
            entered.countDown();
            assertTrue(release.await(5, TimeUnit.SECONDS));
            return null;
          });
      AtomicReference<Throwable> failure = new AtomicReference<>();
      Thread runner =
          new Thread(
              () -> {
                try {
                  f.cores.runAll();
                } catch (Throwable error) {
                  failure.set(error);
                }
              },
              "traft-manual-drive");
      Thread closer =
          new Thread(
              () -> {
                try {
                  f.group.close();
                } catch (Throwable error) {
                  failure.set(error);
                }
              },
              "traft-manual-close");
      runner.start();
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        closer.start();
        await(() -> !f.group.ready());
      } finally {
        release.countDown();
        runner.join(5000);
        if (closer.getState() != Thread.State.NEW) {
          closer.join(5000);
        }
      }
      assertFalse(runner.isAlive());
      assertFalse(closer.isAlive());
      assertNull(failure.get());
      assertTrue(write.isCompletedExceptionally());
      assertTrue(f.machine.applied.isEmpty());
      assertEquals(1, f.machine.stopped);
    }
  }

  /** Exchanges the two peers' frames; anything addressed to the group's node 1 is lost. */
  private static void exchangeWithoutNodeOne(Fixture f) throws Exception {
    Deque<Message> queue = new ArrayDeque<>();
    for (TRaftEngine peer : f.others.values()) {
      queue.addAll(peer.drainMessages());
    }
    int budget = 100000;
    while (!queue.isEmpty()) {
      assertTrue("Peer exchange must finish", budget-- > 0);
      Message message = queue.removeFirst();
      if (message.to != 1) {
        f.others.get(message.to).receive(message);
        for (TRaftEngine peer : f.others.values()) {
          queue.addAll(peer.drainMessages());
        }
      }
    }
  }

  private static List<Message> addressedToNodeOne(TRaftEngine sender) {
    List<Message> messages = new ArrayList<>();
    for (Message message : sender.drainMessages()) {
      if (message.to == 1) {
        messages.add(message);
      }
    }
    return messages;
  }

  private static boolean holdsTimestamp(TRaftEngine engine, long timestamp) {
    for (Entry entry : engine.installedEntries()) {
      if (entry.timestamp == timestamp) {
        return true;
      }
    }
    return false;
  }

  /**
   * Node 1 holds an unreplicated client write when peer 2 wins term 2 with peer 3, whose recovery
   * excludes that write. Returns the client's future; nothing has reached node 1 yet.
   */
  private static CompletableFuture<TSStatus> writeExcludedByARecovery(Fixture f) throws Exception {
    f.pump();
    f.partitioned = true;
    CompletableFuture<TSStatus> client = f.write("excluded", 100);
    f.pump();
    assertFalse("No quorum: the client is still waiting", client.isDone());
    assertTrue(holdsTimestamp(f.engine, 100));
    TRaftEngine next = f.others.get(2);
    next.startElection();
    exchangeWithoutNodeOne(f);
    assertEquals(Role.LEADER, next.role());
    assertFalse(holdsTimestamp(next, 100));
    f.partitioned = false;
    return client;
  }

  /** Only a repair round trip separates node 1 from the new history; drive it to completion. */
  private static void finishRepairAtNodeOne(Fixture f) throws Exception {
    TRaftEngine next = f.others.get(2);
    for (int round = 0; round < 5; round++) {
      next.resend();
      f.receive(addressedToNodeOne(next));
      f.pump();
    }
    assertEquals(next.installedEntries(), f.engine.installedEntries());
    assertFalse(holdsTimestamp(f.engine, 100));
  }

  /**
   * One heartbeat of the new leader, as the one batch Channel.dispatch makes of it. The batch is
   * consumed before the group notices the lost leadership, so a false commit in it reaches the
   * state machine and answers the waiting client with success.
   */
  @Test
  public void heartbeatBatchMustNotAcknowledgeAWriteTheRecoveredHistoryExcluded() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      CompletableFuture<TSStatus> client = writeExcludedByARecovery(f);
      TRaftEngine next = f.others.get(2);
      next.resend();
      CompletableFuture<Void> delivered = f.receive(addressedToNodeOne(next));
      f.pump();
      delivered.get(1, TimeUnit.SECONDS);
      assertTrue("Leadership was lost: the client gets its answer", client.isDone());
      assertTrue(
          "The client must not be told that an excluded write succeeded",
          client.isCompletedExceptionally());
      assertFalse(f.machine.snapshotApplied().contains("excluded"));
      finishRepairAtNodeOne(f);
      assertFalse(f.machine.snapshotApplied().contains("excluded"));
    }
  }

  /** The other order: node 1 hears the term from a NACK first, so its client already failed. */
  @Test
  public void heartbeatAfterANackMustNotApplyAWriteTheRecoveredHistoryExcluded() throws Exception {
    try (Fixture f = new Fixture(3, 8)) {
      CompletableFuture<TSStatus> client = writeExcludedByARecovery(f);
      f.receive(new Message(MessageType.NACK, 3, 1, 2, null, null, null, null, null, 0, null));
      f.pump();
      assertTrue(client.isCompletedExceptionally());
      TRaftEngine next = f.others.get(2);
      next.resend();
      f.receive(addressedToNodeOne(next));
      f.pump();
      assertFalse(f.machine.snapshotApplied().contains("excluded"));
      finishRepairAtNodeOne(f);
      assertFalse(f.machine.snapshotApplied().contains("excluded"));
    }
  }
}
