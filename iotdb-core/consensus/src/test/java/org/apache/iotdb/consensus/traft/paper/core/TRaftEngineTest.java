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
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Proposal;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.RecordType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Report;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Role;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TRaftEngineTest {
  private static final class MemoryStore implements DurableStore {
    final List<DurableRecord> records = new ArrayList<>();
    final List<DurableRecord> deferred = new ArrayList<>();
    RecordType failOn;
    final List<List<RecordType>> batches = new ArrayList<>();
    boolean rejectUndurableHandles;
    int failBatchAfter = -1;
    Runnable beforeBatchReturn = () -> {};

    @Override
    public List<DurableRecord> load() {
      return new ArrayList<>(records);
    }

    @Override
    public void append(DurableRecord record) throws IOException {
      if (record.type == failOn) {
        throw new IOException("injected durable failure");
      }
      // Exercise explicit persistence codec on every state transition.
      records.add(TRaftCodec.decodeRecord(TRaftCodec.encodeRecord(record)));
    }

    @Override
    public void appendDeferred(DurableRecord record) throws IOException {
      deferred.add(record);
      append(record);
    }

    @Override
    public void appendBatch(List<DurableRecord> records) throws IOException {
      List<RecordType> types = new ArrayList<>();
      for (DurableRecord record : records) {
        types.add(record.type);
      }
      batches.add(types);
      for (int i = 0; i < records.size(); i++) {
        if (i == failBatchAfter) {
          throw new IOException("injected partial batch failure");
        }
        append(records.get(i));
      }
      beforeBatchReturn.run();
    }

    @Override
    public Entry residentEntry(Entry entry) {
      if (rejectUndurableHandles) {
        assertTrue(
            "Resident handle must follow durable payload publication",
            records.stream()
                .anyMatch(
                    record ->
                        record.entry != null
                            && record.entry.hasPayload()
                            && record.entry.equals(entry)));
      }
      return entry;
    }
  }

  private static final class Cluster {
    final Map<Integer, TRaftEngine> engines = new LinkedHashMap<>();
    final Map<Integer, MemoryStore> stores = new LinkedHashMap<>();
    final Map<Integer, List<EntryId>> applications = new LinkedHashMap<>();
    final List<Message> network = new ArrayList<>();
    Predicate<Message> drop = ignored -> false;
    int theta = 16;
    boolean durableCommitRecords = true;
    boolean durableAppliedRecords = true;
    // Engines made after this is set compact on their own (TRaftEngine.setCompaction).
    boolean compaction;
    // Engines made after this is set order data per series (TRaftEngine.setSeriesOrder).
    boolean seriesOrder;
    final Set<Integer> members = new HashSet<>();

    Cluster() throws IOException {
      this(3);
    }

    Cluster(int voters) throws IOException {
      for (int i = 1; i <= voters; i++) {
        members.add(i);
      }
      for (int i = 1; i <= voters; i++) {
        stores.put(i, new MemoryStore());
        applications.put(i, new ArrayList<>());
        restart(i);
      }
    }

    void restart(int id) throws IOException {
      TRaftEngine engine =
          new TRaftEngine(
              id,
              members,
              theta,
              100,
              stores.get(id),
              new Application() {
                @Override
                public void apply(Entry entry) {
                  if (!applications.get(id).contains(entry.id)) {
                    applications.get(id).add(entry.id);
                  }
                }

                @Override
                public boolean requiresDurableCommitRecords() {
                  return durableCommitRecords;
                }

                @Override
                public boolean requiresDurableAppliedRecords() {
                  return durableAppliedRecords;
                }
              });
      engine.setCompaction(compaction);
      engine.setSeriesOrder(seriesOrder);
      engines.put(id, engine);
    }

    void tickAll(long now) throws IOException {
      for (TRaftEngine engine : engines.values()) {
        engine.tick(now);
      }
      pump();
    }

    TRaftEngine node(int id) {
      return engines.get(id);
    }

    void collect() {
      for (TRaftEngine engine : engines.values()) {
        network.addAll(engine.drainMessages());
      }
    }

    void pump() throws IOException {
      int budget = 10000;
      collect();
      while (!network.isEmpty()) {
        assertTrue("message loop", budget-- > 0);
        Message message = network.remove(0);
        if (engines.containsKey(message.to) && !drop.test(message)) {
          engines
              .get(message.to)
              .receive(TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(message)));
        }
        collect();
      }
    }

    void elect(int id) throws IOException {
      node(id).startElection();
      pump();
      assertEquals(Role.LEADER, node(id).role());
    }

    Entry append(int id, long ts) throws IOException {
      return node(id).append(Kind.DATA, ts, new byte[] {(byte) ts}, 1);
    }
  }

  private static Cluster seriesCluster() throws IOException {
    Cluster c = new Cluster();
    c.seriesOrder = true;
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
    }
    c.elect(1);
    return c;
  }

  private static Entry appendSeries(Cluster c, int id, long series, long ts) throws IOException {
    return c.node(id).append(Kind.DATA, ts, true, series, new byte[] {(byte) ts}, 1);
  }

  private static TRaftEngine.DataInput seriesInput(long series, long ts) {
    return new TRaftEngine.DataInput(ts, true, series, new byte[] {(byte) ts});
  }

  private static long partitionOf(TRaftEngine engine, EntryId id) {
    for (Entry entry : engine.installedEntries()) {
      if (entry.id.equals(id)) {
        return entry.partition;
      }
    }
    throw new AssertionError("not installed: " + id);
  }

  private static List<TRaftEngine.DataInput> dataInputs(long... timestamps) {
    List<TRaftEngine.DataInput> inputs = new ArrayList<>();
    for (long timestamp : timestamps) {
      inputs.add(new TRaftEngine.DataInput(timestamp, new byte[] {(byte) timestamp}));
    }
    return inputs;
  }

  private static List<Message> ordinaryMessages(List<Message> outgoing, int target) {
    List<Message> result = new ArrayList<>();
    for (Message message : outgoing) {
      if (message.to == target && message.type == MessageType.ENTRY) {
        result.add(message);
      }
    }
    return result;
  }

  @Test
  public void leaderOrdinaryBatchPublishesOnlyAfterDurabilityAndStillNeedsRemoteQuorum()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    TRaftEngine leader = c.node(1);
    MemoryStore store = c.stores.get(1);
    List<Entry> before = leader.installedEntries();
    store.batches.clear();
    store.rejectUndurableHandles = true;
    store.beforeBatchReturn =
        () -> {
          assertEquals(before, leader.installedEntries());
          assertTrue(leader.drainMessages().isEmpty());
        };
    byte[] payload = {10};
    TRaftEngine.DataInput candidate = new TRaftEngine.DataInput(10, payload);
    List<Entry> accepted =
        leader.appendDataBatch(
            Arrays.asList(candidate, new TRaftEngine.DataInput(20, new byte[] {20})), 1);
    payload[0] = 99;
    store.beforeBatchReturn = () -> {};
    assertEquals(2, accepted.size());
    assertEquals(10, accepted.get(0).payload()[0]);
    assertEquals(
        Collections.singletonList(Arrays.asList(RecordType.INSTALL, RecordType.INSTALL)),
        store.batches);
    for (Entry entry : accepted) {
      assertFalse(leader.isCommitted(entry.id));
      assertFalse(leader.isApplied(entry.id));
    }
    c.pump();
    for (Entry entry : accepted) {
      for (int id = 1; id <= 3; id++) {
        assertTrue(c.node(id).isApplied(entry.id));
      }
    }
  }

  @Test
  public void ordinaryBatchOpensTimestampBoundariesInlineAndStopsAtThetaAndBarriers()
      throws Exception {
    Cluster c = new Cluster();
    c.theta = 4;
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
    }
    c.elect(1);
    // A non-increasing timestamp opens the next partition inside the batch: the proposal that
    // fixes the partition before it is pipelined, and the batch goes on.
    List<Entry> minimum =
        c.node(1)
            .appendDataBatch(dataInputs(Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 1), 1);
    assertEquals(3, minimum.size());
    assertEquals(minimum.get(0).partition + 1, minimum.get(2).partition);
    assertTrue(c.node(1).hasPendingProposal());
    // A pending boundary does not stop admission: the next batch joins its partition.
    List<Entry> joined = c.node(1).appendDataBatch(dataInputs(Long.MIN_VALUE + 2), 1);
    assertEquals(1, joined.size());
    assertEquals(minimum.get(2).partition, joined.get(0).partition);
    c.pump();
    assertFalse(c.node(1).hasPendingProposal());
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isCommitted(joined.get(0).id));
    }
    // Theta bounds that partition, which already holds two entries.
    List<Entry> filling = c.node(1).appendDataBatch(dataInputs(10, 20, 30, 40), 1);
    assertEquals(2, filling.size());
    assertTrue(c.node(1).appendDataBatch(dataInputs(30, 40), 1).isEmpty());
    // The single-entry API owns the administrative no-op boundary; the entry joins its partition.
    Entry afterTheta = c.append(1, 30);
    assertNotNull(afterTheta);
    assertEquals(filling.get(0).partition + 1, afterTheta.partition);
    c.pump();
    assertTrue(c.node(1).isApplied(afterTheta.id));
    Entry barrier = c.node(1).append(Kind.BARRIER, 0, new byte[] {7}, 1);
    assertNotNull(barrier);
    c.pump();
    assertTrue(c.node(1).appendDataBatch(dataInputs(40, 50), 1).isEmpty());
  }

  @Test
  public void seriesOrderKeepsInterleavedSeriesInOnePartitionAndSplitsOnlyWithinASeries()
      throws Exception {
    Cluster c = seriesCluster();
    Entry a10 = appendSeries(c, 1, 1, 10);
    Entry b5 = appendSeries(c, 1, 2, 5); // older than series 1, but another series
    assertFalse(c.node(1).hasPendingProposal());
    assertEquals(partitionOf(c.node(1), a10.id), partitionOf(c.node(1), b5.id));
    // Series 1 goes back in time; the rule keeps every series increasing within a partition.
    Entry a8 = appendSeries(c, 1, 1, 8);
    assertTrue(c.node(1).hasPendingProposal());
    c.pump();
    assertEquals(partitionOf(c.node(1), a10.id) + 1, partitionOf(c.node(1), a8.id));
    Entry a9 = appendSeries(c, 1, 1, 9);
    Entry b1 = appendSeries(c, 1, 2, 1);
    assertFalse(c.node(1).hasPendingProposal());
    assertEquals(partitionOf(c.node(1), a8.id), partitionOf(c.node(1), a9.id));
    assertEquals(partitionOf(c.node(1), a8.id), partitionOf(c.node(1), b1.id));
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isCommitted(b1.id));
    }
  }

  @Test
  public void seriesOrderTreatsAKeylessEntryAsConflictingWithEveryPointUpToItsTimestamp()
      throws Exception {
    Cluster c = seriesCluster();
    Entry a10 = appendSeries(c, 1, 1, 10);
    Entry keyless = c.node(1).append(Kind.DATA, 10, new byte[] {1}, 1);
    assertTrue(c.node(1).hasPendingProposal());
    c.pump();
    long opened = partitionOf(c.node(1), keyless.id);
    assertEquals(partitionOf(c.node(1), a10.id) + 1, opened);
    Entry b10 = appendSeries(c, 1, 2, 10); // same timestamp as the keyless entry
    assertTrue(c.node(1).hasPendingProposal());
    c.pump();
    assertEquals(opened + 1, partitionOf(c.node(1), b10.id));
    Entry b11 = appendSeries(c, 1, 2, 11);
    assertFalse(c.node(1).hasPendingProposal());
    assertEquals(opened + 1, partitionOf(c.node(1), b11.id));
  }

  @Test
  public void seriesOrderBatchOpensAPartitionAtEachConflictWithinASeries() throws Exception {
    Cluster c = seriesCluster();
    List<Entry> accepted =
        c.node(1)
            .appendDataBatch(
                Arrays.asList(
                    seriesInput(1, 10), seriesInput(2, 5), seriesInput(1, 11), seriesInput(2, 5)),
                1);
    assertEquals(4, accepted.size());
    assertTrue(c.node(1).hasPendingProposal());
    // The new partition starts with the repeated point alone: series 1 may go on at 12, and the
    // batch view and the single-entry path agree that series 2 at 5 conflicts again.
    List<Entry> again =
        c.node(1).appendDataBatch(Arrays.asList(seriesInput(1, 12), seriesInput(2, 5)), 1);
    assertEquals(2, again.size());
    Entry repeated = appendSeries(c, 1, 2, 5);
    c.pump();
    assertFalse(c.node(1).hasPendingProposal());
    long base = partitionOf(c.node(1), accepted.get(0).id);
    assertEquals(base, partitionOf(c.node(1), accepted.get(2).id));
    assertEquals(base + 1, partitionOf(c.node(1), accepted.get(3).id));
    assertEquals(base + 1, partitionOf(c.node(1), again.get(0).id));
    assertEquals(base + 2, partitionOf(c.node(1), again.get(1).id));
    assertEquals(base + 3, partitionOf(c.node(1), repeated.id));
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isCommitted(repeated.id));
    }
  }

  @Test
  public void aNewLeaderNeverAdmitsARepeatedPointIntoThePartitionThatHoldsIt() throws Exception {
    Cluster c = seriesCluster();
    Entry first = appendSeries(c, 1, 7, 100);
    c.pump();
    c.elect(2);
    Entry again = null;
    for (int attempt = 0; again == null && attempt < 4; attempt++) {
      again = appendSeries(c, 2, 7, 100);
      c.pump();
    }
    assertNotNull(again);
    c.pump();
    assertTrue(c.node(2).isCommitted(first.id));
    assertTrue(c.node(2).isCommitted(again.id));
    assertTrue(partitionOf(c.node(2), first.id) < partitionOf(c.node(2), again.id));
  }

  @Test
  public void oneVoterBatchLeavesSynchronousApplicationOnTheOriginalPath() throws Exception {
    MemoryStore store = new MemoryStore();
    List<EntryId> applied = new ArrayList<>();
    TRaftEngine engine =
        new TRaftEngine(1, Collections.singleton(1), 16, 0, store, entry -> applied.add(entry.id));
    engine.startElection();
    int records = store.records.size();
    assertTrue(engine.appendDataBatch(dataInputs(10, 20), 1).isEmpty());
    assertEquals(records, store.records.size());
    Entry entry = engine.append(Kind.DATA, 10, new byte[] {10}, 1);
    assertTrue(engine.isApplied(entry.id));
    assertEquals(Collections.singletonList(entry.id), applied);
  }

  @Test
  public void followerOrdinaryBatchPreservesHolesAndDeduplicatesOnlyDurableRecords()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(10, 20, 30), 1);
    List<Message> messages = ordinaryMessages(c.node(1).drainMessages(), 2);
    MemoryStore store = c.stores.get(2);
    store.batches.clear();
    List<Entry> before = c.node(2).installedEntries();
    store.beforeBatchReturn =
        () -> {
          assertEquals(before, c.node(2).installedEntries());
          assertTrue(c.node(2).drainMessages().isEmpty());
        };
    assertTrue(
        c.node(2).receiveEntries(Arrays.asList(messages.get(0), messages.get(2), messages.get(2))));
    store.beforeBatchReturn = () -> {};
    assertEquals(
        Collections.singletonList(Arrays.asList(RecordType.INSTALL, RecordType.INSTALL)),
        store.batches);
    for (Message ack : c.node(2).drainMessages()) {
      c.node(1).receive(ack);
    }
    c.pump();
    assertTrue(c.node(1).isApplied(entries.get(0).id));
    assertTrue(c.node(1).isApplied(entries.get(2).id));
    assertFalse(c.node(1).isCommitted(entries.get(1).id));
    assertTrue(c.node(2).isApplied(entries.get(2).id));
    // A late lower timestamp/index is a legal same-partition hole, not a new boundary.
    assertTrue(c.node(2).receiveEntries(Arrays.asList(messages.get(1), messages.get(2))));
    c.pump();
    assertTrue(c.node(1).isApplied(entries.get(1).id));
    assertTrue(c.node(2).isApplied(entries.get(1).id));
    int durableRecords = store.records.size();
    assertTrue(c.node(2).receiveEntries(Arrays.asList(messages.get(0), messages.get(1))));
    assertEquals(durableRecords, store.records.size());
  }

  @Test
  public void followerBatchWithCommitBeforeDataRebuildsAfterDurableReturnCrash() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(10, 20), 1);
    List<Message> messages = ordinaryMessages(c.node(1).drainMessages(), 2);
    List<EntryId> ids = Arrays.asList(entries.get(0).id, entries.get(1).id);
    c.node(2)
        .receive(
            new Message(
                MessageType.COMMIT, 1, 2, c.node(1).term(), null, null, null, null, null, -1, ids));
    MemoryStore store = c.stores.get(2);
    List<Entry> before = c.node(2).installedEntries();
    store.beforeBatchReturn =
        () -> {
          assertEquals(before, c.node(2).installedEntries());
          assertFalse(c.node(2).isApplied(entries.get(0).id));
          throw new IllegalStateException("crash after durable frames before engine replay");
        };
    try {
      c.node(2).receiveEntries(Arrays.asList(messages.get(1), messages.get(0)));
      fail("the synchronous batch must fail closed");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("before engine replay"));
    }
    assertTrue(c.node(2).drainMessages().isEmpty());
    store.beforeBatchReturn = () -> {};
    c.restart(2);
    assertFalse(c.node(2).isApplied(entries.get(0).id));
    c.node(2).applyCommitted();
    assertTrue(c.node(2).isApplied(entries.get(0).id));
    assertTrue(c.node(2).isApplied(entries.get(1).id));
  }

  @Test
  public void partialLeaderInstallBatchFencesWithoutPublishingButReplaysItsDurablePrefix()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    MemoryStore store = c.stores.get(1);
    List<Entry> before = c.node(1).installedEntries();
    store.failBatchAfter = 1;
    try {
      c.node(1).appendDataBatch(dataInputs(10, 20), 1);
      fail("expected partial durable failure");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("partial batch"));
    }
    assertEquals(before, c.node(1).installedEntries());
    assertTrue(c.node(1).drainMessages().isEmpty());
    try {
      c.append(1, 30);
      fail("failed engine must remain fenced");
    } catch (IOException expected) {
      // Recovery, not an in-memory retry, is required after ambiguous durability.
    }
    store.failBatchAfter = -1;
    c.restart(1);
    assertEquals(before.size() + 1, c.node(1).installedEntries().size());
    Entry prefix = c.node(1).installedEntries().get(before.size());
    assertEquals(10, prefix.timestamp);
    assertFalse(c.node(1).isCommitted(prefix.id));
    assertFalse(c.node(1).isApplied(prefix.id));
  }

  @Test
  public void followerBatchPrevalidationRejectsMixedTermControlAndIdentityConflictWithoutMutation()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.node(1).appendDataBatch(dataInputs(10, 20), 1);
    List<Message> messages = ordinaryMessages(c.node(1).drainMessages(), 2);
    Message first = messages.get(0), second = messages.get(1);
    Entry changed =
        new Entry(
            first.entry.id,
            first.entry.partition,
            first.entry.timestamp + 1,
            first.entry.prevSize,
            Kind.DATA,
            new byte[] {99});
    Message collision =
        new Message(
            MessageType.ENTRY,
            1,
            2,
            first.term,
            changed,
            null,
            first.reference,
            null,
            null,
            -1,
            null);
    Message highTerm =
        new Message(
            MessageType.ENTRY,
            1,
            2,
            first.term + 1,
            second.entry,
            null,
            second.reference,
            null,
            null,
            -1,
            null);
    Message control =
        new Message(
            MessageType.COMMIT,
            1,
            2,
            first.term,
            null,
            null,
            null,
            null,
            null,
            -1,
            Collections.emptyList());
    int records = c.stores.get(2).records.size();
    List<Entry> before = c.node(2).installedEntries();
    for (List<Message> invalid :
        Arrays.asList(
            Arrays.asList(first, collision),
            Arrays.asList(first, highTerm),
            Arrays.asList(first, control))) {
      assertFalse(c.node(2).receiveEntries(invalid));
      assertEquals(records, c.stores.get(2).records.size());
      assertEquals(before, c.node(2).installedEntries());
      assertTrue(c.node(2).drainMessages().isEmpty());
    }
    assertTrue(c.node(2).receiveEntries(messages));
  }

  @Test
  public void pendingIncomingKeepsFollowerBatchAndCompletesTheConfirmedProposalOnce()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(10, 20), 1);
    List<Message> original = c.node(1).drainMessages();
    List<Message> messages = ordinaryMessages(original, 2);
    for (Message message : ordinaryMessages(original, 3)) {
      c.node(3).receive(message);
    }
    Entry boundary = c.append(1, 0);
    Message proposal = null;
    for (Message message : c.node(1).drainMessages()) {
      if (message.type == MessageType.PROPOSAL && message.to == 2) {
        proposal = message;
        c.node(2).receive(message);
      } else if (message.to == 3) {
        c.node(3).receive(message);
      }
    }
    assertNotNull(proposal);
    // The complete third replica actually gives quorum evidence; only then deliver CONFIRM
    // ahead of the old ordinary members to the lagging second replica.
    for (Message response : c.node(3).drainMessages()) {
      c.node(1).receive(response);
    }
    for (Message message : c.node(1).drainMessages()) {
      if (message.type == MessageType.CONFIRM && message.to == 2) {
        c.node(2).receive(message);
      }
    }
    c.node(2).drainMessages();
    assertTrue(c.node(2).receiveEntries(messages));
    assertTrue(c.node(2).retainedProposals().contains(proposal.proposal));
    assertEquals(boundary.partition, c.node(2).currentPartition());
    for (Entry entry : entries) {
      assertTrue(c.node(2).installedEntries().contains(entry));
    }
  }

  @Test
  public void losingCandidateOfTheSameTermStepsDownAndReturnsToTheBatchPath() throws Exception {
    Cluster c = new Cluster();
    c.node(1).startElection();
    c.node(3).startElection();
    c.pump();
    assertEquals(Role.LEADER, c.node(1).role());
    assertEquals(c.node(1).term(), c.node(3).term());
    assertEquals(Role.FOLLOWER, c.node(3).role());
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(10, 20), 1);
    assertEquals(2, entries.size());
    assertTrue(c.node(3).receiveEntries(ordinaryMessages(c.node(1).drainMessages(), 3)));
  }

  @Test
  public void entriesOfAnUninstalledOpeningAreStagedSoTheNextProposalNeedsNoFetch()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    // The boundary reaches only the third replica, which confirms it alone.
    c.append(1, 10);
    c.append(1, 0);
    List<Message> sent = c.node(1).drainMessages();
    for (Message message : sent) {
      if (message.to == 3) {
        c.node(3).receive(message);
      } else if (message.to == 2 && message.type == MessageType.ENTRY) {
        c.node(2).receive(message);
      }
    }
    c.node(2).drainMessages();
    for (Message response : c.node(3).drainMessages()) {
      c.node(1).receive(response);
    }
    c.node(1).drainMessages();
    // Ordinary entries of the new partition overtake that boundary at the second replica.
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(20, 30), 1);
    int records = c.stores.get(2).records.size();
    assertFalse(c.node(2).receiveEntries(ordinaryMessages(c.node(1).drainMessages(), 2)));
    assertEquals("one record per staged payload", records + 2, c.stores.get(2).records.size());
    for (Entry entry : entries) {
      assertFalse("staging installs nothing", c.node(2).installedEntries().contains(entry));
    }
  }

  @Test
  public void entriesAndTheNextBoundaryOfAnUnconfirmedOpeningAreAckedBeforeItsConfirm()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.append(1, 0); // a non-increasing timestamp opens partition 1 with a proposal
    // Both followers ACK the proposal; its CONFIRM is held back, as when a later RPC batch
    // overtakes the one that carries it.
    for (Message message : c.node(1).drainMessages()) {
      c.node(message.to).receive(message);
    }
    for (int id = 2; id <= 3; id++) {
      for (Message response : c.node(id).drainMessages()) {
        c.node(1).receive(response);
      }
    }
    assertFalse(c.node(1).hasPendingProposal());
    List<Message> held = c.node(1).drainMessages();
    // Ordinary entries of partition 1 reach both followers before the CONFIRM: each stages the
    // run under the retained opening and acknowledges it in one frame.
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(20, 30), 1);
    List<Message> outgoing = c.node(1).drainMessages();
    for (int id = 2; id <= 3; id++) {
      assertTrue(c.node(id).receiveEntries(ordinaryMessages(outgoing, id)));
      List<Message> acks = c.node(id).drainMessages();
      assertEquals(1, acks.size());
      assertEquals(MessageType.ENTRY_ACK, acks.get(0).type);
      c.node(1).receive(acks.get(0));
      for (Entry entry : entries) {
        assertFalse(c.node(id).installedEntries().contains(entry));
      }
    }
    for (Entry entry : entries) {
      assertTrue(c.node(1).isCommitted(entry.id));
    }
    // The next boundary names the staged members, two partitions ahead of both followers; its
    // base is their retained opening, so they ACK it before the held CONFIRM arrives.
    c.append(1, 5);
    assertTrue(c.node(1).hasPendingProposal());
    for (Message message : c.node(1).drainMessages()) {
      if (message.type == MessageType.PROPOSAL) {
        c.node(message.to).receive(message);
      }
    }
    for (int id = 2; id <= 3; id++) {
      List<MessageType> types = new ArrayList<>();
      for (Message response : c.node(id).drainMessages()) {
        types.add(response.type);
        c.node(1).receive(response);
      }
      assertTrue(types.toString(), types.contains(MessageType.PROPOSAL_ACK));
      assertFalse(types.toString(), types.contains(MessageType.NACK));
      assertFalse(types.toString(), types.contains(MessageType.FETCH_ENTRY));
    }
    assertFalse("the boundary must not wait for a heartbeat", c.node(1).hasPendingProposal());
    for (Message message : held) {
      c.node(message.to).receive(message);
    }
    c.pump(); // messages only: no tick(), so no heartbeat retry
    for (Entry entry : entries) {
      assertTrue(c.node(2).installedEntries().contains(entry));
      assertTrue(c.node(3).installedEntries().contains(entry));
    }
  }

  @Test
  public void volatileCommitBatchDataSurvivesWholeClusterRestartAndFreshRecovery()
      throws Exception {
    Cluster c = volatileCommitCluster();
    c.elect(1);
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(10, 20), 1);
    List<Message> outgoing = c.node(1).drainMessages();
    for (int id = 2; id <= 3; id++) {
      assertTrue(c.node(id).receiveEntries(ordinaryMessages(outgoing, id)));
    }
    c.pump();
    for (Entry entry : entries) {
      assertTrue(c.node(1).isApplied(entry.id));
    }
    for (MemoryStore store : c.stores.values()) {
      assertFalse(
          store.records.stream()
              .anyMatch(
                  record -> record.type == RecordType.COMMIT || record.type == RecordType.APPLIED));
    }
    restartEmptyApplications(c);
    for (int id = 1; id <= 3; id++) {
      c.node(id).applyCommitted();
      assertTrue(
          "Stored batch data alone is not commitment evidence", c.applications.get(id).isEmpty());
    }
    c.elect(2);
    for (Entry entry : entries) {
      for (int id = 1; id <= 3; id++) {
        assertTrue(c.node(id).isApplied(entry.id));
      }
    }
  }

  @Test
  public void partialFollowerBatchEmitsNoAckAndRecoveryKeepsOnlyTheDurablePrefix()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(10, 20), 1);
    List<Message> messages = ordinaryMessages(c.node(1).drainMessages(), 2);
    MemoryStore store = c.stores.get(2);
    List<Entry> before = c.node(2).installedEntries();
    store.failBatchAfter = 1;
    try {
      c.node(2).receiveEntries(messages);
      fail("expected partial follower failure");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("partial batch"));
    }
    assertEquals(before, c.node(2).installedEntries());
    assertTrue(c.node(2).drainMessages().isEmpty());
    store.failBatchAfter = -1;
    c.restart(2);
    assertTrue(c.node(2).installedEntries().contains(entries.get(0)));
    assertFalse(c.node(2).installedEntries().contains(entries.get(1)));
    assertFalse(c.node(2).isCommitted(entries.get(0).id));
    assertFalse(c.node(2).isApplied(entries.get(0).id));
  }

  @Test
  public void firstMinimumTimestampSharesInitialNoopPartition() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    long partition = c.node(1).currentPartition();
    Entry first = c.append(1, Long.MIN_VALUE);
    assertEquals(partition, first.partition);
    assertFalse(c.node(1).hasPendingProposal());
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(first.id));
    }
  }

  @Test
  public void repeatedMinimumTimestampStartsANewPartition() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry first = c.append(1, Long.MIN_VALUE);
    c.pump();
    Entry repeated = c.append(1, Long.MIN_VALUE);
    assertEquals(first.partition + 1, repeated.partition);
    assertTrue(c.node(1).hasPendingProposal());
    c.pump();
    Entry increasing = c.append(1, Long.MIN_VALUE + 1);
    assertEquals(repeated.partition, increasing.partition);
    assertFalse(c.node(1).hasPendingProposal());
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(repeated.id));
      assertTrue(c.node(id).isApplied(increasing.id));
    }
  }

  @Test
  public void restartWithoutDataPreservesAbsentTimestampMaximum() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
    }
    c.elect(1);
    long partition = c.node(1).currentPartition();
    Entry first = c.append(1, Long.MIN_VALUE);
    assertEquals(partition, first.partition);
    assertFalse(c.node(1).hasPendingProposal());
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(first.id));
    }
  }

  @Test
  public void restartWithMinimumTimestampPreservesEqualityBoundary() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry first = c.append(1, Long.MIN_VALUE);
    c.pump();
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
    }
    c.elect(2);
    long partition = c.node(2).currentPartition();
    Entry repeated = c.append(2, Long.MIN_VALUE);
    assertEquals(partition + 1, repeated.partition);
    assertTrue(c.node(2).hasPendingProposal());
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(first.id));
      assertTrue(c.node(id).isApplied(repeated.id));
    }
  }

  @Test
  public void outOfOrderEntryCommitsAndAppliesWhileEarlierEntryIsMissing() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry early = c.append(1, 10);
    Entry late = c.append(1, 20);
    c.drop = m -> m.type == MessageType.ENTRY && m.entry.id.equals(early.id);
    c.pump();
    assertFalse(c.node(1).isCommitted(early.id));
    assertTrue(c.node(1).isCommitted(late.id));
    assertTrue(c.node(2).isApplied(late.id));
    assertFalse(c.node(2).installedEntries().contains(early));
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertTrue(c.node(2).isApplied(early.id));
    assertEquals(Arrays.asList(late.id, early.id), c.applications.get(2));
  }

  @Test
  public void stagedSuccessorDoesNotChangeElectionRankOrInstalledCount() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    int before = c.node(1).installedEntries().size();
    Entry successor = c.append(1, 5);
    c.drop = m -> m.type == MessageType.PROPOSAL_ACK;
    c.pump();
    for (int i = 1; i <= 3; i++) {
      assertEquals(0, c.node(i).currentPartition());
      assertEquals(before, c.node(i).installedEntries().size());
      assertFalse(c.node(i).isCommitted(successor.id));
      assertTrue(c.node(i).retainedProposals().stream().anyMatch(p -> p.id().equals(successor.id)));
    }
  }

  @Test
  public void equalRankDifferentContentsRecoversBothQuorumCommittedEntries() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry a = c.append(1, 10);
    Entry b = c.append(1, 20);
    c.drop =
        m ->
            m.type == MessageType.ENTRY
                && ((m.to == 2 && m.entry.id.equals(a.id))
                    || (m.to == 3 && m.entry.id.equals(b.id)));
    c.pump();
    assertTrue(c.node(1).isCommitted(a.id));
    assertTrue(c.node(1).isCommitted(b.id));
    assertEquals(0, c.node(2).currentRank().compareTo(c.node(3).currentRank()));
    c.engines.remove(1);
    c.drop = ignored -> false;
    c.elect(2);
    assertTrue(c.node(2).isApplied(a.id));
    assertTrue(c.node(2).isApplied(b.id));
    assertTrue(c.node(3).isApplied(a.id));
    assertTrue(c.node(3).isApplied(b.id));
    assertEquals(1, c.node(2).currentPartition());
  }

  @Test
  public void unknownMajorityConfirmedPendingSurvivesInterruptedRecovery() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry a = c.append(1, 10);
    Entry b = c.append(1, 20);
    c.drop =
        m ->
            m.type == MessageType.ENTRY
                && ((m.to == 2 && m.entry.id.equals(a.id))
                    || (m.to == 3 && m.entry.id.equals(b.id)));
    c.pump();
    c.engines.remove(1);
    c.drop = m -> m.type == MessageType.PROPOSAL_ACK;
    c.node(2).startElection();
    c.pump();
    assertEquals(Role.LEADER_CANDIDATE, c.node(2).role());
    assertEquals(0, c.node(2).currentPartition());
    Proposal pending =
        c.node(2).retainedProposals().stream().filter(p -> p.first.id.term == 2).findFirst().get();
    assertTrue(c.node(3).retainedProposals().contains(pending));
    c.engines.remove(2);
    c.restart(1);
    c.drop = ignored -> false;
    // A retained both client entries, so its installed rank outranks C.
    c.node(1).startElection();
    c.pump();
    c.node(1).startElection();
    c.pump();
    assertEquals(Role.LEADER, c.node(1).role());
    assertEquals(2, c.node(1).currentPartition());
    assertTrue(c.node(1).installedEntries().contains(pending.first));
    assertTrue(c.node(1).isApplied(a.id));
    assertTrue(c.node(1).isApplied(b.id));
  }

  @Test
  public void failedDurabilityCannotAcknowledgeAndFencesEngine() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry entry = c.append(1, 10);
    c.collect();
    Message offer =
        c.network.stream().filter(m -> m.type == MessageType.ENTRY && m.to == 2).findFirst().get();
    c.stores.get(2).failOn = RecordType.INSTALL;
    try {
      c.node(2).receive(offer);
      fail("expected durable failure");
    } catch (IOException expected) {
    }
    assertTrue(c.node(2).isFailed());
    assertTrue(c.node(2).drainMessages().isEmpty());
    assertFalse(c.node(2).isCommitted(entry.id));
  }

  @Test
  public void restartRetainsCommitAndApplicationIdentities() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry entry = c.append(1, 10);
    c.pump();
    c.restart(2);
    c.node(1).resend();
    c.pump();
    assertEquals(1, c.applications.get(2).size());
    assertTrue(c.node(2).isApplied(entry.id));
    List<EntryId> rebuilt = new ArrayList<>();
    c.node(2).replayCommitted(e -> rebuilt.add(e.id));
    assertEquals(Arrays.asList(entry.id), rebuilt);
  }

  @Test
  public void duplicateEntryAndAckDoNotIncreaseCountOrQuorum() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry entry = c.append(1, 10);
    c.collect();
    Message offer =
        c.network.stream().filter(m -> m.type == MessageType.ENTRY && m.to == 2).findFirst().get();
    c.network.clear();
    c.node(2).receive(offer);
    c.node(2).receive(offer);
    c.collect();
    assertEquals(2, c.node(2).currentRank().count);
    c.pump();
    assertTrue(c.node(1).isCommitted(entry.id));
    assertEquals(1, c.applications.get(2).size());
  }

  @Test
  public void barrierIsSingletonAndNextDataJoinsTheNoOpPartitionAtOnce() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    Entry barrier = c.node(1).append(Kind.BARRIER, 0, new byte[] {99}, 1);
    c.pump();
    assertTrue(c.node(1).isApplied(barrier.id));
    // A no-op closes the barrier partition; the data enters the partition it opens.
    Entry data = c.append(1, 20);
    assertNotNull(data);
    assertTrue(c.node(1).hasPendingProposal());
    c.pump();
    assertEquals(barrier.partition + 1, data.partition);
    assertEquals(
        1,
        c.node(1).installedEntries().stream()
            .filter(e -> e.partition == barrier.partition)
            .count());
    assertTrue(c.node(1).isApplied(data.id));
  }

  @Test
  public void noOpOnlyPartitionDoesNotRollForeverAndTimeBoundaryCompletes() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.node(1).tick(1000);
    c.pump();
    assertEquals(0, c.node(1).currentPartition());
    c.append(1, 10);
    c.pump();
    c.node(1).tick(1001);
    c.pump();
    assertEquals(1, c.node(1).currentPartition());
    c.node(1).tick(2000);
    c.pump();
    assertEquals(1, c.node(1).currentPartition());
  }

  @Test
  public void leaderCrashBeforeLearningDataAcksStillPreservesEntry() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry entry = c.append(1, 10);
    c.drop = m -> m.type == MessageType.ENTRY_ACK;
    c.pump();
    assertFalse(c.node(1).isCommitted(entry.id));
    c.engines.remove(1);
    c.drop = ignored -> false;
    c.elect(2);
    assertTrue(c.node(2).isApplied(entry.id));
    assertTrue(c.node(3).isApplied(entry.id));
  }

  @Test
  public void higherTermFencesDelayedOldOffersAndAcks() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry entry = c.append(1, 10);
    c.collect();
    Message oldOffer =
        c.network.stream().filter(m -> m.type == MessageType.ENTRY && m.to == 2).findFirst().get();
    c.network.clear();
    c.node(2).startElection();
    c.node(2).drainMessages();
    c.node(2).receive(oldOffer);
    assertFalse(c.node(2).installedEntries().contains(entry));
    assertTrue(c.node(2).drainMessages().stream().noneMatch(m -> m.type == MessageType.ENTRY_ACK));
    assertEquals(2, c.stores.get(2).records.get(c.stores.get(2).records.size() - 1).term);
  }

  @Test
  public void latestConfirmationIncrementallyCatchesUpAnEmptyReplica() throws Exception {
    Cluster c = new Cluster();
    c.drop = m -> m.to == 3;
    c.elect(1);
    c.append(1, 10);
    c.pump();
    c.append(1, 5);
    c.pump();
    c.append(1, 3);
    c.pump();
    assertTrue(c.node(3).installedEntries().isEmpty());
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertEquals(c.node(1).installedEntries(), c.node(3).installedEntries());
    assertEquals(3, c.applications.get(3).size());
  }

  @Test
  public void partialProposalIsNotRetainedAndIsExcludedFromRecovery() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry missing = c.append(1, 10);
    c.drop = m -> m.to == 2 && m.type == MessageType.ENTRY;
    c.pump();
    Entry successor = c.append(1, 5);
    c.drop =
        m -> m.to == 2 && (m.type == MessageType.TRANSFER_ENTRY || m.type == MessageType.CONFIRM);
    c.pump();
    assertFalse(c.node(2).installedEntries().contains(missing));
    assertFalse(c.node(2).retainedProposals().stream().anyMatch(p -> p.id().equals(successor.id)));
    c.restart(2);
    assertFalse(c.node(2).retainedProposals().stream().anyMatch(p -> p.id().equals(successor.id)));
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertTrue(c.node(2).isApplied(successor.id));
  }

  @Test
  public void commitNotificationBeforeDataIsRetainedAcrossRestart() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry entry = c.append(1, 10);
    c.drop = m -> m.to == 2 && m.type == MessageType.ENTRY;
    c.pump();
    assertTrue(c.node(2).isCommitted(entry.id));
    assertFalse(c.node(2).isApplied(entry.id));
    c.restart(2);
    assertTrue(c.node(2).isCommitted(entry.id));
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertTrue(c.node(2).isApplied(entry.id));
  }

  @Test
  public void reorderedDuplicateTrafficAcrossBoundariesPreservesAllWrites() throws Exception {
    for (int seed = 0; seed < 20; seed++) {
      Cluster c = new Cluster();
      c.elect(1);
      Random random = new Random(seed);
      List<EntryId> submitted = new ArrayList<>();
      for (int i = 0; i < 30; i++) {
        Entry entry = c.append(1, i % 4 == 0 ? 1 : i + 100);
        if (entry != null) {
          submitted.add(entry.id);
        }
        c.collect();
        int budget = 10000;
        while (!c.network.isEmpty()) {
          assertTrue(budget-- > 0);
          Message message = c.network.remove(random.nextInt(c.network.size()));
          c.node(message.to).receive(message);
          if (random.nextInt(5) == 0) {
            c.node(message.to).receive(message);
          }
          c.collect();
        }
        c.node(1).resend();
        c.pump();
      }
      for (int node = 1; node <= 3; node++) {
        assertEquals(new HashSet<>(submitted), new HashSet<>(c.applications.get(node)));
      }
    }
  }

  @Test
  public void repeatedCrashRecoveryAndLargeManifestChainRemainExecutable() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    int leader = 1;
    List<EntryId> committed = new ArrayList<>();
    for (int round = 0; round < 6; round++) {
      for (int i = 0; i < 50; i++) {
        Entry entry = c.append(leader, 1);
        c.pump();
        if (entry != null) {
          committed.add(entry.id);
        }
      }
      int old = leader;
      c.engines.remove(old);
      leader = old % 3 + 1;
      c.elect(leader);
      c.restart(old);
      c.node(leader).resend();
      c.pump();
    }
    for (int node = 1; node <= 3; node++) {
      assertEquals(new HashSet<>(committed), new HashSet<>(c.applications.get(node)));
    }
  }

  @Test
  public void oneEntryWindowCountsUnacknowledgedEntriesAndRetriesReuseSlot() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    for (TRaftEngine engine : c.engines.values()) {
      engine.setReplicationWindow(1);
    }
    List<EntryId> submitted = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      submitted.add(c.append(1, i).id);
    }
    assertEquals(1, c.node(1).inFlightEntries(2));
    c.drop = m -> m.type == MessageType.ENTRY_ACK && m.from == 2;
    c.pump();
    assertEquals(1, c.node(1).inFlightEntries(2));
    assertEquals(0, c.node(1).inFlightEntries(3));
    assertEquals(1, c.applications.get(2).size());
    c.node(1).resend();
    c.pump();
    assertEquals(1, c.node(1).inFlightEntries(2));
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertEquals(0, c.node(1).inFlightEntries(2));
    assertEquals(new HashSet<>(submitted), new HashSet<>(c.applications.get(2)));
  }

  @Test
  public void metadataRecoveryFetchesMissingPayloadIncrementallyWithinWindowOne() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<Entry> entries = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      entries.add(c.append(1, i));
    }
    c.drop =
        m ->
            m.type == MessageType.ENTRY
                && ((m.to == 2 && m.entry.timestamp % 2 == 0)
                    || (m.to == 3 && m.entry.timestamp % 2 != 0));
    c.pump();
    c.engines.remove(1);
    c.node(2).setReplicationWindow(1);
    c.node(3).setReplicationWindow(1);
    c.drop = m -> m.type == MessageType.TRANSFER_ENTRY;
    c.node(2).startElection();
    c.pump();
    assertEquals(Role.LEADER_CANDIDATE, c.node(2).role());
    assertFalse(c.node(2).hasPendingProposal());
    assertEquals(1, c.node(2).outstandingFetches(3));
    assertEquals(1, c.node(3).inFlightEntries(2));
    assertEquals(4, c.node(2).installedEntries().size());
    assertTrue(c.node(2).retainedProposals().stream().noneMatch(p -> p.first.id.term == 2));
    c.drop = ignored -> false;
    c.node(2).resend();
    c.pump();
    assertEquals(Role.LEADER, c.node(2).role());
    for (Entry entry : entries) {
      assertTrue(c.node(2).isApplied(entry.id));
      assertTrue(c.node(3).isApplied(entry.id));
    }
    assertEquals(0, c.node(2).outstandingFetches(3));
    assertEquals(0, c.node(2).inFlightEntries(3));
  }

  @Test
  public void reportAndProposalEncodingDoNotReadPayloadAndRoundSharesOneLoad() throws Exception {
    Entry resident = new Entry(new EntryId(1, 1), 0, 1, 0, Kind.DATA, new byte[1024 * 1024]);
    AtomicInteger reads = new AtomicInteger();
    Entry lazy =
        Entry.lazy(
            resident,
            () -> {
              reads.incrementAndGet();
              return resident.payload();
            });
    Proposal proposal = new Proposal(null, Collections.emptyList(), lazy, true);
    TRaftProtocol.Report report =
        new TRaftProtocol.Report(Arrays.asList(lazy), Arrays.asList(lazy), Arrays.asList(proposal));
    Message reportMessage =
        new Message(MessageType.REPORT, 1, 2, 1, null, null, null, null, report, -1, null);
    long reportBound = TRaftCodec.encodedMessageUpperBound(reportMessage);
    assertEquals(0, reads.get());
    byte[] reportBytes = TRaftCodec.encodeMessage(reportMessage);
    assertTrue(reportBytes.length <= reportBound);
    assertTrue(reportBytes.length < 1024);
    assertEquals(0, reads.get());
    Message decoded = TRaftCodec.decodeMessage(reportBytes);
    assertFalse(decoded.report.stored.get(0).hasPayload());
    assertEquals(resident.payloadSize(), decoded.report.stored.get(0).payloadSize());
    Message proposalMessage =
        new Message(MessageType.PROPOSAL, 1, 2, 1, null, proposal, null, null, null, -1, null);
    TRaftCodec.encodeMessage(proposalMessage);
    assertEquals(0, reads.get());
    Map<EntryId, byte[]> round = new HashMap<>();
    for (int target : Arrays.asList(2, 3)) {
      Message data =
          new Message(
              MessageType.ENTRY, 1, target, 1, lazy, null, resident.id, null, null, -1, null);
      int before = reads.get();
      long bound = TRaftCodec.encodedMessageUpperBound(data);
      assertEquals(before, reads.get());
      assertTrue(TRaftCodec.encodeMessage(data, round).length <= bound);
    }
    assertEquals(1, reads.get());
  }

  @Test
  public void lostTransferAckIsRetriedWithinSameWindowSlot() throws Exception {
    Cluster c = new Cluster();
    for (TRaftEngine engine : c.engines.values()) {
      engine.setReplicationWindow(1);
    }
    c.drop = m -> m.to == 3;
    c.elect(1);
    for (int i = 0; i < 5; i++) {
      c.append(1, 1);
      c.pump();
    }
    c.drop = m -> m.type == MessageType.TRANSFER_ACK && m.from == 3;
    c.node(1).resend();
    c.pump();
    assertEquals(1, c.node(1).inFlightEntries(3));
    assertTrue(c.node(3).outstandingFetches(1) <= 1);
    c.drop = ignored -> false;
    for (int i = 0; i < 8; i++) {
      c.node(1).resend();
      c.node(3).resend();
      c.pump();
    }
    assertEquals(c.node(1).installedEntries(), c.node(3).installedEntries());
    assertEquals(0, c.node(1).inFlightEntries(3));
    assertEquals(5, c.applications.get(3).size());
  }

  @Test
  public void reportPagesKeepRangeManifestWholeAndOnlyCompleteSnapshotCounts() throws Exception {
    Cluster c = new Cluster();
    c.theta = 128;
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
      c.node(id).setControlPageBytes(1024);
    }
    c.elect(1);
    for (int i = 0; i < 150; i++) {
      c.append(1, i);
      c.pump();
    }
    List<Message> held = new ArrayList<>();
    c.drop =
        m -> {
          if (m.type == MessageType.REPORT) {
            held.add(m);
            return true;
          }
          return m.from == 3 || m.to == 3;
        };
    c.node(2).startElection();
    c.pump();
    assertEquals(Role.LEADER_CANDIDATE, c.node(2).role());
    assertEquals(1, held.size());
    Message first = held.get(0);
    assertTrue(first.report.totalPages > 20);
    Message last = requestedReportPage(c.node(1), first, first.report.totalPages - 1);
    Map<EntryId, Integer> sliceCounts = new HashMap<>();
    int largestSlice = 0;
    for (TRaftProtocol.ProposalSlice slice : last.report.proposalSlices) {
      sliceCounts.put(slice.first.id, sliceCounts.getOrDefault(slice.first.id, 0) + 1);
      largestSlice = Math.max(largestSlice, slice.members.size());
    }
    long progress = c.node(2).electionProgress();
    c.node(2).receive(last);
    assertTrue(c.node(2).electionProgress() > progress);
    progress = c.node(2).electionProgress();
    c.node(2).receive(last);
    assertEquals(progress, c.node(2).electionProgress());
    TRaftProtocol.Report otherGeneration =
        new TRaftProtocol.Report(
            last.report.installed,
            last.report.stored,
            last.report.retained,
            "different-reporter-incarnation",
            last.report.pageIndex,
            last.report.totalPages,
            last.report.proposalSlices);
    c.node(2)
        .receive(
            new Message(
                MessageType.REPORT,
                last.from,
                last.to,
                last.term,
                null,
                null,
                null,
                null,
                otherGeneration,
                -1,
                null));
    assertEquals(progress, c.node(2).electionProgress());
    c.node(2).drainMessages();
    for (int index = 0; index < first.report.totalPages - 1; index++) {
      assertEquals(Role.LEADER_CANDIDATE, c.node(2).role());
      assertFalse(c.node(2).hasPendingProposal());
      Message page = requestedReportPage(c.node(1), first, index);
      assertEquals(first.report.generation, page.report.generation);
      byte[] encoded = TRaftCodec.encodeMessage(page);
      assertTrue(encoded.length <= 1024);
      assertTrue(encoded.length <= TRaftCodec.encodedMessageUpperBound(page));
      for (TRaftProtocol.ProposalSlice slice : page.report.proposalSlices) {
        sliceCounts.put(slice.first.id, sliceCounts.getOrDefault(slice.first.id, 0) + 1);
        largestSlice = Math.max(largestSlice, slice.members.size());
      }
      c.node(2).receive(TRaftCodec.decodeMessage(encoded));
      if (index < first.report.totalPages - 2) {
        c.node(2).drainMessages();
      }
    }
    // An ordinary manifest is a range: one slice of a page carries all of its members.
    assertTrue(sliceCounts.values().stream().allMatch(count -> count == 1));
    assertTrue(largestSlice >= 128);
    assertTrue(c.node(2).hasPendingProposal());
    c.drop = m -> m.from == 3 || m.to == 3;
    c.pump();
    assertEquals(Role.LEADER, c.node(2).role());
    assertEquals(c.applications.get(1), c.applications.get(2));
    progress = c.node(2).electionProgress();
    c.node(2).receive(first);
    assertEquals(progress, c.node(2).electionProgress());
  }

  private static Message requestedReportPage(TRaftEngine source, Message first, int index)
      throws Exception {
    TRaftProtocol.Report request =
        new TRaftProtocol.Report(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            first.report.generation,
            index,
            first.report.totalPages,
            Collections.emptyList());
    source.receive(
        new Message(
            MessageType.REPORT_REQUEST,
            first.to,
            first.from,
            first.term,
            null,
            null,
            null,
            null,
            request,
            -1,
            null));
    List<Message> replies = source.drainMessages();
    for (Message reply : replies) {
      if (reply.type == MessageType.REPORT) {
        return reply;
      }
    }
    throw new AssertionError("missing report page");
  }

  @Test
  public void lostReportPageRetriesSameGenerationAndVotesDoNotRenewOnDuplicates() throws Exception {
    Cluster c = new Cluster();
    for (TRaftEngine engine : c.engines.values()) {
      engine.setControlPageBytes(1024);
    }
    c.elect(1);
    for (int i = 0; i < 40; i++) {
      c.append(1, i);
      c.pump();
    }
    List<Message> lost = new ArrayList<>();
    c.drop =
        m -> {
          if (m.type == MessageType.REPORT && m.report.pageIndex == 1 && lost.isEmpty()) {
            lost.add(m);
            return true;
          }
          return m.from == 3 || m.to == 3;
        };
    c.node(2).startElection();
    c.pump();
    assertEquals(1, lost.size());
    assertEquals(Role.LEADER_CANDIDATE, c.node(2).role());
    long progress = c.node(2).electionProgress();
    c.node(2)
        .receive(
            new Message(
                MessageType.VOTE, 1, 2, c.node(2).term(), null, null, null, null, null, -1, null));
    assertEquals(progress, c.node(2).electionProgress());
    Message retry = requestedReportPage(c.node(1), lost.get(0), 1);
    assertArrayEquals(TRaftCodec.encodeMessage(lost.get(0)), TRaftCodec.encodeMessage(retry));
    c.node(2).resend();
    c.pump();
    assertEquals(Role.LEADER, c.node(2).role());
    long voterProgress = c.node(1).electionProgress();
    c.node(1)
        .receive(
            new Message(
                MessageType.VOTE_REQUEST,
                2,
                1,
                c.node(2).term(),
                null,
                null,
                null,
                c.node(2).currentRank(),
                null,
                -1,
                null));
    assertEquals(voterProgress, c.node(1).electionProgress());
  }

  @Test
  public void reverseRecoveryReceiptsAdvanceOnceEvenWhenRepeatedFetchReopensTheSlot()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    for (int i = 1; i <= 6; i++) {
      c.append(1, i);
    }
    c.drop =
        m ->
            m.type == MessageType.ENTRY
                && ((m.to == 2 && m.entry.timestamp % 2 == 0)
                    || (m.to == 3 && m.entry.timestamp % 2 != 0));
    c.pump();
    c.engines.remove(1);
    c.node(2).setReplicationWindow(1);
    c.node(3).setReplicationWindow(1);
    List<Message> reverseTransfers = new ArrayList<>();
    c.drop =
        m -> {
          if (m.type == MessageType.TRANSFER_ENTRY && m.to == 3) {
            reverseTransfers.add(m);
            return true;
          }
          return false;
        };
    c.node(2).startElection();
    c.pump();
    assertEquals(Role.LEADER_CANDIDATE, c.node(2).role());
    assertTrue(c.node(2).hasPendingProposal());
    assertEquals(1, reverseTransfers.size());
    c.node(2).resend();
    List<Message> recoveryRetries = c.node(2).drainMessages();
    assertTrue(recoveryRetries.stream().anyMatch(m -> m.type == MessageType.PROPOSAL));
    assertTrue(recoveryRetries.stream().anyMatch(m -> m.type == MessageType.TRANSFER_ENTRY));
    assertTrue(
        recoveryRetries.stream()
            .noneMatch(
                m ->
                    m.type == MessageType.ENTRY
                        || m.type == MessageType.CONFIRM
                        || m.type == MessageType.COMMIT));
    Message transferred = reverseTransfers.get(0);
    c.node(3).receive(transferred);
    List<Message> responses = c.node(3).drainMessages();
    Message receipt =
        responses.stream()
            .filter(m -> m.type == MessageType.TRANSFER_ACK)
            .findFirst()
            .orElseThrow(AssertionError::new);
    long progress = c.node(2).electionProgress();
    c.node(2).receive(receipt);
    assertEquals(progress + 1, c.node(2).electionProgress());
    assertEquals(Role.LEADER_CANDIDATE, c.node(2).role());
    assertTrue(c.node(2).hasPendingProposal());
    progress = c.node(2).electionProgress();
    c.node(2).receive(receipt);
    assertEquals(progress, c.node(2).electionProgress());
    c.node(2)
        .receive(
            new Message(
                MessageType.FETCH_ENTRY,
                3,
                2,
                c.node(2).term(),
                null,
                null,
                transferred.entry.id,
                null,
                null,
                -1,
                null));
    assertEquals(1, c.node(2).inFlightEntries(3));
    c.node(2).drainMessages();
    c.node(2).receive(receipt);
    assertEquals(0, c.node(2).inFlightEntries(3));
    assertEquals(progress, c.node(2).electionProgress());
    for (Message response : responses) {
      if (response.type != MessageType.TRANSFER_ACK) {
        c.network.add(response);
      }
    }
    c.drop = ignored -> false;
    c.node(2).resend();
    c.node(3).resend();
    c.pump();
    assertEquals(Role.LEADER, c.node(2).role());
    assertEquals(new HashSet<>(c.applications.get(2)), new HashSet<>(c.applications.get(3)));
  }

  @Test
  public void recoveryCommitHistoryIsJournaledInBoundedBatchesAndCrashRetainsOldFrontier()
      throws Exception {
    Cluster c = new Cluster();
    c.theta = 128;
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
    }
    c.elect(1);
    long previousFrontier = c.node(2).committedFrontier();
    int previouslyKnown = c.node(2).committedIds().size();
    c.drop = message -> message.type == MessageType.COMMIT && message.to == 2;
    for (int i = 0; i < 1100; i++) {
      c.append(1, i);
      c.pump();
    }
    assertEquals(previouslyKnown, c.node(2).committedIds().size());
    assertTrue(c.node(2).installedEntries().size() > 1024);
    List<EntryId> expectedApplied = new ArrayList<>(c.applications.get(1));
    int beforeRecovery = c.stores.get(2).records.size();
    c.engines.remove(1);
    c.drop = ignored -> false;
    c.elect(2);
    List<DurableRecord> records = c.stores.get(2).records;
    List<DurableRecord> commits = new ArrayList<>();
    int firstCommitIndex = -1;
    for (int i = beforeRecovery; i < records.size(); i++) {
      DurableRecord record = records.get(i);
      if (record.type == RecordType.COMMIT) {
        if (firstCommitIndex == -1) {
          firstCommitIndex = i;
        }
        commits.add(record);
        assertTrue(record.identities.size() <= 1024);
        TRaftCodec.decodeRecord(TRaftCodec.encodeRecord(record));
      }
    }
    assertEquals(2, commits.size());
    assertEquals(1024, commits.get(0).identities.size());
    assertEquals(previousFrontier, commits.get(0).frontier);
    assertTrue(commits.get(1).frontier > previousFrontier);
    assertEquals(expectedApplied, c.applications.get(2));

    MemoryStore interrupted = new MemoryStore();
    interrupted.records.addAll(records.subList(0, firstCommitIndex + 1));
    TRaftEngine partial =
        new TRaftEngine(
            2, new HashSet<>(Arrays.asList(1, 2, 3)), c.theta, 100, interrupted, entry -> {});
    assertEquals(previousFrontier, partial.committedFrontier());
    assertEquals(previouslyKnown + 1024, partial.committedIds().size());
    List<EntryId> partialApplied = new ArrayList<>();
    partial.replayCommitted(entry -> partialApplied.add(entry.id));
    assertTrue(expectedApplied.containsAll(partialApplied));
    assertTrue(partialApplied.size() < expectedApplied.size());

    c.restart(2);
    List<EntryId> replayed = new ArrayList<>();
    c.node(2).replayCommitted(entry -> replayed.add(entry.id));
    assertEquals(expectedApplied, replayed);
  }

  @Test
  public void sequentialOpenPartitionWorkAndCommitNoticesGrowLinearly() throws Exception {
    Cluster c = new Cluster();
    c.theta = 4096;
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
    }
    c.elect(1);
    long before = c.node(1).schedulingEntryVisits();
    AtomicInteger advertisedIds = new AtomicInteger();
    c.drop =
        message -> {
          if (message.type == MessageType.COMMIT) {
            assertTrue(message.identities.size() <= 1);
            advertisedIds.addAndGet(message.identities.size());
          }
          return false;
        };
    for (int i = 1; i <= 1500; i++) {
      Entry entry = c.append(1, i);
      c.pump();
      assertTrue(c.node(1).isApplied(entry.id));
    }
    assertEquals(0, c.node(1).currentPartition());
    assertEquals(3000, advertisedIds.get());
    assertTrue(
        "ordinary scheduling must not revisit a growing open prefix",
        c.node(1).schedulingEntryVisits() - before <= 4500);
  }

  @Test
  public void boundedCyclicCommitPagesRepairLostDeltasAfterFollowerRestart() throws Exception {
    Cluster c = new Cluster();
    c.theta = 4096;
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
    }
    c.elect(1);
    c.drop = message -> message.type == MessageType.COMMIT && message.to == 2;
    for (int i = 1; i <= 2200; i++) {
      c.append(1, i);
      c.pump();
    }
    assertTrue(c.applications.get(2).isEmpty());
    c.restart(2);
    AtomicInteger lostPages = new AtomicInteger();
    AtomicInteger largestPage = new AtomicInteger();
    c.drop =
        message -> {
          if (message.type == MessageType.COMMIT) {
            assertTrue(message.identities.size() <= 1024);
            largestPage.accumulateAndGet(message.identities.size(), Math::max);
            return message.to == 2 && lostPages.getAndIncrement() == 0;
          }
          return false;
        };
    for (int round = 0; round < 7; round++) {
      c.node(1).resend();
      c.pump();
    }
    assertEquals(1024, largestPage.get());
    assertEquals(new HashSet<>(c.applications.get(1)), new HashSet<>(c.applications.get(2)));
    c.restart(2);
    List<EntryId> rebuilt = new ArrayList<>();
    c.node(2).replayCommitted(entry -> rebuilt.add(entry.id));
    assertEquals(new HashSet<>(c.applications.get(1)), new HashSet<>(rebuilt));
  }

  @Test
  public void readySiblingsDoNotWaitForAHoleAndClosedFrontierRepairsLostNotices() throws Exception {
    Cluster c = new Cluster();
    c.theta = 256;
    for (int id = 1; id <= 3; id++) {
      c.restart(id);
    }
    c.elect(1);
    Entry hole = c.append(1, 1);
    c.drop =
        message ->
            (message.type == MessageType.ENTRY && message.entry.id.equals(hole.id))
                || (message.type == MessageType.COMMIT && message.to == 2);
    c.pump();
    for (int i = 2; i <= 120; i++) {
      Entry sibling = c.append(1, i);
      c.pump();
      assertTrue(c.node(1).isApplied(sibling.id));
    }
    assertFalse(c.node(1).isCommitted(hole.id));
    assertEquals(119, c.applications.get(1).size());
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertTrue(c.node(1).isApplied(hole.id));
    Entry boundary = c.append(1, 0);
    c.pump();
    assertTrue(c.node(2).isApplied(boundary.id));
    assertEquals(new HashSet<>(c.applications.get(1)), new HashSet<>(c.applications.get(2)));
  }

  @Test
  public void proposalAckChecksOnlyWindowIdentitiesAndExactAncestry() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<Entry> history = new ArrayList<>();
    for (int i = 0; i < 256; i++) {
      history.add(c.append(1, 0));
      c.pump();
    }
    Entry outsidePrefix = c.append(1, 1);
    c.pump();
    TRaftEngine leader = c.node(1);
    leader.setReplicationWindow(4);
    Proposal latest = leader.retainedProposals().get(leader.retainedProposals().size() - 1);
    for (Entry entry :
        Arrays.asList(history.get(0), history.get(70), history.get(150), outsidePrefix)) {
      leader.receive(
          new Message(
              MessageType.FETCH_ENTRY,
              3,
              1,
              leader.term(),
              null,
              null,
              entry.id,
              null,
              null,
              -1,
              null));
    }
    leader.drainMessages();
    assertEquals(4, leader.inFlightEntries(3));
    long before = leader.schedulingEntryVisits();
    leader.receive(
        new Message(
            MessageType.PROPOSAL_ACK,
            3,
            1,
            leader.term(),
            null,
            null,
            latest.id(),
            null,
            null,
            -1,
            null));
    assertEquals(
        "the opening proposal does not certify later same-partition entries",
        1,
        leader.inFlightEntries(3));
    assertTrue(
        "bounded window cleanup must not flatten 256 predecessors",
        leader.schedulingEntryVisits() - before < 64);
    leader.receive(
        new Message(
            MessageType.TRANSFER_ACK,
            3,
            1,
            leader.term(),
            null,
            null,
            outsidePrefix.id,
            null,
            null,
            -1,
            null));
    assertEquals(0, leader.inFlightEntries(3));
  }

  @Test
  public void nackBeforeOpeningConfirmationRetriesWithoutScanningThePartition() throws Exception {
    Cluster c = new Cluster();
    c.drop = message -> message.type == MessageType.CONFIRM && message.to == 2;
    c.elect(1);
    Entry entry = c.append(1, 10);
    c.pump();
    assertFalse(c.node(2).isApplied(entry.id));
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertTrue(c.node(2).isApplied(entry.id));
  }

  @Test
  public void snapshotReplayApplicationDoesNotRequirePerEntryAppliedJournalRecords()
      throws Exception {
    MemoryStore store = new MemoryStore();
    List<EntryId> effects = new ArrayList<>();
    Application application =
        new Application() {
          @Override
          public void apply(Entry entry) {
            if (!effects.contains(entry.id)) {
              effects.add(entry.id);
            }
          }

          @Override
          public boolean requiresDurableAppliedRecords() {
            return false;
          }
        };
    TRaftEngine engine = new TRaftEngine(1, Collections.singleton(1), 16, 0, store, application);
    engine.startElection();
    Entry entry = engine.append(Kind.DATA, 1, new byte[] {1}, 1);
    assertTrue(engine.isApplied(entry.id));
    assertEquals(Collections.singletonList(entry.id), effects);
    assertTrue(store.records.stream().noneMatch(record -> record.type == RecordType.APPLIED));
    effects.clear();
    TRaftEngine restarted = new TRaftEngine(1, Collections.singleton(1), 16, 0, store, application);
    assertTrue(restarted.isCommitted(entry.id));
    assertFalse(restarted.isApplied(entry.id));
    restarted.replayCommitted(application);
    restarted.applyCommitted();
    assertTrue(restarted.isApplied(entry.id));
    assertEquals(Collections.singletonList(entry.id), effects);
    assertTrue(store.records.stream().noneMatch(record -> record.type == RecordType.APPLIED));
  }

  @Test
  public void closedFrontierBeforeMissingDataSurvivesRestartAndLaterInstallation()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry missing = c.append(1, 10);
    c.drop =
        message ->
            message.to == 2
                && ((message.type == MessageType.ENTRY && message.entry.id.equals(missing.id))
                    || message.type == MessageType.PROPOSAL
                    || message.type == MessageType.CONFIRM);
    c.pump();
    Entry successor = c.append(1, 0);
    c.pump();
    assertEquals(0, c.node(2).committedFrontier());
    assertTrue(c.node(2).isCommitted(successor.id));
    assertTrue(c.applications.get(2).isEmpty());
    c.restart(2);
    assertTrue(c.applications.get(2).isEmpty());
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertEquals(Arrays.asList(missing.id, successor.id), c.applications.get(2));
    assertTrue(c.node(2).isApplied(missing.id));
    assertTrue(c.node(2).isApplied(successor.id));
  }

  private static Cluster volatileCommitCluster() throws IOException {
    Cluster cluster = new Cluster();
    cluster.durableCommitRecords = false;
    cluster.durableAppliedRecords = false;
    restartEmptyApplications(cluster);
    return cluster;
  }

  private static void restartEmptyApplications(Cluster cluster) throws IOException {
    cluster.network.clear();
    cluster.engines.clear();
    for (int id : cluster.members) {
      cluster.applications.get(id).clear();
      cluster.restart(id);
    }
  }

  private static void assertNoCommitOrAppliedRecords(Cluster cluster) {
    for (MemoryStore store : cluster.stores.values()) {
      assertTrue(
          store.records.stream()
              .noneMatch(
                  record -> record.type == RecordType.COMMIT || record.type == RecordType.APPLIED));
    }
  }

  private static Application rebuildingApplication(List<EntryId> state) {
    return new Application() {
      @Override
      public void apply(Entry entry) {
        if (!state.contains(entry.id)) {
          state.add(entry.id);
        }
      }

      @Override
      public boolean requiresDurableCommitRecords() {
        return false;
      }

      @Override
      public boolean requiresDurableAppliedRecords() {
        return false;
      }
    };
  }

  @Test
  public void volatileCommitRejectsDurableAppliedContractBeforeRecovery() throws Exception {
    MemoryStore store = new MemoryStore();
    Application incompatible =
        new Application() {
          @Override
          public void apply(Entry entry) {}

          @Override
          public boolean requiresDurableCommitRecords() {
            return false;
          }
        };
    try {
      new TRaftEngine(1, Collections.singleton(1), 16, 0, store, incompatible);
      fail("APPLIED records cannot outlive forgotten commitment in this adapter contract");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("volatile commitment"));
    }
    assertTrue(store.records.isEmpty());
  }

  @Test
  public void volatileCommitFullRestartPreservesAcknowledgedWritesWithoutCommitNotices()
      throws Exception {
    Cluster c = volatileCommitCluster();
    c.elect(1);
    c.drop = message -> message.type == MessageType.COMMIT;
    List<EntryId> acknowledged = new ArrayList<>();
    for (long timestamp : new long[] {10, 20, -1, 30}) {
      Entry entry = c.append(1, timestamp);
      c.pump();
      assertTrue(c.node(1).isApplied(entry.id));
      acknowledged.add(entry.id);
    }
    assertNoCommitOrAppliedRecords(c);
    restartEmptyApplications(c);
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).committedIds().isEmpty());
      c.node(id).applyCommitted();
      assertTrue(c.applications.get(id).isEmpty());
    }
    c.drop = ignored -> false;
    c.elect(2);
    for (int id = 1; id <= 3; id++) {
      assertEquals(new HashSet<>(acknowledged), new HashSet<>(c.applications.get(id)));
    }
    assertNoCommitOrAppliedRecords(c);
  }

  @Test
  public void volatileCommitRestartWithNoQuorumCannotApplyStoredAcknowledgedData()
      throws Exception {
    Cluster c = volatileCommitCluster();
    c.elect(1);
    Entry acknowledged = c.append(1, 10);
    c.pump();
    assertTrue(c.node(1).isApplied(acknowledged.id));
    restartEmptyApplications(c);
    c.engines.remove(2);
    c.engines.remove(3);
    c.node(1).startElection();
    c.pump();
    c.node(1).resend();
    c.pump();
    c.node(1).applyCommitted();
    assertEquals(Role.CANDIDATE, c.node(1).role());
    assertFalse(c.node(1).isCommitted(acknowledged.id));
    assertFalse(c.node(1).isApplied(acknowledged.id));
    assertTrue(c.applications.get(1).isEmpty());
    assertNoCommitOrAppliedRecords(c);
  }

  @Test
  public void volatileCommitFollowerRestartRelearnsLostOpenPartitionNotices() throws Exception {
    Cluster c = volatileCommitCluster();
    c.elect(1);
    Entry acknowledged = c.append(1, 10);
    c.pump();
    c.applications.get(2).clear();
    c.restart(2);
    c.node(2).applyCommitted();
    assertFalse(c.node(2).isCommitted(acknowledged.id));
    assertTrue(c.applications.get(2).isEmpty());
    c.drop = message -> message.type == MessageType.COMMIT && message.to == 2;
    c.node(1).resend();
    c.pump();
    assertTrue(c.applications.get(2).isEmpty());
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertEquals(Collections.singletonList(acknowledged.id), c.applications.get(2));
    assertNoCommitOrAppliedRecords(c);
  }

  @Test
  public void volatileCommitHoleDoesNotBlockSiblingAndRecoveryPreservesThatSibling()
      throws Exception {
    Cluster c = volatileCommitCluster();
    c.elect(1);
    Entry hole = c.append(1, 10);
    Entry sibling = c.append(1, 20);
    c.drop = message -> message.type == MessageType.ENTRY && message.entry.id.equals(hole.id);
    c.pump();
    assertFalse(c.node(1).isCommitted(hole.id));
    assertTrue(c.node(1).isApplied(sibling.id));
    assertEquals(Collections.singletonList(sibling.id), c.applications.get(1));
    restartEmptyApplications(c);
    // Only the sibling was quorum durable; electing from B,C may discard the uncommitted hole.
    c.engines.remove(1);
    c.drop = ignored -> false;
    c.elect(2);
    assertTrue(c.node(2).isApplied(sibling.id));
    assertTrue(c.node(3).isApplied(sibling.id));
    assertFalse(c.node(2).installedEntries().contains(hole));
    assertEquals(Collections.singletonList(sibling.id), c.applications.get(2));
    assertNoCommitOrAppliedRecords(c);
  }

  @Test
  public void volatileCommitInterruptedRecoveryInheritsDurablePendingProposal() throws Exception {
    Cluster c = volatileCommitCluster();
    c.elect(1);
    Entry a = c.append(1, 10);
    Entry b = c.append(1, 20);
    c.drop =
        message ->
            message.type == MessageType.ENTRY
                && ((message.to == 2 && message.entry.id.equals(a.id))
                    || (message.to == 3 && message.entry.id.equals(b.id)));
    c.pump();
    assertTrue(c.node(1).isApplied(a.id));
    assertTrue(c.node(1).isApplied(b.id));
    c.engines.remove(1);
    c.drop = message -> message.type == MessageType.PROPOSAL_ACK;
    c.node(2).startElection();
    c.pump();
    assertEquals(Role.LEADER_CANDIDATE, c.node(2).role());
    Proposal pending =
        c.node(2).retainedProposals().stream()
            .filter(proposal -> proposal.first.id.term == 2)
            .findFirst()
            .get();
    assertTrue(c.node(3).retainedProposals().contains(pending));
    restartEmptyApplications(c);
    c.drop = ignored -> false;
    c.elect(2);
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).installedEntries().contains(pending.first));
      assertTrue(c.node(id).isApplied(a.id));
      assertTrue(c.node(id).isApplied(b.id));
    }
    assertNoCommitOrAppliedRecords(c);
  }

  @Test
  public void volatileCommitRestoresSnapshotThenWaitsForEvidenceForItsSuffix() throws Exception {
    MemoryStore store = new MemoryStore();
    List<EntryId> state = new ArrayList<>();
    TRaftEngine engine =
        new TRaftEngine(1, Collections.singleton(1), 16, 0, store, rebuildingApplication(state));
    engine.startElection();
    Entry inSnapshot = engine.append(Kind.DATA, 10, new byte[] {1}, 1);
    List<EntryId> snapshot = new ArrayList<>(state);
    Entry afterSnapshot = engine.append(Kind.BARRIER, 0, new byte[] {2}, 2);
    assertTrue(engine.isApplied(afterSnapshot.id));
    state.clear();
    state.addAll(snapshot);
    Application restored = rebuildingApplication(state);
    TRaftEngine restarted = new TRaftEngine(1, Collections.singleton(1), 16, 0, store, restored);
    restarted.replayCommitted(restored);
    restarted.applyCommitted();
    assertEquals(Collections.singletonList(inSnapshot.id), state);
    assertFalse(restarted.isApplied(afterSnapshot.id));
    restarted.startElection();
    assertEquals(Arrays.asList(inSnapshot.id, afterSnapshot.id), state);
    assertTrue(restarted.isApplied(inSnapshot.id));
    assertTrue(restarted.isApplied(afterSnapshot.id));
    assertTrue(
        store.records.stream()
            .noneMatch(
                record -> record.type == RecordType.COMMIT || record.type == RecordType.APPLIED));
  }

  @Test
  public void volatileCommitRetainsLegacyCommitAndAppliedJournalCompatibility() throws Exception {
    MemoryStore store = new MemoryStore();
    List<EntryId> legacyState = new ArrayList<>();
    TRaftEngine legacy =
        new TRaftEngine(
            1, Collections.singleton(1), 16, 0, store, entry -> legacyState.add(entry.id));
    legacy.startElection();
    Entry old = legacy.append(Kind.DATA, 10, new byte[] {1}, 1);
    assertTrue(store.records.stream().anyMatch(record -> record.type == RecordType.COMMIT));
    assertTrue(store.records.stream().anyMatch(record -> record.type == RecordType.APPLIED));
    int legacyEnd = store.records.size();
    List<EntryId> rebuilt = new ArrayList<>();
    Application application = rebuildingApplication(rebuilt);
    TRaftEngine migrated = new TRaftEngine(1, Collections.singleton(1), 16, 0, store, application);
    migrated.replayCommitted(application);
    migrated.applyCommitted();
    assertEquals(Collections.singletonList(old.id), rebuilt);
    migrated.startElection();
    Entry recent = migrated.append(Kind.DATA, 20, new byte[] {2}, 2);
    assertTrue(migrated.isApplied(recent.id));
    assertTrue(
        store.records.subList(legacyEnd, store.records.size()).stream()
            .noneMatch(
                record -> record.type == RecordType.COMMIT || record.type == RecordType.APPLIED));
    rebuilt.clear();
    TRaftEngine again = new TRaftEngine(1, Collections.singleton(1), 16, 0, store, application);
    again.replayCommitted(application);
    again.applyCommitted();
    assertEquals(Collections.singletonList(old.id), rebuilt);
    assertFalse(again.isCommitted(recent.id));
    again.startElection();
    assertEquals(Arrays.asList(old.id, recent.id), rebuilt);
    assertTrue(again.isApplied(recent.id));
  }

  @Test
  public void volatileCommitBeforePayloadIsRelearnedAfterRestartFromClosedFrontier()
      throws Exception {
    Cluster c = volatileCommitCluster();
    c.elect(1);
    Entry missing = c.append(1, 10);
    c.drop =
        message -> message.to == 2 && message.entry != null && message.entry.id.equals(missing.id);
    c.pump();
    assertTrue(c.node(1).isApplied(missing.id));
    assertTrue(
        "The commit notification overtook its missing payload", c.node(2).isCommitted(missing.id));
    assertFalse(c.node(2).isApplied(missing.id));
    c.applications.get(2).clear();
    c.restart(2);
    assertFalse(
        "Volatile knowledge is not silently reconstructed from stored state",
        c.node(2).isCommitted(missing.id));
    c.node(2).applyCommitted();
    assertTrue(c.applications.get(2).isEmpty());
    c.drop =
        message ->
            message.to == 2
                && ((message.entry != null && message.entry.id.equals(missing.id))
                    || message.type == MessageType.PROPOSAL
                    || message.type == MessageType.CONFIRM);
    Entry successor = c.append(1, 0);
    c.pump();
    assertEquals(0, c.node(2).committedFrontier());
    assertTrue(c.node(2).isCommitted(successor.id));
    assertTrue(c.applications.get(2).isEmpty());
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertEquals(Arrays.asList(missing.id, successor.id), c.applications.get(2));
    assertTrue(c.node(2).isApplied(missing.id));
    assertTrue(c.node(2).isApplied(successor.id));
    assertNoCommitOrAppliedRecords(c);
  }

  private static void assertRepairCannotStrandBoundary(boolean volatileCommit) throws Exception {
    Cluster c = volatileCommit ? volatileCommitCluster() : new Cluster();
    c.elect(1);
    Entry first = c.append(1, 10);
    Entry boundary = c.append(1, 0);
    c.drop = message -> message.type == MessageType.ENTRY && message.entry.id.equals(first.id);
    c.pump();
    c.drop = ignored -> false;
    for (int round = 0; round < 4; round++) {
      c.node(1).resend();
      c.pump();
    }
    assertFalse(
        "Repair receipt must not erase ordinary-entry retry needed to confirm the boundary",
        c.node(1).hasPendingProposal());
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(first.id));
      assertTrue(c.node(id).isApplied(boundary.id));
    }
    if (volatileCommit) {
      assertNoCommitOrAppliedRecords(c);
    }
  }

  @Test
  public void repairReceiptCannotStrandOrdinaryEntryBeforeBoundaryConfirmation() throws Exception {
    assertRepairCannotStrandBoundary(false);
  }

  @Test
  public void volatileCommitRepairReceiptCannotStrandBoundaryConfirmation() throws Exception {
    assertRepairCannotStrandBoundary(true);
  }

  private static void assertRepairRestoresIntentWithoutQuorumEvidence(boolean nackFirst)
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry entry = c.append(1, 10);
    TRaftEngine leader = c.node(1);
    leader.receive(
        new Message(
            MessageType.FETCH_ENTRY,
            2,
            1,
            leader.term(),
            null,
            null,
            entry.id,
            null,
            null,
            -1,
            null));
    leader.drainMessages();
    assertEquals(1, leader.inFlightEntries(2));
    if (nackFirst) {
      // The NACK belongs to the old ENTRY attempt but reaches the replacement repair slot.
      leader.receive(
          new Message(
              MessageType.NACK,
              2,
              1,
              leader.term(),
              null,
              null,
              entry.id,
              null,
              null,
              leader.currentPartition(),
              null));
      assertEquals(0, leader.inFlightEntries(2));
    }
    Message receipt =
        new Message(
            MessageType.TRANSFER_ACK,
            2,
            1,
            leader.term(),
            null,
            null,
            entry.id,
            null,
            null,
            -1,
            null);
    leader.receive(receipt);
    List<Message> retry = leader.drainMessages();
    assertEquals(1, leader.inFlightEntries(2));
    assertTrue(
        retry.stream()
            .anyMatch(
                message ->
                    message.type == MessageType.ENTRY
                        && message.to == 2
                        && message.entry.id.equals(entry.id)));
    assertFalse(
        "Repair receipt is not installed-entry quorum evidence", leader.isCommitted(entry.id));
    assertFalse(leader.isApplied(entry.id));
    leader.receive(receipt);
    assertEquals(
        "A duplicate repair ACK must leave the newer ENTRY slot intact",
        1,
        leader.inFlightEntries(2));
    assertTrue(leader.drainMessages().isEmpty());
    leader.resend();
    List<Message> retried = leader.drainMessages();
    assertTrue(
        retried.stream()
            .anyMatch(
                message ->
                    message.type == MessageType.ENTRY
                        && message.to == 2
                        && message.entry.id.equals(entry.id)));
    c.network.addAll(retried);
    c.pump();
    assertTrue(leader.isApplied(entry.id));
    assertEquals(0, leader.inFlightEntries(2));
  }

  @Test
  public void lateRepairAckDoesNotClearNewOrdinarySlotOrSupplyQuorumEvidence() throws Exception {
    assertRepairRestoresIntentWithoutQuorumEvidence(false);
  }

  @Test
  public void oldEntryNackBeforeRepairAckStillRestoresOrdinaryIntent() throws Exception {
    assertRepairRestoresIntentWithoutQuorumEvidence(true);
  }

  @Test
  public void oldEntryNackAndLostRepairReceiptStillRetryOrdinaryEntry() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry first = c.append(1, 10);
    Entry boundary = c.append(1, 0);
    c.drop =
        message -> {
          if (message.type == MessageType.ENTRY && message.entry.id.equals(first.id)) {
            return true;
          }
          if (message.type == MessageType.TRANSFER_ACK && first.id.equals(message.reference)) {
            // Deliver the older ordinary attempt's NACK while the repair owns this ID's slot,
            // then lose the actual repair receipt. The follower already stored the payload.
            c.network.add(
                0,
                new Message(
                    MessageType.NACK,
                    message.from,
                    1,
                    message.term,
                    null,
                    null,
                    first.id,
                    null,
                    null,
                    0,
                    null));
            return true;
          }
          return false;
        };
    c.pump();
    assertTrue(c.node(1).hasPendingProposal());
    assertFalse(c.node(1).isCommitted(first.id));
    c.drop = ignored -> false;
    for (int round = 0; round < 4; round++) {
      c.node(1).resend();
      c.pump();
    }
    assertFalse(c.node(1).hasPendingProposal());
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(first.id));
      assertTrue(c.node(id).isApplied(boundary.id));
    }
  }

  @Test
  public void lateAncestorConfirmationDoesNotExpandItsInstalledHistory() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    for (int i = 0; i < 200; i++) {
      c.append(1, 0);
      c.pump();
    }
    TRaftEngine follower = c.node(2);
    Proposal old = follower.retainedProposals().get(100);
    List<Entry> before = follower.installedEntries();
    long work = follower.schedulingEntryVisits();
    follower.receive(
        new Message(
            MessageType.CONFIRM, 1, 2, follower.term(), null, old, null, null, null, -1, null));
    assertEquals(before, follower.installedEntries());
    assertTrue(
        "An exact installed ancestor needs only indexed ancestry lookup",
        follower.schedulingEntryVisits() - work < 32);
  }

  @Test
  public void confirmationOfAnotherBranchCannotUseInstalledAncestorFastPath() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    for (int i = 0; i < 5; i++) {
      c.append(1, 0);
      c.pump();
    }
    TRaftEngine follower = c.node(2);
    Proposal original = follower.retainedProposals().get(1);
    Entry otherFirst =
        new Entry(
            new EntryId(follower.term() + 1, 10000),
            original.first.partition,
            0,
            original.members.size(),
            Kind.NOOP,
            new byte[0]);
    Proposal other = new Proposal(original.base, original.members, otherFirst, true);
    List<Entry> before = follower.installedEntries();
    try {
      follower.receive(
          new Message(
              MessageType.CONFIRM,
              1,
              2,
              follower.term() + 1,
              null,
              other,
              null,
              null,
              null,
              -1,
              null));
      fail("Same partition number is not an exact ancestor identity");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("late confirmation changes closed prefix"));
    }
    assertEquals(before, follower.installedEntries());
  }

  @Test
  public void fixedSeedReorderingLossAndDuplicatesEventuallyDrainSmallWindowsAndBoundaries()
      throws Exception {
    for (long seed : new long[] {17, 28, 41, 54}) {
      Cluster c = (seed % 4 == 1) ? volatileCommitCluster() : new Cluster();
      c.theta = 5;
      restartEmptyApplications(c);
      for (TRaftEngine engine : c.engines.values()) {
        engine.setReplicationWindow(2);
      }
      c.elect(1);
      Random random = new Random(seed);
      List<EntryId> accepted = new ArrayList<>();
      int round;
      for (round = 0; round < 1200; round++) {
        for (int admission = 0; admission < 8 && accepted.size() < 120; admission++) {
          if (c.node(1).hasPendingProposal()) {
            break;
          }
          int sequence = accepted.size();
          Kind kind = sequence % 23 == 22 ? Kind.BARRIER : Kind.DATA;
          long timestamp = sequence % 3 == 2 ? 0 : sequence * 10L;
          Entry entry = c.node(1).append(kind, timestamp, new byte[] {(byte) sequence}, 1);
          if (entry != null) {
            accepted.add(entry.id);
          }
        }
        for (TRaftEngine engine : c.engines.values()) {
          engine.resend();
        }
        c.collect();
        for (int delivery = 0; delivery < 64 && !c.network.isEmpty(); delivery++) {
          Message message = c.network.remove(random.nextInt(c.network.size()));
          boolean unreliable = round < 200;
          if (unreliable && random.nextInt(100) < 17) {
            continue;
          }
          if (unreliable && random.nextInt(100) < 9) {
            c.network.add(message);
          }
          c.node(message.to).receive(TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(message)));
          c.collect();
        }
        if (round >= 200
            && accepted.size() == 120
            && c.applications.values().stream()
                .allMatch(applied -> applied.containsAll(accepted))) {
          break;
        }
      }
      assertTrue(
          "finite faults must drain, seed=" + seed + ", accepted=" + accepted.size(), round < 1200);
      assertEquals(120, accepted.size());
      c.pump();
      Map<EntryId, Long> partition = new HashMap<>();
      for (Entry entry : c.node(1).installedEntries()) {
        partition.put(entry.id, entry.partition);
      }
      for (int id = 1; id <= 3; id++) {
        assertEquals(new HashSet<>(accepted), new HashSet<>(c.applications.get(id)));
        assertEquals(accepted.size(), c.applications.get(id).size());
        long previous = -1;
        for (EntryId applied : c.applications.get(id)) {
          assertTrue(
              "partition application order, seed=" + seed, partition.get(applied) >= previous);
          previous = partition.get(applied);
          assertTrue(c.node(id).isCommitted(applied));
          assertTrue(c.node(id).isApplied(applied));
        }
      }
    }
  }

  private static Message protocolMessage(
      MessageType type,
      int from,
      int to,
      long term,
      Entry entry,
      Proposal proposal,
      EntryId reference) {
    return new Message(type, from, to, term, entry, proposal, reference, null, null, -1, null);
  }

  @Test
  public void normalFirstPayloadUsesDurableBatchWithoutFetchRoundTrip() throws Exception {
    Cluster c = new Cluster();
    for (MemoryStore store : c.stores.values()) {
      store.rejectUndurableHandles = true;
    }
    c.elect(1);
    c.append(1, 10);
    c.pump();
    Entry boundary = c.append(1, 0);
    AtomicInteger payloadProposals = new AtomicInteger();
    AtomicInteger firstFetches = new AtomicInteger();
    c.drop =
        message -> {
          if (message.type == MessageType.PROPOSAL
              && message.entry != null
              && message.entry.id.equals(boundary.id)) {
            payloadProposals.incrementAndGet();
            assertFalse("Manifest first remains metadata", message.proposal.first.hasPayload());
          }
          if (message.type == MessageType.FETCH_ENTRY && boundary.id.equals(message.reference)) {
            firstFetches.incrementAndGet();
          }
          return false;
        };
    c.pump();
    assertEquals(2, payloadProposals.get());
    assertEquals(0, firstFetches.get());
    for (int node = 1; node <= 3; node++) {
      assertEquals(
          Collections.singletonList(Arrays.asList(RecordType.ENTRY, RecordType.RETAIN_PROPOSAL)),
          c.stores.get(node).batches);
      assertTrue(c.node(node).isApplied(boundary.id));
    }
  }

  @Test
  public void firstReceiptReleasesPayloadWithoutCertifyingOrRepeatedlyOccupyingWindow()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    c.node(1).setReplicationWindow(1);
    Entry boundary = c.append(1, 0);
    Message carried =
        c.node(1).drainMessages().stream()
            .filter(message -> message.to == 2 && message.type == MessageType.PROPOSAL)
            .findFirst()
            .get();
    assertNotNull(carried.entry);
    c.node(2).receive(carried);
    // A follower that can ACK answers with the PROPOSAL_ACK alone: that certificate releases the
    // payload slot. Here it is lost.
    List<Message> answers = c.node(2).drainMessages();
    assertTrue(answers.stream().anyMatch(message -> message.type == MessageType.PROPOSAL_ACK));
    assertFalse(answers.stream().anyMatch(message -> message.type == MessageType.TRANSFER_ACK));
    // A receipt alone, as from a follower that stored the first but cannot ACK yet, releases the
    // payload slot without certifying anything.
    c.node(1)
        .receive(
            protocolMessage(
                MessageType.TRANSFER_ACK, 2, 1, c.node(1).term(), null, null, boundary.id));
    assertEquals(0, c.node(1).inFlightEntries(2));
    assertTrue(c.node(1).hasPendingProposal());
    assertFalse(c.node(1).isCommitted(boundary.id));
    c.node(1).resend();
    List<Message> retries = c.node(1).drainMessages();
    assertTrue(
        retries.stream()
            .anyMatch(
                message ->
                    message.to == 2
                        && message.type == MessageType.PROPOSAL
                        && message.entry == null));
    assertFalse(
        retries.stream()
            .anyMatch(
                message ->
                    message.to == 2
                        && message.type == MessageType.PROPOSAL
                        && message.entry != null));
    c.network.addAll(retries);
    c.pump();
    assertTrue(c.node(1).isApplied(boundary.id));
  }

  @Test
  public void fetchGraceLetsInFlightMembersArriveAndFetchesOnlyAfterItExpires() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    c.node(2).setFetchGrace(300);
    AtomicInteger fetches = new AtomicInteger();
    List<Message> held = new ArrayList<>();
    // 0: deliver entries to node 2; 1: hold them back for later; 2: lose them.
    AtomicInteger entriesToTwo = new AtomicInteger(1);
    c.drop =
        message -> {
          if (message.from == 2 && message.type == MessageType.FETCH_ENTRY) {
            fetches.incrementAndGet();
          }
          if (message.to == 2 && message.type == MessageType.ENTRY && message.entry != null) {
            int mode = entriesToTwo.get();
            if (mode == 1) {
              held.add(message);
            }
            return mode != 0;
          }
          return false;
        };
    // The entry for ts 20 is still "in flight" to node 2 when the boundary naming it arrives.
    Entry member = c.append(1, 20);
    c.pump();
    Entry boundary = c.append(1, 0);
    c.pump();
    assertFalse(c.node(2).isApplied(boundary.id));
    assertEquals("a member in flight is not fetched within the grace", 0, fetches.get());
    c.node(2).tick(0);
    c.pump();
    assertEquals(0, fetches.get());
    entriesToTwo.set(0);
    c.network.addAll(held);
    held.clear();
    c.pump();
    assertTrue(c.node(2).isApplied(member.id));
    assertTrue(c.node(2).isApplied(boundary.id));
    assertEquals("the late entry completed the proposal without a fetch", 0, fetches.get());

    // A member that never arrives is fetched once the grace has passed.
    entriesToTwo.set(2);
    Entry lost = c.append(1, 30);
    c.pump();
    Entry second = c.append(1, 5);
    c.pump();
    assertEquals(0, fetches.get());
    Thread.sleep(350);
    c.node(2).tick(0);
    c.pump();
    assertTrue("the grace expired: the member is fetched", fetches.get() >= 1);
    assertTrue(c.node(2).isApplied(lost.id));
    assertTrue(c.node(2).isApplied(second.id));
  }

  private static void assertMissingPrefixAndLostFirstReceipt(int window) throws Exception {
    Cluster c = new Cluster();
    for (TRaftEngine engine : c.engines.values()) {
      engine.setReplicationWindow(window);
    }
    c.elect(1);
    c.drop = message -> message.to == 3 || message.from == 3;
    Entry first = c.append(1, 10);
    c.pump();
    Entry second = c.append(1, 20);
    c.pump();
    // A new leader has the full durable prefix but no stale transfer slots to node 3.
    c.elect(2);
    Entry unreplicated = c.append(2, 30);
    c.pump();
    Entry boundary = c.append(2, 0);
    c.collect();
    Message carried =
        c.network.stream()
            .filter(
                message ->
                    message.to == 3
                        && message.type == MessageType.PROPOSAL
                        && message.entry != null
                        && message.entry.id.equals(boundary.id))
            .findFirst()
            .orElse(null);
    if (carried == null) {
      // The ordinary DATA occupying this peer's small window is no longer needed
      // after the healthy peer certifies the successor. Deliver its old NACK to
      // exercise a carried proposal queued behind that slot.
      c.node(2)
          .receive(
              protocolMessage(
                  MessageType.NACK, 3, 2, c.node(2).term(), null, null, unreplicated.id));
      c.collect();
      carried =
          c.network.stream()
              .filter(
                  message ->
                      message.to == 3
                          && message.type == MessageType.PROPOSAL
                          && message.entry != null
                          && message.entry.id.equals(boundary.id))
              .findFirst()
              .get();
    }
    c.pump();
    assertFalse("Healthy peer confirmed the current proposal", c.node(2).hasPendingProposal());
    assertTrue(c.node(2).isApplied(boundary.id));
    c.node(3).receive(carried);
    assertFalse(
        "Stored first is not a retained complete proposal",
        c.node(3).retainedProposals().stream().anyMatch(p -> p.id().equals(boundary.id)));
    assertFalse(c.node(3).isApplied(boundary.id));
    AtomicInteger receipts = new AtomicInteger();
    AtomicInteger duplicatePayloads = new AtomicInteger();
    c.drop =
        message -> {
          for (int peer = 1; peer <= 3; peer++) {
            for (int destination = 1; destination <= 3; destination++) {
              assertTrue(c.node(peer).inFlightEntries(destination) <= window);
              assertTrue(c.node(peer).outstandingFetches(destination) <= window);
            }
          }
          if (message.to == 3
              && message.type == MessageType.PROPOSAL
              && message.entry != null
              && boundary.id.equals(message.entry.id)) {
            duplicatePayloads.incrementAndGet();
          }
          if (message.from == 3
              && message.type == MessageType.TRANSFER_ACK
              && boundary.id.equals(message.reference)) {
            return receipts.incrementAndGet() == 1;
          }
          // Until the duplicate receipt is exercised, do not let a stronger complete-prefix
          // ACK release the same payload slot and make the lost-receipt case vacuous at w=2.
          return message.from == 3
              && message.type == MessageType.PROPOSAL_ACK
              && boundary.id.equals(message.reference)
              && receipts.get() < 2;
        };
    c.pump();
    for (int retry = 0; retry < 8 && (retry == 0 || !c.node(3).isApplied(boundary.id)); retry++) {
      c.node(2).resend();
      c.node(3).resend();
      c.pump();
    }
    assertTrue(
        "Lost first receipt must be repeated after current is confirmed", receipts.get() >= 2);
    assertTrue(duplicatePayloads.get() >= 1);
    assertTrue(c.node(3).isApplied(first.id));
    assertTrue(c.node(3).isApplied(second.id));
    assertTrue(c.node(3).isApplied(boundary.id));
  }

  @Test
  public void windowOneMissingPrefixAndLostFirstReceiptDrainAfterConfirmation() throws Exception {
    assertMissingPrefixAndLostFirstReceipt(1);
  }

  @Test
  public void windowTwoMissingPrefixAndLostFirstReceiptDrainAfterConfirmation() throws Exception {
    assertMissingPrefixAndLostFirstReceipt(2);
  }

  @Test
  public void combinedProposalOverBudgetFallsBackToMetadataWithoutTakingPayloadSlot()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry previous = c.append(1, 10);
    c.pump();
    Proposal base = c.node(1).retainedProposals().get(0);
    byte[] bytes = new byte[1024];
    Entry expected =
        new Entry(
            new EntryId(c.node(1).term(), previous.id.index + 1),
            previous.partition + 1,
            0,
            2,
            Kind.DATA,
            bytes);
    Proposal manifest =
        new Proposal(
            base.id(), Arrays.asList(base.id(), previous.id), Entry.metadata(expected), false);
    Message combined =
        protocolMessage(MessageType.PROPOSAL, 1, 2, c.node(1).term(), expected, manifest, null);
    long budget =
        Math.max(
            TRaftCodec.encodedMessageUpperBound(
                protocolMessage(
                    MessageType.TRANSFER_ENTRY, 1, 2, c.node(1).term(), expected, null, null)),
            TRaftCodec.encodedMessageUpperBound(
                protocolMessage(
                    MessageType.PROPOSAL, 1, 2, c.node(1).term(), null, manifest, null)));
    assertTrue(TRaftCodec.encodedMessageUpperBound(combined) > budget);
    c.node(1).setProposalPayloadBudget(budget);
    c.node(1).setReplicationWindow(1);
    Entry boundary = c.node(1).append(Kind.DATA, 0, bytes, 1);
    assertEquals(expected, boundary);
    assertEquals(0, c.node(1).inFlightEntries(2));
    List<Message> messages = c.node(1).drainMessages();
    assertTrue(
        messages.stream()
            .filter(message -> message.type == MessageType.PROPOSAL)
            .allMatch(message -> message.entry == null));
    AtomicInteger fetched = new AtomicInteger();
    c.drop =
        message -> {
          if (message.type == MessageType.FETCH_ENTRY && boundary.id.equals(message.reference)) {
            fetched.incrementAndGet();
          }
          return false;
        };
    c.network.addAll(messages);
    c.pump();
    assertEquals(2, fetched.get());
    for (int node = 1; node <= 3; node++) {
      assertTrue(c.node(node).isApplied(boundary.id));
    }
  }

  private static void assertBatchFailureFencesWithoutPublishing(boolean follower) throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    int target = follower ? 2 : 1;
    MemoryStore store = c.stores.get(target);
    int retainedBefore = c.node(target).retainedProposals().size();
    TRaftProtocol.Rank rankBefore = c.node(target).currentRank();
    store.failOn = RecordType.RETAIN_PROPOSAL;
    try {
      if (follower) {
        c.append(1, 0);
        Message proposal =
            c.node(1).drainMessages().stream()
                .filter(message -> message.type == MessageType.PROPOSAL && message.to == 2)
                .findFirst()
                .get();
        c.node(2).receive(proposal);
      } else {
        c.append(1, 0);
      }
      fail("The injected second-frame failure must fence the engine");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("injected"));
    }
    assertTrue(c.node(target).isFailed());
    assertTrue(
        "No data receipt or prefix ACK may escape the failed batch",
        c.node(target).drainMessages().isEmpty());
    DurableRecord durablePrefix = store.records.get(store.records.size() - 1);
    assertEquals(RecordType.ENTRY, durablePrefix.type);
    Entry staged = durablePrefix.entry;
    assertEquals(retainedBefore, c.node(target).retainedProposals().size());
    store.failOn = null;
    c.restart(target);
    assertEquals(0, rankBefore.compareTo(c.node(target).currentRank()));
    assertEquals(retainedBefore, c.node(target).retainedProposals().size());
    assertFalse(c.node(target).installedEntries().contains(staged));
    assertFalse(c.node(target).isCommitted(staged.id));
    c.node(target)
        .receive(
            protocolMessage(
                MessageType.FETCH_ENTRY,
                target == 1 ? 2 : 1,
                target,
                c.node(target).term(),
                null,
                null,
                staged.id));
    assertTrue(
        c.node(target).drainMessages().stream()
            .anyMatch(
                message ->
                    message.type == MessageType.TRANSFER_ENTRY && message.entry.equals(staged)));
  }

  @Test
  public void leaderBatchPartialDurabilityFencesAndRestartsAsUnretainedStagedData()
      throws Exception {
    assertBatchFailureFencesWithoutPublishing(false);
  }

  @Test
  public void followerBatchPartialDurabilityCannotEmitReceiptOrProposalAck() throws Exception {
    assertBatchFailureFencesWithoutPublishing(true);
  }

  @Test
  public void carriedFirstMustMatchAllImmutableManifestMetadataBeforeDurability() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    byte[] payload = new byte[] {7, 8};
    Entry boundary = c.node(1).append(Kind.DATA, 0, payload, 1);
    payload[0] = 99;
    Message valid =
        c.node(1).drainMessages().stream()
            .filter(message -> message.to == 2 && message.type == MessageType.PROPOSAL)
            .findFirst()
            .get();
    assertArrayEquals(new byte[] {7, 8}, valid.entry.payload());
    Entry first = valid.entry;
    List<Entry> mismatches =
        Arrays.asList(
            new Entry(
                new EntryId(first.id.term + 1, first.id.index),
                first.partition,
                first.timestamp,
                first.prevSize,
                first.kind,
                first.payload()),
            new Entry(
                new EntryId(first.id.term, first.id.index + 1),
                first.partition,
                first.timestamp,
                first.prevSize,
                first.kind,
                first.payload()),
            new Entry(
                first.id,
                first.partition + 1,
                first.timestamp,
                first.prevSize,
                first.kind,
                first.payload()),
            new Entry(
                first.id,
                first.partition,
                first.timestamp + 1,
                first.prevSize,
                first.kind,
                first.payload()),
            new Entry(
                first.id,
                first.partition,
                first.timestamp,
                first.prevSize + 1,
                first.kind,
                first.payload()),
            new Entry(
                first.id,
                first.partition,
                first.timestamp,
                first.prevSize,
                Kind.BARRIER,
                first.payload()),
            new Entry(
                first.id,
                first.partition,
                first.timestamp,
                first.prevSize,
                first.kind,
                new byte[] {7}),
            Entry.metadata(first));
    int durableBefore = c.stores.get(2).records.size();
    for (Entry mismatch : mismatches) {
      try {
        c.node(2)
            .receive(
                protocolMessage(
                    MessageType.PROPOSAL, 1, 2, valid.term, mismatch, valid.proposal, null));
        fail("A carried first with mismatched metadata/payload must be rejected");
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains("immutable metadata"));
      }
      assertEquals(durableBefore, c.stores.get(2).records.size());
      assertTrue(c.node(2).drainMessages().isEmpty());
    }
    c.node(2).receive(TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(valid)));
    assertTrue(
        c.node(2).drainMessages().stream()
            .anyMatch(
                message ->
                    message.type == MessageType.PROPOSAL_ACK
                        && boundary.id.equals(message.reference)));
  }

  @Test
  public void transportBudgetFallbackIsNotAReceiptAndDoesNotDeleteRepairOrOrdinarySlots()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry ordinary = c.append(1, 10);
    c.node(1).fallbackProposalPayload(2, ordinary.id);
    assertEquals(1, c.node(1).inFlightEntries(2));
    c.pump();
    Entry boundary = c.append(1, 0);
    c.node(1).drainMessages();
    assertTrue(c.node(1).isProposalPayloadInFlight(2, boundary.id));
    c.node(1)
        .receive(
            protocolMessage(
                MessageType.FETCH_ENTRY, 2, 1, c.node(1).term(), null, null, boundary.id));
    assertFalse(c.node(1).isProposalPayloadInFlight(2, boundary.id));
    c.node(1).fallbackProposalPayload(2, boundary.id);
    assertEquals(
        "Repair slot survives an old proposal budget rejection", 1, c.node(1).inFlightEntries(2));
    assertFalse(c.node(1).isCommitted(boundary.id));
    c.node(1).drainMessages();
    c.node(1).resend();
    List<Message> retry = c.node(1).drainMessages();
    assertTrue(
        retry.stream()
            .anyMatch(
                message ->
                    message.to == 2
                        && message.type == MessageType.TRANSFER_ENTRY
                        && message.entry.id.equals(boundary.id)));
    assertTrue(
        retry.stream()
            .anyMatch(
                message ->
                    message.to == 2
                        && message.type == MessageType.PROPOSAL
                        && message.entry == null));
    assertFalse(
        retry.stream()
            .anyMatch(
                message ->
                    message.to == 2
                        && message.type == MessageType.PROPOSAL
                        && message.entry != null));
  }

  @Test
  public void transportBudgetFallbackReleasesConfirmedHistoricalCarriedSlot() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    Entry boundary = c.append(1, 0);
    List<Message> sent = c.node(1).drainMessages();
    c.node(2)
        .receive(
            sent.stream()
                .filter(message -> message.to == 2 && message.type == MessageType.PROPOSAL)
                .findFirst()
                .get());
    for (Message response : c.node(2).drainMessages()) {
      c.node(1).receive(response);
    }
    assertFalse(c.node(1).hasPendingProposal());
    assertTrue(c.node(1).isProposalPayloadInFlight(3, boundary.id));
    c.node(1).drainMessages();
    c.node(1).fallbackProposalPayload(3, boundary.id);
    assertFalse(c.node(1).isProposalPayloadInFlight(3, boundary.id));
    assertEquals(0, c.node(1).inFlightEntries(3));
    assertTrue(
        c.node(1).drainMessages().stream()
            .anyMatch(
                message ->
                    message.to == 3
                        && message.type == MessageType.PROPOSAL
                        && message.entry == null
                        && message.proposal.id().equals(boundary.id)));
  }

  @Test
  public void reverseHistoricalManifestChainUsesLinearIncomingWorkAndReleasesWaiters()
      throws Exception {
    int count = 600;
    TRaftEngine follower =
        new TRaftEngine(
            2, new HashSet<>(Arrays.asList(1, 2, 3)), 2, 0, new MemoryStore(), entry -> {});
    List<Proposal> chain = new ArrayList<>();
    Proposal prior = null;
    for (int part = 0; part < count; part++) {
      Entry first =
          new Entry(
              new EntryId(1, part + 1),
              part,
              part,
              part == 0 ? 0 : 1,
              part == 0 ? Kind.NOOP : Kind.DATA,
              part == 0 ? new byte[0] : new byte[] {1});
      Proposal proposal =
          new Proposal(
              prior == null ? null : prior.id(),
              prior == null ? Collections.emptyList() : Collections.singletonList(prior.id()),
              first,
              part == 0);
      chain.add(proposal);
      prior = proposal;
    }
    long work = follower.schedulingEntryVisits();
    for (int i = count - 1; i >= 0; i--) {
      follower.receive(
          protocolMessage(
              i == count - 1 ? MessageType.CONFIRM : MessageType.PROPOSAL,
              1,
              2,
              1,
              null,
              chain.get(i),
              null));
      follower.drainMessages();
    }
    assertEquals(count, follower.retainedProposals().size());
    assertEquals(count, follower.installedEntries().size());
    assertEquals(count - 1, follower.currentPartition());
    assertEquals(0, follower.pendingIncomingWork());
    assertTrue(
        "Reverse arrival must not repeatedly scan all blocked descendants",
        follower.schedulingEntryVisits() - work < count * 12L);
    work = follower.schedulingEntryVisits();
    follower.receive(
        protocolMessage(MessageType.CONFIRM, 1, 2, 1, null, chain.get(count - 1), null));
    assertEquals(0, follower.pendingIncomingWork());
    assertTrue(follower.schedulingEntryVisits() - work < 16);
  }

  @Test
  public void missingPayloadsRefillFetchWindowAndInstalledPredecessorWakesWithoutNewProposal()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.node(2).setReplicationWindow(1);
    Entry ordinary = c.append(1, 10);
    Entry boundary = c.append(1, 0);
    List<Message> sent = c.node(1).drainMessages();
    Message full =
        sent.stream()
            .filter(message -> message.to == 2 && message.type == MessageType.PROPOSAL)
            .findFirst()
            .get();
    c.node(2)
        .receive(protocolMessage(MessageType.PROPOSAL, 1, 2, full.term, null, full.proposal, null));
    Message fetchMember =
        c.node(2).drainMessages().stream()
            .filter(message -> message.type == MessageType.FETCH_ENTRY)
            .findFirst()
            .get();
    assertEquals(ordinary.id, fetchMember.reference);
    c.node(2)
        .receive(
            protocolMessage(MessageType.TRANSFER_ENTRY, 1, 2, full.term, ordinary, null, null));
    Message fetchFirst =
        c.node(2).drainMessages().stream()
            .filter(message -> message.type == MessageType.FETCH_ENTRY)
            .findFirst()
            .get();
    assertEquals(boundary.id, fetchFirst.reference);
    c.node(2)
        .receive(
            protocolMessage(MessageType.TRANSFER_ENTRY, 1, 2, full.term, boundary, null, null));
    assertFalse(c.node(2).retainedProposals().stream().anyMatch(p -> p.id().equals(boundary.id)));
    assertTrue(c.node(2).pendingIncomingWork() > 0);
    c.node(2).drainMessages();
    c.node(2)
        .receive(
            sent.stream()
                .filter(message -> message.to == 2 && message.type == MessageType.ENTRY)
                .findFirst()
                .get());
    assertTrue(
        "Installing the predecessor is an explicit wake-up event",
        c.node(2).drainMessages().stream()
            .anyMatch(
                message ->
                    message.type == MessageType.PROPOSAL_ACK
                        && boundary.id.equals(message.reference)));
    assertEquals(0, c.node(2).pendingIncomingWork());
    assertFalse("Prefix ACK remains staged until confirmation", c.node(2).isApplied(boundary.id));
  }

  @Test
  public void higherTermClearsIncomingDependenciesAndOldPayloadCannotReviveThem() throws Exception {
    TRaftEngine follower =
        new TRaftEngine(
            2, new HashSet<>(Arrays.asList(1, 2, 3)), 2, 0, new MemoryStore(), entry -> {});
    Entry root = new Entry(new EntryId(1, 1), 0, 0, 0, Kind.NOOP, new byte[0]);
    Proposal initial = new Proposal(null, Collections.emptyList(), root, true);
    Entry first = new Entry(new EntryId(1, 2), 1, 1, 1, Kind.DATA, new byte[] {1});
    Proposal pending = new Proposal(root.id, Collections.singletonList(root.id), first, false);
    follower.receive(protocolMessage(MessageType.PROPOSAL, 1, 2, 1, null, pending, null));
    assertTrue(follower.pendingIncomingWork() > 0);
    follower.receive(
        new Message(
            MessageType.VOTE_REQUEST,
            3,
            2,
            2,
            null,
            null,
            null,
            new TRaftProtocol.Rank(0, -1, 0),
            null,
            -1,
            null));
    assertEquals(0, follower.pendingIncomingWork());
    follower.drainMessages();
    follower.receive(protocolMessage(MessageType.PROPOSAL, 1, 2, 1, null, initial, null));
    assertEquals(0, follower.pendingIncomingWork());
    assertTrue(follower.retainedProposals().isEmpty());
    follower.receive(protocolMessage(MessageType.CONFIRM, 3, 2, 2, null, initial, null));
    assertEquals(1, follower.retainedProposals().size());
    assertEquals(0, follower.pendingIncomingWork());
    follower.receive(protocolMessage(MessageType.CONFIRM, 3, 2, 2, null, pending, null));
    assertEquals(2, follower.installedEntries().size());
    assertEquals(0, follower.pendingIncomingWork());
  }

  private static List<Proposal> incrementalInstallChain(int count) {
    List<Proposal> chain = new ArrayList<>();
    for (int part = 0; part < count; part++) {
      Entry first =
          new Entry(
              new EntryId(1, part + 1),
              part,
              part,
              part == 0 ? 0 : 1,
              part == 0 ? Kind.NOOP : Kind.DATA,
              part == 0 ? new byte[0] : new byte[] {1});
      Proposal prior = part == 0 ? null : chain.get(part - 1);
      chain.add(
          new Proposal(
              prior == null ? null : prior.id(),
              prior == null ? Collections.emptyList() : Collections.singletonList(prior.id()),
              first,
              part == 0));
    }
    return chain;
  }

  private static TRaftEngine installationFollower(MemoryStore store, List<EntryId> applications)
      throws IOException {
    return new TRaftEngine(
        2, new HashSet<>(Arrays.asList(1, 2, 3)), 2, 0, store, entry -> applications.add(entry.id));
  }

  private static long assertIncrementalConfirmationWork(int count) throws Exception {
    MemoryStore store = new MemoryStore();
    TRaftEngine follower = installationFollower(store, new ArrayList<>());
    List<Proposal> chain = incrementalInstallChain(count);
    // Old-term manifests can be retained as history, without installing any boundary yet.
    for (Proposal proposal : chain) {
      follower.receive(protocolMessage(MessageType.PROPOSAL, 1, 2, 2, null, proposal, null));
      follower.drainMessages();
    }
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, chain.get(0), null));
    follower.drainMessages();
    long start = follower.schedulingEntryVisits();
    for (int target = 7; target < count + 7; target += 7) {
      int position = Math.min(target, count - 1);
      follower.receive(
          protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, chain.get(position), null));
      follower.drainMessages();
      assertEquals(position + 1, follower.installedEntries().size());
      if (position == count - 1) {
        break;
      }
    }
    long work = follower.schedulingEntryVisits() - start;
    assertTrue(
        "Repeated missing-confirmation jumps must inspect only new segments: " + work,
        work < count * 20L);
    TRaftEngine restarted = installationFollower(store, new ArrayList<>());
    assertEquals(follower.installedEntries(), restarted.installedEntries());
    assertEquals(follower.currentPartition(), restarted.currentPartition());
    assertTrue(
        "Journal replay must also avoid cumulative prefix rebuilding",
        restarted.schedulingEntryVisits() < count * 12L);
    return work;
  }

  @Test
  public void repeatedConfirmationJumpsAndRestartInstallOnlyTheNewExactAncestorSuffix()
      throws Exception {
    long small = assertIncrementalConfirmationWork(600);
    long large = assertIncrementalConfirmationWork(1200);
    assertTrue("Doubling history must not quadruple installation work", large < small * 2.2 + 100);
  }

  @Test
  public void jumpFillsOpenHolesPreservesCommitApplyStateAndLateAncestorKeepsCurrentOpen()
      throws Exception {
    MemoryStore store = new MemoryStore();
    List<EntryId> applications = new ArrayList<>();
    TRaftEngine follower = installationFollower(store, applications);
    Entry initial = new Entry(new EntryId(1, 1), 0, 0, 0, Kind.NOOP, new byte[0]);
    Proposal root = new Proposal(null, Collections.emptyList(), initial, true);
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 1, null, root, null));
    Entry missing = new Entry(new EntryId(1, 2), 0, 10, 0, Kind.DATA, new byte[] {2});
    Entry sibling = new Entry(new EntryId(1, 3), 0, 20, 0, Kind.DATA, new byte[] {3});
    follower.receive(protocolMessage(MessageType.ENTRY, 1, 2, 1, sibling, null, root.id()));
    follower.receive(
        new Message(
            MessageType.COMMIT,
            1,
            2,
            1,
            null,
            null,
            null,
            null,
            null,
            -1,
            Arrays.asList(initial.id, sibling.id)));
    assertEquals(Collections.singletonList(sibling.id), applications);
    Entry nextFirst = new Entry(new EntryId(1, 4), 1, 0, 3, Kind.DATA, new byte[] {4});
    Proposal next =
        new Proposal(
            root.id(), Arrays.asList(initial.id, missing.id, sibling.id), nextFirst, false);
    Entry nextSibling = new Entry(new EntryId(1, 5), 1, 30, 3, Kind.DATA, new byte[] {5});
    Entry recoveryNoop = new Entry(new EntryId(2, 6), 2, 0, 2, Kind.NOOP, new byte[0]);
    Proposal target =
        new Proposal(next.id(), Arrays.asList(nextFirst.id, nextSibling.id), recoveryNoop, true);
    follower.receive(protocolMessage(MessageType.PROPOSAL, 1, 2, 2, null, target, null));
    follower.receive(protocolMessage(MessageType.PROPOSAL, 1, 2, 2, null, next, null));
    follower.receive(protocolMessage(MessageType.TRANSFER_ENTRY, 1, 2, 2, missing, null, null));
    follower.receive(protocolMessage(MessageType.TRANSFER_ENTRY, 1, 2, 2, nextSibling, null, null));
    assertFalse(follower.installedEntries().contains(missing));
    assertEquals(0, follower.currentPartition());
    follower.receive(
        new Message(
            MessageType.COMMIT,
            1,
            2,
            2,
            null,
            null,
            null,
            null,
            null,
            0,
            Arrays.asList(nextFirst.id, nextSibling.id)));
    assertEquals(Collections.singletonList(sibling.id), applications);
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, target, null));
    assertEquals(Arrays.asList(sibling.id, missing.id, nextFirst.id, nextSibling.id), applications);
    assertEquals(6, follower.installedEntries().size());
    assertTrue(follower.isCommitted(missing.id));
    Entry open = new Entry(new EntryId(2, 7), 2, 40, 2, Kind.DATA, new byte[] {7});
    follower.receive(protocolMessage(MessageType.ENTRY, 1, 2, 2, open, null, target.id()));
    List<Entry> before = follower.installedEntries();
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, root, null));
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, next, null));
    assertEquals(before, follower.installedEntries());
    assertFalse(follower.isCommitted(open.id));
    follower.receive(
        new Message(
            MessageType.COMMIT,
            1,
            2,
            2,
            null,
            null,
            null,
            null,
            null,
            1,
            Arrays.asList(recoveryNoop.id, open.id)));
    assertEquals(
        Arrays.asList(sibling.id, missing.id, nextFirst.id, nextSibling.id, open.id), applications);
    List<EntryId> rebuilt = new ArrayList<>();
    TRaftEngine restart = installationFollower(store, rebuilt);
    restart.replayCommitted(entry -> rebuilt.add(entry.id));
    assertEquals(before, restart.installedEntries());
    assertEquals(new HashSet<>(applications), new HashSet<>(rebuilt));
    assertEquals(applications.size(), rebuilt.size());
    assertEquals(follower.committedIds(), restart.committedIds());
  }

  @Test
  public void ancestorExtensionExcludingAnUncommittedOpenEntryUsesFullReconciliation()
      throws Exception {
    MemoryStore store = new MemoryStore();
    TRaftEngine follower = installationFollower(store, new ArrayList<>());
    Entry rootEntry = new Entry(new EntryId(1, 1), 0, 0, 0, Kind.NOOP, new byte[0]);
    Proposal root = new Proposal(null, Collections.emptyList(), rootEntry, true);
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 1, null, root, null));
    Entry dropped = new Entry(new EntryId(1, 2), 0, 1000, 0, Kind.DATA, new byte[] {2});
    follower.receive(protocolMessage(MessageType.ENTRY, 1, 2, 1, dropped, null, root.id()));
    Entry first = new Entry(new EntryId(2, 10), 1, 0, 1, Kind.NOOP, new byte[0]);
    Proposal reconciled =
        new Proposal(root.id(), Collections.singletonList(root.id()), first, true);
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, reconciled, null));
    assertEquals(Arrays.asList(rootEntry, first), follower.installedEntries());
    assertFalse(follower.installedEntries().contains(dropped));
    TRaftEngine restarted = installationFollower(store, new ArrayList<>());
    assertEquals(follower.installedEntries(), restarted.installedEntries());
  }

  @Test
  public void differentOpeningBranchReconcilesOpenSuffixButCannotReplaceClosedPartition()
      throws Exception {
    MemoryStore store = new MemoryStore();
    TRaftEngine follower = installationFollower(store, new ArrayList<>());
    List<Proposal> original = incrementalInstallChain(3);
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 1, null, original.get(0), null));
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 1, null, original.get(1), null));
    Entry other = new Entry(new EntryId(2, 20), 1, 0, 1, Kind.NOOP, new byte[0]);
    Proposal branch =
        new Proposal(
            original.get(0).id(), Collections.singletonList(original.get(0).id()), other, true);
    Entry last = new Entry(new EntryId(2, 21), 2, 1, 1, Kind.DATA, new byte[] {21});
    Proposal target =
        new Proposal(branch.id(), Collections.singletonList(branch.id()), last, false);
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, target, null));
    follower.receive(protocolMessage(MessageType.PROPOSAL, 1, 2, 2, null, branch, null));
    assertEquals(Arrays.asList(original.get(0).first, other, last), follower.installedEntries());
    List<Entry> before = follower.installedEntries();
    int records = store.records.size();
    try {
      follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, original.get(2), null));
      fail("A branch cannot replace a partition already closed by confirmation");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("closed"));
    }
    assertEquals(before, follower.installedEntries());
    assertTrue(
        store.records.subList(records, store.records.size()).stream()
            .noneMatch(record -> record.type == RecordType.INSTALL));
  }

  @Test
  public void failedJumpInstallForceDoesNotPublishAnyOfTheValidatedExtension() throws Exception {
    MemoryStore store = new MemoryStore();
    TRaftEngine follower = installationFollower(store, new ArrayList<>());
    List<Proposal> chain = incrementalInstallChain(12);
    for (Proposal proposal : chain) {
      follower.receive(protocolMessage(MessageType.PROPOSAL, 1, 2, 2, null, proposal, null));
      follower.drainMessages();
    }
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, chain.get(0), null));
    follower.drainMessages();
    List<Entry> before = follower.installedEntries();
    store.failOn = RecordType.INSTALL;
    try {
      follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, chain.get(11), null));
      fail("INSTALL force is the visibility boundary");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("injected"));
    }
    assertTrue(follower.isFailed());
    assertEquals(before, follower.installedEntries());
    assertTrue(follower.drainMessages().isEmpty());
    store.failOn = null;
    TRaftEngine restarted = installationFollower(store, new ArrayList<>());
    assertEquals(before, restarted.installedEntries());
    restarted.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, chain.get(11), null));
    assertEquals(12, restarted.installedEntries().size());
  }

  private static long assertAdjacentIncompletePredecessorWork(int count) throws Exception {
    MemoryStore store = new MemoryStore();
    TRaftEngine follower = installationFollower(store, new ArrayList<>());
    List<Proposal> chain = new ArrayList<>();
    Map<EntryId, Entry> missingData = new HashMap<>();
    for (int part = 0; part < count; part++) {
      Entry first =
          new Entry(
              new EntryId(1, 2L * part + 1), part, 0, part == 0 ? 0 : 2, Kind.NOOP, new byte[0]);
      Entry data =
          new Entry(
              new EntryId(1, 2L * part + 2), part, part, first.prevSize, Kind.DATA, new byte[] {1});
      missingData.put(data.id, data);
      Proposal previous = part == 0 ? null : chain.get(part - 1);
      Proposal proposal =
          new Proposal(
              previous == null ? null : previous.id(),
              previous == null
                  ? Collections.emptyList()
                  : Arrays.asList(previous.id(), new EntryId(1, 2L * part)),
              first,
              part == 0);
      chain.add(proposal);
      follower.receive(protocolMessage(MessageType.PROPOSAL, 1, 2, 2, null, proposal, null));
      for (Message message : follower.drainMessages()) {
        if (message.type == MessageType.FETCH_ENTRY) {
          follower.receive(
              protocolMessage(
                  MessageType.TRANSFER_ENTRY,
                  1,
                  2,
                  2,
                  missingData.get(message.reference),
                  null,
                  null));
        }
      }
      follower.drainMessages();
    }
    follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, chain.get(0), null));
    follower.drainMessages();
    long before = follower.schedulingEntryVisits();
    for (int part = 1; part < count; part++) {
      // The preceding DATA is durable but has not been installed by ordinary ENTRY.
      follower.receive(protocolMessage(MessageType.CONFIRM, 1, 2, 2, null, chain.get(part), null));
      follower.drainMessages();
    }
    long work = follower.schedulingEntryVisits() - before;
    assertEquals(2 * count - 1, follower.installedEntries().size());
    assertTrue(
        "Adjacent incomplete predecessors must not rebuild history: " + work, work < count * 24L);
    TRaftEngine restart = installationFollower(store, new ArrayList<>());
    assertEquals(follower.installedEntries(), restart.installedEntries());
    assertTrue(restart.schedulingEntryVisits() < count * 14L);
    return work;
  }

  @Test
  public void adjacentConfirmationsWithOneMissingPredecessorMemberHaveLinearInstallWork()
      throws Exception {
    long small = assertAdjacentIncompletePredecessorWork(600);
    long large = assertAdjacentIncompletePredecessorWork(1200);
    assertTrue(large < small * 2.2 + 100);
  }

  @Test
  public void followerRunAcknowledgesEveryIdentityInOneFrameAndLeaderCommitsAll() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<Entry> entries = c.node(1).appendDataBatch(dataInputs(10, 20, 30), 1);
    List<Message> messages = ordinaryMessages(c.node(1).drainMessages(), 2);
    assertTrue(c.node(2).receiveEntries(messages));
    List<Message> acks = new ArrayList<>();
    for (Message message : c.node(2).drainMessages()) {
      if (message.type == MessageType.ENTRY_ACK) {
        acks.add(message);
      }
    }
    assertEquals("One durable run, one acknowledgment frame", 1, acks.size());
    Message ack = acks.get(0);
    assertEquals(entries.get(0).id, ack.reference);
    assertEquals(Arrays.asList(entries.get(1).id, entries.get(2).id), ack.identities);
    for (Entry entry : entries) {
      assertFalse(c.node(1).isCommitted(entry.id));
    }
    c.node(1).receive(TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(ack)));
    for (Entry entry : entries) {
      assertTrue("Each listed identity counts as its own ACK", c.node(1).isCommitted(entry.id));
      assertTrue(c.node(1).isApplied(entry.id));
    }
  }

  @Test
  public void confirmedBoundaryMarkerIsDeferredWhileEvidenceRecordsStayForced() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    for (MemoryStore store : c.stores.values()) {
      store.deferred.clear();
    }
    Entry boundary = c.append(1, 5);
    assertNotNull(boundary);
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(boundary.id));
      MemoryStore store = c.stores.get(id);
      assertEquals("Exactly the installation marker is deferred", 1, store.deferred.size());
      DurableRecord marker = store.deferred.get(0);
      assertEquals(RecordType.INSTALL, marker.type);
      assertNull(marker.entry);
      assertEquals(boundary.id, marker.reference);
      for (DurableRecord record : store.records) {
        if (record.type == RecordType.ENTRY || record.type == RecordType.RETAIN_PROPOSAL) {
          assertFalse("Evidence records stay forced", store.deferred.contains(record));
        }
      }
    }
  }

  /** A crash before the deferred marker reached disk loses the marker and everything after it. */
  private static void truncateAtDeferredMarker(MemoryStore store, EntryId boundary) {
    int cut = -1;
    for (int i = 0; i < store.records.size(); i++) {
      DurableRecord record = store.records.get(i);
      if (record.type == RecordType.INSTALL
          && record.entry == null
          && boundary.equals(record.reference)) {
        cut = i;
      }
    }
    assertTrue("the deferred marker must exist", cut >= 0);
    store.records.subList(cut, store.records.size()).clear();
    store.deferred.clear();
  }

  @Test
  public void followerLosingItsDeferredMarkerIsReinstalledByTheNextConfirmation() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    Entry boundary = c.append(1, 5);
    c.pump();
    long partition = c.node(1).currentPartition();
    truncateAtDeferredMarker(c.stores.get(2), boundary.id);
    c.restart(2);
    assertEquals(
        "the retained proposal is not installed", partition - 1, c.node(2).currentPartition());
    assertFalse(c.node(2).isApplied(boundary.id));
    c.node(1).resend();
    c.pump();
    assertEquals(partition, c.node(2).currentPartition());
    assertTrue(c.node(2).isApplied(boundary.id));
    Entry next = c.append(1, 40);
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(next.id));
    }
  }

  @Test
  public void leaderLosingItsDeferredMarkerAfterClientVisibleCommitKeepsTheEntry()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    Entry boundary = c.append(1, 5);
    c.pump();
    assertTrue("client-visible commit", c.node(1).isApplied(boundary.id));
    // The leader crashes right after answering the client, before any later forced record
    // covered its deferred marker; the forced retained proposal and payloads survive on it and
    // on the followers that acknowledged them.
    truncateAtDeferredMarker(c.stores.get(1), boundary.id);
    c.restart(1);
    assertFalse(c.node(1).isApplied(boundary.id));
    c.elect(2);
    for (int id = 1; id <= 3; id++) {
      assertTrue(
          "Leader Completeness after the old leader lost its marker",
          c.node(id).isApplied(boundary.id));
    }
    Entry more = c.append(2, 50);
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(more.id));
    }
  }

  @Test
  public void majorityLosingDeferredMarkersStillRecoversTheConfirmedBoundary() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.append(1, 10);
    c.pump();
    Entry boundary = c.append(1, 5);
    c.pump();
    truncateAtDeferredMarker(c.stores.get(1), boundary.id);
    truncateAtDeferredMarker(c.stores.get(2), boundary.id);
    c.restart(1);
    c.restart(2);
    assertFalse(c.node(1).isApplied(boundary.id));
    assertFalse(c.node(2).isApplied(boundary.id));
    // The candidate that lost its marker inherits the forced retained proposal from the reports.
    c.elect(2);
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(boundary.id));
    }
    Entry next = c.append(2, 60);
    c.pump();
    for (int id = 1; id <= 3; id++) {
      assertTrue(c.node(id).isApplied(next.id));
    }
  }

  private static final boolean[] COMMIT_MODES = {false, true};

  private static Cluster commitModeCluster(boolean durableCommit, int voters) throws IOException {
    Cluster c = new Cluster(voters);
    c.durableCommitRecords = durableCommit;
    c.durableAppliedRecords = durableCommit;
    restartEmptyApplications(c);
    return c;
  }

  /**
   * Replica 1 leads term 1 and installs an entry whose frames are lost, so nobody acknowledged it.
   * Replica 2 recovers term 2 with replica 3 and closes that partition without the entry.
   */
  private static Entry failoverDroppingASuffix(Cluster c) throws IOException {
    c.elect(1);
    Entry dropped = c.append(1, 10);
    c.node(1).drainMessages();
    c.drop = message -> message.from == 1 || message.to == 1;
    c.elect(2);
    assertFalse(c.node(2).installedEntries().contains(dropped));
    assertEquals(dropped.partition, c.node(2).committedFrontier());
    return dropped;
  }

  private static void assertNotCommittedAnywhere(Cluster c, Entry dropped) {
    for (int id : c.members) {
      assertFalse(
          "Replica " + id + " calls a dropped entry committed", c.node(id).isCommitted(dropped.id));
      assertFalse(
          "Replica " + id + " applied a dropped entry: " + c.applications,
          c.applications.get(id).contains(dropped.id));
      assertFalse(c.node(id).installedEntries().contains(dropped));
    }
  }

  private static void assertAppliedAgreement(Cluster c) {
    Map<Long, EntryId> chosen = new HashMap<>();
    for (List<EntryId> applied : c.applications.values()) {
      for (EntryId id : applied) {
        EntryId other = chosen.putIfAbsent(id.index, id);
        assertTrue(
            "State Machine Safety at index " + id.index + ": " + c.applications,
            other == null || other.equals(id));
      }
    }
  }

  private static void deliverCommitNoticesOnly(Cluster c, int from, int to) throws IOException {
    c.node(from).resend();
    for (Message message : c.node(from).drainMessages()) {
      if (message.to == to && message.type == MessageType.COMMIT) {
        c.node(to).receive(TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(message)));
      }
    }
    c.node(to).drainMessages();
  }

  @Test
  public void frontierDoesNotCommitADroppedOpenSuffixWhenFramesArriveInOrder() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      Entry dropped = failoverDroppingASuffix(c);
      c.drop = ignored -> false;
      // One heartbeat, nothing lost or reordered: CONFIRM and then COMMIT. A recovery proposal
      // never carries its first entry, so installing it needs a FETCH and COMMIT arrives first.
      c.node(2).resend();
      c.pump();
      assertEquals(c.node(2).installedEntries(), c.node(1).installedEntries());
      assertNotCommittedAnywhere(c, dropped);
      Entry later = c.append(2, 20);
      c.pump();
      for (int id : c.members) {
        assertEquals(Collections.singletonList(later.id), c.applications.get(id));
      }
    }
  }

  @Test
  public void frontierDoesNotCommitADroppedOpenSuffixWithoutAnyFrameLost() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      c.elect(1);
      // Replica 1's link is slow, not lossy: its frames arrive in order, after the failover.
      List<Message> delayed = new ArrayList<>();
      c.drop =
          message -> {
            if (message.from == 1 || message.to == 1) {
              delayed.add(message);
              return true;
            }
            return false;
          };
      Entry dropped = c.append(1, 10);
      c.pump();
      c.elect(2);
      assertFalse(c.node(2).installedEntries().contains(dropped));
      c.drop = ignored -> false;
      c.network.addAll(delayed);
      c.pump();
      c.node(2).resend();
      c.pump();
      assertEquals(c.node(2).installedEntries(), c.node(1).installedEntries());
      assertNotCommittedAnywhere(c, dropped);
    }
  }

  /** The new leader's first DATA entry reuses a log index of the suffix it dropped. */
  @Test
  public void droppedEntryAndTheNewEntryAtItsIndexAreNotBothApplied() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      c.elect(1);
      c.append(1, 10);
      Entry dropped = c.append(1, 11);
      c.node(1).drainMessages();
      c.drop = message -> message.from == 1 || message.to == 1;
      c.elect(2);
      Entry replacement = c.append(2, 20);
      c.pump();
      assertEquals(dropped.id.index, replacement.id.index);
      c.drop = ignored -> false;
      c.node(2).resend();
      c.pump();
      c.node(2).resend();
      c.pump();
      assertAppliedAgreement(c);
      for (int id : c.members) {
        assertEquals(Collections.singletonList(replacement.id), c.applications.get(id));
      }
    }
  }

  @Test
  public void frontierAloneDoesNotCommitADroppedOpenSuffixAcrossARestart() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      c.elect(1);
      Entry member = c.append(1, 10);
      c.pump();
      Entry dropped = c.append(1, 20);
      c.node(1).drainMessages();
      c.drop = message -> message.from == 1 || message.to == 1;
      c.elect(2);
      c.drop = ignored -> false;
      deliverCommitNoticesOnly(c, 2, 1);
      assertEquals(0, c.node(1).committedFrontier());
      assertFalse(c.node(1).isCommitted(dropped.id));
      assertFalse(c.node(1).isApplied(dropped.id));
      c.restart(1);
      c.node(1).applyCommitted();
      assertFalse(c.node(1).isCommitted(dropped.id));
      assertFalse(c.node(1).isApplied(dropped.id));
      c.node(2).resend();
      c.pump();
      c.node(2).resend();
      c.pump();
      assertEquals(c.node(2).installedEntries(), c.node(1).installedEntries());
      assertTrue(c.node(1).isApplied(member.id));
      assertNotCommittedAnywhere(c, dropped);
    }
  }

  @Test
  public void holderOfADroppedSuffixStaysElectable() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      Entry dropped = failoverDroppingASuffix(c);
      c.drop = ignored -> false;
      c.node(2).resend();
      c.pump();
      c.drop = message -> message.from == 2 || message.to == 2;
      // False commit knowledge would fence this: "recovery omitted a known committed identity".
      c.elect(1);
      assertFalse(c.node(1).installedEntries().contains(dropped));
      assertFalse(c.node(1).isApplied(dropped.id));
    }
  }

  /** The holder knows the frontier but has not closed that partition when it wins next. */
  @Test
  public void holderWithAKnownFrontierOverItsOpenPartitionRecoversWithoutTheSuffix()
      throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      c.elect(1);
      Entry member = c.append(1, 10);
      c.pump();
      Entry dropped = c.append(1, 20);
      c.node(1).drainMessages();
      // Replica 3 retains the recovery proposal but never learns its confirmation.
      c.drop =
          message ->
              message.from == 1
                  || message.to == 1
                  || (message.to == 3
                      && (message.type == MessageType.CONFIRM
                          || message.type == MessageType.COMMIT));
      c.elect(2);
      c.drop = ignored -> false;
      deliverCommitNoticesOnly(c, 2, 1);
      assertEquals(0, c.node(1).committedFrontier());
      c.drop = message -> message.from == 2 || message.to == 2;
      c.elect(1);
      Entry later = c.append(1, 50);
      c.pump();
      c.node(1).resend();
      c.pump();
      assertFalse(c.node(1).installedEntries().contains(dropped));
      assertFalse(c.node(1).isApplied(dropped.id));
      for (int id : new int[] {1, 3}) {
        assertTrue(c.node(id).isApplied(member.id));
        assertTrue(c.node(id).isApplied(later.id));
      }
    }
  }

  @Test
  public void frontierDoesNotCommitADroppedSuffixOnAPlainFollower() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 5);
      c.elect(1);
      c.drop = message -> message.type == MessageType.ENTRY && message.to != 3;
      Entry dropped = c.append(1, 10);
      c.pump();
      assertTrue(c.node(3).installedEntries().contains(dropped));
      assertFalse("Two ACKs of five", c.node(1).isCommitted(dropped.id));
      c.drop =
          message -> message.from == 1 || message.to == 1 || message.from == 3 || message.to == 3;
      c.elect(2);
      c.drop = ignored -> false;
      c.node(2).resend();
      c.pump();
      c.node(2).resend();
      c.pump();
      for (int id : c.members) {
        assertEquals(c.node(2).installedEntries(), c.node(id).installedEntries());
      }
      assertNotCommittedAnywhere(c, dropped);
    }
  }

  @Test
  public void frontierFarAheadDoesNotCommitADroppedSuffixOfALaterPartition() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      c.elect(1);
      c.append(1, 10);
      c.pump();
      c.append(1, 5);
      c.pump();
      assertEquals(1, c.node(1).currentPartition());
      Entry dropped = c.append(1, 20);
      c.node(1).drainMessages();
      assertEquals(1, dropped.partition);
      c.drop = message -> message.from == 1 || message.to == 1;
      c.elect(2);
      // The new leader moves on: each lower timestamp is a boundary.
      for (long round = 1; round <= 2; round++) {
        assertNotNull(c.append(2, 100 * round));
        c.pump();
        assertNotNull(c.append(2, 100 * round - 1));
        c.pump();
      }
      assertTrue(c.node(2).committedFrontier() > dropped.partition);
      c.drop = ignored -> false;
      for (int i = 0; i < 4; i++) {
        c.node(2).resend();
        c.pump();
      }
      assertEquals(c.node(2).installedEntries(), c.node(1).installedEntries());
      assertNotCommittedAnywhere(c, dropped);
      assertEquals(c.applications.get(2), c.applications.get(1));
      assertAppliedAgreement(c);
    }
  }

  /**
   * The member is known committed only through the frontier, which overtakes the proposal that
   * closes the partition. Nothing repeats that frontier as news, so the installation must mark it.
   */
  @Test
  public void frontierCommitsMembersWhenAReconciliationClosesThePartition() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      c.elect(1);
      c.drop = message -> message.type == MessageType.ENTRY_ACK;
      Entry member = c.append(1, 10);
      c.pump();
      assertFalse(c.node(1).isCommitted(member.id));
      Entry dropped = c.append(1, 20);
      c.node(1).drainMessages();
      c.drop = message -> message.from == 1 || message.to == 1;
      c.elect(2);
      assertTrue(c.node(2).installedEntries().contains(member));
      c.drop = ignored -> false;
      deliverCommitNoticesOnly(c, 2, 1);
      assertFalse(c.node(1).isCommitted(member.id));
      assertFalse(c.node(1).isCommitted(dropped.id));
      c.drop = message -> message.to == 1 && message.type == MessageType.COMMIT;
      c.node(2).resend();
      c.pump();
      c.node(2).resend();
      c.pump();
      assertEquals(c.node(2).installedEntries(), c.node(1).installedEntries());
      assertTrue(c.node(1).isApplied(member.id));
      assertFalse(c.node(1).isApplied(dropped.id));
    }
  }

  /** As above when every local entry is a member, so the install is an in-place extension. */
  @Test
  public void frontierCommitsMembersWhenAnExtensionClosesThePartition() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = commitModeCluster(durable, 3);
      c.elect(1);
      c.drop = message -> message.type == MessageType.ENTRY_ACK;
      Entry first = c.append(1, 10);
      Entry second = c.append(1, 20);
      c.pump();
      assertFalse(c.node(1).isCommitted(first.id));
      assertTrue(c.node(2).installedEntries().contains(first));
      // The boundary's ACKs certify both members, so no identity notice will ever name them.
      List<Message> toTwo = new ArrayList<>();
      c.drop =
          message -> {
            if (message.to == 2) {
              toTwo.add(message);
              return true;
            }
            return message.type == MessageType.ENTRY_ACK;
          };
      assertNotNull(c.append(1, 5));
      c.pump();
      assertEquals(0, c.node(1).committedFrontier());
      c.drop = ignored -> false;
      for (Message message : toTwo) {
        if (message.type == MessageType.COMMIT) {
          c.node(2).receive(TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(message)));
        }
      }
      assertEquals(0, c.node(2).currentPartition());
      assertEquals(0, c.node(2).committedFrontier());
      assertFalse(c.node(2).isCommitted(first.id));
      c.drop = message -> message.to == 2 && message.type == MessageType.COMMIT;
      for (Message message : toTwo) {
        if (message.type != MessageType.COMMIT) {
          c.network.add(message);
        }
      }
      c.pump();
      assertEquals(1, c.node(2).currentPartition());
      assertTrue(c.node(2).isApplied(first.id));
      assertTrue(c.node(2).isApplied(second.id));
    }
  }

  private static Message frame(
      MessageType type, long term, Entry entry, Proposal proposal, EntryId reference) {
    return new Message(type, 1, 2, term, entry, proposal, reference, null, null, -1, null);
  }

  private static Message message(
      MessageType type, int from, int to, long term, Entry entry, EntryId reference) {
    return new Message(type, from, to, term, entry, null, reference, null, null, -1, null);
  }

  private static List<EntryId> fetched(List<Message> messages) {
    List<EntryId> identities = new ArrayList<>();
    for (Message message : messages) {
      if (message.type == MessageType.FETCH_ENTRY) {
        identities.add(message.reference);
      }
    }
    return identities;
  }

  @Test
  public void bytesArrivingByAnotherRouteReleaseTheirFetchSlots() throws Exception {
    MemoryStore store = new MemoryStore();
    TRaftEngine f =
        new TRaftEngine(2, new HashSet<>(Arrays.asList(1, 2, 3)), 16, 0, store, entry -> {});
    f.setReplicationWindow(4);
    Entry root = new Entry(new EntryId(1, 1), 0, 0, 0, Kind.NOOP, new byte[0]);
    Proposal p0 = new Proposal(null, Collections.emptyList(), root, true);
    Entry a1 = new Entry(new EntryId(1, 2), 0, 1, 0, Kind.DATA, new byte[] {1});
    Entry a2 = new Entry(new EntryId(1, 3), 0, 2, 0, Kind.DATA, new byte[] {2});
    Entry a3 = new Entry(new EntryId(1, 4), 0, 3, 0, Kind.DATA, new byte[] {3});
    Entry f1 = new Entry(new EntryId(1, 5), 1, 0, 4, Kind.DATA, new byte[] {5});
    Proposal p1 = new Proposal(root.id, Arrays.asList(root.id, a1.id, a2.id, a3.id), f1, false);
    Entry b1 = new Entry(new EntryId(1, 6), 1, 11, 4, Kind.DATA, new byte[] {6});
    Entry b2 = new Entry(new EntryId(1, 7), 1, 12, 4, Kind.DATA, new byte[] {7});
    Entry b3 = new Entry(new EntryId(1, 8), 1, 13, 4, Kind.DATA, new byte[] {8});
    Entry f2 = new Entry(new EntryId(1, 9), 2, 0, 4, Kind.DATA, new byte[] {9});
    Proposal p2 = new Proposal(f1.id, Arrays.asList(f1.id, b1.id, b2.id, b3.id), f2, false);
    Entry c1 = new Entry(new EntryId(1, 10), 2, 21, 4, Kind.DATA, new byte[] {10});
    Entry f3 = new Entry(new EntryId(1, 11), 3, 0, 2, Kind.DATA, new byte[] {11});
    Proposal p3 = new Proposal(f2.id, Arrays.asList(f2.id, c1.id), f3, false);

    f.receive(frame(MessageType.CONFIRM, 1, null, p0, null));
    f.receive(frame(MessageType.PROPOSAL, 1, f1, p1, null));
    assertEquals(Arrays.asList(a1.id, a2.id, a3.id), fetched(f.drainMessages()));
    // Installed ordinary entries.
    for (Entry entry : Arrays.asList(a1, a2, a3)) {
      f.receive(frame(MessageType.ENTRY, 1, entry, null, root.id));
    }
    assertTrue(f.retainedProposals().contains(p1));
    assertEquals(0, f.outstandingFetches(1));
    f.receive(frame(MessageType.CONFIRM, 1, null, p1, null));
    f.drainMessages();
    // The next proposal can use the whole window at once, and a heartbeat retries nothing stale.
    f.receive(frame(MessageType.PROPOSAL, 1, f2, p2, null));
    assertEquals(Arrays.asList(b1.id, b2.id, b3.id), fetched(f.drainMessages()));
    for (Entry entry : Arrays.asList(b1, b2, b3)) {
      f.receive(frame(MessageType.ENTRY, 1, entry, null, f1.id));
    }
    assertEquals(0, f.outstandingFetches(1));
    f.drainMessages();
    f.resend();
    assertTrue(fetched(f.drainMessages()).isEmpty());
    // A staged entry of an opening that is not installed here: NACKed, stored, slot released.
    f.receive(frame(MessageType.PROPOSAL, 1, f3, p3, null));
    assertEquals(Collections.singletonList(c1.id), fetched(f.drainMessages()));
    assertEquals(1, f.outstandingFetches(1));
    f.receive(frame(MessageType.ENTRY, 1, c1, null, f2.id));
    assertEquals(0, f.outstandingFetches(1));
    assertFalse(f.installedEntries().contains(c1));
    f.drainMessages();
    // A TRANSFER the leader had already sent still frees the leader's slot, and writes nothing.
    int records = store.records.size();
    f.receive(frame(MessageType.TRANSFER_ENTRY, 1, a1, null, null));
    f.receive(frame(MessageType.TRANSFER_ENTRY, 1, c1, null, null));
    assertEquals(records, store.records.size());
    int receipts = 0;
    for (Message message : f.drainMessages()) {
      if (message.type == MessageType.TRANSFER_ACK) {
        receipts++;
      }
    }
    assertEquals(2, receipts);
  }

  /**
   * The livelock the fuzz found with window 2. Both fetch slots hold identities whose bytes arrived
   * by ENTRY, so the first entry of the next proposal, which only a FETCH can bring, is never
   * requested. Each retry of a stale FETCH reaches the leader while retried ENTRY frames fill its
   * flight, and the queued TRANSFER is dropped as covered by the follower's own proposal ACK, so no
   * TRANSFER ever frees a slot.
   */
  @Test
  public void staleFetchSlotsDoNotStarveAFollowerWhileTheLeadersFlightStaysFull() throws Exception {
    Cluster c = new Cluster();
    for (int id : c.members) {
      c.node(id).setReplicationWindow(2);
    }
    c.elect(1);
    TRaftEngine leader = c.node(1);
    TRaftEngine lagging = c.node(2);
    List<Message> toTwo = new ArrayList<>();
    c.drop =
        message -> {
          if (message.to == 2) {
            toTwo.add(message);
            return true;
          }
          return message.from == 2;
        };
    c.append(1, 10);
    c.append(1, 20);
    assertNotNull(c.append(1, 5));
    c.pump();
    assertEquals(1, leader.currentPartition());
    // The confirmation overtakes the members; the FETCHes it causes are lost, the members arrive.
    c.drop = message -> message.from == 2 && message.type == MessageType.FETCH_ENTRY;
    for (Message message : toTwo) {
      if (message.type == MessageType.CONFIRM) {
        c.network.add(message);
      }
    }
    for (Message message : toTwo) {
      if (message.type != MessageType.CONFIRM) {
        c.network.add(message);
      }
    }
    toTwo.clear();
    c.pump();
    assertEquals(1, lagging.currentPartition());
    // Now a first entry can come only through a FETCH. Replica 3 confirms the next boundary alone,
    // and two entries of the partition it opens fill the leader's flight to replica 2.
    leader.setProposalPayloadBudget(1);
    List<Message> fromTwo = new ArrayList<>();
    c.drop =
        message -> {
          if (message.to == 2) {
            toTwo.add(message);
            return true;
          }
          if (message.from == 2) {
            fromTwo.add(message);
            return true;
          }
          return false;
        };
    assertNotNull(c.append(1, 15));
    c.pump();
    assertEquals(2, leader.currentPartition());
    c.append(1, 30);
    c.append(1, 40);
    c.pump();
    assertEquals(2, leader.inFlightEntries(2));
    int rounds = 0;
    while (lagging.currentPartition() != leader.currentPartition()) {
      assertTrue(
          "Follower stalled with "
              + lagging.outstandingFetches(1)
              + " fetch slots taken and the leader's flight at "
              + leader.inFlightEntries(2),
          rounds++ < 100);
      // The leader's retries fill its flight, then the follower's retries arrive ...
      leader.resend();
      lagging.resend();
      c.pump();
      for (Message message : fromTwo) {
        leader.receive(message);
      }
      fromTwo.clear();
      c.pump();
      // ... and only afterwards do the NACKs of the retried entries release that flight.
      for (Message message : toTwo) {
        lagging.receive(message);
      }
      toTwo.clear();
      c.pump();
      for (Message message : fromTwo) {
        leader.receive(message);
      }
      fromTwo.clear();
      c.pump();
    }
    assertEquals(0, lagging.outstandingFetches(1));
  }

  private static List<Message> ofType(List<Message> messages, MessageType type, int to) {
    List<Message> result = new ArrayList<>();
    for (Message message : messages) {
      if (message.type == type && message.to == to) {
        result.add(message);
      }
    }
    return result;
  }

  private static long records(MemoryStore store, RecordType type, EntryId id) {
    return store.records.stream()
        .filter(record -> record.type == type && record.entry != null && record.entry.id.equals(id))
        .count();
  }

  private static Proposal retainedAt(TRaftEngine engine, long partition) {
    for (Proposal proposal : engine.retainedProposals()) {
      if (proposal.first.partition == partition) {
        return proposal;
      }
    }
    throw new AssertionError("No retained proposal opens partition " + partition);
  }

  /** The replica answers a FETCH with the payload it holds: bytes are observable without a peek. */
  private static void assertServesPayload(TRaftEngine engine, Entry entry) throws IOException {
    engine.drainMessages();
    engine.receive(message(MessageType.FETCH_ENTRY, 3, 2, engine.term(), null, entry.id));
    List<Message> answers = ofType(engine.drainMessages(), MessageType.TRANSFER_ENTRY, 3);
    assertEquals(1, answers.size());
    assertArrayEquals(entry.payload(), answers.get(0).entry.payload());
  }

  /**
   * Replica 1 leads; its boundary to partition 1 is confirmed through replica 3 alone, so replica 2
   * still has partition 0 open. Two ENTRY frames of the new partition are ready for replica 2.
   */
  private static final class LaggingFollower {
    final Cluster c = new Cluster();
    final TRaftEngine follower;
    final Entry boundary;
    final List<Entry> open;
    final List<Message> toTwo;
    Message nextBoundary;

    LaggingFollower() throws IOException {
      c.elect(1);
      follower = c.node(2);
      c.append(1, 10);
      boundary = c.append(1, 0);
      for (Message message : c.node(1).drainMessages()) {
        if (message.to == 3) {
          c.node(3).receive(message);
        } else if (message.to == 2 && message.type == MessageType.ENTRY) {
          follower.receive(message);
        }
      }
      follower.drainMessages();
      for (Message response : c.node(3).drainMessages()) {
        c.node(1).receive(response);
      }
      c.node(1).drainMessages();
      assertEquals(1, c.node(1).currentPartition());
      assertEquals(0, follower.currentPartition());
      open = c.node(1).appendDataBatch(dataInputs(20, 30), 1);
      toTwo = ordinaryMessages(c.node(1).drainMessages(), 2);
      assertEquals(2, toTwo.size());
    }

    /**
     * A further boundary names the open entries. Replica 2 installs partition 1 and sees that
     * boundary's metadata only, so it fetches the entries. Returns those FETCH requests.
     */
    List<Message> fetchingTheOpenEntries() throws IOException {
      assertNotNull(c.append(1, 25));
      nextBoundary = ofType(c.node(1).drainMessages(), MessageType.PROPOSAL, 2).get(0);
      long term = nextBoundary.term;
      follower.receive(frame(MessageType.CONFIRM, term, null, retainedAt(c.node(1), 1), null));
      follower.drainMessages();
      assertEquals(1, follower.currentPartition());
      follower.receive(frame(MessageType.PROPOSAL, term, null, nextBoundary.proposal, null));
      return ofType(follower.drainMessages(), MessageType.FETCH_ENTRY, 1);
    }

    /** That boundary's confirmation: its members no longer need to be installed first. */
    void confirmNextBoundary() throws IOException {
      follower.receive(
          frame(MessageType.CONFIRM, nextBoundary.term, null, nextBoundary.proposal, null));
      follower.drainMessages();
    }

    Message transfer(Entry entry) {
      return frame(MessageType.TRANSFER_ENTRY, c.node(1).term(), entry, null, null);
    }
  }

  @Test
  public void duplicateIdentitiesInARunAreStagedOnceAndNackWavesWriteNothing() throws Exception {
    LaggingFollower s = new LaggingFollower();
    Message first = s.toTwo.get(0);
    Message second = s.toTwo.get(1);
    MemoryStore store = s.c.stores.get(2);
    int before = store.records.size();
    assertFalse(s.follower.receiveEntries(Arrays.asList(first, first, second, second)));
    assertEquals(1, records(store, RecordType.ENTRY, first.entry.id));
    assertEquals(before + 2, store.records.size());
    for (int wave = 0; wave < 20; wave++) {
      assertFalse(s.follower.receiveEntries(s.toTwo));
      for (Message message : s.toTwo) {
        s.follower.receive(message);
      }
    }
    assertEquals("Repeated waves do not grow the journal", before + 2, store.records.size());
    s.c.restart(2);
    assertServesPayload(s.c.node(2), first.entry);
    assertServesPayload(s.c.node(2), second.entry);
  }

  @Test
  public void duplicateTransfersInARunAreStoredOnceAndRenewProgressOnce() throws Exception {
    LaggingFollower s = new LaggingFollower();
    // Both open entries and the boundary's own first entry.
    assertEquals(3, s.fetchingTheOpenEntries().size());
    Message first = s.transfer(s.open.get(0));
    Message second = s.transfer(s.open.get(1));
    MemoryStore store = s.c.stores.get(2);
    int batches = store.batches.size();
    long progress = s.follower.electionProgress();
    s.follower.receiveTransfers(Arrays.asList(first, first, second));
    assertEquals("One force", batches + 1, store.batches.size());
    assertEquals(1, records(store, RecordType.ENTRY, first.entry.id));
    assertEquals(2, s.follower.electionProgress() - progress);
    assertEquals(1, s.follower.outstandingFetches(1));
    assertEquals(3, ofType(s.follower.drainMessages(), MessageType.TRANSFER_ACK, 1).size());
    s.c.restart(2);
    assertServesPayload(s.c.node(2), first.entry);
  }

  private static List<EntryId> identities(List<Entry> entries) {
    List<EntryId> identities = new ArrayList<>();
    for (Entry entry : entries) {
      identities.add(entry.id);
    }
    return identities;
  }

  /** Staging certifies nothing: rank, log and replies are as if the frames had not arrived. */
  @Test
  public void stagingChangesNoCountRankOrInstallationAndSurvivesARestartAsBytesOnly()
      throws Exception {
    LaggingFollower s = new LaggingFollower();
    TRaftProtocol.Rank rank = s.follower.currentRank();
    List<Entry> installed = s.follower.installedEntries();
    assertFalse(s.follower.receiveEntries(s.toTwo));
    for (Message message : s.toTwo) {
      s.follower.receive(message);
    }
    List<Message> replies = s.follower.drainMessages();
    assertEquals(2, ofType(replies, MessageType.NACK, 1).size());
    assertTrue(ofType(replies, MessageType.ENTRY_ACK, 1).isEmpty());
    assertEquals(0, rank.compareTo(s.follower.currentRank()));
    assertEquals(installed, s.follower.installedEntries());
    s.c.restart(2);
    assertEquals(0, rank.compareTo(s.c.node(2).currentRank()));
    assertEquals(installed, s.c.node(2).installedEntries());
    assertServesPayload(s.c.node(2), s.open.get(0));
  }

  /** Bytes held only as stored stay out of the recovery of a quorum that lacks them installed. */
  @Test
  public void stagedEntriesOfACrashedLeaderStayOutOfARecoveryOnEitherWinner() throws Exception {
    for (int winner : new int[] {2, 3}) {
      LaggingFollower s = new LaggingFollower();
      Cluster c = s.c;
      for (Message message : s.toTwo) {
        s.follower.receive(message);
      }
      s.follower.drainMessages();
      c.drop = message -> message.to == 1 || message.from == 1;
      c.elect(winner);
      for (Entry entry : s.open) {
        assertFalse(c.node(winner).installedEntries().contains(entry));
        for (int id : new int[] {2, 3}) {
          assertFalse(c.node(id).isCommitted(entry.id));
          assertFalse(c.node(id).isApplied(entry.id));
        }
      }
      Entry later = c.append(winner, 100);
      c.pump();
      for (int id : new int[] {2, 3}) {
        assertTrue(c.node(id).isApplied(later.id));
      }
      // The crashed leader's open entries lose to the closed recovered partition.
      c.drop = ignored -> false;
      c.node(winner).resend();
      c.pump();
      c.node(winner).resend();
      c.pump();
      assertEquals(c.node(winner).installedEntries(), c.node(1).installedEntries());
    }
  }

  /** A reporter that installed them keeps them: the union reads installed, staging only serves. */
  @Test
  public void stagedEntriesInstalledAtAReporterAreMergedOnceAndAllReplicasAgree() throws Exception {
    LaggingFollower s = new LaggingFollower();
    Cluster c = s.c;
    for (Message message : s.toTwo) {
      s.follower.receive(message);
    }
    s.follower.drainMessages();
    c.drop = message -> message.to == 3 || message.from == 3;
    c.restart(1);
    c.elect(1);
    for (Entry entry : s.open) {
      assertEquals(1, Collections.frequency(c.node(1).installedEntries(), entry));
      assertTrue(s.follower.installedEntries().contains(entry));
    }
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    c.node(1).resend();
    c.pump();
    for (int id : c.members) {
      assertEquals(c.node(1).installedEntries(), c.node(id).installedEntries());
      assertEquals(new HashSet<>(c.applications.get(1)), new HashSet<>(c.applications.get(id)));
    }
  }

  @Test
  public void anEntryManyPartitionsAheadIsNackedAndStagedWithoutMovingThePartition()
      throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    c.drop =
        message ->
            message.to == 2
                && message.type != MessageType.VOTE_REQUEST
                && message.type != MessageType.REPORT_REQUEST;
    for (int i = 1; i <= 3; i++) {
      c.append(1, 100 * i);
      c.append(1, 100 * i - 1);
      c.pump();
    }
    long partition = c.node(2).currentPartition();
    assertTrue(c.node(1).currentPartition() > partition + 1);
    c.drop = ignored -> false;
    List<Entry> run = c.node(1).appendDataBatch(dataInputs(1000, 1001), 1);
    List<Message> toTwo = ordinaryMessages(c.node(1).drainMessages(), 2);
    TRaftProtocol.Rank rank = c.node(2).currentRank();
    assertFalse(c.node(2).receiveEntries(toTwo));
    assertEquals(0, rank.compareTo(c.node(2).currentRank()));
    assertEquals(partition, c.node(2).currentPartition());
    assertTrue("False selects the per-message path", c.node(2).drainMessages().isEmpty());
    assertServesPayload(c.node(2), run.get(0));
    for (Message message : toTwo) {
      c.node(2).receive(message);
    }
    assertEquals(2, ofType(c.node(2).drainMessages(), MessageType.NACK, 1).size());
    c.node(1).resend();
    c.pump();
    c.node(1).resend();
    c.pump();
    assertEquals(c.node(1).installedEntries(), c.node(2).installedEntries());
  }

  @Test
  public void storedEntriesInstallWithTheirOpeningsConfirmAndARetryOnlyAcks() throws Exception {
    LaggingFollower s = new LaggingFollower();
    MemoryStore store = s.c.stores.get(2);
    // The opening is unknown: the run is stored and refused.
    assertFalse(s.follower.receiveEntries(s.toTwo));
    s.follower.receive(
        frame(MessageType.CONFIRM, s.toTwo.get(0).term, null, retainedAt(s.c.node(1), 1), null));
    s.follower.drainMessages();
    assertEquals(1, s.follower.currentPartition());
    // The confirmation installs the opening with the stored entries of its partition.
    assertEquals(3, s.follower.currentRank().count);
    for (Entry entry : s.open) {
      assertTrue(s.follower.installedEntries().contains(entry));
    }
    int records = store.records.size();
    // A retried run counts nothing twice, writes nothing and acknowledges in one frame.
    for (int retry = 0; retry < 2; retry++) {
      assertTrue(s.follower.receiveEntries(s.toTwo));
      assertEquals(3, s.follower.currentRank().count);
      assertEquals(records, store.records.size());
      List<Message> acks = ofType(s.follower.drainMessages(), MessageType.ENTRY_ACK, 1);
      assertEquals(1, acks.size());
      assertEquals(1, acks.get(0).identities.size());
    }
  }

  /** A run that overtakes the proposal it completes: installed first, then the next partition. */
  @Test
  public void aRunCompletingAConfirmedProposalInstallsThenOpensTheNextPartition() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<Entry> run = c.node(1).appendDataBatch(dataInputs(10, 20, 30), 1);
    List<Message> original = c.node(1).drainMessages();
    for (Message message : ordinaryMessages(original, 3)) {
      c.node(3).receive(message);
    }
    Entry boundary = c.append(1, 0);
    List<Message> sent = c.node(1).drainMessages();
    for (Message message : sent) {
      if (message.to == 3) {
        c.node(3).receive(message);
      }
    }
    for (Message response : c.node(3).drainMessages()) {
      c.node(1).receive(response);
    }
    List<Message> confirmed = c.node(1).drainMessages();
    c.node(2).receive(ofType(sent, MessageType.PROPOSAL, 2).get(0));
    for (Message message : ofType(confirmed, MessageType.CONFIRM, 2)) {
      c.node(2).receive(message);
    }
    c.node(2).drainMessages();
    assertEquals(0, c.node(2).currentPartition());
    assertTrue(c.node(2).receiveEntries(ordinaryMessages(original, 2)));
    assertEquals(boundary.partition, c.node(2).currentPartition());
    List<Message> replies = c.node(2).drainMessages();
    assertEquals(1, ofType(replies, MessageType.ENTRY_ACK, 1).size());
    for (Message message : ofType(replies, MessageType.NACK, 1)) {
      assertNull(message.reference);
    }
    assertEquals(c.node(1).installedEntries(), c.node(2).installedEntries());
    for (Entry entry : run) {
      assertEquals(1, records(c.stores.get(2), RecordType.INSTALL, entry.id));
    }
  }

  /** Frames a correct leader never sends: each failed check selects the per-message path. */
  @Test
  public void aRunThatFailsAnInstallationCheckIsNotInstalledInOneBatch() throws Exception {
    Entry root = new Entry(new EntryId(1, 1), 0, 0, 0, Kind.NOOP, new byte[0]);
    Proposal p0 = new Proposal(null, Collections.emptyList(), root, true);
    Entry a = new Entry(new EntryId(2, 2), 0, 10, 0, Kind.DATA, new byte[] {1});
    Entry b = new Entry(new EntryId(1, 3), 0, 20, 0, Kind.DATA, new byte[] {2});
    Entry c = new Entry(new EntryId(1, 4), 0, 30, 0, Kind.DATA, new byte[] {3});
    Entry f1 = new Entry(new EntryId(1, 5), 1, 40, 1, Kind.DATA, new byte[] {4});
    Proposal p1 = new Proposal(root.id, Collections.singletonList(root.id), f1, false);
    Entry d = new Entry(new EntryId(1, 6), 1, 50, 1, Kind.DATA, new byte[] {5});
    MemoryStore store = new MemoryStore();
    TRaftEngine f =
        new TRaftEngine(2, new HashSet<>(Arrays.asList(1, 2, 3)), 16, 0, store, entry -> {});
    f.receive(frame(MessageType.CONFIRM, 1, null, p0, null));
    // Reference: not the installed opening.
    assertNoBatchInstall(f, store, frame(MessageType.ENTRY, 1, b, null, f1.id), c);
    // Partition: not the open one.
    assertNoBatchInstall(f, store, frame(MessageType.ENTRY, 1, d, null, root.id), c);
    // Entry term: not the opening's. It is of the current term, so its bytes are staged.
    f.receive(frame(MessageType.VOTE_REQUEST, 2, null, null, null));
    assertNoBatchInstall(f, store, frame(MessageType.ENTRY, 2, a, null, root.id), b);
    assertEquals(1, records(store, RecordType.ENTRY, a.id));
    // Staging takes entries of the current term only.
    Entry older = new Entry(new EntryId(1, 7), 1, 60, 1, Kind.DATA, new byte[] {6});
    assertNoBatchInstall(f, store, frame(MessageType.ENTRY, 2, older, null, f1.id), a);
    assertEquals(0, records(store, RecordType.ENTRY, older.id));
    // Frozen: a retained later proposal excludes it.
    TRaftEngine g =
        new TRaftEngine(
            2, new HashSet<>(Arrays.asList(1, 2, 3)), 16, 0, new MemoryStore(), e -> {});
    g.receive(frame(MessageType.CONFIRM, 1, null, p0, null));
    g.receive(frame(MessageType.PROPOSAL, 1, f1, p1, null));
    assertTrue(g.retainedProposals().contains(p1));
    Message excluded = frame(MessageType.ENTRY, 1, b, null, root.id);
    assertFalse(g.receiveEntries(Arrays.asList(excluded, excluded)));
    assertFalse(g.installedEntries().contains(b));
    // Confirmation closes the partition without the excluded identity.
    g.receive(frame(MessageType.CONFIRM, 1, null, p1, null));
    assertEquals(1, g.currentPartition());
    assertFalse(g.installedEntries().contains(b));
    // Barrier: a partition that a barrier opened takes no ordinary entry.
    Entry barrier = new Entry(new EntryId(1, 5), 1, 40, 1, Kind.BARRIER, new byte[] {4});
    Proposal barrierOpening =
        new Proposal(root.id, Collections.singletonList(root.id), barrier, false);
    MemoryStore barrierStore = new MemoryStore();
    TRaftEngine h =
        new TRaftEngine(2, new HashSet<>(Arrays.asList(1, 2, 3)), 16, 0, barrierStore, e -> {});
    h.receive(frame(MessageType.CONFIRM, 1, null, p0, null));
    h.receive(frame(MessageType.CONFIRM, 1, null, barrierOpening, null));
    assertEquals(1, h.currentPartition());
    assertNoBatchInstall(h, barrierStore, frame(MessageType.ENTRY, 1, d, null, barrier.id), older);
  }

  private static void assertNoBatchInstall(
      TRaftEngine f, MemoryStore store, Message crafted, Entry companion) throws IOException {
    List<Entry> before = f.installedEntries();
    assertFalse(
        f.receiveEntries(
            Arrays.asList(
                crafted,
                frame(MessageType.ENTRY, crafted.term, companion, null, crafted.reference))));
    assertEquals(before, f.installedEntries());
    assertEquals(0, records(store, RecordType.INSTALL, crafted.entry.id));
    assertTrue(ofType(f.drainMessages(), MessageType.ENTRY_ACK, 1).isEmpty());
  }

  /** A run costs one force and ends the deferral: the next frame wakes its proposal at once. */
  @Test
  public void aTransferRunUsesOneForceAndLeavesNoDeferral() throws Exception {
    LaggingFollower s = new LaggingFollower();
    assertEquals(3, s.fetchingTheOpenEntries().size());
    s.confirmNextBoundary();
    MemoryStore store = s.c.stores.get(2);
    int batches = store.batches.size();
    s.follower.receiveTransfers(
        Arrays.asList(s.transfer(s.open.get(0)), s.transfer(s.open.get(1))));
    assertEquals(batches + 1, store.batches.size());
    assertEquals(2, ofType(s.follower.drainMessages(), MessageType.TRANSFER_ACK, 1).size());
    assertEquals(1, s.follower.currentPartition());
    s.follower.receive(s.transfer(s.nextBoundary.entry));
    assertEquals(0, s.follower.outstandingFetches(1));
    assertEquals(2, s.follower.currentPartition());
    assertEquals(1, ofType(s.follower.drainMessages(), MessageType.PROPOSAL_ACK, 1).size());
    assertEquals(0, s.follower.pendingIncomingWork());
  }

  @Test
  public void aTransferRunCompletingItsProposalAcknowledgesInTheSameCall() throws Exception {
    LaggingFollower s = new LaggingFollower();
    assertEquals(3, s.fetchingTheOpenEntries().size());
    s.confirmNextBoundary();
    s.follower.receiveTransfers(
        Arrays.asList(
            s.transfer(s.open.get(0)),
            s.transfer(s.open.get(1)),
            s.transfer(s.nextBoundary.entry)));
    assertEquals(1, ofType(s.follower.drainMessages(), MessageType.PROPOSAL_ACK, 1).size());
    assertEquals(2, s.follower.currentPartition());
    assertEquals(0, s.follower.pendingIncomingWork());
  }

  @Test
  public void unrequestedTransfersInARunAreNotStored() throws Exception {
    LaggingFollower s = new LaggingFollower();
    MemoryStore store = s.c.stores.get(2);
    int records = store.records.size();
    s.follower.receiveTransfers(
        Arrays.asList(s.transfer(s.open.get(0)), s.transfer(s.open.get(1))));
    assertEquals(records, store.records.size());
    assertTrue(ofType(s.follower.drainMessages(), MessageType.TRANSFER_ACK, 1).isEmpty());
  }

  @Test
  public void aFailedTransferRunFencesWithoutAnAckAndKeepsTheDurablePrefix() throws Exception {
    LaggingFollower s = new LaggingFollower();
    assertEquals(3, s.fetchingTheOpenEntries().size());
    MemoryStore store = s.c.stores.get(2);
    store.failBatchAfter = 1;
    try {
      s.follower.receiveTransfers(
          Arrays.asList(s.transfer(s.open.get(0)), s.transfer(s.open.get(1))));
      fail("The injected failure must surface");
    } catch (IOException expected) {
      assertTrue(expected.getMessage().contains("injected"));
    }
    assertTrue(s.follower.isFailed());
    assertTrue(s.follower.drainMessages().isEmpty());
    store.failBatchAfter = -1;
    s.c.restart(2);
    assertFalse(s.c.node(2).isFailed());
    assertServesPayload(s.c.node(2), s.open.get(0));
    for (int round = 0; round < 3; round++) {
      s.c.node(1).resend();
      s.c.pump();
    }
    assertEquals(s.c.node(1).installedEntries(), s.c.node(2).installedEntries());
  }

  /** Bytes that came by ENTRY while requested: late TRANSFERs are only acknowledged. */
  @Test
  public void transfersOfStoredIdentitiesAreAcknowledgedWithoutJournalOrProgress()
      throws Exception {
    LaggingFollower s = new LaggingFollower();
    assertEquals(3, s.fetchingTheOpenEntries().size());
    for (Message message : s.toTwo) {
      s.follower.receive(message);
    }
    s.follower.drainMessages();
    assertEquals(1, s.follower.outstandingFetches(1));
    long progress = s.follower.electionProgress();
    int records = s.c.stores.get(2).records.size();
    s.follower.receiveTransfers(
        Arrays.asList(s.transfer(s.open.get(0)), s.transfer(s.open.get(1))));
    assertEquals(1, s.follower.outstandingFetches(1));
    assertEquals(records, s.c.stores.get(2).records.size());
    assertEquals(progress, s.follower.electionProgress());
    assertEquals(2, ofType(s.follower.drainMessages(), MessageType.TRANSFER_ACK, 1).size());
  }

  @Test
  public void aHigherTermInsideATransferRunClearsItsRequestsAndLeavesNoPendingWork()
      throws Exception {
    LaggingFollower s = new LaggingFollower();
    assertEquals(3, s.fetchingTheOpenEntries().size());
    long term = s.follower.term();
    s.follower.receiveTransfers(
        Arrays.asList(
            s.transfer(s.open.get(0)),
            message(MessageType.TRANSFER_ENTRY, 3, 2, term + 1, s.open.get(1), null)));
    assertEquals(term + 1, s.follower.term());
    assertEquals(0, s.follower.outstandingFetches(1));
    assertEquals(0, s.follower.pendingIncomingWork());
  }

  /** One proposal, many missing members, every kind of frame lost once, heartbeats only. */
  @Test
  public void smallWindowsDrainOnHeartbeatsWithLostFetchTransferAndReceipts() throws Exception {
    for (int window = 1; window <= 5; window++) {
      for (int lossMask = 0; lossMask < 8; lossMask++) {
        final int mask = lossMask;
        Cluster c = new Cluster();
        c.theta = 32;
        c.restart(1);
        c.restart(2);
        c.restart(3);
        for (int id : c.members) {
          c.node(id).setReplicationWindow(window);
        }
        c.elect(1);
        int total = 3 * window + 2;
        for (int i = 0; i < total; i++) {
          assertNotNull(c.append(1, 10 + i));
          for (Message message : c.node(1).drainMessages()) {
            if (message.to == 3) {
              c.network.add(message);
            }
          }
          c.pump();
        }
        Entry boundary = c.append(1, 0);
        assertNotNull(boundary);
        final Set<EntryId> lost = new HashSet<>();
        c.drop =
            message -> {
              if (message.to != 2 && message.from != 2) {
                return false;
              }
              if (message.type == MessageType.ENTRY || message.type == MessageType.NACK) {
                return true;
              }
              return (message.type == MessageType.FETCH_ENTRY
                      && (mask & 1) != 0
                      && lost.add(message.reference))
                  || (message.type == MessageType.TRANSFER_ENTRY
                      && (mask & 2) != 0
                      && lost.add(message.entry.id))
                  || (message.type == MessageType.TRANSFER_ACK
                      && (mask & 4) != 0
                      && lost.add(message.reference));
            };
        c.pump();
        int rounds = 0;
        while (!identities(c.node(2).installedEntries()).contains(boundary.id)) {
          assertTrue(
              "Window "
                  + window
                  + " loss mask "
                  + mask
                  + " stalled, pending "
                  + c.node(2).pendingIncomingWork(),
              rounds++ < 2000);
          c.node(1).resend();
          c.node(2).resend();
          c.pump();
        }
        assertEquals(c.node(1).installedEntries(), c.node(2).installedEntries());
        assertEquals(0, c.node(2).pendingIncomingWork());
      }
    }
  }

  /** Staged bytes with missing predecessors: the proposal's own identity is also a dependency. */
  @Test
  public void aProposalWaitingOnInstalledMembersWakesOnTheLastOneOnly() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry a = c.append(1, 10);
    Entry b = c.append(1, 20);
    List<Message> first = c.node(1).drainMessages();
    assertNotNull(c.append(1, 0));
    Message proposal = ofType(c.node(1).drainMessages(), MessageType.PROPOSAL, 2).get(0);
    c.node(2).receive(proposal);
    assertEquals(2, ofType(c.node(2).drainMessages(), MessageType.FETCH_ENTRY, 1).size());
    c.node(2).receive(frame(MessageType.TRANSFER_ENTRY, proposal.term, a, null, null));
    c.node(2).receive(frame(MessageType.TRANSFER_ENTRY, proposal.term, b, null, null));
    assertFalse(c.node(2).retainedProposals().contains(proposal.proposal));
    c.node(2).drainMessages();
    c.node(2).receive(ofType(first, MessageType.ENTRY, 2).get(0));
    assertFalse(c.node(2).retainedProposals().contains(proposal.proposal));
    assertTrue(ofType(c.node(2).drainMessages(), MessageType.PROPOSAL_ACK, 1).isEmpty());
    c.node(2).receive(ofType(first, MessageType.ENTRY, 2).get(1));
    assertTrue(c.node(2).retainedProposals().contains(proposal.proposal));
    assertEquals(1, ofType(c.node(2).drainMessages(), MessageType.PROPOSAL_ACK, 1).size());
    assertEquals(0, c.node(2).pendingIncomingWork());
  }

  /** A confirmed descendant also installs a proposal that waits on members with deps left. */
  @Test
  public void aConfirmedDescendantReleasesAProposalWaitingOnMembers() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    Entry a = c.append(1, 10);
    Entry b = c.append(1, 20);
    for (Message message : ordinaryMessages(c.node(1).drainMessages(), 3)) {
      c.node(3).receive(message);
    }
    assertNotNull(c.append(1, 0));
    List<Message> sent = c.node(1).drainMessages();
    Message proposal = ofType(sent, MessageType.PROPOSAL, 2).get(0);
    c.node(3).receive(ofType(sent, MessageType.PROPOSAL, 3).get(0));
    c.network.addAll(c.node(3).drainMessages());
    // Replica 3 alone confirms this boundary and the next one.
    c.drop = message -> message.to == 2 || message.from == 2;
    c.pump();
    assertNotNull(c.append(1, 30));
    Entry second = c.append(1, 1);
    assertNotNull(second);
    List<Message> toTwo = new ArrayList<>();
    c.drop =
        message -> {
          if (message.to == 2) {
            toTwo.add(message);
            return true;
          }
          return message.from == 2;
        };
    c.pump();
    assertEquals(second.partition, c.node(1).currentPartition());
    // Replica 2: the first boundary's manifest with bytes only, then the descendant's CONFIRM.
    c.node(2).receive(proposal);
    c.node(2).receive(frame(MessageType.TRANSFER_ENTRY, proposal.term, a, null, null));
    c.node(2).receive(frame(MessageType.TRANSFER_ENTRY, proposal.term, b, null, null));
    assertFalse(c.node(2).retainedProposals().contains(proposal.proposal));
    c.node(2).drainMessages();
    Message confirm = null;
    for (Message message : ofType(toTwo, MessageType.CONFIRM, 2)) {
      confirm = message;
    }
    assertNotNull(confirm);
    // Ordinary proposals are confirmed by reference; replica 2 fetches the manifest it lacks.
    assertNull(confirm.proposal);
    assertEquals(second.id, confirm.reference);
    c.node(2).receive(confirm);
    c.drop = ignored -> false;
    int rounds = 0;
    while (c.node(2).currentPartition() != second.partition) {
      assertTrue("Stalled with pending " + c.node(2).pendingIncomingWork(), rounds++ < 50);
      c.node(1).resend();
      c.node(2).resend();
      c.pump();
    }
    assertEquals(0, c.node(2).pendingIncomingWork());
    assertEquals(c.node(1).installedEntries(), c.node(2).installedEntries());
  }

  /** A retained later proposal freezes the partition; staged bytes outside it stay stored only. */
  @Test
  public void stagingIgnoresAFrozenPredecessorButInstallationStaysGuarded() throws Exception {
    Entry root = new Entry(new EntryId(1, 1), 0, 0, 0, Kind.NOOP, new byte[0]);
    Proposal p0 = new Proposal(null, Collections.emptyList(), root, true);
    Entry member = new Entry(new EntryId(1, 2), 0, 10, 0, Kind.DATA, new byte[] {1});
    Entry excluded = new Entry(new EntryId(1, 3), 0, 20, 0, Kind.DATA, new byte[] {2});
    Entry excluded2 = new Entry(new EntryId(1, 4), 0, 30, 0, Kind.DATA, new byte[] {3});
    Entry first = new Entry(new EntryId(1, 5), 1, 40, 2, Kind.DATA, new byte[] {4});
    Proposal frozen = new Proposal(root.id, Arrays.asList(root.id, member.id), first, false);
    TRaftEngine f =
        new TRaftEngine(
            2, new HashSet<>(Arrays.asList(1, 2, 3)), 16, 0, new MemoryStore(), e -> {});
    f.receive(frame(MessageType.CONFIRM, 1, null, p0, null));
    f.receive(frame(MessageType.ENTRY, 1, member, null, root.id));
    f.receive(frame(MessageType.PROPOSAL, 1, first, frozen, null));
    assertTrue(f.retainedProposals().contains(frozen));
    f.drainMessages();
    List<Entry> before = f.installedEntries();
    List<Message> run =
        Arrays.asList(
            frame(MessageType.ENTRY, 1, excluded, null, root.id),
            frame(MessageType.ENTRY, 1, excluded2, null, root.id));
    assertFalse(f.receiveEntries(run));
    assertEquals(before, f.installedEntries());
    for (Message message : run) {
      f.receive(message);
    }
    List<Message> replies = f.drainMessages();
    assertEquals(2, ofType(replies, MessageType.NACK, 1).size());
    assertTrue(ofType(replies, MessageType.ENTRY_ACK, 1).isEmpty());
    assertEquals(before, f.installedEntries());
    f.receive(frame(MessageType.CONFIRM, 1, null, frozen, null));
    assertEquals(1, f.currentPartition());
    assertEquals(Arrays.asList(root.id, member.id, first.id), identities(f.installedEntries()));
  }

  @Test
  public void aCandidateThatLostItsTermHoldsNoCandidateState() throws Exception {
    Cluster c = new Cluster(5);
    c.node(1).startElection();
    c.node(3).startElection();
    c.pump();
    assertEquals(Role.LEADER, c.node(1).role());
    assertEquals(c.node(1).term(), c.node(3).term());
    assertEquals(Role.FOLLOWER, c.node(3).role());
    assertEquals(1, c.node(3).leaderId());
    long term = c.node(3).term();
    // A late vote does not revive the candidacy.
    c.node(3).receive(message(MessageType.VOTE, 2, 3, term, null, null));
    assertEquals(Role.FOLLOWER, c.node(3).role());
    // It grants no second vote in the term and it answers the winner's report request.
    c.node(3).drainMessages();
    c.node(3)
        .receive(
            new Message(
                MessageType.VOTE_REQUEST,
                2,
                3,
                term,
                null,
                null,
                null,
                new TRaftProtocol.Rank(99, 99, 99),
                null,
                -1,
                null));
    assertTrue(ofType(c.node(3).drainMessages(), MessageType.VOTE, 2).isEmpty());
    c.node(3).receive(message(MessageType.REPORT_REQUEST, 1, 3, term, null, null));
    assertEquals(1, ofType(c.node(3).drainMessages(), MessageType.REPORT, 1).size());
    c.restart(3);
    assertEquals(Role.FOLLOWER, c.node(3).role());
    assertEquals(term, c.node(3).term());
  }

  @Test
  public void leaderAckBookkeepingShrinksToTheOpenPartitionAlsoAfterRecovery() throws Exception {
    Cluster c = new Cluster();
    c.elect(1);
    List<EntryId> written = new ArrayList<>();
    for (long ts = 10; written.size() < 10 * c.theta; ts++) {
      Entry entry = c.append(1, ts);
      if (entry != null) {
        written.add(entry.id);
      }
      c.pump();
    }
    assertTrue(c.node(1).currentPartition() >= 9);
    Map<String, Integer> sizes = c.node(1).stateSizes();
    assertTrue(
        "only the open partition needs entry ACKs: " + sizes, sizes.get("entryAcks") <= c.theta);
    assertEquals("a confirmed proposal keeps no ACK set", 0, (int) sizes.get("proposalAcks"));
    for (int id : c.members) {
      if (id != 1) {
        assertEquals(0, (int) c.node(id).stateSizes().get("entryAcks"));
      }
      for (EntryId entry : written) {
        assertTrue(c.node(id).isApplied(entry));
      }
    }
    // The recovery confirmation certifies the whole inherited history; the passed partitions
    // must not regrow their per-identity evidence.
    c.elect(2);
    sizes = c.node(2).stateSizes();
    assertTrue("recovery recreated entry ACKs: " + sizes, sizes.get("entryAcks") <= c.theta);
    assertEquals(0, (int) sizes.get("proposalAcks"));
    Entry next = c.append(2, 100_000);
    c.pump();
    for (int id : c.members) {
      assertTrue(c.node(id).isApplied(next.id));
    }
  }

  /** Three voters with partitions of {@code theta} entries and empty journals. */
  private static Cluster smallPartitionCluster(int theta) throws IOException {
    Cluster c = new Cluster();
    c.theta = theta;
    for (int id : c.members) {
      c.stores.put(id, new MemoryStore());
      c.applications.put(id, new ArrayList<>());
      c.restart(id);
    }
    return c;
  }

  /** Append the clock's next timestamps on the leader until its open partition reaches target. */
  private static List<Entry> writeUntilPartition(Cluster c, int leader, long target, long[] clock)
      throws IOException {
    List<Entry> written = new ArrayList<>();
    while (c.node(leader).currentPartition() < target) {
      Entry entry = c.append(leader, clock[0]++);
      if (entry != null) {
        written.add(entry);
      }
      c.pump();
    }
    // A heartbeat carries the final commit frontier to every follower.
    c.node(leader).resend();
    c.pump();
    return written;
  }

  private static List<Entry> above(List<Entry> entries, long frontier) {
    List<Entry> result = new ArrayList<>();
    for (Entry entry : entries) {
      if (entry.partition > frontier) {
        result.add(entry);
      }
    }
    return result;
  }

  private static List<DurableRecord> compactRecords(MemoryStore store) {
    List<DurableRecord> result = new ArrayList<>();
    for (DurableRecord record : store.records) {
      if (record.type == RecordType.COMPACT) {
        result.add(record);
      }
    }
    return result;
  }

  @Test
  public void compactionDropsClosedPartitionsAndAnswersStayExact() throws Exception {
    Cluster c = smallPartitionCluster(4);
    c.elect(1);
    long[] clock = {10};
    List<Entry> written = writeUntilPartition(c, 1, 8, clock);
    for (int round = 0; round < 3; round++) {
      long frontier = c.node(1).currentPartition() - 2;
      List<Entry> history = c.node(1).installedEntries();
      for (int id : c.members) {
        assertEquals(history, c.node(id).installedEntries());
        c.node(id).compactThrough(frontier);
        c.node(id).checkCompactionInvariants();
        assertEquals(frontier, c.node(id).compactedThrough());
        EntryId anchor = c.node(id).compactionAnchor();
        assertEquals(frontier + 1, retainedAt(c.node(id), frontier + 1).first.partition);
        assertEquals(retainedAt(c.node(id), frontier + 1).id(), anchor);
        assertEquals(above(history, frontier), c.node(id).installedEntries());
        Map<String, Integer> sizes = c.node(id).stateSizes();
        int window = c.node(id).installedEntries().size();
        assertTrue(window <= 2 * c.theta);
        for (String key :
            Arrays.asList("installed", "stored", "knownCommitted", "applied", "entryAcks")) {
          assertTrue(key + " " + sizes, sizes.get(key) <= window);
        }
        assertTrue("retained " + sizes, sizes.get("retained") <= 3);
        assertTrue("partitions " + sizes, sizes.get("partitions") <= 2);
        for (Entry entry : written) {
          assertTrue(entry.id + " committed", c.node(id).isCommitted(entry.id));
          assertTrue(entry.id + " applied", c.node(id).isApplied(entry.id));
        }
        EntryId foreign = new EntryId(written.get(0).id.term + 5, written.get(0).id.index);
        assertFalse(c.node(id).isCommitted(foreign));
        assertFalse(c.node(id).isApplied(foreign));
        DurableRecord last = compactRecords(c.stores.get(id)).get(round);
        assertEquals(frontier, last.frontier);
        assertEquals(anchor, last.reference);
        assertTrue(
            "COMPACT costs no force of its own",
            c.stores.get(id).deferred.stream().anyMatch(r -> r.type == RecordType.COMPACT));
      }
      written.addAll(writeUntilPartition(c, 1, c.node(1).currentPartition() + 3, clock));
      for (int id : c.members) {
        c.node(id).checkCompactionInvariants();
        for (Entry entry : written) {
          assertTrue(c.node(id).isApplied(entry.id));
        }
      }
    }
  }

  @Test
  public void compactionRefusesWhatThisReplicaHasNotClosedCommittedAndApplied() throws Exception {
    Cluster c = smallPartitionCluster(4);
    c.elect(1);
    long[] clock = {10};
    writeUntilPartition(c, 1, 3, clock);
    for (long frontier : new long[] {c.node(1).currentPartition(), -1}) {
      try {
        c.node(2).compactThrough(frontier);
        fail("compacting the open partition or nothing must be refused: " + frontier);
      } catch (IllegalStateException expected) {
        assertEquals(-1, c.node(2).compactedThrough());
      }
    }
    c.node(2).compactThrough(1);
    try {
      c.node(2).compactThrough(1);
      fail("the frontier only moves forward");
    } catch (IllegalStateException expected) {
      assertEquals(1, c.node(2).compactedThrough());
    }
  }

  @Test
  public void compactedTimestampMaximumStillOpensABoundary() throws Exception {
    Cluster c = smallPartitionCluster(4);
    c.elect(1);
    Entry high = c.append(1, 1_000);
    c.pump();
    long[] clock = {1};
    // Every later write is below the maximum, so each one opens its own partition.
    writeUntilPartition(c, 1, high.partition + 5, clock);
    long frontier = c.node(1).currentPartition() - 2;
    for (int id : c.members) {
      c.node(id).compactThrough(frontier);
      c.node(id).checkCompactionInvariants();
      assertFalse(c.node(id).installedEntries().contains(high));
    }
    long open = c.node(1).currentPartition();
    Entry lower = c.node(1).append(Kind.DATA, 500, new byte[] {5}, 1);
    assertEquals("a compacted maximum still forces a boundary", open + 1, lower.partition);
    c.pump();
    for (int id : c.members) {
      assertTrue(c.node(id).isApplied(lower.id));
    }
    // A restart rebuilds the maximum from the installed suffix and the compacted partitions.
    c.restart(2);
    c.node(2).checkCompactionInvariants();
  }

  @Test
  public void durableRestartReplaysCompactionWithoutApplyingAgain() throws Exception {
    Cluster c = smallPartitionCluster(4);
    c.elect(1);
    long[] clock = {10};
    List<Entry> written = writeUntilPartition(c, 1, 6, clock);
    long frontier = c.node(1).currentPartition() - 2;
    for (int id : c.members) {
      c.node(id).compactThrough(frontier);
    }
    List<Entry> suffix = c.node(2).installedEntries();
    EntryId anchor = c.node(2).compactionAnchor();
    int applications = c.applications.get(2).size();
    AtomicInteger calls = new AtomicInteger();
    c.engines.put(
        2,
        new TRaftEngine(
            2,
            c.members,
            c.theta,
            100,
            c.stores.get(2),
            entry -> {
              calls.incrementAndGet();
            }));
    assertEquals("APPLIED records cover every compacted entry", 0, calls.get());
    c.node(2).checkCompactionInvariants();
    assertEquals(frontier, c.node(2).compactedThrough());
    assertEquals(anchor, c.node(2).compactionAnchor());
    assertEquals(suffix, c.node(2).installedEntries());
    for (Entry entry : written) {
      assertTrue(c.node(2).isCommitted(entry.id));
      assertTrue(c.node(2).isApplied(entry.id));
    }
    c.restart(2);
    assertEquals(applications, c.applications.get(2).size());
    List<Entry> later = writeUntilPartition(c, 1, c.node(1).currentPartition() + 2, clock);
    written.addAll(later);
    for (Entry entry : written) {
      assertTrue(c.node(2).isApplied(entry.id));
    }
    assertEquals(applications + later.size(), c.applications.get(2).size());
    c.node(2).checkCompactionInvariants();
  }

  private static void assertAppliedEverywhere(Cluster c, List<Entry> entries) {
    for (int id : c.members) {
      c.node(id).checkCompactionInvariants();
      for (Entry entry : entries) {
        assertTrue("n" + id + " applied " + entry.id, c.node(id).isApplied(entry.id));
      }
    }
  }

  @Test
  public void candidatesRecoverWhetherTheyCompactedLessOrMoreThanTheirReporters() throws Exception {
    Cluster c = smallPartitionCluster(4);
    List<Report> reports = new ArrayList<>();
    c.drop =
        m -> {
          if (m.type == MessageType.REPORT && m.report != null) {
            reports.add(m.report);
          }
          return false;
        };
    c.elect(1);
    long[] clock = {10};
    List<Entry> written = writeUntilPartition(c, 1, 10, clock);
    long open = c.node(1).currentPartition();
    c.node(1).compactThrough(open - 1);
    c.node(2).compactThrough(open - 5);
    c.node(3).compactThrough(open - 3);
    reports.clear();
    // The least compacted replica recovers from more compacted reports.
    c.elect(2);
    assertEquals(2, reports.size());
    for (Report report : reports) {
      assertTrue(
          "reports carry only the uncompacted suffix",
          report.installed.size() + report.stored.size() <= 2 * 6 * c.theta);
    }
    written.addAll(writeUntilPartition(c, 2, c.node(2).currentPartition() + 3, clock));
    assertAppliedEverywhere(c, written);
    // The most compacted replica then recovers from less compacted reports.
    c.node(1).compactThrough(c.node(1).currentPartition() - 1);
    reports.clear();
    c.elect(1);
    for (Report report : reports) {
      assertTrue(report.installed.size() + report.stored.size() <= 2 * 8 * c.theta);
    }
    written.addAll(writeUntilPartition(c, 1, c.node(1).currentPartition() + 3, clock));
    assertAppliedEverywhere(c, written);
    long frontier = c.node(1).currentPartition() - 2;
    for (int id : c.members) {
      c.node(id).compactThrough(frontier);
    }
    List<Entry> suffix = c.node(1).installedEntries();
    for (int id : c.members) {
      assertEquals(suffix, c.node(id).installedEntries());
    }
  }

  @Test
  public void recoveryAfterCompactionUnionsTheOpenPartitionOfItsReports() throws Exception {
    Cluster c = smallPartitionCluster(8);
    c.elect(1);
    long[] clock = {10};
    List<Entry> written = writeUntilPartition(c, 1, 4, clock);
    for (int id : c.members) {
      c.node(id).compactThrough(c.node(id).currentPartition() - 1);
    }
    Entry a = c.append(1, clock[0]++);
    c.drop = m -> m.type == MessageType.ENTRY && m.to == 3;
    c.pump();
    Entry b = c.append(1, clock[0]++);
    c.drop = m -> m.type == MessageType.ENTRY && m.to == 2;
    c.pump();
    assertTrue(c.node(1).isCommitted(a.id));
    assertTrue(c.node(1).isCommitted(b.id));
    assertFalse(c.node(3).installedEntries().contains(a));
    assertFalse(c.node(2).installedEntries().contains(b));
    // Node 1 disappears; node 2 unions the open partition held apart by 2 and 3.
    c.drop = m -> m.from == 1 || m.to == 1;
    c.elect(2);
    written.add(a);
    written.add(b);
    for (int id = 2; id <= 3; id++) {
      c.node(id).checkCompactionInvariants();
      assertTrue(c.node(id).installedEntries().contains(a));
      assertTrue(c.node(id).installedEntries().contains(b));
      for (Entry entry : written) {
        assertTrue(c.node(id).isApplied(entry.id));
      }
    }
  }

  @Test
  public void replayedOldTrafficAfterCompactionCausesNoFencingRegrowthOrFetchLoop()
      throws Exception {
    Cluster c = smallPartitionCluster(4);
    List<Message> history = new ArrayList<>();
    c.drop =
        m -> {
          history.add(m);
          return false;
        };
    c.elect(1);
    long[] clock = {10};
    List<Entry> written = writeUntilPartition(c, 1, 8, clock);
    c.drop = ignored -> false;
    long frontier = c.node(1).currentPartition() - 1;
    for (int id : c.members) {
      // The anchor is every replica's installed opening: its heartbeat CONFIRM is stale too.
      c.node(id).compactThrough(frontier);
    }
    c.node(1).resend();
    c.pump();
    Map<Integer, Map<String, Integer>> before = new HashMap<>();
    Map<Integer, Integer> records = new HashMap<>();
    for (int id : c.members) {
      before.put(id, c.node(id).stateSizes());
      records.put(id, c.stores.get(id).records.size());
    }
    for (Message old : history) {
      c.network.add(old);
    }
    c.pump();
    for (int round = 0; round < 3; round++) {
      for (int id : c.members) {
        c.node(id).resend();
      }
      c.pump();
    }
    for (int id : c.members) {
      TRaftEngine node = c.node(id);
      assertFalse(node.isFailed());
      node.checkCompactionInvariants();
      assertEquals("n" + id + " waits on stale work", 0, node.pendingIncomingWork());
      for (int peer : c.members) {
        assertEquals("n" + id + " fetches from " + peer, 0, node.outstandingFetches(peer));
      }
      Map<String, Integer> after = node.stateSizes();
      for (String key :
          Arrays.asList("installed", "stored", "retained", "knownCommitted", "applied")) {
        assertEquals("n" + id + " regrew " + key, before.get(id).get(key), after.get(key));
      }
      assertEquals(
          "n" + id + " journaled stale traffic",
          (int) records.get(id),
          c.stores.get(id).records.size());
    }
    // A late repair of a compacted entry is receipted only for the canonical identity.
    Entry compacted = written.get(0);
    Entry foreign =
        new Entry(
            new EntryId(compacted.id.term + 3, compacted.id.index),
            compacted.partition,
            compacted.timestamp,
            compacted.prevSize,
            Kind.DATA,
            new byte[] {9});
    long term = c.node(2).term();
    for (Entry transfer : Arrays.asList(compacted, foreign)) {
      c.node(2).drainMessages();
      c.node(2)
          .receive(
              new Message(
                  MessageType.TRANSFER_ENTRY,
                  1,
                  2,
                  term,
                  transfer,
                  null,
                  null,
                  null,
                  null,
                  -1,
                  null));
      List<Message> acks = ofType(c.node(2).drainMessages(), MessageType.TRANSFER_ACK, 1);
      assertEquals(transfer == compacted ? 1 : 0, acks.size());
    }
    assertEquals((int) records.get(2), c.stores.get(2).records.size());
    written.addAll(writeUntilPartition(c, 1, c.node(1).currentPartition() + 2, clock));
    assertAppliedEverywhere(c, written);
  }

  /** Small partitions, and every engine compacts as far as the cluster cap allows. */
  private static Cluster compactingCluster(int theta) throws IOException {
    Cluster c = new Cluster();
    c.theta = theta;
    c.compaction = true;
    for (int id : c.members) {
      c.stores.put(id, new MemoryStore());
      c.applications.put(id, new ArrayList<>());
      c.restart(id);
    }
    return c;
  }

  /** Close one partition per round and tick every replica after each. */
  private static List<Entry> writeAndTick(Cluster c, int leader, int partitions, long[] clock)
      throws IOException {
    List<Entry> written = new ArrayList<>();
    for (int round = 0; round < partitions; round++) {
      written.addAll(writeUntilPartition(c, leader, c.node(leader).currentPartition() + 1, clock));
      c.tickAll(clock[0] * 10);
      c.tickAll(clock[0] * 10 + 1);
    }
    return written;
  }

  private static void assertBoundedByTheWindow(Cluster c) {
    for (int id : c.members) {
      TRaftEngine node = c.node(id);
      node.checkCompactionInvariants();
      long open = node.currentPartition();
      assertTrue(
          "n" + id + " compacted " + node.compactedThrough() + " of " + open,
          node.compactedThrough() >= open - 3);
      Map<String, Integer> sizes = node.stateSizes();
      for (String key :
          Arrays.asList(
              "installed", "stored", "knownCommitted", "applied", "entryAcks", "partitions")) {
        assertTrue("n" + id + " " + key + " " + sizes, sizes.get(key) <= 4 * c.theta);
      }
      assertTrue("n" + id + " " + sizes, sizes.get("retained") <= 5);
    }
  }

  @Test
  public void compactionFollowsTheClusterCapAndKeepsStateWithinAFewPartitions() throws Exception {
    Cluster c = compactingCluster(4);
    List<Message> commits = new ArrayList<>();
    c.drop =
        m -> {
          if (m.type == MessageType.COMMIT && m.reference != null) {
            commits.add(m);
          }
          return false;
        };
    c.elect(1);
    long[] clock = {10};
    List<Entry> written = writeAndTick(c, 1, 40, clock);
    assertBoundedByTheWindow(c);
    assertAppliedEverywhere(c, written);
    assertFalse("the leader advertises its cap in COMMIT", commits.isEmpty());
    List<Entry> suffix = c.node(1).installedEntries();
    for (int id : c.members) {
      assertEquals(suffix, c.node(id).installedEntries());
      assertTrue(compactRecords(c.stores.get(id)).size() >= 30);
    }
    // A new leader keeps compacting from what its predecessor's cap allowed.
    c.elect(2);
    written.addAll(writeAndTick(c, 2, 10, clock));
    assertBoundedByTheWindow(c);
    assertAppliedEverywhere(c, written);
  }

  @Test
  public void anEntryARecoveryDroppedStaysUncommittedOnceItsPartitionIsCompacted()
      throws Exception {
    Cluster c = compactingCluster(4);
    Entry dropped = failoverDroppingASuffix(c);
    c.drop = ignored -> false;
    long[] clock = {100};
    List<Entry> written = writeAndTick(c, 2, 8, clock);
    for (int id : c.members) {
      TRaftEngine node = c.node(id);
      node.checkCompactionInvariants();
      assertTrue(
          "n" + id + " compacted the dropped entry's partition",
          node.compactedThrough() >= dropped.partition);
      assertFalse("n" + id + " calls the dropped entry committed", node.isCommitted(dropped.id));
      assertFalse(node.isApplied(dropped.id));
      assertFalse(c.applications.get(id).contains(dropped.id));
      assertEquals(0, node.pendingIncomingWork());
    }
    assertAppliedEverywhere(c, written);
  }

  @Test
  public void compactionStopsWhileAPeerIsCutOffAndResumesAfterItCatchesUp() throws Exception {
    Cluster c = compactingCluster(4);
    c.elect(1);
    long[] clock = {10};
    List<Entry> written = writeAndTick(c, 1, 6, clock);
    assertBoundedByTheWindow(c);
    long closedByThree = c.node(3).currentPartition() - 1;
    c.drop = m -> m.from == 3 || m.to == 3;
    written.addAll(writeAndTick(c, 1, 4, clock));
    // A new term clears the leader's retransmission state: node 3 will have to fetch its gap.
    c.elect(2);
    written.addAll(writeAndTick(c, 2, 4, clock));
    for (int id = 1; id <= 2; id++) {
      assertTrue(
          "n" + id + " compacted past what node 3 closed",
          c.node(id).compactedThrough() <= closedByThree);
      assertTrue(c.node(id).installedEntries().size() > 4 * c.theta);
      c.node(id).checkCompactionInvariants();
    }
    EntryId anchor = c.node(2).compactionAnchor();
    List<Message> fetches = new ArrayList<>();
    c.drop =
        m -> {
          if (m.from == 3
              && (m.type == MessageType.FETCH_ENTRY || m.type == MessageType.FETCH_PROPOSAL)) {
            fetches.add(m);
          }
          return false;
        };
    for (int round = 0; round < 6; round++) {
      c.node(2).resend();
      c.pump();
    }
    assertEquals(c.node(2).currentPartition(), c.node(3).currentPartition());
    assertFalse("node 3 repaired its gap", fetches.isEmpty());
    for (Message fetch : fetches) {
      assertTrue(
          "node 3 fetched " + fetch.reference + " below the cap opening " + anchor,
          fetch.reference.index >= anchor.index);
    }
    written.addAll(writeAndTick(c, 2, 6, clock));
    assertTrue(c.node(2).compactedThrough() > closedByThree);
    assertBoundedByTheWindow(c);
    assertAppliedEverywhere(c, written);
  }

  private static Cluster volatileSmallPartitionCluster(int theta) throws IOException {
    Cluster c = smallPartitionCluster(theta);
    c.durableCommitRecords = false;
    c.durableAppliedRecords = false;
    restartEmptyApplications(c);
    return c;
  }

  /** A crash before the deferred COMPACT reached disk loses it and everything after it. */
  private static void truncateAtLastCompaction(MemoryStore store) {
    int cut = -1;
    for (int i = 0; i < store.records.size(); i++) {
      if (store.records.get(i).type == RecordType.COMPACT) {
        cut = i;
      }
    }
    assertTrue("a COMPACT record must exist", cut >= 0);
    store.records.subList(cut, store.records.size()).clear();
    store.deferred.clear();
  }

  @Test
  public void volatileRestartAppliesCompactedPartitionsOnceInOrderAndTheSuffixWaits()
      throws Exception {
    Cluster c = volatileSmallPartitionCluster(4);
    c.elect(1);
    long[] clock = {10};
    List<Entry> written = writeUntilPartition(c, 1, 3, clock);
    // A lower timestamp opens a boundary of its own: partition order is not timestamp order.
    written.add(c.node(1).append(Kind.DATA, 5, new byte[] {5}, 1));
    c.pump();
    written.addAll(writeUntilPartition(c, 1, 8, clock));
    long frontier = c.node(1).currentPartition() - 2;
    for (int id : c.members) {
      c.node(id).compactThrough(frontier);
    }
    List<Entry> compacted = new ArrayList<>();
    List<Entry> suffix = new ArrayList<>();
    for (Entry entry : written) {
      (entry.partition <= frontier ? compacted : suffix).add(entry);
    }
    assertFalse(suffix.isEmpty());
    // Node 2 restarts alone, with an empty state machine and no leader in reach.
    c.applications.get(2).clear();
    c.drop = m -> m.from == 2 || m.to == 2;
    c.restart(2);
    List<EntryId> rebuilt = new ArrayList<>(c.applications.get(2));
    assertEquals("each compacted entry exactly once", compacted.size(), rebuilt.size());
    long last = -1;
    Map<EntryId, Entry> byId = new HashMap<>();
    for (Entry entry : written) {
      byId.put(entry.id, entry);
    }
    for (EntryId id : rebuilt) {
      Entry entry = byId.get(id);
      assertNotNull("only compacted client entries are applied: " + id, entry);
      assertTrue(entry.partition <= frontier);
      assertTrue("partition order", entry.partition >= last);
      last = entry.partition;
    }
    for (Entry entry : suffix) {
      assertFalse("the uncertified suffix waits", c.node(2).isApplied(entry.id));
      assertFalse(c.node(2).isCommitted(entry.id));
    }
    c.node(2).checkCompactionInvariants();
    c.drop = ignored -> false;
    c.node(1).resend();
    c.pump();
    assertEquals(written.size(), c.applications.get(2).size());
    assertEquals(written.size(), new HashSet<>(c.applications.get(2)).size());
    assertAppliedEverywhere(c, written);
  }

  @Test
  public void lostDeferredCompactionFallsBackToFullReplay() throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = durable ? smallPartitionCluster(4) : volatileSmallPartitionCluster(4);
      c.elect(1);
      long[] clock = {10};
      List<Entry> written = writeUntilPartition(c, 1, 6, clock);
      List<Entry> history = c.node(2).installedEntries();
      c.node(2).compactThrough(c.node(2).currentPartition() - 2);
      truncateAtLastCompaction(c.stores.get(2));
      if (!durable) {
        // The snapshot/replay contract: the state machine restarts empty.
        c.applications.get(2).clear();
      }
      c.restart(2);
      assertEquals(-1, c.node(2).compactedThrough());
      assertEquals("the whole history is replayed", history, c.node(2).installedEntries());
      c.node(2).checkCompactionInvariants();
      c.node(1).resend();
      c.pump();
      written.addAll(writeUntilPartition(c, 1, c.node(1).currentPartition() + 2, clock));
      assertAppliedEverywhere(c, written);
      assertEquals(written.size(), c.applications.get(2).size());
      c.node(2).compactThrough(c.node(2).currentPartition() - 2);
      c.node(2).checkCompactionInvariants();
    }
  }

  @Test
  public void followerLosingItsMarkerAfterCompactionReinstallsWithoutFetchingCompactedData()
      throws Exception {
    for (boolean durable : COMMIT_MODES) {
      Cluster c = durable ? smallPartitionCluster(4) : volatileSmallPartitionCluster(4);
      c.elect(1);
      long[] clock = {10};
      List<Entry> written = writeUntilPartition(c, 1, 6, clock);
      long frontier = c.node(1).currentPartition() - 2;
      for (int id : c.members) {
        c.node(id).compactThrough(frontier);
      }
      EntryId anchor = c.node(1).compactionAnchor();
      Entry boundary = c.node(1).append(Kind.DATA, 1, new byte[] {1}, 1);
      c.pump();
      written.add(boundary);
      long partition = c.node(1).currentPartition();
      truncateAtDeferredMarker(c.stores.get(2), boundary.id);
      c.restart(2);
      assertEquals(partition - 1, c.node(2).currentPartition());
      assertEquals(frontier, c.node(2).compactedThrough());
      List<Message> fetches = new ArrayList<>();
      c.drop =
          m -> {
            if (m.type == MessageType.FETCH_ENTRY || m.type == MessageType.FETCH_PROPOSAL) {
              fetches.add(m);
            }
            return false;
          };
      c.node(1).resend();
      c.pump();
      assertEquals(partition, c.node(2).currentPartition());
      for (Message fetch : fetches) {
        assertTrue("fetch of compacted " + fetch.reference, fetch.reference.index > anchor.index);
      }
      c.drop = ignored -> false;
      written.addAll(writeUntilPartition(c, 1, c.node(1).currentPartition() + 2, clock));
      assertAppliedEverywhere(c, written);
    }
  }

  @Test
  public void compactedTimestampMaximumSurvivesALeaderRestart() throws Exception {
    Cluster c = smallPartitionCluster(4);
    c.elect(1);
    Entry high = c.append(1, 1_000);
    c.pump();
    long[] clock = {1};
    writeUntilPartition(c, 1, high.partition + 5, clock);
    long frontier = c.node(1).currentPartition() - 2;
    for (int id : c.members) {
      c.node(id).compactThrough(frontier);
    }
    c.restart(1);
    c.elect(1);
    long open = c.node(1).currentPartition();
    Entry lower = c.node(1).append(Kind.DATA, 500, new byte[] {5}, 1);
    assertEquals("the restored maximum still forces a boundary", open + 1, lower.partition);
    c.pump();
    assertAppliedEverywhere(c, Collections.singletonList(lower));
  }
}
