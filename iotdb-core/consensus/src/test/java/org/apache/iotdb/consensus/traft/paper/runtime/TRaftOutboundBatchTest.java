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
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.traft.paper.core.TRaftCodec;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.EntryId;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Proposal;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Transport scheduling tests with deterministic completion, independent of socket timing. */
public class TRaftOutboundBatchTest {
  private static final class ManualExecutor implements Executor {
    final Deque<Runnable> tasks = new ArrayDeque<>();
    boolean rejectNext;

    @Override
    public void execute(Runnable task) {
      if (rejectNext) {
        rejectNext = false;
        throw new RejectedExecutionException("injected");
      }
      tasks.addLast(task);
    }

    void runNext() {
      tasks.removeFirst().run();
    }
  }

  private static TRaftGroup.Channel channel(
      RaftConfig.Replication limits,
      ManualExecutor network,
      AtomicBoolean closed,
      List<List<Integer>> delivered) {
    return new TRaftGroup.Channel(
        new TEndPoint("127.0.0.1", 1234),
        limits,
        network,
        closed::get,
        (endpoint, batch) -> {
          List<Integer> values = new ArrayList<>();
          for (ByteBuffer frame : batch) {
            values.add(frame.getInt(frame.position()));
          }
          delivered.add(values);
          return Collections.emptyList();
        });
  }

  private static List<TRaftGroup.Frame> reserve(TRaftGroup.Channel channel, int count) {
    List<TRaftGroup.Frame> frames = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      byte[] bytes = ByteBuffer.allocate(4).putInt(i).array();
      String key = "entry-" + i;
      assertTrue(channel.reserve(bytes.length, key));
      frames.add(new TRaftGroup.Frame(bytes, key, bytes.length));
    }
    return frames;
  }

  @Test
  public void eventFramesAreVisibleTogetherBeforeTheFirstDispatch() {
    RaftConfig.Replication limits = new RaftConfig.Replication();
    limits.maxEntriesPerAppend = 64;
    ManualExecutor network = new ManualExecutor();
    List<List<Integer>> delivered = new ArrayList<>();
    TRaftGroup.Channel channel = channel(limits, network, new AtomicBoolean(), delivered);
    channel.offerReserved(reserve(channel, 10));
    assertEquals("One event must not eagerly schedule ten singleton RPCs", 1, network.tasks.size());
    assertTrue(delivered.isEmpty());
    network.runNext();
    assertEquals(Collections.singletonList(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)), delivered);
    assertEquals(0, channel.reservedBytes);
    assertEquals(0, channel.active);
  }

  @Test
  public void byteBatchWindowAndDuplicateBudgetsIncludeQueuedAndActiveFrames() {
    RaftConfig.Replication limits = new RaftConfig.Replication();
    limits.maxEntriesPerAppend = 3;
    limits.maxBytesPerAppend = 10;
    limits.maxInflightBatches = 2;
    limits.maxInflightBytes = 28;
    ManualExecutor network = new ManualExecutor();
    List<List<Integer>> delivered = new ArrayList<>();
    TRaftGroup.Channel channel = channel(limits, network, new AtomicBoolean(), delivered);
    channel.offerReserved(reserve(channel, 7));
    assertEquals(2, network.tasks.size());
    assertEquals(28, channel.reservedBytes);
    assertFalse(channel.reserve(1, null));
    assertFalse(channel.reserve(4, "entry-0"));
    while (!network.tasks.isEmpty()) {
      assertTrue(network.tasks.size() <= 2);
      network.runNext();
    }
    assertEquals(
        Arrays.asList(
            Arrays.asList(0, 1), Arrays.asList(2, 3), Arrays.asList(4, 5), Arrays.asList(6)),
        delivered);
    assertEquals(0, channel.reservedBytes);
    assertTrue(channel.outstandingData.isEmpty());
    assertTrue("A later retry must not be suppressed", channel.reserve(4, "entry-0"));
    channel.cancel(4, "entry-0");
  }

  @Test
  public void cancelledAndClosedEventBatchesReleaseUpperBoundReservations() {
    ManualExecutor network = new ManualExecutor();
    AtomicBoolean closed = new AtomicBoolean();
    TRaftGroup.Channel channel =
        channel(new RaftConfig.Replication(), network, closed, new ArrayList<>());
    assertTrue(channel.reserve(100, "one"));
    List<TRaftGroup.Frame> frames =
        Collections.singletonList(new TRaftGroup.Frame(new byte[4], "one", 100));
    channel.cancelReserved(frames);
    assertEquals(0, channel.reservedBytes);
    assertTrue(channel.reserve(100, "one"));
    closed.set(true);
    channel.offerReserved(frames);
    assertEquals(0, channel.reservedBytes);
    assertTrue(channel.outstandingData.isEmpty());
    assertTrue(network.tasks.isEmpty());
  }

  @Test
  public void failedDeliveryAndRejectedSchedulingPermitProtocolRetry() {
    ManualExecutor network = new ManualExecutor();
    List<List<Integer>> delivered = new ArrayList<>();
    TRaftGroup.Channel channel =
        channel(new RaftConfig.Replication(), network, new AtomicBoolean(), delivered);
    network.rejectNext = true;
    channel.offerReserved(reserve(channel, 1));
    assertEquals(0, channel.reservedBytes);
    assertEquals(0, channel.active);
    channel.offerReserved(reserve(channel, 1));
    network.runNext();
    assertEquals(Collections.singletonList(Collections.singletonList(0)), delivered);
    TRaftGroup.Channel failing =
        new TRaftGroup.Channel(
            new TEndPoint("127.0.0.1", 1234),
            new RaftConfig.Replication(),
            network,
            () -> false,
            (endpoint, batch) -> {
              throw new java.io.IOException("injected");
            });
    failing.offerReserved(reserve(failing, 1));
    network.runNext();
    assertEquals(0, failing.reservedBytes);
    assertEquals(0, failing.active);
    assertTrue(failing.reserve(4, "entry-0"));
    failing.cancel(4, "entry-0");
  }

  private static Message message(
      MessageType type, int to, long term, EntryId id, Proposal proposal, Collection<EntryId> ids) {
    return new Message(type, 1, to, term, null, proposal, id, null, null, -1, ids);
  }

  @Test
  public void onlyIdenticalSameDrainControlsAreCoalesced() {
    EntryId id = new EntryId(1, 7);
    Message ack = message(MessageType.ENTRY_ACK, 2, 1, id, null, null);
    Message ackCopy = message(MessageType.ENTRY_ACK, 2, 1, id, null, null);
    Message otherPeer = message(MessageType.ENTRY_ACK, 3, 1, id, null, null);
    Message otherTerm = message(MessageType.ENTRY_ACK, 2, 2, id, null, null);
    Message otherKind = message(MessageType.TRANSFER_ACK, 2, 1, id, null, null);
    Message commit = message(MessageType.COMMIT, 2, 1, null, null, Collections.singletonList(id));
    List<Message> batch =
        new ArrayList<>(
            Arrays.asList(ack, ackCopy, otherPeer, otherTerm, otherKind, commit, commit));
    TRaftGroup.coalesceControlMessages(batch);
    assertEquals(Arrays.asList(ack, otherPeer, otherTerm, otherKind, commit, commit), batch);
    List<Message> retry = new ArrayList<>(Collections.singletonList(ackCopy));
    TRaftGroup.coalesceControlMessages(retry);
    assertEquals("Deduplication must not suppress a later retry", 1, retry.size());
  }

  @Test
  public void sameProposalIdentityWithChangedContentIsNotHidden() {
    Entry first = new Entry(new EntryId(1, 9), 0, 0, 0, Kind.NOOP, new byte[0]);
    Proposal proposal = new Proposal(null, Collections.emptyList(), first, false);
    Proposal changed =
        new Proposal(null, Collections.singletonList(new EntryId(1, 8)), first, false);
    Message original = message(MessageType.PROPOSAL, 2, 1, null, proposal, null);
    Message duplicate = message(MessageType.PROPOSAL, 2, 1, null, proposal, null);
    Message conflicting = message(MessageType.PROPOSAL, 2, 1, null, changed, null);
    Message confirm = message(MessageType.CONFIRM, 2, 1, null, proposal, null);
    List<Message> batch = new ArrayList<>(Arrays.asList(original, duplicate, conflicting, confirm));
    TRaftGroup.coalesceControlMessages(batch);
    assertEquals(Arrays.asList(original, conflicting, confirm), batch);
  }

  @Test
  public void proposalPayloadUsesTheDataBudgetAndPermitsRetryAfterDelivery() throws Exception {
    Entry first = new Entry(new EntryId(1, 9), 1, 10, 0, Kind.DATA, new byte[32]);
    Proposal proposal = new Proposal(null, Collections.emptyList(), Entry.metadata(first), false);
    Message payload =
        new Message(MessageType.PROPOSAL, 1, 2, 1, first, proposal, null, null, null, -1, null);
    Message metadata = message(MessageType.PROPOSAL, 2, 1, null, proposal, null);
    long budget = TRaftCodec.encodedMessageUpperBound(payload);
    RaftConfig.Replication limits = new RaftConfig.Replication();
    limits.maxInflightBytes = budget;
    ManualExecutor network = new ManualExecutor();
    List<List<Integer>> delivered = new ArrayList<>();
    TRaftGroup.Channel channel = channel(limits, network, new AtomicBoolean(), delivered);
    String key = TRaftGroup.dataKey(payload);
    assertNotNull(key);
    assertNull("Metadata retries do not own a data slot", TRaftGroup.dataKey(metadata));
    assertTrue(channel.reserve(budget, key));
    assertFalse(
        "Preparing the same first entry twice must not duplicate its payload",
        channel.reserve(budget, key));
    byte[] encoded = TRaftCodec.encodeMessage(payload);
    channel.offerReserved(Collections.singletonList(new TRaftGroup.Frame(encoded, key, budget)));
    assertEquals(encoded.length, channel.reservedBytes);
    assertFalse("Active RPCs remain covered by the byte budget", channel.reserve(budget, null));
    assertFalse("Active payload proposals remain deduplicated", channel.reserve(0, key));
    network.runNext();
    assertEquals(0, channel.reservedBytes);
    assertTrue(
        "Protocol retry remains possible after the transport attempt",
        channel.reserve(budget, key));
    channel.cancel(budget, key);
  }

  @Test
  public void piggybackUsesExistingWireFieldsWithoutControlCoalescing() throws Exception {
    Entry first = new Entry(new EntryId(3, 9), 1, -2, 0, Kind.DATA, new byte[] {1, 2, 3});
    Proposal proposal = new Proposal(null, Collections.emptyList(), Entry.metadata(first), false);
    Message payload =
        new Message(MessageType.PROPOSAL, 1, 2, 3, first, proposal, null, null, null, -1, null);
    Message metadata = message(MessageType.PROPOSAL, 2, 3, null, proposal, null);
    List<Message> batch = new ArrayList<>(Arrays.asList(payload, payload, metadata, metadata));
    TRaftGroup.coalesceControlMessages(batch);
    assertEquals(Arrays.asList(payload, payload, metadata), batch);
    Message decoded = TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(payload));
    assertEquals(MessageType.PROPOSAL, decoded.type);
    assertEquals(first, decoded.entry);
    assertArrayEquals(first.payload(), decoded.entry.payload());
    assertEquals(proposal, decoded.proposal);
    assertFalse("The proposal itself stays metadata-only", decoded.proposal.first.hasPayload());
    Message oldPath = TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(metadata));
    assertNull(oldPath.entry);
    assertEquals(proposal, oldPath.proposal);
  }

  @Test
  public void commitNoticeOnlyBatchesWaitForDataOrAnExplicitFlush() {
    RaftConfig.Replication limits = new RaftConfig.Replication();
    limits.maxEntriesPerAppend = 64;
    ManualExecutor network = new ManualExecutor();
    List<List<Integer>> delivered = new ArrayList<>();
    TRaftGroup.Channel channel = channel(limits, network, new AtomicBoolean(), delivered);
    for (int i = 0; i < 2; i++) {
      assertTrue(channel.reserve(4, null));
      channel.offerReserved(
          Collections.singletonList(
              new TRaftGroup.Frame(ByteBuffer.allocate(4).putInt(i).array(), null, 4, true)),
          true);
    }
    assertTrue("Notices alone never schedule an RPC", network.tasks.isEmpty());
    assertEquals(2, channel.deferredFrames());
    assertTrue(channel.reserve(4, "entry-2"));
    channel.offerReserved(
        Collections.singletonList(
            new TRaftGroup.Frame(ByteBuffer.allocate(4).putInt(2).array(), "entry-2", 4, false)),
        true);
    assertEquals(0, channel.deferredFrames());
    network.runNext();
    assertEquals(
        "Held notices leave ahead of the data frame that released them",
        Collections.singletonList(Arrays.asList(0, 1, 2)),
        delivered);
    assertTrue(channel.reserve(4, null));
    channel.offerReserved(
        Collections.singletonList(
            new TRaftGroup.Frame(ByteBuffer.allocate(4).putInt(3).array(), null, 4, true)),
        true);
    assertTrue(network.tasks.isEmpty());
    channel.flushDeferred();
    network.runNext();
    assertEquals(Arrays.asList(Arrays.asList(0, 1, 2), Collections.singletonList(3)), delivered);
    assertTrue(channel.reserve(4, null));
    channel.offerReserved(
        Collections.singletonList(
            new TRaftGroup.Frame(ByteBuffer.allocate(4).putInt(4).array(), null, 4, true)),
        false);
    assertEquals("Urgent notices dispatch immediately", 1, network.tasks.size());
    network.runNext();
    assertEquals(3, delivered.size());
    assertEquals(0, channel.reservedBytes);
  }
}
