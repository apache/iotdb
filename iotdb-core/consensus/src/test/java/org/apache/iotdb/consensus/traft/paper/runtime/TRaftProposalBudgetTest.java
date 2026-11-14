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
import org.apache.iotdb.consensus.traft.paper.core.TRaftEngine;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableRecord;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.DurableStore;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Entry;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Kind;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Message;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.MessageType;
import org.apache.iotdb.consensus.traft.paper.core.TRaftProtocol.Role;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Exercises the real runtime reservation branch with deterministic protocol delivery. */
public class TRaftProposalBudgetTest {
  private static final long BYTE_LIMIT = 4096;

  private static final class Fixture {
    final Map<Integer, TRaftEngine> nodes = new LinkedHashMap<>();
    final List<Message> delivered = new ArrayList<>();
    final TRaftEngine leader;
    final Entry boundary;
    final Message firstAttempt;

    Fixture() throws IOException {
      Set<Integer> members = new HashSet<>(Arrays.asList(1, 2, 3));
      for (int id : members) {
        final List<DurableRecord> journal = new ArrayList<>();
        DurableStore store =
            new DurableStore() {
              @Override
              public List<DurableRecord> load() {
                return new ArrayList<>(journal);
              }

              @Override
              public void append(DurableRecord record) {
                journal.add(record);
              }
            };
        TRaftEngine engine = new TRaftEngine(id, members, 1000, 0, store, entry -> {});
        engine.setReplicationWindow(1);
        engine.setProposalPayloadBudget(BYTE_LIMIT);
        nodes.put(id, engine);
      }
      leader = nodes.get(1);
      leader.startElection();
      pump();
      assertEquals(Role.LEADER, leader.role());
      leader.append(Kind.DATA, 10, new byte[] {1}, 0);
      pump();
      boundary = leader.append(Kind.DATA, 0, new byte[1024], 0);
      // Drop this complete first delivery. Only the leader's retry intent remains.
      firstAttempt =
          leader.drainMessages().stream()
              .filter(message -> message.to == 2 && message.type == MessageType.PROPOSAL)
              .findFirst()
              .get();
      assertNotNull(firstAttempt.entry);
      assertEquals(1, leader.inFlightEntries(2));
    }

    void pump() throws IOException {
      Deque<Message> queue = new ArrayDeque<>();
      int budget = 10000;
      for (; ; ) {
        for (TRaftEngine node : nodes.values()) {
          queue.addAll(node.drainMessages());
        }
        if (queue.isEmpty()) {
          return;
        }
        assertTrue("Protocol must make bounded progress", budget-- > 0);
        Message message = queue.removeFirst();
        delivered.add(message);
        nodes.get(message.to).receive(TRaftCodec.decodeMessage(TRaftCodec.encodeMessage(message)));
      }
    }
  }

  private static TRaftGroup.Channel channel(AtomicBoolean closed) {
    RaftConfig.Replication limits = new RaftConfig.Replication();
    limits.maxInflightBytes = BYTE_LIMIT;
    return new TRaftGroup.Channel(
        new TEndPoint("127.0.0.1", 1234),
        limits,
        Runnable::run,
        closed::get,
        (endpoint, batch) -> Collections.emptyList());
  }

  private static void assertBudgetFallback(boolean controlsOccupyBytes) throws Exception {
    Fixture fixture = new Fixture();
    TRaftGroup.Channel channel = channel(new AtomicBoolean());
    fixture.leader.resend();
    Message retry =
        fixture.leader.drainMessages().stream()
            .filter(
                message ->
                    message.to == 2
                        && message.type == MessageType.PROPOSAL
                        && message.entry != null)
            .findFirst()
            .get();
    long frameBytes = TRaftCodec.encodedMessageUpperBound(retry);
    long controls = controlsOccupyBytes ? BYTE_LIMIT - frameBytes + 1 : 0;
    assertTrue(frameBytes < BYTE_LIMIT);
    assertTrue(channel.reserve(controls, null));
    assertFalse(
        TRaftGroup.reserveFrame(
            channel,
            fixture.leader,
            retry,
            frameBytes,
            TRaftGroup.dataKey(retry),
            controlsOccupyBytes));
    assertEquals("Only control reservations remain", controls, channel.reservedBytes);
    assertEquals(
        "The rejected carried-first must free the one-entry repair window",
        0,
        fixture.leader.inFlightEntries(2));
    assertFalse(
        "Budget fallback is not a prefix quorum acknowledgement",
        fixture.leader.isCommitted(fixture.boundary.id));
    List<Message> fallback = fixture.leader.drainMessages();
    assertTrue(
        fallback.stream()
            .anyMatch(
                message ->
                    message.to == 2
                        && message.type == MessageType.PROPOSAL
                        && message.entry == null));
    for (Message message : fallback) {
      if (message.to == 2 && message.type == MessageType.PROPOSAL) {
        long metadataBytes = TRaftCodec.encodedMessageUpperBound(message);
        assertTrue(
            "Metadata still fits beside this round's controls",
            channel.reserve(metadataBytes, null));
        channel.cancel(metadataBytes, null);
        fixture.nodes.get(2).receive(message);
      }
    }
    channel.cancel(controls, null);
    assertFalse(
        "A stale copy from the same drain must not bypass the released engine window",
        TRaftGroup.reserveFrame(
            channel, fixture.leader, retry, frameBytes, TRaftGroup.dataKey(retry), true));
    assertEquals(0, channel.reservedBytes);
    fixture.leader.resend();
    fixture.pump();
    assertTrue(
        fixture.delivered.stream()
            .anyMatch(
                message ->
                    message.from == 2
                        && message.type == MessageType.FETCH_ENTRY
                        && fixture.boundary.id.equals(message.reference)));
    for (TRaftEngine node : fixture.nodes.values()) {
      assertTrue(node.isApplied(fixture.boundary.id));
    }
  }

  @Test
  public void lostFirstFrameFallsBackWhenSameDrainControlsConsumeTheRemainingBudget()
      throws Exception {
    assertBudgetFallback(true);
  }

  @Test
  public void exhaustedRoundPayloadBudgetAlsoReleasesTheCarriedFirstWindow() throws Exception {
    assertBudgetFallback(false);
  }

  @Test
  public void duplicateOrClosedChannelDoesNotDowngradeTheProtocolAttempt() throws Exception {
    Fixture fixture = new Fixture();
    AtomicBoolean closed = new AtomicBoolean();
    TRaftGroup.Channel channel = channel(closed);
    Message message = fixture.firstAttempt;
    long bytes = TRaftCodec.encodedMessageUpperBound(message);
    String key = TRaftGroup.dataKey(message);
    assertTrue(channel.reserve(bytes, key));
    assertFalse(TRaftGroup.reserveFrame(channel, fixture.leader, message, bytes, key, false));
    assertEquals(
        "An active duplicate is not a byte-budget fallback", 1, fixture.leader.inFlightEntries(2));
    channel.cancel(bytes, key);
    closed.set(true);
    assertFalse(TRaftGroup.reserveFrame(channel, fixture.leader, message, bytes, key, true));
    assertEquals(1, fixture.leader.inFlightEntries(2));
    fixture.leader.resend();
    assertTrue(
        fixture.leader.drainMessages().stream()
            .anyMatch(
                retry ->
                    retry.to == 2 && retry.type == MessageType.PROPOSAL && retry.entry != null));
  }
}
