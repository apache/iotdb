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

package org.apache.iotdb.consensus.raft.core;

import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.event.Event;
import org.apache.iotdb.consensus.raft.core.ingest.RequestMeta;
import org.apache.iotdb.consensus.raft.core.listener.ProtocolListener;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.log.VoteKey;
import org.apache.iotdb.consensus.raft.core.msg.AckStatus;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteReq;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteRes;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.policy.ProtocolPolicy;
import org.apache.iotdb.consensus.raft.core.profile.RaftProfile;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The Durable of a deposed leader's write says nothing about the copy that replaced its entry in
 * the meantime: that copy's own write is still queued behind it. Marked durable, it would be
 * acknowledged and applied before it is on disk (the store still holds the old copy).
 */
public class DurableOfAReplacedCopyTest {

  private static <T> T first(List<Action> actions, Class<T> type) {
    for (Action a : actions) {
      if (type.isInstance(a)) {
        return type.cast(a);
      }
    }
    return null;
  }

  private static LogEntry entry(long index, long term) {
    return new LogEntry(
        index, term, EntryType.DATA, EntryMeta.EMPTY, ByteBuffer.wrap(new byte[] {42}));
  }

  @Test
  public void theDurableOfADeposedLeadersWriteDoesNotMarkTheNewCopy() {
    LogIndex log = new LogIndex();
    log.put(entry(0, 1), true);
    RaftCore core =
        new RaftCore(
            1,
            Membership.ofVoters(Arrays.asList(1, 2, 3)),
            log,
            1,
            -1,
            -1,
            false,
            new RaftProfile(),
            ProtocolPolicy.production(),
            RaftCoreConfig.forTests(),
            ProtocolListener.NOOP,
            FaultInjector.NONE,
            RandomSource.ZERO,
            Mutations.NONE);
    // N1 leads term 2
    core.onEvent(new Event.ElectionTimeout());
    List<Action> a = core.onEvent(new Event.Inbound(2, new RequestVoteRes(2, true, true), null));
    core.onEvent(new Event.MetaDurable(first(a, Action.PersistMeta.class).token));
    a = core.onEvent(new Event.Inbound(2, new RequestVoteRes(2, true, false), null));
    core.onEvent(new Event.Durable(first(a, Action.Persist.class).token));
    assertEquals(Role.LEADER, core.role());
    // a client write: stored, its write queued
    a =
        core.onEvent(
            new Event.ClientWrite(7, ByteBuffer.wrap(new byte[] {1}), RequestMeta.timed(1L), null));
    Action.Persist oldWrite = first(a, Action.Persist.class);
    long i = oldWrite.entries.get(0).index;
    assertFalse(log.isDurable(i));
    // deposed in memory; the new leader's copy of the slot replaces it, its write queued as well
    core.onEvent(new Event.Inbound(2, new RequestVoteReq(3, 2, VoteKey.raft(1, 1), false), null));
    assertEquals(Role.FOLLOWER, core.role());
    AppendEntriesReq ae =
        new AppendEntriesReq(
            3, 2, i - 1, 2, Collections.singletonList(entry(i, 3)), i, 3, false, false, 17, 0);
    a = core.onEvent(new Event.Inbound(2, ae, null));
    assertNotNull(first(a, Action.Persist.class));
    assertEquals(3, log.termAt(i));
    // the old write gets durable: that is not the slot's copy
    core.onEvent(new Event.Durable(oldWrite.token));
    assertFalse(log.isDurable(i));
    assertTrue(log.durableContiguousEnd() < i);
    // a retransmission of the new copy: neither acknowledged nor applied yet
    AppendEntriesReq dup =
        new AppendEntriesReq(
            3, 2, i - 1, 2, Collections.singletonList(entry(i, 3)), i, 3, false, false, 18, 0);
    a = core.onEvent(new Event.Inbound(2, dup, null));
    Action.Apply apply = first(a, Action.Apply.class);
    assertTrue(apply == null || apply.toInclusive < i);
    Action.Reply reply = first(a, Action.Reply.class);
    if (reply != null) {
      for (EntryAck ack : ((AppendEntriesRes) reply.res).acks) {
        assertFalse(ack.toString(), ack.status == AckStatus.ACK && ack.index == i);
      }
    }
  }
}
