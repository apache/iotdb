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
import org.apache.iotdb.consensus.raft.core.listener.ProtocolListener;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.log.VoteKey;
import org.apache.iotdb.consensus.raft.core.msg.AckStatus;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesReq;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteReq;
import org.apache.iotdb.consensus.raft.core.msg.RequestVoteRes;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.policy.ProtocolPolicy;
import org.apache.iotdb.consensus.raft.core.profile.ProtocolProfile;
import org.apache.iotdb.consensus.raft.core.profile.RaftProfile;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Deterministic tests at the persistence boundary, without a simulated protocol implementation. */
public class RaftDurabilitySafetyTest {
  private static RaftCore follower(LogIndex log) {
    return follower(log, new RaftProfile());
  }

  private static RaftCore follower(LogIndex log, ProtocolProfile profile) {
    return new RaftCore(
        2,
        Membership.ofVoters(Arrays.asList(1, 2, 3)),
        log,
        1,
        -1,
        -1,
        false,
        profile,
        ProtocolPolicy.production(),
        RaftCoreConfig.forTests(),
        ProtocolListener.NOOP,
        FaultInjector.NONE,
        RandomSource.ZERO,
        Mutations.NONE);
  }

  private static LogEntry entry(long index, long term) {
    return new LogEntry(
        index, term, EntryType.DATA, EntryMeta.EMPTY, ByteBuffer.wrap(new byte[] {42}));
  }

  private static AppendEntriesReq append(long prev, List<LogEntry> entries, long commit) {
    return new AppendEntriesReq(
        1, 1, prev, prev < 0 ? 0 : 1, entries, commit, commit < 0 ? 0 : 1, false, false, 17, 0);
  }

  private static <T> T first(List<Action> actions, Class<T> type) {
    for (Action action : actions) {
      if (type.isInstance(action)) {
        return type.cast(action);
      }
    }
    return null;
  }

  @Test
  public void voteIsRepliedOnlyAfterPersistenceAndCannotBeCastTwice() {
    RaftCore core = follower(new LogIndex());
    List<Action> actions =
        core.onEvent(
            new Event.Inbound(1, new RequestVoteReq(1, 1, VoteKey.raft(0, -1), false), null));
    assertNull(first(actions, Action.Reply.class));
    Action.PersistMeta vote = first(actions, Action.PersistMeta.class);
    assertNotNull(vote);
    RequestVoteRes granted =
        (RequestVoteRes)
            first(core.onEvent(new Event.MetaDurable(vote.token)), Action.Reply.class).res;
    assertTrue(granted.granted);
    RequestVoteRes rejected =
        (RequestVoteRes)
            first(
                    core.onEvent(
                        new Event.Inbound(
                            3, new RequestVoteReq(1, 3, VoteKey.raft(0, -1), false), null)),
                    Action.Reply.class)
                .res;
    assertFalse(rejected.granted);
  }

  @Test
  public void higherTermCandidateStillNeedsAnUpToDateLog() {
    LogIndex log = new LogIndex();
    log.put(entry(0, 1), true);
    RaftCore core = follower(log);
    List<Action> actions =
        core.onEvent(
            new Event.Inbound(3, new RequestVoteReq(2, 3, VoteKey.raft(0, -1), false), null));
    assertNull(first(actions, Action.Reply.class));
    Action.PersistMeta term = first(actions, Action.PersistMeta.class);
    RequestVoteRes rejected =
        (RequestVoteRes)
            first(core.onEvent(new Event.MetaDurable(term.token)), Action.Reply.class).res;
    assertFalse(rejected.granted);
    assertEquals(2, rejected.term);
  }

  @Test
  public void followerAcknowledgesAndAppliesOnlyAfterFsync() {
    RaftCore core = follower(new LogIndex());
    List<Action> actions =
        core.onEvent(
            new Event.Inbound(1, append(-1, Collections.singletonList(entry(0, 1)), 0), null));
    assertNull(first(actions, Action.Reply.class));
    assertNull(first(actions, Action.Apply.class));
    Action.Persist write = first(actions, Action.Persist.class);
    List<Action> durable = core.onEvent(new Event.Durable(write.token));
    AppendEntriesRes response = (AppendEntriesRes) first(durable, Action.Reply.class).res;
    assertEquals(AckStatus.ACK, response.acks.get(0).status);
    assertNotNull(first(durable, Action.Apply.class));
  }

  @Test
  public void missingPredecessorIsRejectedWithoutPersisting() {
    RaftCore core = follower(new LogIndex());
    List<Action> actions =
        core.onEvent(
            new Event.Inbound(1, append(0, Collections.singletonList(entry(1, 1)), -1), null));
    assertNull(first(actions, Action.Persist.class));
    AppendEntriesRes response = (AppendEntriesRes) first(actions, Action.Reply.class).res;
    assertEquals(AckStatus.REJECT, response.acks.get(0).status);
  }

  @Test
  public void oldTermFsyncCannotApplyItsCommitNoticeAfterTermChanges() {
    RaftCore core = follower(new LogIndex());
    Action.Persist write =
        first(
            core.onEvent(
                new Event.Inbound(1, append(-1, Collections.singletonList(entry(0, 1)), 0), null)),
            Action.Persist.class);
    core.onEvent(new Event.Inbound(3, new RequestVoteReq(2, 3, VoteKey.raft(1, 0), false), null));
    List<Action> durable = core.onEvent(new Event.Durable(write.token));
    assertNull(first(durable, Action.Apply.class));
    AppendEntriesRes response = (AppendEntriesRes) first(durable, Action.Reply.class).res;
    assertEquals(2, response.term);
    assertTrue(response.acks.isEmpty());
  }








}
