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

package org.apache.iotdb.consensus.raft.core.commit;

import org.apache.iotdb.consensus.raft.core.Membership;
import org.apache.iotdb.consensus.raft.core.action.Action;
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.EntryType;
import org.apache.iotdb.consensus.raft.core.log.LogEntry;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.msg.AppendEntriesRes;
import org.apache.iotdb.consensus.raft.core.msg.EntryAck;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;
import org.apache.iotdb.consensus.raft.core.replication.FollowerProgress;
import org.apache.iotdb.consensus.raft.core.replication.Replicator;
import org.apache.iotdb.consensus.raft.core.replication.SchedulerContext;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ContiguousQuorumCommitPolicyTest {

  /** A replicator whose acks are set directly by the test. */
  private static final class StubReplicator implements Replicator {
    final Map<Long, Set<Integer>> acks = new HashMap<>();

    void ack(long index, int... followers) {
      Set<Integer> s = acks.computeIfAbsent(index, k -> new HashSet<>());
      for (int f : followers) {
        s.add(f);
      }
    }

    @Override
    public boolean isAcked(long index, int follower) {
      return acks.getOrDefault(index, Collections.emptySet()).contains(follower);
    }

    @Override
    public boolean isDurableAt(long index, int follower) {
      return isAcked(index, follower);
    }

    @Override
    public void onBecomeLeader(LogView log, Membership m, long ci, long la) {}

    @Override
    public List<Action> onEntriesDurable(List<LogEntry> entries, SchedulerContext ctx) {
      return Collections.emptyList();
    }

    @Override
    public List<Action> schedule(SchedulerContext ctx) {
      return Collections.emptyList();
    }

    @Override
    public List<Action> onAck(int from, EntryAck ack, AppendEntriesRes res, SchedulerContext ctx) {
      return Collections.emptyList();
    }

    @Override
    public void onTruncated(int from, long truncatedFrom, SchedulerContext ctx) {}

    @Override
    public void onAppliedReported(int from, long appliedIndex, SchedulerContext ctx) {}

    @Override
    public void onRpcFailed(int to, long[] indexes, SchedulerContext ctx) {}

    @Override
    public boolean hasWork(SchedulerContext ctx) {
      return false;
    }

    @Override
    public void onCommitted(long index, LogView log) {}

    @Override
    public void onMembershipChanged(Membership membership, LogView log) {}

    @Override
    public void onSnapshotSent(int follower, long lastIncludedIndex, SnapshotMetaExt ext) {}

    @Override
    public FollowerProgress progress(int follower) {
      return null;
    }

    @Override
    public Collection<? extends FollowerProgress> progressAll() {
      return Collections.emptyList();
    }
  }

  private static LogIndex log(long... terms) {
    LogIndex log = new LogIndex();
    for (int i = 0; i < terms.length; i++) {
      log.put(
          new LogEntry(i, terms[i], EntryType.DATA, EntryMeta.EMPTY, ByteBuffer.allocate(0)), true);
    }
    return log;
  }

  private static final Membership THREE = Membership.ofVoters(Arrays.asList(1, 2, 3));
  private final ContiguousQuorumCommitPolicy policy = new ContiguousQuorumCommitPolicy();

  @Test
  public void commitsTheContiguousQuorumPrefixOnly() {
    LogIndex log = log(2, 2, 2, 2);
    StubReplicator r = new StubReplicator();
    r.ack(0, 2);
    r.ack(1, 3);
    r.ack(3, 2); // e2 not acked by anyone: e3 cannot be committed
    assertEquals(
        Arrays.asList(0L, 1L),
        policy.advance(1, 2, -1, log, r, CommitDecorator.NONE, THREE, Mutations.NONE));
  }

  @Test
  public void currentTermRuleStopsAtOldTermEntries() {
    LogIndex log = log(1, 1, 2);
    StubReplicator r = new StubReplicator();
    r.ack(0, 2);
    r.ack(1, 2);
    // only old-term entries are quorum-acked: nothing commits (Raft Fig. 8)
    assertEquals(
        Collections.emptyList(),
        policy.advance(1, 2, -1, log, r, CommitDecorator.NONE, THREE, Mutations.NONE));
    r.ack(2, 3);
    // the current-term entry is acked: the whole prefix commits
    assertEquals(
        Arrays.asList(0L, 1L, 2L),
        policy.advance(1, 2, -1, log, r, CommitDecorator.NONE, THREE, Mutations.NONE));
  }

  @Test
  public void leaderCountsItselfOnlyWhileVoter() {
    LogIndex log = log(2, 2);
    StubReplicator r = new StubReplicator();
    r.ack(0, 2);
    assertEquals(
        Arrays.asList(0L),
        policy.advance(1, 2, -1, log, r, CommitDecorator.NONE, THREE, Mutations.NONE));
    Membership without = THREE.without(1, 5); // leader removed itself: voters {2,3}, majority 2
    assertEquals(
        Collections.emptyList(),
        policy.advance(1, 2, -1, log, r, CommitDecorator.NONE, without, Mutations.NONE));
    r.ack(0, 3);
    assertEquals(
        Arrays.asList(0L),
        policy.advance(1, 2, -1, log, r, CommitDecorator.NONE, without, Mutations.NONE));
  }

  @Test
  public void decoratorCanHoldBackTheCandidate() {
    LogIndex log = log(2, 2, 2);
    StubReplicator r = new StubReplicator();
    r.ack(0, 2);
    r.ack(1, 2);
    r.ack(2, 2);
    CommitDecorator holdLast = (candidate, commitIndex, view) -> Math.min(candidate, 1);
    assertEquals(
        Arrays.asList(0L, 1L), policy.advance(1, 2, -1, log, r, holdLast, THREE, Mutations.NONE));
  }

  @Test
  public void referenceMutationsProduceTheWrongShapes() {
    LogIndex log = log(2, 2, 2);
    StubReplicator r = new StubReplicator();
    r.ack(2, 2); // only the last entry is quorum-acked
    assertEquals(
        Collections.emptyList(),
        policy.advance(1, 2, -1, log, r, CommitDecorator.NONE, THREE, Mutations.NONE));
    // BUG shape (E-2): quorum == commit, canonical order ignored
    assertEquals(
        Arrays.asList(2L),
        policy.advance(
            1, 2, -1, log, r, CommitDecorator.NONE, THREE, Mutations.of("commit_on_quorum")));
    // BUG shape (E-8): the leader's own durable copy is not counted
    r.ack(0, 2);
    assertEquals(
        Arrays.asList(0L),
        policy.advance(1, 2, -1, log, r, CommitDecorator.NONE, THREE, Mutations.NONE));
    assertEquals(
        Collections.emptyList(),
        policy.advance(
            1, 2, -1, log, r, CommitDecorator.NONE, THREE, Mutations.of("quorum_excludes_leader")));
  }
}
