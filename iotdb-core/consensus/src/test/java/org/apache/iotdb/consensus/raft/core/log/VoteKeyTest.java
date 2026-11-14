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

package org.apache.iotdb.consensus.raft.core.log;

import org.apache.iotdb.consensus.raft.core.election.RaftVoteKeyPolicy;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class VoteKeyTest {

  @Test
  public void raftComparesTermThenIndex() {
    assertTrue(VoteKey.compareRaft(VoteKey.raft(2, 0), VoteKey.raft(1, 9)) > 0);
    assertTrue(VoteKey.compareRaft(VoteKey.raft(2, 3), VoteKey.raft(2, 4)) < 0);
    assertEquals(0, VoteKey.compareRaft(VoteKey.raft(2, 4), VoteKey.raft(2, 4)));
  }

  @Test
  public void tripleComparesTermRhoSigma() {
    assertTrue(VoteKey.compareTriple(VoteKey.triple(3, 0, 0, 0), VoteKey.triple(2, 5, 5, 5)) > 0);
    assertTrue(VoteKey.compareTriple(VoteKey.triple(2, 9, 1, 0), VoteKey.triple(2, 3, 0, 9)) > 0);
    assertTrue(VoteKey.compareTriple(VoteKey.triple(2, 9, 1, 2), VoteKey.triple(2, 3, 1, 3)) < 0);
  }

  @Test
  public void raftPolicyUsesDurableContiguousEnd() {
    LogIndex log = new LogIndex();
    log.put(new LogEntry(0, 1, EntryType.DATA, EntryMeta.EMPTY, ByteBuffer.allocate(0)), true);
    log.put(new LogEntry(1, 2, EntryType.DATA, EntryMeta.EMPTY, ByteBuffer.allocate(0)), false);
    RaftVoteKeyPolicy p = new RaftVoteKeyPolicy();
    VoteKey k = p.extract(log, SnapshotMetaExt.NONE, -1, Mutations.NONE, false);
    assertEquals(1, k.lastTerm);
    assertEquals(0, k.lastIndex);
    log.markDurable(1);
    k = p.extract(log, SnapshotMetaExt.NONE, -1, Mutations.NONE, false);
    assertEquals(2, k.lastTerm);
    assertEquals(1, k.lastIndex);
    VoteKey empty = p.extract(new LogIndex(), SnapshotMetaExt.NONE, -1, Mutations.NONE, false);
    assertEquals(-1, empty.lastIndex);
    assertEquals(0, empty.lastTerm);
  }
}
