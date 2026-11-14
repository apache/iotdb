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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Volatile kernel state (the persistent mirrors currentTerm/votedFor live here too). */
public final class RaftState {

  public Role role = Role.FOLLOWER;
  public long currentTerm;
  public int votedFor = -1;
  public int leaderId = -1;
  public long commitIndex = -1;
  public long lastApplied = -1;

  /** Highest index for which an Apply action has already been issued. */
  public long applyIssuedUpTo = -1;

  /** Follower: max leaderCommit learned (Python known_commit). */
  public long knownLeaderCommit = -1;

  public final Set<Integer> votes = new TreeSet<>();
  public final Set<Integer> preVotes = new TreeSet<>();
  public Membership membership;
  public long leaderFirstIndexOfTerm = -1;
  public boolean leaderReady;

  /**
   * Leader recovering its predecessors' log (capability leaderRecovery): writes, membership changes
   * and transfers are refused and the election entry is deferred until recovery ends.
   */
  public boolean recovering;

  public long lastQuorumContactNanos;

  /**
   * Leader: request id -> index of the client writes of this tenure that are not acknowledged yet
   * (Python my_reqs without the acknowledged ones), in write order: those still uncommitted when
   * the tenure ends are completed with NOT_LEADER.
   */
  public final Map<Long, Long> myReqs = new LinkedHashMap<>();

  /**
   * Leader: index -> request id of the writes in {@link #myReqs}, so that the client of a committed
   * index is found in O(1). An entry is removed once its client is acknowledged.
   */
  public final Map<Long, Long> reqByIndex = new HashMap<>();

  /**
   * Leader: committed indexes this tenure (Python committed). Under out-of-order commit only the
   * committed indexes beyond {@link #commitIndex} (the contiguous committed prefix).
   */
  public final TreeSet<Long> committed = new TreeSet<>();

  /** TRaft follower: term of the leader we completed the fence handshake with (-1 = none). */
  public long fencedTerm = -1;

  public boolean restarted;

  public RaftState(Membership membership) {
    this.membership = membership;
  }

  public boolean isLeader() {
    return role == Role.LEADER;
  }
}
