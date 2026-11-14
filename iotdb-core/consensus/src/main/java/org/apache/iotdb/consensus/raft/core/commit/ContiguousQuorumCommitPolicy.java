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
import org.apache.iotdb.consensus.raft.core.log.EntryMeta;
import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.replication.Replicator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The single commit rule shared by Raft and TRaft: scan the contiguous quorum-acked prefix from
 * {@code commitIndex + 1}, stop at the last entry of the current term (Raft Fig. 8), then apply the
 * decorator. {@link Replicator#isAcked} abstracts Raft's prefix-closed matchIndex and TRaft's
 * per-index ackset; the leader counts itself only while it is a voter (Raft thesis §4.2.2).
 *
 * <p>The {@code commit_on_quorum} / {@code commit_ignore_barrier} / {@code quorum_excludes_leader}
 * reference mutations are reproduced here so the module E cases port unchanged.
 */
public final class ContiguousQuorumCommitPolicy implements CommitPolicy {

  /** The policy is stateless: every kernel may share this instance. */
  public static final ContiguousQuorumCommitPolicy INSTANCE = new ContiguousQuorumCommitPolicy();

  @Override
  public List<Long> advance(
      int self,
      long currentTerm,
      long commitIndex,
      LogView log,
      Replicator replicator,
      CommitDecorator decorator,
      Membership membership,
      Mutations mut) {
    List<Long> out = new ArrayList<>();
    if (mut.has("commit_on_quorum")) {
      for (long i : log.slots()) {
        if (i > commitIndex && quorumAcked(self, i, log, replicator, membership, mut)) {
          out.add(i);
        }
      }
      return out;
    }
    if (mut.has("commit_ignore_barrier")) {
      Map<Long, List<Long>> byRho = new TreeMap<>();
      for (long i : log.slots()) {
        byRho.computeIfAbsent(rhoOf(log, i), k -> new ArrayList<>()).add(i);
      }
      for (List<Long> idxs : byRho.values()) {
        for (long i : idxs) {
          if (i <= commitIndex) {
            continue;
          }
          if (quorumAcked(self, i, log, replicator, membership, mut)) {
            out.add(i);
          } else {
            break;
          }
        }
      }
      return out;
    }
    long q = commitIndex;
    while (log.has(q + 1)
        && log.isDurable(q + 1)
        && quorumAcked(self, q + 1, log, replicator, membership, mut)) {
      q++;
    }
    long limit = commitIndex;
    for (long i = q; i > commitIndex; i--) {
      if (log.termAt(i) == currentTerm) {
        limit = i;
        break;
      }
    }
    limit = decorator.restrict(limit, commitIndex, log);
    for (long i = commitIndex + 1; i <= limit; i++) {
      out.add(i);
    }
    return out;
  }

  private static long rhoOf(LogView log, long index) {
    EntryMeta m = log.metaAt(index);
    return m instanceof EntryMeta.Partitioned ? ((EntryMeta.Partitioned) m).rho() : 0;
  }

  static boolean quorumAcked(
      int self,
      long index,
      LogView log,
      Replicator replicator,
      Membership membership,
      Mutations mut) {
    int acked = 0;
    for (int v : membership.voters()) {
      if (v == self) {
        if (!mut.has("quorum_excludes_leader") && log.isDurable(index)) {
          acked++;
        }
      } else if (replicator.isAcked(index, v)) {
        acked++;
      }
    }
    return acked >= membership.majority();
  }
}
