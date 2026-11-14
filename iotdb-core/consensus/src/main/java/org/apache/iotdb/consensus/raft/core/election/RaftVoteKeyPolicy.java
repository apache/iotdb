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

package org.apache.iotdb.consensus.raft.core.election;

import org.apache.iotdb.consensus.raft.core.log.LogView;
import org.apache.iotdb.consensus.raft.core.log.VoteKey;
import org.apache.iotdb.consensus.raft.core.policy.Mutations;
import org.apache.iotdb.consensus.raft.core.profile.SnapshotMetaExt;

/** Classic Raft: (lastTerm, lastIndex) of the contiguous durable log. */
public final class RaftVoteKeyPolicy implements VoteKeyPolicy {

  @Override
  public VoteKey extract(
      LogView log, SnapshotMetaExt ext, long commitBound, Mutations mut, boolean restarted) {
    long last = log.durableContiguousEnd();
    long term = last >= log.firstIndex() ? log.termAt(last) : log.snapshotTerm();
    return VoteKey.raft(term, last);
  }

  @Override
  public int compare(VoteKey candidate, VoteKey mine, Mutations mut) {
    return VoteKey.compareRaft(candidate, mine);
  }
}
