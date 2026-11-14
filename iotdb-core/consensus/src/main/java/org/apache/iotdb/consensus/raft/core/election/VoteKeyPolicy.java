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

/** Strategy point: the up-to-date comparison key of an election (Algorithm 1 for TRaft). */
public interface VoteKeyPolicy {

  /**
   * Extract this node's key from its durable log. {@code commitBound} is the index the node knows
   * to be committed (for TRaft's PAIR mode); {@code restarted} lets the reference mutation {@code
   * rebuild_frontier_by_count} apply only after a restart.
   */
  VoteKey extract(
      LogView log, SnapshotMetaExt ext, long commitBound, Mutations mut, boolean restarted);

  /** Negative if {@code candidate} is less up to date than {@code mine}. */
  int compare(VoteKey candidate, VoteKey mine, Mutations mut);
}
