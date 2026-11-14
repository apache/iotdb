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

package org.apache.iotdb.consensus.raft.core.replication;

import java.util.Map;
import java.util.Set;

/**
 * Read-only introspection of a leader's replication bookkeeping, consumed by the invariant checker
 * (LIVE-1) and by status RPCs. Sets are snapshots or unmodifiable views.
 */
public interface LeaderStateView {

  /** index -> followers that have not yet durably received it. */
  Map<Long, Set<Integer>> pendingFollowers();

  /**
   * {@code pendingFollowers().size()}, the status RPC's {@code pendingSize}; implementations
   * override it with a computation that builds no map (the status is computed on the event thread).
   */
  default int pendingCount() {
    return pendingFollowers().size();
  }

  /** Nodes (including the leader) that durably hold {@code index}. */
  Set<Integer> rcv(long index);

  /** Nodes whose contiguous frontier covers {@code index}. */
  Set<Integer> ackset(long index);
}
