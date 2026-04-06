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

package org.apache.iotdb.consensus.traft;

/**
 * Snapshot transfer payload.
 *
 * <p>Besides the standard Raft snapshot boundary, TRaft also ships time-partition metadata so a
 * follower can resume partition assignment consistently after compaction.
 */
class TRaftInstallSnapshotRequest {

  private final int leaderId;
  private final long term;
  private final long lastIncludedIndex;
  private final long lastIncludedTerm;
  private final long historicalMaxTimestamp;
  private final long lastPartitionIndex;
  private final long lastPartitionCount;
  private final byte[] peers;
  private final byte[] snapshot;

  TRaftInstallSnapshotRequest(
      int leaderId,
      long term,
      long lastIncludedIndex,
      long lastIncludedTerm,
      long historicalMaxTimestamp,
      long lastPartitionIndex,
      long lastPartitionCount,
      byte[] peers,
      byte[] snapshot) {
    this.leaderId = leaderId;
    this.term = term;
    this.lastIncludedIndex = lastIncludedIndex;
    this.lastIncludedTerm = lastIncludedTerm;
    this.historicalMaxTimestamp = historicalMaxTimestamp;
    this.lastPartitionIndex = lastPartitionIndex;
    this.lastPartitionCount = lastPartitionCount;
    this.peers = peers.clone();
    this.snapshot = snapshot.clone();
  }

  int getLeaderId() {
    return leaderId;
  }

  long getTerm() {
    return term;
  }

  long getLastIncludedIndex() {
    return lastIncludedIndex;
  }

  long getLastIncludedTerm() {
    return lastIncludedTerm;
  }

  long getHistoricalMaxTimestamp() {
    return historicalMaxTimestamp;
  }

  long getLastPartitionIndex() {
    return lastPartitionIndex;
  }

  long getLastPartitionCount() {
    return lastPartitionCount;
  }

  byte[] getPeers() {
    return peers.clone();
  }

  byte[] getSnapshot() {
    return snapshot.clone();
  }
}
