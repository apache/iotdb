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
 * In-memory form of a RequestVote RPC.
 *
 * <p>Partition metadata is still carried because TRaft logs keep time-partition information, but
 * election safety is decided by the standard Raft freshness pair: lastLogTerm and lastLogIndex.
 */
class TRaftVoteRequest {

  private final int candidateId;
  private final long term;
  private final long lastLogIndex;
  private final long lastLogTerm;
  private final long partitionIndex;
  private final long currentPartitionIndexCount;

  TRaftVoteRequest(
      int candidateId,
      long term,
      long lastLogIndex,
      long lastLogTerm,
      long partitionIndex,
      long currentPartitionIndexCount) {
    this.candidateId = candidateId;
    this.term = term;
    this.lastLogIndex = lastLogIndex;
    this.lastLogTerm = lastLogTerm;
    this.partitionIndex = partitionIndex;
    this.currentPartitionIndexCount = currentPartitionIndexCount;
  }

  int getCandidateId() {
    return candidateId;
  }

  long getTerm() {
    return term;
  }

  long getLastLogIndex() {
    return lastLogIndex;
  }

  long getLastLogTerm() {
    return lastLogTerm;
  }

  long getPartitionIndex() {
    return partitionIndex;
  }

  long getCurrentPartitionIndexCount() {
    return currentPartitionIndexCount;
  }
}
