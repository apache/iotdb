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

package org.apache.iotdb.consensus.raft.core.policy;

/**
 * Switches for every mechanism the Python reference implementation does not have. Under {@link
 * FidelityProfile#PYTHON} the kernel is message-equivalent to {@code node.py}; under {@link
 * FidelityProfile#PRODUCTION} all of them are enabled.
 */
public final class ProtocolPolicy {

  public enum CommitGranularity {
    ENTRY,
    PARTITION
  }

  public enum VoteKeyMode {
    TRIPLE,
    PAIR
  }

  /** Append a NOOP (TRaft: NOOP|BARRIER) entry on election. */
  public boolean noopOnElection;

  /** R1: the first data entry of a new term opens a new partition. */
  public boolean newTermOpensPartition;

  /** PreVote round before incrementing the term. */
  public boolean preVote;

  /** Leader-term fence handshake before a follower enters partition mode. */
  public boolean leaderFence;

  /** Send an empty AppendEntries to every peer after each committed entry (= Python CN). */
  public boolean eagerCommitNotify;

  /**
   * After the fence handshake, seed {@code v.rho} with the follower's reported working partition.
   */
  public boolean initVRhoFromReport;

  /** R2: chained contiguous frontier. */
  public boolean chainValidation;

  /** Delete requests raise T_max. */
  public boolean deleteRaisesTmax;

  /** RPC timeout clears in-flight state (= cluster.retransmit). */
  public boolean rpcTimeoutAsRetransmit;

  /** Raft replicator starts in PROBE mode and falls back to PROBE on REJECT. */
  public boolean probeMode;

  /** Followers reply to empty AppendEntries (heartbeats / commit notifications). */
  public boolean replyToHeartbeat;

  public int maxBatchEntries;
  public int maxInflightRpcs;

  /** Exponential retry wait after an RPC failure (0 = retry at once; the reference has none). */
  public long basicRetryWaitMs;

  public long maxRetryWaitMs;

  /** L3: an invariant violation throws instead of only being reported. */
  public boolean strictAsserts;

  public CommitGranularity commitGranularity;
  public VoteKeyMode voteKey;

  public static ProtocolPolicy python() {
    ProtocolPolicy p = new ProtocolPolicy();
    p.noopOnElection = false;
    p.newTermOpensPartition = false;
    p.preVote = false;
    p.leaderFence = false;
    p.eagerCommitNotify = true;
    p.initVRhoFromReport = false;
    p.chainValidation = false;
    p.deleteRaisesTmax = false;
    p.rpcTimeoutAsRetransmit = false;
    p.probeMode = false;
    p.replyToHeartbeat = false;
    p.maxBatchEntries = 1;
    p.maxInflightRpcs = Integer.MAX_VALUE;
    p.basicRetryWaitMs = 0;
    p.maxRetryWaitMs = 0;
    p.strictAsserts = false;
    p.commitGranularity = CommitGranularity.ENTRY;
    p.voteKey = VoteKeyMode.TRIPLE;
    return p;
  }

  public static ProtocolPolicy production() {
    ProtocolPolicy p = new ProtocolPolicy();
    p.noopOnElection = true;
    p.newTermOpensPartition = true;
    p.preVote = true;
    p.leaderFence = true;
    p.eagerCommitNotify = false;
    p.initVRhoFromReport = true;
    p.chainValidation = false;
    p.deleteRaisesTmax = false;
    p.rpcTimeoutAsRetransmit = true;
    p.probeMode = true;
    p.replyToHeartbeat = true;
    p.maxBatchEntries = 256;
    p.maxInflightRpcs = 8;
    p.basicRetryWaitMs = 100;
    p.maxRetryWaitMs = 5000;
    p.strictAsserts = false;
    p.commitGranularity = CommitGranularity.ENTRY;
    p.voteKey = VoteKeyMode.TRIPLE;
    return p;
  }

  public ProtocolPolicy copy() {
    ProtocolPolicy p = new ProtocolPolicy();
    p.noopOnElection = noopOnElection;
    p.newTermOpensPartition = newTermOpensPartition;
    p.preVote = preVote;
    p.leaderFence = leaderFence;
    p.eagerCommitNotify = eagerCommitNotify;
    p.initVRhoFromReport = initVRhoFromReport;
    p.chainValidation = chainValidation;
    p.deleteRaisesTmax = deleteRaisesTmax;
    p.rpcTimeoutAsRetransmit = rpcTimeoutAsRetransmit;
    p.probeMode = probeMode;
    p.replyToHeartbeat = replyToHeartbeat;
    p.maxBatchEntries = maxBatchEntries;
    p.maxInflightRpcs = maxInflightRpcs;
    p.basicRetryWaitMs = basicRetryWaitMs;
    p.maxRetryWaitMs = maxRetryWaitMs;
    p.strictAsserts = strictAsserts;
    p.commitGranularity = commitGranularity;
    p.voteKey = voteKey;
    return p;
  }
}
