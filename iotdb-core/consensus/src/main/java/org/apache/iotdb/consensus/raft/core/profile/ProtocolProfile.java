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

package org.apache.iotdb.consensus.raft.core.profile;

import org.apache.iotdb.consensus.raft.core.NodeEnv;
import org.apache.iotdb.consensus.raft.core.apply.ApplyPolicy;
import org.apache.iotdb.consensus.raft.core.commit.CommitDecorator;
import org.apache.iotdb.consensus.raft.core.commit.CommitPolicy;
import org.apache.iotdb.consensus.raft.core.commit.ContiguousQuorumCommitPolicy;
import org.apache.iotdb.consensus.raft.core.election.VoteKeyPolicy;
import org.apache.iotdb.consensus.raft.core.ingest.EntryFactory;
import org.apache.iotdb.consensus.raft.core.ingest.LeaderTransition;
import org.apache.iotdb.consensus.raft.core.log.ChainValidator;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.replication.AcceptGate;
import org.apache.iotdb.consensus.raft.core.replication.Replicator;
import org.apache.iotdb.consensus.raft.core.replication.SchedulerContext;

/**
 * The strategy points through which TRaft, ParallelRaft (or any variant) extends the Raft kernel.
 * The defaults of the later points (capabilities, commit / apply policies, profile state) keep the
 * kernel on its original Raft paths.
 */
public interface ProtocolProfile {

  String name();

  EntryMetaCodec metaCodec();

  EntryFactory entryFactory();

  VoteKeyPolicy voteKeyPolicy();

  /** Follower-side gate; may subscribe to the log index (TRaft's working-partition tracker). */
  AcceptGate acceptGate(LogIndex log, NodeEnv env);

  /** A fresh leader-side sender for one leadership. */
  Replicator replicator(SchedulerContext ctx);

  CommitDecorator commitDecorator();

  LeaderTransition leaderTransition();

  /**
   * Use verified immutable file checkpoints with exact state restoration and journaled installs.
   * This is a durable format capability; optional proactive replication can be disabled separately.
   */
  default boolean usesFileCheckpoints() {
    return false;
  }

  /** Compress snapshot fragments in transit; disabled for existing protocols. */
  default boolean compressSnapshotTransfers() {
    return false;
  }

  default ChainValidator chainValidator() {
    return ChainValidator.CONTIGUOUS;
  }

  /** Protocol summary at the current snapshot boundary (decoded on restart / install). */
  default SnapshotMetaExt decodeSnapshotMeta(byte[] bytes) {
    return SnapshotMetaExt.NONE;
  }

  /** Protocol summary of the log prefix ending at {@code upTo} (written with a snapshot). */
  default SnapshotMetaExt snapshotMeta(
      org.apache.iotdb.consensus.raft.core.log.LogView log, long upTo, SnapshotMetaExt previous) {
    return SnapshotMetaExt.NONE;
  }

  /**
   * Kernel capability switches (out-of-order commit, acks beyond holes, dated overwrite, leader
   * recovery). Raft / TRaft: {@link Capabilities#NONE}, every switch off.
   */
  default Capabilities capabilities() {
    return Capabilities.NONE;
  }

  /**
   * The leader's commit rule; read once per kernel. Default: the stateless contiguous quorum rule
   * shared by Raft and TRaft.
   */
  default CommitPolicy commitPolicy() {
    return ContiguousQuorumCommitPolicy.INSTANCE;
  }

  /**
   * The node's apply policy, read once per kernel (so one instance per kernel / restart); null (the
   * default) keeps the kernel's built-in prefix application.
   */
  default ApplyPolicy applyPolicy() {
    return null;
  }

  /**
   * Protocol state persisted atomically with term / votedFor (ParallelRaft: sync number and
   * promise). Empty for Raft / TRaft, whose meta file then keeps its original format.
   */
  default byte[] encodeProfileMeta() {
    return new byte[0];
  }

  /**
   * Restore the state written by {@link #encodeProfileMeta()} (empty on a fresh node). Called once
   * on a fresh profile instance, before the kernel is constructed.
   */
  default void restoreProfileMeta(byte[] bytes) {}
}
