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
import org.apache.iotdb.consensus.raft.core.commit.CommitDecorator;
import org.apache.iotdb.consensus.raft.core.election.RaftVoteKeyPolicy;
import org.apache.iotdb.consensus.raft.core.election.VoteKeyPolicy;
import org.apache.iotdb.consensus.raft.core.ingest.EntryFactory;
import org.apache.iotdb.consensus.raft.core.ingest.LeaderTransition;
import org.apache.iotdb.consensus.raft.core.ingest.RaftEntryFactory;
import org.apache.iotdb.consensus.raft.core.ingest.RaftLeaderTransition;
import org.apache.iotdb.consensus.raft.core.log.EntryMetaCodec;
import org.apache.iotdb.consensus.raft.core.log.LogIndex;
import org.apache.iotdb.consensus.raft.core.replication.AcceptGate;
import org.apache.iotdb.consensus.raft.core.replication.PipelinedReplicator;
import org.apache.iotdb.consensus.raft.core.replication.RaftAcceptGate;
import org.apache.iotdb.consensus.raft.core.replication.Replicator;
import org.apache.iotdb.consensus.raft.core.replication.SchedulerContext;

/** Plain Raft. */
public final class RaftProfile implements ProtocolProfile {

  private final RaftEntryFactory entryFactory = new RaftEntryFactory();
  private final RaftVoteKeyPolicy voteKeyPolicy = new RaftVoteKeyPolicy();
  private final RaftLeaderTransition leaderTransition = new RaftLeaderTransition();

  @Override
  public String name() {
    return "Raft";
  }

  @Override
  public EntryMetaCodec metaCodec() {
    return EntryMetaCodec.EMPTY;
  }

  @Override
  public EntryFactory entryFactory() {
    return entryFactory;
  }

  @Override
  public VoteKeyPolicy voteKeyPolicy() {
    return voteKeyPolicy;
  }

  @Override
  public AcceptGate acceptGate(LogIndex log, NodeEnv env) {
    return new RaftAcceptGate();
  }

  @Override
  public Replicator replicator(SchedulerContext ctx) {
    return new PipelinedReplicator();
  }

  @Override
  public CommitDecorator commitDecorator() {
    return CommitDecorator.NONE;
  }

  @Override
  public LeaderTransition leaderTransition() {
    return leaderTransition;
  }
}
