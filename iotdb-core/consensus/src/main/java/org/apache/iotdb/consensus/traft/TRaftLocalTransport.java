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

import org.apache.iotdb.consensus.common.Peer;

import java.io.IOException;

/**
 * Same-process fast path used in unit tests and colocated deployments.
 *
 * <p>The actual distributed path still goes through {@link TRaftRpcTransport}; this registry-backed
 * path just avoids needless RPC when the destination consensus instance already lives in the same
 * JVM.
 */
class TRaftLocalTransport implements TRaftTransport {

  @Override
  public TRaftAppendEntriesResponse appendEntries(Peer peer, TRaftAppendEntriesRequest request)
      throws IOException {
    TRaftConsensus consensus = TRaftNodeRegistry.resolveConsensus(peer.getEndpoint()).orElse(null);
    if (consensus == null) {
      return null;
    }
    return consensus.receiveAppendEntries(peer.getGroupId(), request);
  }

  @Override
  public TRaftVoteResult requestVote(Peer peer, TRaftVoteRequest request) throws IOException {
    TRaftConsensus consensus = TRaftNodeRegistry.resolveConsensus(peer.getEndpoint()).orElse(null);
    if (consensus == null) {
      return null;
    }
    return consensus.receiveVoteRequest(peer.getGroupId(), request);
  }

  @Override
  public TRaftInstallSnapshotResponse installSnapshot(
      Peer peer, TRaftInstallSnapshotRequest request) throws IOException {
    TRaftConsensus consensus = TRaftNodeRegistry.resolveConsensus(peer.getEndpoint()).orElse(null);
    if (consensus == null) {
      return null;
    }
    return consensus.receiveInstallSnapshot(peer.getGroupId(), request);
  }

  @Override
  public TRaftTriggerElectionResponse triggerElection(Peer peer) throws IOException {
    TRaftConsensus consensus = TRaftNodeRegistry.resolveConsensus(peer.getEndpoint()).orElse(null);
    if (consensus == null) {
      return null;
    }
    return consensus.receiveTriggerElection(peer.getGroupId());
  }
}
