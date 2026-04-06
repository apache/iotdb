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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.thrift.TException;

import java.io.IOException;

/** DataNode internal RPC transport used for cross-process TRaft communication. */
class TRaftRpcTransport implements TRaftTransport {

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager =
      new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
          .createClientManager(new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  @Override
  public TRaftAppendEntriesResponse appendEntries(Peer peer, TRaftAppendEntriesRequest request)
      throws IOException {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(peer.getEndpoint())) {
      return TRaftSerializationUtils.fromThrift(
          client.sendTRaftAppendEntries(TRaftSerializationUtils.toThrift(request, peer)));
    } catch (ClientManagerException | TException e) {
      throw new IOException("Failed to send TRaft append entries", e);
    }
  }

  @Override
  public TRaftVoteResult requestVote(Peer peer, TRaftVoteRequest request) throws IOException {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(peer.getEndpoint())) {
      return TRaftSerializationUtils.fromThrift(
          client.sendTRaftRequestVote(TRaftSerializationUtils.toThrift(request, peer)));
    } catch (ClientManagerException | TException e) {
      throw new IOException("Failed to send TRaft vote request", e);
    }
  }

  @Override
  public TRaftInstallSnapshotResponse installSnapshot(
      Peer peer, TRaftInstallSnapshotRequest request) throws IOException {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(peer.getEndpoint())) {
      return TRaftSerializationUtils.fromThrift(
          client.sendTRaftInstallSnapshot(TRaftSerializationUtils.toThrift(request, peer)));
    } catch (ClientManagerException | TException e) {
      throw new IOException("Failed to send TRaft install snapshot", e);
    }
  }

  @Override
  public TRaftTriggerElectionResponse triggerElection(Peer peer) throws IOException {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(peer.getEndpoint())) {
      return TRaftSerializationUtils.fromThrift(
          client.sendTRaftTriggerElection(TRaftSerializationUtils.toThrift(peer)));
    } catch (ClientManagerException | TException e) {
      throw new IOException("Failed to trigger TRaft election", e);
    }
  }

  @Override
  public void close() {
    clientManager.close();
  }
}
