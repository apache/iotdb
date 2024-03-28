/*
 * Licensed to the Apache Software Foundation (ASF) under one  * or more contributor license agreements.  See the NOTICE file  * distributed with this work for additional information  * regarding copyright ownership.  The ASF licenses this file  * to you under the Apache License, Version 2.0 (the  * "License"); you may not use this file except in compliance  * with the License.  You may obtain a copy of the License at  *  *     http://www.apache.org/licenses/LICENSE-2.0  *  * Unless required by applicable law or agreed to in writing,  * software distributed under the License is distributed on an  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY  * KIND, either express or implied.  See the License for the  * specific language governing permissions and limitations  * under the License.
 */

package org.apache.iotdb.consensus.natraft.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendCompressedSingleEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntriesRequest;
import org.apache.iotdb.consensus.raft.thrift.AppendEntryResult;
import org.apache.iotdb.consensus.raft.thrift.ExecuteReq;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SyncClientAdaptor convert the async of AsyncClient method call to a sync one by synchronizing on
 * an AtomicReference of the return value of an RPC, and wait for at most connectionTimeoutInMS
 * until the reference is set by the handler or the request timeouts.
 */
@SuppressWarnings("java:S2274") // enable timeout
public class SyncClientAdaptor {

  private static final Logger logger = LoggerFactory.getLogger(SyncClientAdaptor.class);
  private static RaftConfig config;

  private SyncClientAdaptor() {
    // static class
  }

  public static boolean matchTerm(
      AsyncRaftServiceClient client,
      TEndPoint target,
      long prevLogIndex,
      long prevLogTerm,
      ConsensusGroupId groupId)
      throws TException, InterruptedException {
    try {
      GenericHandler<Boolean> matchTermHandler = new GenericHandler<>(target);
      client.matchTerm(
          prevLogIndex, prevLogTerm, groupId.convertToTConsensusGroupId(), matchTermHandler);
      Boolean result = matchTermHandler.getResult(config.getConnectionTimeoutInMS());
      return result != null && result;
    } catch (NullPointerException e) {
      logger.error("match term null exception", e);
      return false;
    }
  }

  public static TSStatus executeRequest(
      AsyncRaftServiceClient client,
      IConsensusRequest request,
      ConsensusGroupId groupId,
      TEndPoint receiver)
      throws IOException, TException, InterruptedException {
    AtomicReference<TSStatus> status = new AtomicReference<>();
    ExecuteReq req = new ExecuteReq();
    req.requestBytes = request.serializeToByteBuffer();
    req.setGroupId(groupId.convertToTConsensusGroupId());

    client.executeRequest(req, new ForwardRequestHandler(status, request, receiver));
    synchronized (status) {
      if (status.get() == null) {
        status.wait(config.getConnectionTimeoutInMS());
      }
    }
    return status.get();
  }

  public static AppendEntryResult appendEntries(
      AsyncRaftServiceClient client, AppendEntriesRequest request)
      throws TException, InterruptedException {
    GenericHandler<AppendEntryResult> matchTermHandler = new GenericHandler<>(client.getEndpoint());
    client.appendEntries(request, matchTermHandler);
    return matchTermHandler.getResult(config.getConnectionTimeoutInMS());
  }

  public static AppendEntryResult appendCompressedEntries(
      AsyncRaftServiceClient client, AppendCompressedEntriesRequest request)
      throws TException, InterruptedException {
    GenericHandler<AppendEntryResult> matchTermHandler = new GenericHandler<>(client.getEndpoint());
    client.appendCompressedEntries(request, matchTermHandler);
    return matchTermHandler.getResult(config.getConnectionTimeoutInMS());
  }

  public static AppendEntryResult appendCompressedSingleEntries(
      AsyncRaftServiceClient client, AppendCompressedSingleEntriesRequest request)
      throws TException, InterruptedException {
    GenericHandler<AppendEntryResult> matchTermHandler = new GenericHandler<>(client.getEndpoint());
    client.appendCompressedSingleEntries(request, matchTermHandler);
    return matchTermHandler.getResult(config.getConnectionTimeoutInMS());
  }

  public static TSStatus forceElection(AsyncRaftServiceClient client, ConsensusGroupId groupId)
      throws TException, InterruptedException {
    GenericHandler<TSStatus> matchTermHandler = new GenericHandler<>(client.getEndpoint());
    client.forceElection(groupId.convertToTConsensusGroupId(), matchTermHandler);
    return matchTermHandler.getResult(config.getConnectionTimeoutInMS());
  }

  public static ByteBuffer readFile(
      AsyncRaftServiceClient client, String remotePath, long offset, int fetchSize)
      throws InterruptedException, TException {
    GenericHandler<ByteBuffer> handler = new GenericHandler<>(client.getEndpoint());

    client.readFile(remotePath, offset, fetchSize, handler);
    return handler.getResult(config.getConnectionTimeoutInMS());
  }

  public static void setConfig(RaftConfig config) {
    SyncClientAdaptor.config = config;
  }
}
