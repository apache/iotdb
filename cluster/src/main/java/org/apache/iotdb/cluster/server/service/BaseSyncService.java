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

package org.apache.iotdb.cluster.server.service;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.IOUtils;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSyncService implements RaftService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(BaseSyncService.class);
  RaftMember member;
  String name;

  BaseSyncService(RaftMember member) {
    this.member = member;
    this.name = member.getName();
  }

  @Override
  public HeartBeatResponse sendHeartbeat(HeartBeatRequest request) {
    return member.processHeartbeatRequest(request);
  }

  @Override
  public long startElection(ElectionRequest request) {
    return member.processElectionRequest(request);
  }

  @Override
  public long appendEntry(AppendEntryRequest request) throws TException {
    try {
      return member.appendEntry(request);
    } catch (UnknownLogTypeException e) {
      throw new TException(e);
    }
  }

  @Override
  public long appendEntries(AppendEntriesRequest request) throws TException {
    try {
      return member.appendEntries(request);
    } catch (BufferUnderflowException e) {
      logger.error("Underflow buffers {} of logs from {}", request.getEntries(),
          request.getPrevLogIndex() + 1);
      throw new TException(e);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public long requestCommitIndex(Node header)
      throws TException {
    long commitIndex = member.getCommitIndex();
    if (commitIndex != Long.MIN_VALUE) {
      return commitIndex;
    }

    member.waitLeader();
    Client client = member.getSyncClient(member.getLeader());
    if (client == null) {
      throw new TException(new LeaderUnknownException(member.getAllNodes()));
    }
    try {
      commitIndex = client.requestCommitIndex(header);
    } catch (TException e) {
      client.getInputProtocol().getTransport().close();
      throw e;
    } finally {
      putBackSyncClient(client);
    }
    return commitIndex;
  }

  void putBackSyncClient(Client client) {
    if (client instanceof SyncDataClient) {
      ((SyncDataClient) client).putBack();
    } else {
      ((SyncMetaClient) client).putBack();
    }
  }

  @Override
  public ByteBuffer readFile(String filePath, long offset, int length) throws TException {
    try {
      return IOUtils.readFile(filePath, offset, length);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public void removeHardLink(String hardLinkPath) throws TException {
    try {
      Files.deleteIfExists(new File(hardLinkPath).toPath());
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public boolean matchTerm(long index, long term, Node header) {
    return member.matchLog(index, term);
  }

  @Override
  public TSStatus executeNonQueryPlan(ExecutNonQueryReq request) throws TException {
    if (member.getCharacter() != NodeCharacter.LEADER) {
      // forward the plan to the leader
      Client client = member.getSyncClient(member.getLeader());
      if (client != null) {
        TSStatus status;
        try {
          status = client.executeNonQueryPlan(request);
        } catch (TException e) {
          client.getInputProtocol().getTransport().close();
          throw e;
        } finally {
          putBackSyncClient(client);
        }
        return status;
      } else {
        return StatusUtils.NO_LEADER;
      }
    }

    try {
      return member.executeNonQueryPlan(request);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
}
