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

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

public abstract class BaseAsyncService implements RaftService.AsyncIface {

  RaftMember member;
  String name;

  public BaseAsyncService(RaftMember member) {
    this.member = member;
    this.name = member.getName();
  }

  @Override
  public void sendHeartbeat(HeartBeatRequest request,
      AsyncMethodCallback<HeartBeatResponse> resultHandler) {
    resultHandler.onComplete(member.sendHeartbeat(request));
  }

  @Override
  public void startElection(ElectionRequest request, AsyncMethodCallback<Long> resultHandler) {
    resultHandler.onComplete(member.startElection(request));
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback<Long> resultHandler) {
    try {
      resultHandler.onComplete(member.appendEntry(request));
    } catch (UnknownLogTypeException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback<Long> resultHandler) {
    try {
      resultHandler.onComplete(member.appendEntries(request));
    } catch (UnknownLogTypeException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
    long commitIndex = member.requestCommitIndex(header);
    if (commitIndex != Long.MIN_VALUE) {
      resultHandler.onComplete(commitIndex);
      return;
    }

    member.waitLeader();
    AsyncClient client = member.getAsyncClient(member.getLeader());
    if (client == null) {
      resultHandler.onError(new LeaderUnknownException(member.getAllNodes()));
      return;
    }
    try {
      client.requestCommitIndex(header, resultHandler);
    } catch (TException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void readFile(String filePath, long offset, int length,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    try {
      resultHandler.onComplete(member.readFile(filePath, offset, length));
    } catch (IOException e) {
      resultHandler.onError(e);
    }
  }

  @Override
  public void matchTerm(long index, long term, Node header,
      AsyncMethodCallback<Boolean> resultHandler) {
    resultHandler.onComplete(member.matchTerm(index, term));
  }

  @Override
  public void executeNonQueryPlan(ExecutNonQueryReq request,
      AsyncMethodCallback<TSStatus> resultHandler) {
    if (member.getCharacter() != NodeCharacter.LEADER) {
      // forward the plan to the leader
      AsyncClient client = member.getAsyncClient(member.getLeader());
      if (client != null) {
        try {
          client.executeNonQueryPlan(request, resultHandler);
        } catch (TException e) {
          resultHandler.onError(e);
        }
      } else {
        resultHandler.onComplete(StatusUtils.NO_LEADER);
      }
      return;
    }

    try {
      resultHandler.onComplete(member.executeNonQueryPlan(request));
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }
}
