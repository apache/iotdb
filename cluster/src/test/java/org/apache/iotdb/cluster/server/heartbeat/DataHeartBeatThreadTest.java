/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.heartbeat;

import static org.junit.Assert.assertEquals;

import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;

public class DataHeartBeatThreadTest extends HeartBeatThreadTest {

  private TestLogManager dataLogManager;

  @Override
  RaftMember getMember() {
    return new TestDataGroupMember() {
      @Override
      public LogManager getLogManager() {
        return dataLogManager;
      }

      @Override
      public AsyncClient connectNode(Node node) {
        return getClient(node);
      }

      @Override
      public MetaGroupMember getMetaGroupMember() {
        return (MetaGroupMember) DataHeartBeatThreadTest.super.getMember();
      }
    };
  }

  @Override
  AsyncClient getClient(Node node) {
    return new TestClient(node.nodeIdentifier) {
      @Override
      public void sendHeartBeat(HeartBeatRequest request,
          AsyncMethodCallback<HeartBeatResponse> resultHandler) {
        new Thread(() -> {
          if (testHeartBeat) {
            assertEquals(TestUtils.getNode(0), request.getLeader());
            assertEquals(13, request.getCommitLogIndex());
            assertEquals(10, request.getTerm());
            assertEquals(TestUtils.getNode(0), request.getHeader());
            synchronized (receivedNodes) {
              receivedNodes.add(getSerialNum());
              for (int i = 1; i < 10; i++) {
                if (!receivedNodes.contains(i)) {
                  return;
                }
              }
              testThread.interrupt();
            }
          } else if (respondToElection) {
            synchronized (testThread) {
              testThread.notifyAll();
            }
          }
        }).start();
      }

      @Override
      public void startElection(ElectionRequest request,
          AsyncMethodCallback<Long> resultHandler) {
        new Thread(() -> {
          assertEquals(TestUtils.getNode(0), request.getElector());
          assertEquals(11, request.getTerm());
          assertEquals(9, request.getLastLogIndex());
          assertEquals(8, request.getLastLogTerm());
          assertEquals(11, request.getDataLogLastTerm());
          assertEquals(12, request.getDataLogLastIndex());
          if (respondToElection) {
            resultHandler.onComplete(Response.RESPONSE_AGREE);
          }
        }).start();
      }
    };
  }

  @Override
  @Before
  public void setUp() {
    super.setUp();
    dataLogManager = new TestLogManager();
    dataLogManager.setLastLogTerm(11);
    dataLogManager.setLastLogId(12);
    dataLogManager.setCommitLogIndex(13);
  }

  @Override
  HeartBeatThread getHeartBeatThread(RaftMember member) {
    return new DataHeartBeatThread((DataGroupMember) member);
  }
}