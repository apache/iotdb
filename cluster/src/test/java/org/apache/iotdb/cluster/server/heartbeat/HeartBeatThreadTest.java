/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.heartbeat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

public class HeartBeatThreadTest {

  RaftMember member;
  private HeartBeatThread heartBeatThread;
  TestLogManager logManager;
  Thread testThread;
  boolean respondToElection;
  boolean testHeartBeat;

  Set<Integer> receivedNodes = new HashSet<>();

  RaftMember getMember() {
    return new TestMetaGroupMember() {
      @Override
      public LogManager getLogManager() {
        return HeartBeatThreadTest.this.logManager;
      }

      @Override
      public AsyncClient connectNode(Node node) {
        return getClient(node);
      }
    };
  }

  AsyncClient getClient(Node node) {
    return new TestClient(node.nodeIdentifier) {
      @Override
      public void sendHeartBeat(HeartBeatRequest request,
          AsyncMethodCallback<HeartBeatResponse> resultHandler) {
        new Thread(() -> {
          if (testHeartBeat) {
            assertEquals(TestUtils.getNode(0), request.getLeader());
            assertEquals(7, request.getCommitLogIndex());
            assertEquals(10, request.getTerm());
            assertNull(request.getHeader());
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
          if (respondToElection) {
            resultHandler.onComplete(Response.RESPONSE_AGREE);
          }
        }).start();
      }
    };
  }

  HeartBeatThread getHeartBeatThread(RaftMember member) {
    return new HeartBeatThread(member);
  }

  @Before
  public void setUp() {
    logManager = new TestLogManager();
    member = getMember();
    heartBeatThread = getHeartBeatThread(member);
    testThread = new Thread(heartBeatThread);
    member.getTerm().set(10);
    logManager.setLastLogId(9);
    logManager.setLastLogTerm(8);
    logManager.setCommitLogIndex(7);

    respondToElection = false;
    testHeartBeat = false;
  }

  @Test
  public void testAsLeader() throws InterruptedException {
    testHeartBeat = true;
    member.setCharacter(NodeCharacter.LEADER);
    synchronized (receivedNodes) {
      testThread.start();
    }
    testThread.join(10 * 1000);
    for (int i = 1; i < 10; i++) {
      assertTrue(receivedNodes.contains(i));
    }
  }

  @Test
  public void testAsFollower() throws InterruptedException {
    int prevTimeOut = RaftServer.connectionTimeoutInMS;
    RaftServer.connectionTimeoutInMS = 500;
    member.setCharacter(NodeCharacter.FOLLOWER);
    member.setLastHeartBeatReceivedTime(System.currentTimeMillis());
    respondToElection = false;
    try {
      testThread.start();
      Thread.sleep(1000);
      assertEquals(NodeCharacter.ELECTOR, member.getCharacter());
      testThread.interrupt();
      testThread.join();
    } finally {
      RaftServer.connectionTimeoutInMS = prevTimeOut;
    }
  }

  @Test
  public void testAsElector() throws InterruptedException {
    member.setCharacter(NodeCharacter.ELECTOR);
    respondToElection = true;
    synchronized (testThread) {
      testThread.start();
      testThread.wait(10 * 1000);
    }
    assertEquals(NodeCharacter.LEADER, member.getCharacter());
    testThread.interrupt();
    testThread.join();
  }

  @Test
  public void testSingleNode() throws InterruptedException {
    member.getAllNodes().clear();
    member.getAllNodes().add(TestUtils.getNode(0));
    member.setCharacter(NodeCharacter.ELECTOR);
    synchronized (testThread) {
      testThread.start();
      testThread.wait(500);
    }
    assertEquals(NodeCharacter.LEADER, member.getCharacter());
    testThread.interrupt();
    testThread.join();
  }
}