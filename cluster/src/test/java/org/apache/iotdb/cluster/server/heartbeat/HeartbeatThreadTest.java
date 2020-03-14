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
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

public class HeartbeatThreadTest {

  RaftMember member;
  TestLogManager logManager;
  Thread testThread;
  boolean respondToElection;
  boolean testHeartbeat;

  Set<Integer> receivedNodes = new HashSet<>();
  PartitionGroup partitionGroup;

  RaftMember getMember() {
    return new TestMetaGroupMember() {
      @Override
      public LogManager getLogManager() {
        return HeartbeatThreadTest.this.logManager;
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
      public void sendHeartbeat(HeartbeatRequest request,
          AsyncMethodCallback<HeartbeatResponse> resultHandler) {
        new Thread(() -> {
          if (testHeartbeat) {
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

  HeartbeatThread getHeartbeatThread(RaftMember member) {
    return new HeartbeatThread(member);
  }

  @Before
  public void setUp() {
    logManager = new TestLogManager();
    member = getMember();

    HeartbeatThread heartBeatThread = getHeartbeatThread(member);
    testThread = new Thread(heartBeatThread);
    member.getTerm().set(10);
    logManager.setLastLogId(9);
    logManager.setLastLogTerm(8);
    logManager.setCommitLogIndex(7);

    respondToElection = false;
    testHeartbeat = false;
    partitionGroup = new PartitionGroup();
    for (int i = 0; i < 10; i++) {
      partitionGroup.add(TestUtils.getNode(i));
    }
    member.setAllNodes(partitionGroup);
    member.setThisNode(TestUtils.getNode(0));
    receivedNodes.clear();
  }

  @Test
  public void testAsLeader() throws InterruptedException {
    testHeartbeat = true;
    member.setCharacter(NodeCharacter.LEADER);
    member.setLeader(member.getThisNode());
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
    member.setLastHeartbeatReceivedTime(System.currentTimeMillis());
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