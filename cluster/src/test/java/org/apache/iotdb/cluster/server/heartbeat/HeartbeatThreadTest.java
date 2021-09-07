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

import org.apache.iotdb.cluster.common.TestAsyncClient;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@SuppressWarnings("java:S2699")
public class HeartbeatThreadTest {

  RaftMember member;
  TestLogManager logManager;
  Thread testThread;
  boolean respondToElection;
  boolean testHeartbeat;

  Set<Integer> receivedNodes = new ConcurrentSkipListSet<>();
  PartitionGroup partitionGroup;
  private boolean prevUseAsyncServer;

  RaftMember getMember() {
    return new TestMetaGroupMember() {
      @Override
      public RaftLogManager getLogManager() {
        return HeartbeatThreadTest.this.logManager;
      }

      @Override
      public void updateHardState(long currentTerm, Node leader) {}

      @Override
      public AsyncClient getAsyncClient(Node node) {
        return getClient(node);
      }

      @Override
      public AsyncClient getAsyncClient(Node node, boolean activatedOnly) {
        return getClient(node);
      }

      @Override
      public AsyncClient getAsyncHeartbeatClient(Node node) {
        return getClient(node);
      }
    };
  }

  AsyncClient getClient(Node node) {
    return new TestAsyncClient(node.nodeIdentifier) {
      @Override
      public void sendHeartbeat(
          HeartBeatRequest request, AsyncMethodCallback<HeartBeatResponse> resultHandler) {
        new Thread(
                () -> {
                  if (testHeartbeat) {
                    assertEquals(TestUtils.getNode(0), request.getLeader());
                    assertEquals(6, request.getCommitLogIndex());
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
                })
            .start();
      }

      @Override
      public void startElection(ElectionRequest request, AsyncMethodCallback<Long> resultHandler) {
        new Thread(
                () -> {
                  assertEquals(TestUtils.getNode(0), request.getElector());
                  assertEquals(11, request.getTerm());
                  assertEquals(6, request.getLastLogIndex());
                  assertEquals(6, request.getLastLogTerm());
                  if (respondToElection) {
                    resultHandler.onComplete(Response.RESPONSE_AGREE);
                  }
                })
            .start();
      }
    };
  }

  HeartbeatThread getHeartbeatThread(RaftMember member) {
    return new HeartbeatThread(member);
  }

  @Before
  public void setUp() throws Exception {
    ClusterConstant.setElectionMaxWaitMs(50L);
    RaftServer.setHeartbeatIntervalMs(100L);
    RaftServer.setElectionTimeoutMs(1000L);
    prevUseAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    logManager = new TestLogManager(1);
    member = getMember();

    HeartbeatThread heartBeatThread = getHeartbeatThread(member);
    testThread = new Thread(heartBeatThread);
    member.getTerm().set(10);
    List<Log> logs = TestUtils.prepareTestLogs(7);
    logManager.append(logs);
    logManager.commitTo(6);

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

  @After
  public void tearDown() throws InterruptedException, IOException, StorageEngineException {
    logManager.close();
    member.closeLogManager();
    member.stop();
    logManager = null;
    member = null;
    testThread.interrupt();
    testThread.join();
    File dir = new File(SyncLogDequeSerializer.getLogDir(1));
    for (File file : dir.listFiles()) {
      file.delete();
    }
    dir.delete();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(prevUseAsyncServer);
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testAsLeader() throws InterruptedException {
    testHeartbeat = true;
    member.setCharacter(NodeCharacter.LEADER);
    member.setLeader(member.getThisNode());
    synchronized (receivedNodes) {
      testThread.start();
    }
    begin:
    while (true) {
      for (int i = 1; i < 10; i++) {
        if (!receivedNodes.contains(i)) {
          continue begin;
        }
      }
      break;
    }
    testThread.interrupt();
    testThread.join();
  }

  @Test
  public void testAsFollower() throws InterruptedException {
    int prevTimeOut = RaftServer.getConnectionTimeoutInMS();
    RaftServer.setConnectionTimeoutInMS(500);
    member.setCharacter(NodeCharacter.FOLLOWER);
    member.setLastHeartbeatReceivedTime(System.currentTimeMillis());
    respondToElection = false;
    try {
      testThread.start();
      while (!NodeCharacter.ELECTOR.equals(member.getCharacter())) {}

      testThread.interrupt();
      testThread.join();
    } finally {
      RaftServer.setConnectionTimeoutInMS(prevTimeOut);
    }
  }

  @Test
  public void testAsElector() throws InterruptedException {
    member.setCharacter(NodeCharacter.ELECTOR);
    respondToElection = true;
    testThread.start();
    while (!NodeCharacter.LEADER.equals(member.getCharacter())) {}

    testThread.interrupt();
    testThread.join();
  }

  @Test
  public void testSingleNode() throws InterruptedException {
    member.getAllNodes().clear();
    member.getAllNodes().add(TestUtils.getNode(0));
    member.setCharacter(NodeCharacter.ELECTOR);
    testThread.start();
    while (!NodeCharacter.LEADER.equals(member.getCharacter())) {}

    testThread.interrupt();
    testThread.join();
  }
}
