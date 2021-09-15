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
import org.apache.iotdb.cluster.common.TestDataGroupMember;
import org.apache.iotdb.cluster.common.TestLogManager;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.manage.RaftLogManager;
import org.apache.iotdb.cluster.log.manage.serializable.SyncLogDequeSerializer;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.exception.StorageEngineException;

import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DataHeartbeatThreadTest extends HeartbeatThreadTest {

  private TestLogManager dataLogManager;
  private MetaGroupMember metaGroupMember = (MetaGroupMember) super.getMember();

  @Override
  RaftMember getMember() {
    return new TestDataGroupMember() {
      @Override
      public RaftLogManager getLogManager() {
        return dataLogManager;
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

      @Override
      public MetaGroupMember getMetaGroupMember() {
        return metaGroupMember;
      }
    };
  }

  @Override
  AsyncClient getClient(Node node) {
    return new TestAsyncClient(node.nodeIdentifier) {
      @Override
      public void sendHeartbeat(
          HeartBeatRequest request, AsyncMethodCallback<HeartBeatResponse> resultHandler) {
        new Thread(
                () -> {
                  if (testHeartbeat) {
                    assertEquals(TestUtils.getNode(0), request.getLeader());
                    assertEquals(13, request.getCommitLogIndex());
                    assertEquals(10, request.getTerm());
                    assertEquals(TestUtils.getRaftNode(0, 0), request.getHeader());
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
                  assertEquals(13, request.getLastLogIndex());
                  assertEquals(13, request.getLastLogTerm());
                  if (respondToElection) {
                    resultHandler.onComplete(Response.RESPONSE_AGREE);
                  }
                })
            .start();
      }
    };
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    dataLogManager = new TestLogManager(2);
    List<Log> logs = TestUtils.prepareTestLogs(14);
    dataLogManager.append(logs);
    dataLogManager.commitTo(13);
  }

  @Override
  @After
  public void tearDown() throws InterruptedException, IOException, StorageEngineException {
    dataLogManager.close();
    dataLogManager = null;
    metaGroupMember.closeLogManager();
    metaGroupMember = null;
    File dir = new File(SyncLogDequeSerializer.getLogDir(2));
    for (File file : dir.listFiles()) {
      file.delete();
    }
    dir.delete();
    super.tearDown();
  }

  @Override
  HeartbeatThread getHeartbeatThread(RaftMember member) {
    return new DataHeartbeatThread((DataGroupMember) member);
  }
}
