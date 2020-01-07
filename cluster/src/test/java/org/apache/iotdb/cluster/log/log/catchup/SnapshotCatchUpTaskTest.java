/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.log.catchup;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestLog;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestSnapshot;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.catchup.LogCatchUpTask;
import org.apache.iotdb.cluster.log.catchup.SnapshotCatchUpTask;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

public class SnapshotCatchUpTaskTest {

  private List<Log> receivedLogs = new ArrayList<>();
  private Snapshot receivedSnapshot;
  private Node header = new Node();
  private boolean testLeadershipFlag;

  private RaftMember sender = new TestMetaGroupMember() {
    @Override
    public AsyncClient connectNode(Node node) {
      try {
        return new TestClient() {
          @Override
          public void appendEntry(AppendEntryRequest request,
              AsyncMethodCallback<Long> resultHandler) {
            new Thread(() -> {
              TestLog testLog = new TestLog();
              testLog.deserialize(request.entry);
              receivedLogs.add(testLog);
              resultHandler.onComplete(Response.RESPONSE_AGREE);
            }).start();
          }

          @Override
          public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
            new Thread(() -> {
              receivedSnapshot = new TestSnapshot();
              receivedSnapshot.deserialize(request.snapshotBytes);
              if (testLeadershipFlag) {
                sender.setCharacter(NodeCharacter.ELECTOR);
              }
              resultHandler.onComplete(null);
            }).start();
          }
        };
      } catch (IOException e) {
        return null;
      }
    }

    @Override
    public Node getHeader() {
      return header;
    }
  };

  @Before
  public void setUp() {
    testLeadershipFlag = false;
    receivedSnapshot = null;
    receivedLogs.clear();
  }

  @Test
  public void testCatchUp() {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Log log = new TestLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      log.setPreviousLogIndex(i - 1L);
      log.setPreviousLogTerm(i - 1L);
      logList.add(log);
    }
    Snapshot snapshot = new TestSnapshot(9989);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    SnapshotCatchUpTask task = new SnapshotCatchUpTask(logList, snapshot, receiver, sender);
    task.run();

    assertEquals(logList, receivedLogs);
    assertEquals(snapshot, receivedSnapshot);
  }

  @Test
  public void testLeadershipLost() {
    testLeadershipFlag = true;
    // the leadership will be lost after sending the snapshot
    List<Log> logList = TestUtils.prepareTestLogs(10);
    Snapshot snapshot = new TestSnapshot(9989);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new SnapshotCatchUpTask(logList, snapshot, receiver, sender);
    task.run();

    assertEquals(snapshot, receivedSnapshot);
    assertTrue(receivedLogs.isEmpty());
  }

  @Test
  public void testNoLeadership() {
    // the leadership is lost from the beginning
    List<Log> logList = TestUtils.prepareTestLogs(10);
    Snapshot snapshot = new TestSnapshot(9989);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.ELECTOR);
    LogCatchUpTask task = new SnapshotCatchUpTask(logList, snapshot, receiver, sender);
    task.run();

    assertNull(receivedSnapshot);
    assertTrue(receivedLogs.isEmpty());
  }
}