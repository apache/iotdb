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

package org.apache.iotdb.cluster.log.catchup;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestLog;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

public class LogCatchUpTaskTest {

  private List<Log> receivedLogs = new ArrayList<>();
  private Node header = new Node();
  private boolean testLeadershipFlag;

  private RaftMember sender = new TestMetaGroupMember() {
    @Override
    public AsyncClient connectNode(Node node) {
      return new TestClient() {
        @Override
        public void appendEntry(AppendEntryRequest request,
            AsyncMethodCallback<Long> resultHandler) {
          new Thread(() -> {
            TestLog testLog = new TestLog();
            testLog.deserialize(request.entry);
            receivedLogs.add(testLog);
            if (testLeadershipFlag && testLog.getCurrLogIndex() == 4) {
              sender.setCharacter(NodeCharacter.ELECTOR);
            }
            resultHandler.onComplete(Response.RESPONSE_AGREE);
          }).start();
        }

        @Override
        public void appendEntries(AppendEntriesRequest request,
            AsyncMethodCallback<Long> resultHandler) {
          new Thread(() -> {
            TestLog testLog = new TestLog();
            for (ByteBuffer byteBuffer : request.getEntries()) {
              testLog = new TestLog();
              testLog.deserialize(byteBuffer);
              receivedLogs.add(testLog);
            }

            if (testLeadershipFlag && testLog.getCurrLogIndex() == 255) {
              sender.setCharacter(NodeCharacter.ELECTOR);
            }
            resultHandler.onComplete(Response.RESPONSE_AGREE);
          }).start();
        }
      };
    }

    @Override
    public Node getHeader() {
      return header;
    }
  };

  @Before
  public void setUp() {
    testLeadershipFlag = false;
  }

  @Test
  public void testCatchUp() throws InterruptedException, TException, LeaderUnknownException {
    List<Log> logList = TestUtils.prepareTestLogs(10);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new LogCatchUpTask(logList, receiver, sender, false);
    task.call();

    assertEquals(logList, receivedLogs);
  }

  @Test
  public void testLeadershipLost() throws InterruptedException, TException, LeaderUnknownException {
    testLeadershipFlag = true;
    // the leadership will be lost after sending 5 logs
    List<Log> logList = TestUtils.prepareTestLogs(10);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new LogCatchUpTask(logList, receiver, sender, false);
    task.setUseBatch(false);
    try {
      task.call();
      fail("Expected LeaderUnknownException");
    } catch (TException | InterruptedException e) {
      fail(e.getMessage());
    } catch (LeaderUnknownException e) {
      assertEquals("The leader is unknown in this group [Node(ip:192.168.0.0, metaPort:9003, "
          + "nodeIdentifier:0, dataPort:40010), Node(ip:192.168.0.1, metaPort:9003, "
          + "nodeIdentifier:1, dataPort:40010), Node(ip:192.168.0.2, metaPort:9003, "
          + "nodeIdentifier:2, dataPort:40010), Node(ip:192.168.0.3, metaPort:9003, "
          + "nodeIdentifier:3, dataPort:40010), Node(ip:192.168.0.4, metaPort:9003, "
          + "nodeIdentifier:4, dataPort:40010), Node(ip:192.168.0.5, metaPort:9003, "
          + "nodeIdentifier:5, dataPort:40010), Node(ip:192.168.0.6, metaPort:9003, "
          + "nodeIdentifier:6, dataPort:40010), Node(ip:192.168.0.7, metaPort:9003, "
          + "nodeIdentifier:7, dataPort:40010), Node(ip:192.168.0.8, metaPort:9003, "
          + "nodeIdentifier:8, dataPort:40010), Node(ip:192.168.0.9, metaPort:9003, "
          + "nodeIdentifier:9, dataPort:40010)]", e.getMessage());
    }

    assertEquals(logList.subList(0, 5), receivedLogs);
  }

  @Test
  public void testCatchUpInBatch() throws InterruptedException, TException, LeaderUnknownException {
    List<Log> logList = TestUtils.prepareTestLogs(10);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new LogCatchUpTask(logList, receiver, sender, true);
    task.call();

    assertEquals(logList, receivedLogs);
  }

  @Test
  public void testCatchUpInBatch2()
      throws InterruptedException, TException, LeaderUnknownException {
    List<Log> logList = TestUtils.prepareTestLogs(500);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new LogCatchUpTask(logList, receiver, sender, true);
    task.call();

    assertEquals(logList, receivedLogs);
  }

  @Test
  public void testLeadershipLostInBatch()
      throws InterruptedException, TException, LeaderUnknownException {
    testLeadershipFlag = true;
    // the leadership will be lost after sending 256 logs
    List<Log> logList = TestUtils.prepareTestLogs(300);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new LogCatchUpTask(logList, receiver, sender, true);
    task.call();

    assertEquals(logList.subList(0, 256), receivedLogs);
  }
}