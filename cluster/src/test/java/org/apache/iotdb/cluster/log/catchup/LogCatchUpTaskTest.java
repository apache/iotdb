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

import org.apache.iotdb.cluster.common.TestAsyncClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestSyncClient;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LogCatchUpTaskTest {

  private List<Log> receivedLogs = new ArrayList<>();
  private RaftNode header = new RaftNode(new Node(), 0);
  private boolean testLeadershipFlag;
  private boolean prevUseAsyncServer;

  private RaftMember sender =
      new TestMetaGroupMember() {

        @Override
        public AsyncClient getAsyncClient(Node node, boolean activatedOnly) {
          return getAsyncClient(node);
        }

        @Override
        public AsyncClient getAsyncClient(Node node) {
          return new TestAsyncClient() {
            @Override
            public void appendEntry(
                AppendEntryRequest request, AsyncMethodCallback<Long> resultHandler) {
              new Thread(
                      () -> {
                        try {
                          resultHandler.onComplete(dummyAppendEntry(request));
                        } catch (UnknownLogTypeException e) {
                          fail(e.getMessage());
                        }
                      })
                  .start();
            }

            @Override
            public void appendEntries(
                AppendEntriesRequest request, AsyncMethodCallback<Long> resultHandler) {
              new Thread(
                      () -> {
                        try {
                          resultHandler.onComplete(dummyAppendEntries(request));
                        } catch (UnknownLogTypeException e) {
                          fail(e.getMessage());
                        }
                      })
                  .start();
            }
          };
        }

        @Override
        public Client getSyncClient(Node node) {
          return new TestSyncClient() {
            @Override
            public long appendEntry(AppendEntryRequest request) throws TException {
              try {
                return dummyAppendEntry(request);
              } catch (UnknownLogTypeException e) {
                throw new TException(e);
              }
            }

            @Override
            public long appendEntries(AppendEntriesRequest request) throws TException {
              try {
                return dummyAppendEntries(request);
              } catch (UnknownLogTypeException e) {
                throw new TException(e);
              }
            }
          };
        }

        @Override
        public RaftNode getHeader() {
          return header;
        }
      };

  private long dummyAppendEntry(AppendEntryRequest request) throws UnknownLogTypeException {
    LogParser parser = LogParser.getINSTANCE();
    Log testLog = parser.parse(request.entry);
    receivedLogs.add(testLog);
    if (testLeadershipFlag && testLog.getCurrLogIndex() == 4) {
      sender.setCharacter(NodeCharacter.ELECTOR);
    }
    return Response.RESPONSE_AGREE;
  }

  private long dummyAppendEntries(AppendEntriesRequest request) throws UnknownLogTypeException {
    LogParser parser = LogParser.getINSTANCE();
    Log testLog;
    for (ByteBuffer byteBuffer : request.getEntries()) {
      testLog = parser.parse(byteBuffer);
      receivedLogs.add(testLog);
      if (testLog != null && testLeadershipFlag && testLog.getCurrLogIndex() >= 1023) {
        // return a larger term to indicate that the leader has changed
        return sender.getTerm().get() + 1;
      }
    }

    return Response.RESPONSE_AGREE;
  }

  @Before
  public void setUp() {
    prevUseAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    testLeadershipFlag = false;
  }

  @After
  public void tearDown() throws Exception {
    sender.stop();
    sender.closeLogManager();
    EnvironmentUtils.cleanAllDir();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(prevUseAsyncServer);
  }

  @Test
  public void testCatchUpAsync() throws InterruptedException, TException, LeaderUnknownException {
    catchUpTest(10, false);
  }

  @Test
  public void testCatchUpInBatch() throws InterruptedException, TException, LeaderUnknownException {
    catchUpTest(10, true);
  }

  @Test
  public void testCatchUpInBatch2()
      throws InterruptedException, TException, LeaderUnknownException {
    catchUpTest(500, true);
  }

  public void catchUpTest(int logSize, boolean useBatch)
      throws InterruptedException, TException, LeaderUnknownException {
    List<Log> logList = TestUtils.prepareTestLogs(logSize);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new LogCatchUpTask(logList, receiver, 0, sender, useBatch);
    task.call();

    assertEquals(logList, receivedLogs);
  }

  @Test
  public void testCatchUpSync() throws InterruptedException, TException, LeaderUnknownException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);

    try {
      List<Log> logList = TestUtils.prepareTestLogs(10);
      Node receiver = new Node();
      sender.setCharacter(NodeCharacter.LEADER);
      LogCatchUpTask task = new LogCatchUpTask(logList, receiver, 0, sender, false);
      task.call();

      assertEquals(logList, receivedLogs);
    } finally {
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    }
  }

  @Test
  public void testLeadershipLost() {
    testLeadershipFlag = true;
    // the leadership will be lost after sending 5 logs
    List<Log> logList = TestUtils.prepareTestLogs(10);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new LogCatchUpTask(logList, receiver, 0, sender, false);
    task.setUseBatch(false);
    try {
      task.call();
      fail("Expected LeaderUnknownException");
    } catch (TException | InterruptedException e) {
      fail(e.getMessage());
    } catch (LeaderUnknownException e) {
      assertEquals(
          "The leader is unknown in this group [Node(internalIp:192.168.0.0, metaPort:9003, "
              + "nodeIdentifier:0, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.1, metaPort:9003, "
              + "nodeIdentifier:1, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.2, metaPort:9003, "
              + "nodeIdentifier:2, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.3, metaPort:9003, "
              + "nodeIdentifier:3, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.4, metaPort:9003, "
              + "nodeIdentifier:4, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.5, metaPort:9003, "
              + "nodeIdentifier:5, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.6, metaPort:9003, "
              + "nodeIdentifier:6, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.7, metaPort:9003, "
              + "nodeIdentifier:7, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.8, metaPort:9003, "
              + "nodeIdentifier:8, dataPort:40010, clientPort:6667, clientIp:0.0.0.0), Node(internalIp:192.168.0.9, metaPort:9003, "
              + "nodeIdentifier:9, dataPort:40010, clientPort:6667, clientIp:0.0.0.0)]",
          e.getMessage());
    }

    assertEquals(logList.subList(0, 5), receivedLogs);
  }

  @Test
  public void testLeadershipLostInBatch()
      throws InterruptedException, TException, LeaderUnknownException {
    testLeadershipFlag = true;
    // the leadership will be lost after sending 256 logs
    List<Log> logList = TestUtils.prepareTestLogs(1030);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    LogCatchUpTask task = new LogCatchUpTask(logList, receiver, 0, sender, true);
    task.call();

    assertEquals(logList.subList(0, 1024), receivedLogs);
  }

  @Test
  public void testSmallFrameSize() throws InterruptedException, TException, LeaderUnknownException {
    int preFrameSize = IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize();
    try {
      // thrift frame size is small so the logs must be sent in more than one batch
      List<Log> logList = TestUtils.prepareTestLogs(500);
      int singleLogSize = logList.get(0).serialize().limit();
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setThriftMaxFrameSize(100 * singleLogSize + IoTDBConstant.LEFT_SIZE_IN_REQUEST);
      Node receiver = new Node();
      sender.setCharacter(NodeCharacter.LEADER);
      LogCatchUpTask task = new LogCatchUpTask(logList, receiver, 0, sender, true);
      task.call();

      assertEquals(logList, receivedLogs);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setThriftMaxFrameSize(preFrameSize);
    }
  }

  @Test
  public void testVerySmallFrameSize()
      throws InterruptedException, TException, LeaderUnknownException {
    int preFrameSize = IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize();
    try {
      // thrift frame size is too small so no logs can be sent successfully
      List<Log> logList = TestUtils.prepareTestLogs(500);
      IoTDBDescriptor.getInstance().getConfig().setThriftMaxFrameSize(0);
      Node receiver = new Node();
      sender.setCharacter(NodeCharacter.LEADER);
      LogCatchUpTask task = new LogCatchUpTask(logList, receiver, 0, sender, true);
      task.call();

      assertTrue(receivedLogs.isEmpty());
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setThriftMaxFrameSize(preFrameSize);
    }
  }
}
