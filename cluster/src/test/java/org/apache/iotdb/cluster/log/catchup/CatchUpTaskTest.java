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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestLog;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogParser;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.logtypes.EmptyContentLog;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Peer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

public class CatchUpTaskTest {

  private List<Log> receivedLogs = new ArrayList<>();
  private Snapshot receivedSnapshot;
  private long leaderCommit;
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
            Log log = receivedLogs.get(receivedLogs.size() - 1);
            Log testLog = null;
            try {
              testLog = LogParser.getINSTANCE().parse(request.entry);
            } catch (Exception e) {
            }
            if (testLog.getCurrLogIndex() == log.getCurrLogIndex() + 1) {
              leaderCommit = Math.max(request.leaderCommit, leaderCommit);
              receivedLogs.add(testLog);
              resultHandler.onComplete(Response.RESPONSE_AGREE);
              return;
            }
            if (testLog.getCurrLogIndex() == log.getCurrLogIndex()) {
              leaderCommit = Math.max(request.leaderCommit, leaderCommit);
              resultHandler.onComplete(Response.RESPONSE_AGREE);
              return;
            }
            resultHandler.onComplete(Response.RESPONSE_LOG_MISMATCH);
          }).start();
        }

        @Override
        public void appendEntries(AppendEntriesRequest request,
            AsyncMethodCallback<Long> resultHandler) {
          new Thread(() -> {
            TestLog testLog;
            for (ByteBuffer byteBuffer : request.getEntries()) {
              testLog = new TestLog();
              testLog.deserialize(byteBuffer);
              receivedLogs.add(testLog);
            }
            leaderCommit = Math.max(request.leaderCommit, leaderCommit);
            resultHandler.onComplete(Response.RESPONSE_AGREE);
          }).start();
        }

        @Override
        public void matchTerm(long index, long term, Node header,
            AsyncMethodCallback<Boolean> resultHandler) {
          new Thread(() -> {
            if (receivedLogs.isEmpty()) {
              resultHandler.onComplete(true);
              return;
            } else {
              for (Log receivedLog : receivedLogs) {
                if (receivedLog.getCurrLogTerm() == term
                    && receivedLog.getCurrLogIndex() == index) {
                  resultHandler.onComplete(true);
                  return;
                }
              }
            }
            resultHandler.onComplete(false);
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
    receivedSnapshot = null;
    receivedLogs = new ArrayList<>();
    EmptyContentLog log = new EmptyContentLog();
    log.setCurrLogIndex(-1);
    log.setCurrLogTerm(-1);
    receivedLogs.add(log);
  }

  @Test
  public void testCatchUpSingle() throws Exception {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Log log = new EmptyContentLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      logList.add(log);
    }
    sender.getLogManager().append(logList);
    sender.getLogManager().commitTo(9, false);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    Peer peer = new Peer(10);
    peer.setCatchUp(false);
    peer.setNextIndex(0);
    CatchUpTask task = new CatchUpTask(receiver, peer, sender);
    ClusterDescriptor.getInstance().getConfig().setUseBatchInLogCatchUp(false);
    task.run();

    assertEquals(logList, receivedLogs.subList(1, receivedLogs.size()));
    assertEquals(9, leaderCommit);
  }

  @Test
  public void testCatchUpBatch() throws Exception {
    List<Log> logList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Log log = new EmptyContentLog();
      log.setCurrLogIndex(i);
      log.setCurrLogTerm(i);
      logList.add(log);
    }
    sender.getLogManager().append(logList);
    sender.getLogManager().commitTo(9, false);
    Node receiver = new Node();
    sender.setCharacter(NodeCharacter.LEADER);
    Peer peer = new Peer(10);
    peer.setCatchUp(false);
    peer.setNextIndex(0);
    CatchUpTask task = new CatchUpTask(receiver, peer, sender);
    task.run();

    assertEquals(logList, receivedLogs.subList(1, receivedLogs.size()));
    assertEquals(9, leaderCommit);
  }
}