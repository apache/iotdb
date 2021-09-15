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

package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.common.TestAsyncClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestSyncClient;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.UnknownLogTypeException;
import org.apache.iotdb.cluster.log.LogDispatcher.SendLogRequest;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LogDispatcherTest {

  private RaftMember raftMember;
  private Map<Log, AtomicInteger> appendedEntries;
  private Set<Node> downNode;

  @Before
  public void setUp() {
    appendedEntries = new ConcurrentSkipListMap<>();
    downNode = new HashSet<>();
    raftMember =
        new TestMetaGroupMember() {
          @Override
          public AsyncClient getSendLogAsyncClient(Node node) {
            return new TestAsyncClient() {
              @Override
              public void appendEntry(
                  AppendEntryRequest request, AsyncMethodCallback<Long> resultHandler) {
                new Thread(
                        () -> {
                          if (!downNode.contains(node)) {
                            try {
                              resultHandler.onComplete(mockedAppendEntry(request));
                            } catch (UnknownLogTypeException e) {
                              resultHandler.onError(e);
                            }
                          }
                        })
                    .start();
              }

              @Override
              public void appendEntries(
                  AppendEntriesRequest request, AsyncMethodCallback<Long> resultHandler) {
                new Thread(
                        () -> {
                          if (!downNode.contains(node)) {
                            try {
                              resultHandler.onComplete(mockedAppendEntries(request));
                            } catch (UnknownLogTypeException e) {
                              resultHandler.onError(e);
                            }
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
                  if (!downNode.contains(node)) {
                    return mockedAppendEntry(request);
                  }
                  return -1;
                } catch (UnknownLogTypeException e) {
                  throw new TException(e);
                }
              }

              @Override
              public long appendEntries(AppendEntriesRequest request) throws TException {
                try {
                  if (!downNode.contains(node)) {
                    return mockedAppendEntries(request);
                  }
                  return -1;
                } catch (UnknownLogTypeException e) {
                  throw new TException(e);
                }
              }
            };
          }
        };
    PartitionGroup allNodes = new PartitionGroup();
    for (int i = 0; i < 10; i++) {
      allNodes.add(TestUtils.getNode(i));
    }
    raftMember.setAllNodes(allNodes);
    raftMember.setCharacter(NodeCharacter.LEADER);
  }

  private long mockedAppendEntry(AppendEntryRequest request) throws UnknownLogTypeException {
    LogParser logParser = LogParser.getINSTANCE();
    Log parse = logParser.parse(request.entry.duplicate());
    appendedEntries.computeIfAbsent(parse, p -> new AtomicInteger()).incrementAndGet();
    return Response.RESPONSE_AGREE;
  }

  private long mockedAppendEntries(AppendEntriesRequest request) throws UnknownLogTypeException {
    List<ByteBuffer> entries = request.getEntries();
    List<Log> logs = new ArrayList<>();
    for (ByteBuffer entry : entries) {
      LogParser logParser = LogParser.getINSTANCE();
      Log parse = logParser.parse(entry.duplicate());
      logs.add(parse);
    }
    for (Log log : logs) {
      appendedEntries.computeIfAbsent(log, p -> new AtomicInteger()).incrementAndGet();
    }
    return Response.RESPONSE_AGREE;
  }

  @Test
  public void testAsync() throws InterruptedException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    LogDispatcher dispatcher = new LogDispatcher(raftMember);
    try {
      List<Log> logs = TestUtils.prepareTestLogs(10);
      for (Log log : logs) {
        SendLogRequest request = raftMember.buildSendLogRequest(log);
        dispatcher.offer(request);
      }
      while (!checkResult(logs, 9)) {
        // wait
      }
    } finally {
      dispatcher.close();
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    }
  }

  @Test
  public void testSync() throws InterruptedException {
    boolean useAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);
    LogDispatcher dispatcher = new LogDispatcher(raftMember);
    try {
      List<Log> logs = TestUtils.prepareTestLogs(10);
      for (Log log : logs) {
        SendLogRequest request = raftMember.buildSendLogRequest(log);
        dispatcher.offer(request);
      }
      while (!checkResult(logs, 9)) {
        // wait
      }
    } finally {
      dispatcher.close();
      ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(useAsyncServer);
    }
  }

  @Test
  public void testWithFailure() throws InterruptedException {
    for (int i = 1; i < 4; i++) {
      downNode.add(TestUtils.getNode(i));
    }
    LogDispatcher dispatcher = new LogDispatcher(raftMember);
    try {
      List<Log> logs = TestUtils.prepareTestLogs(10);
      for (Log log : logs) {
        SendLogRequest request = raftMember.buildSendLogRequest(log);
        dispatcher.offer(request);
      }
      while (!checkResult(logs, 6)) {
        // wait
      }
    } finally {
      dispatcher.close();
    }
  }

  @Test
  public void testWithLargeLog() throws InterruptedException {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setThriftMaxFrameSize(64 * 1024 + IoTDBConstant.LEFT_SIZE_IN_REQUEST);
    for (int i = 1; i < 4; i++) {
      downNode.add(TestUtils.getNode(i));
    }
    LogDispatcher dispatcher = new LogDispatcher(raftMember);
    try {
      List<Log> logs = TestUtils.prepareLargeTestLogs(20);
      for (Log log : logs) {
        SendLogRequest request = raftMember.buildSendLogRequest(log);
        dispatcher.offer(request);
      }
      while (!checkResult(logs, 6)) {
        // wait
      }
    } finally {
      dispatcher.close();
    }
  }

  @SuppressWarnings("java:S2925")
  public boolean checkResult(List<Log> logs, int requestedSuccess) throws InterruptedException {
    for (Log log : logs) {
      AtomicInteger atomicInteger = appendedEntries.get(log);
      if (atomicInteger == null) {
        Thread.sleep(10);
        return false;
      }
      if (atomicInteger.get() != requestedSuccess) {
        Thread.sleep(10);
        return false;
      }
    }
    return true;
  }

  @After
  public void tearDown() throws Exception {
    raftMember.stop();
    raftMember.closeLogManager();
    EnvironmentUtils.cleanAllDir();
  }
}
