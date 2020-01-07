/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query.manage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.common.TestMetaClient;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.Before;
import org.junit.Test;

public class QueryCoordinatorTest {

  private MetaGroupMember metaGroupMember;
  private TestMetaClient metaClient;
  private Map<Node, NodeStatus> nodeStatusMap;
  private Map<Node, Long> nodeLatencyMap;
  private QueryCoordinator coordinator = QueryCoordinator.getINSTANCE();

  @Before
  public void setUp() throws IOException {
    nodeStatusMap = new HashMap<>();
    nodeLatencyMap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      NodeStatus status = new NodeStatus();
      TNodeStatus nodeStatus = new TNodeStatus();
      status.setStatus(nodeStatus);
      status.setLastResponseLatency(i);
      Node node = TestUtils.getNode(i);
      nodeStatusMap.put(node, status);
      // nodes with smaller num has lower latency
      nodeLatencyMap.put(node, i * 10L);
    }
    metaClient = new TestMetaClient(new Factory(),  new TAsyncClientManager(), TestUtils.getNode(0),
        null) {
      @Override
      public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
        new Thread(() -> {
          try {
            Thread.sleep(nodeLatencyMap.get(getNode()));
          } catch (InterruptedException e) {
            // ignored
          }
          resultHandler.onComplete(nodeStatusMap.get(getNode()).getStatus());
        }).start();
      }
    };

    metaGroupMember = new MetaGroupMember() {
      @Override
      public AsyncClient connectNode(Node node) {
        metaClient.setNode(node);
        return metaClient;
      }
    };
    coordinator.setMetaGroupMember(metaGroupMember);
  }

  @Test
  public void test() {
    List<Node> orderedNodes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      orderedNodes.add(TestUtils.getNode(i));
    }
    List<Node> unorderedNodes = new ArrayList<>(orderedNodes);
    Collections.shuffle(unorderedNodes);

    List<Node> reorderedNodes = coordinator.reorderNodes(unorderedNodes);
    assertEquals(orderedNodes, reorderedNodes);
  }
}