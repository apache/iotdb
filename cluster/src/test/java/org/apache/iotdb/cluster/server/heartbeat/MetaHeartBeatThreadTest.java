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

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.common.TestClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.member.RaftMember;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;

public class MetaHeartBeatThreadTest extends HeartBeatThreadTest {

  private Set<Node> idConflictNodes = new HashSet<>();
  private ByteBuffer partitionTableBuffer;
  private PartitionTable partitionTable = new PartitionTable() {
    @Override
    public PartitionGroup route(String storageGroupName, long timestamp) {
      return null;
    }

    @Override
    public int getPartitionKey(String storageGroupName, long timestamp) {
      return PartitionUtils.calculateStorageGroupSlot(storageGroupName, timestamp, getTotalSlotNumbers());
    }

    @Override
    public PartitionGroup route(int slot) {
      return null;
    }

    @Override
    public Node routeToHeader(String storageGroupName, long timestamp) {
      return null;
    }

    @Override
    public PartitionGroup addNode(Node node) {
      return null;
    }

    @Override
    public NodeRemovalResult removeNode(Node node) {
      return null;
    }

    @Override
    public List<PartitionGroup> getLocalGroups() {
      return null;
    }

    @Override
    public PartitionGroup getHeaderGroup(Node header) {
      return null;
    }

    @Override
    public ByteBuffer serialize() {
      return partitionTableBuffer;
    }

    @Override
    public void deserialize(ByteBuffer buffer) {

    }

    @Override
    public List<Node> getAllNodes() {
      return null;
    }

    @Override
    public Map<Integer, Node> getPreviousNodeMap(Node node) {
      return null;
    }

    @Override
    public List<Integer> getNodeSlots(Node header) {
      return null;
    }

    @Override
    public Map<Node, List<Integer>> getAllNodeSlots() {
      return null;
    }

    @Override
    public int getTotalSlotNumbers() {
      return 100;
    }

    @Override
    public MManager getMManager() {
      return MManager.getInstance();
    }
  };

  @Override
  RaftMember getMember() {
    return new TestMetaGroupMember() {

      @Override
      public LogManager getLogManager() {
        return MetaHeartBeatThreadTest.this.logManager;
      }

      @Override
      public AsyncClient connectNode(Node node) {
        return getClient(node);
      }

      @Override
      public Set<Node> getIdConflictNodes() {
        return MetaHeartBeatThreadTest.this.idConflictNodes;
      }

      @Override
      public boolean isNodeBlind(Node node) {
        return 6 <= node.getNodeIdentifier() && node.getNodeIdentifier() < 9;
      }

      @Override
      public PartitionTable getPartitionTable() {
        return MetaHeartBeatThreadTest.this.partitionTable;
      }
    };
  }

  @Override
  AsyncClient getClient(Node node) {
    return new TestClient(node.nodeIdentifier) {
      @Override
      public void sendHeartBeat(HeartBeatRequest request,
          AsyncMethodCallback<HeartBeatResponse> resultHandler) {
        HeartBeatRequest requestCopy = new HeartBeatRequest(request);
        new Thread(() -> {
          if (testHeartBeat) {
            assertEquals(TestUtils.getNode(0), requestCopy.getLeader());
            assertEquals(7, requestCopy.getCommitLogIndex());
            assertEquals(10, requestCopy.getTerm());
            assertNull(requestCopy.getHeader());
            if (node.getNodeIdentifier() < 3) {
              assertTrue(requestCopy.isRegenerateIdentifier());
            } else if (node.getNodeIdentifier() < 6) {
              assertTrue(requestCopy.isRequireIdentifier());
            } else if (node.getNodeIdentifier() < 9) {
              assertEquals(partitionTableBuffer, requestCopy .partitionTableBytes);
            }
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

  @Override
  @Before
  public void setUp() {
    super.setUp();
    for (int i = 0; i < 3; i++) {
      idConflictNodes.add(TestUtils.getNode(i));
    }
    for (Node node : member.getAllNodes()) {
      if (3 <= node.getNodeIdentifier() && node.getNodeIdentifier() < 6) {
        node.unsetNodeIdentifier();
      }
    }
    partitionTableBuffer = ByteBuffer.allocate(1024);
    partitionTableBuffer.put("Just a partition table".getBytes());
  }

  @Override
  HeartBeatThread getHeartBeatThread(RaftMember member) {
    return new MetaHeartBeatThread((MetaGroupMember) member);
  }
}