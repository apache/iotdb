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

package org.apache.iotdb.confignode.manager.node;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfo;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeManagerStatusReasonTest {
  private static final int NODE_ID = 1;
  private static final int UNREPORTED_NODE_ID = 2;

  private NodeInfo nodeInfo;
  private NodeManager nodeManager;
  private AtomicReference<NodeStatistics> currentStatistics;

  @Before
  public void setUp() {
    nodeInfo = mock(NodeInfo.class);
    IManager manager = mock(IManager.class);
    LoadManager loadManager = mock(LoadManager.class);
    PartitionManager partitionManager = mock(PartitionManager.class);
    when(manager.getLoadManager()).thenReturn(loadManager);
    when(manager.getPartitionManager()).thenReturn(partitionManager);
    when(partitionManager.getAllReplicaSets()).thenReturn(Collections.emptyList());
    nodeManager = new NodeManager(manager, nodeInfo);

    currentStatistics =
        new AtomicReference<>(
            new NodeStatistics(1, NodeStatus.ReadOnly, NodeStatus.MANUAL, Long.MAX_VALUE));
    NodeStatistics running = new NodeStatistics(2, NodeStatus.Running, null, 0);
    // Simulate a heartbeat arriving immediately after the first statistics read. Reading
    // status and reason separately would mix ReadOnly with the new heartbeat's null reason.
    when(loadManager.getNodeStatisticsSnapshot())
        .thenAnswer(
            invocation -> Collections.singletonMap(NODE_ID, currentStatistics.getAndSet(running)));
    when(loadManager.getNodeStatus(NODE_ID))
        .thenAnswer(invocation -> currentStatistics.getAndSet(running).getStatus());
    when(loadManager.getNodeStatusReason(NODE_ID))
        .thenAnswer(invocation -> currentStatistics.get().getStatusReason());
    when(loadManager.getNodeStatus(UNREPORTED_NODE_ID)).thenReturn(NodeStatus.Unknown);
  }

  @Test
  public void testDataNodeStatusAndReasonRemainPairedDuringHeartbeatUpdate() {
    when(nodeInfo.getRegisteredDataNodes())
        .thenReturn(
            Arrays.asList(
                dataNodeConfiguration(NODE_ID), dataNodeConfiguration(UNREPORTED_NODE_ID)));

    List<TDataNodeInfo> result = nodeManager.getRegisteredDataNodeInfoList();

    assertEquals(2, result.size());
    assertEquals(NodeStatus.ReadOnly.getStatus(), result.get(0).getStatus());
    assertEquals(NodeStatus.MANUAL, result.get(0).getStatusReason());
    assertEquals(NodeStatus.Unknown.getStatus(), result.get(1).getStatus());
    assertNull(result.get(1).getStatusReason());
    assertEquals(NodeStatus.Running, currentStatistics.get().getStatus());
    assertNull(currentStatistics.get().getStatusReason());
  }

  @Test
  public void testConfigNodeStatusAndReasonRemainPairedDuringHeartbeatUpdate() {
    when(nodeInfo.getRegisteredConfigNodes())
        .thenReturn(
            Arrays.asList(configNodeLocation(NODE_ID), configNodeLocation(UNREPORTED_NODE_ID)));

    List<TConfigNodeInfo> result = nodeManager.getRegisteredConfigNodeInfoList();

    assertEquals(2, result.size());
    assertEquals(NodeStatus.ReadOnly.getStatus(), result.get(0).getStatus());
    assertEquals(NodeStatus.MANUAL, result.get(0).getStatusReason());
    assertEquals(NodeStatus.Unknown.getStatus(), result.get(1).getStatus());
    assertNull(result.get(1).getStatusReason());
    assertEquals(NodeStatus.Running, currentStatistics.get().getStatus());
    assertNull(currentStatistics.get().getStatusReason());
  }

  private static TDataNodeConfiguration dataNodeConfiguration(int nodeId) {
    return new TDataNodeConfiguration()
        .setLocation(
            new TDataNodeLocation()
                .setDataNodeId(nodeId)
                .setClientRpcEndPoint(new TEndPoint("127.0.0.1", 6667 + nodeId)))
        .setResource(new TNodeResource(1, 1024));
  }

  private static TConfigNodeLocation configNodeLocation(int nodeId) {
    return new TConfigNodeLocation()
        .setConfigNodeId(nodeId)
        .setInternalEndPoint(new TEndPoint("127.0.0.1", 10710 + nodeId));
  }
}
