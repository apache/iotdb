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

package org.apache.iotdb.confignode.manager.load.balancer;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.exception.NotEnoughDataNodeException;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RegionBalancerTest {

  private static final int STOPPED_DATA_NODE_ID = 1;
  private static final int REMOVING_DATA_NODE_ID = 2;

  private final IManager configManager = mock(IManager.class);
  private final NodeManager nodeManager = mock(NodeManager.class);
  private final ProcedureManager procedureManager = mock(ProcedureManager.class);
  private final ClusterSchemaManager clusterSchemaManager = mock(ClusterSchemaManager.class);
  private final PartitionManager partitionManager = mock(PartitionManager.class);
  private final LoadManager loadManager = mock(LoadManager.class);

  private final TDataNodeConfiguration stoppedDataNode =
      new TDataNodeConfiguration()
          .setLocation(
              new TDataNodeLocation(
                  STOPPED_DATA_NODE_ID,
                  new TEndPoint("127.0.0.1", 2000),
                  new TEndPoint("127.0.0.1", 2001),
                  new TEndPoint("127.0.0.1", 2002),
                  new TEndPoint("127.0.0.1", 2003),
                  new TEndPoint("127.0.0.1", 2004)));
  private final TDataNodeConfiguration removingDataNode =
      new TDataNodeConfiguration()
          .setLocation(
              new TDataNodeLocation(
                  REMOVING_DATA_NODE_ID,
                  new TEndPoint("127.0.0.1", 3000),
                  new TEndPoint("127.0.0.1", 3001),
                  new TEndPoint("127.0.0.1", 3002),
                  new TEndPoint("127.0.0.1", 3003),
                  new TEndPoint("127.0.0.1", 3004)));

  private final RegionBalancer regionBalancer = new RegionBalancer(configManager);

  @Before
  public void setUp() {
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
    when(configManager.getClusterSchemaManager()).thenReturn(clusterSchemaManager);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
  }

  @Test
  public void testStoppedDataNodesAreKeptAsAllocationCandidates() throws Exception {
    // With an empty allotment only the candidate filtering runs. The stub below locks the exact
    // status candidate list: if Stopped is dropped from the production call, the stub no longer
    // matches and the test fails.
    when(procedureManager.getRemovingDataNodeIds()).thenReturn(Collections.emptySet());
    when(nodeManager.filterDataNodeThroughStatus(
            NodeStatus.Running, NodeStatus.Unknown, NodeStatus.Stopped))
        .thenReturn(Arrays.asList(stoppedDataNode, removingDataNode));

    regionBalancer.genRegionGroupsAllocationPlan(
        Collections.emptyMap(), TConsensusGroupType.DataRegion);

    verify(nodeManager)
        .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.Unknown, NodeStatus.Stopped);
  }

  @Test
  public void testRemovingDataNodesAreExcludedFromCandidates() throws Exception {
    when(procedureManager.getRemovingDataNodeIds())
        .thenReturn(new HashSet<>(Collections.singletonList(REMOVING_DATA_NODE_ID)));
    // The mocked status filter returns both nodes; the in-progress removal must drop the second.
    when(nodeManager.filterDataNodeThroughStatus(
            NodeStatus.Running, NodeStatus.Unknown, NodeStatus.Stopped))
        .thenReturn(Arrays.asList(stoppedDataNode, removingDataNode));
    when(clusterSchemaManager.getReplicationFactor(eq("db1"), eq(TConsensusGroupType.DataRegion)))
        .thenReturn(3);

    try {
      regionBalancer.genRegionGroupsAllocationPlan(
          Collections.singletonMap("db1", 1), TConsensusGroupType.DataRegion);
      Assert.fail("Expected NotEnoughDataNodeException");
    } catch (NotEnoughDataNodeException e) {
      // The remaining candidates keep the Stopped node but not the node being removed.
      Assert.assertTrue(
          "Stopped node should be kept: " + e.getMessage(),
          e.getMessage().contains("dataNodeId:" + STOPPED_DATA_NODE_ID));
      Assert.assertFalse(
          "Removing node should be excluded: " + e.getMessage(),
          e.getMessage().contains("dataNodeId:" + REMOVING_DATA_NODE_ID));
    }
  }
}
