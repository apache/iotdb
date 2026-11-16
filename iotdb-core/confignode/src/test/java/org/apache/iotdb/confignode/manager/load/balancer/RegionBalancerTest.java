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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
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
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.argThat;
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
    // With an empty allotment only the candidate filtering runs. Verify the supplied predicate
    // accepts Running, Unknown and Stopped while excluding ReadOnly and Removing.
    when(procedureManager.getRemovingDataNodeIds()).thenReturn(Collections.emptySet());
    when(nodeManager.filterDataNodeThroughStatus(
            ArgumentMatchers.<Function<NodeStatus, Boolean>>any()))
        .thenReturn(Arrays.asList(stoppedDataNode, removingDataNode));

    regionBalancer.genRegionGroupsAllocationPlan(
        Collections.emptyMap(), TConsensusGroupType.DataRegion);

    verify(nodeManager)
        .filterDataNodeThroughStatus(
            argThat(
                (Function<NodeStatus, Boolean> predicate) ->
                    predicate != null
                        && predicate.apply(NodeStatus.Running)
                        && predicate.apply(NodeStatus.Unknown)
                        && predicate.apply(NodeStatus.Stopped)
                        && !predicate.apply(NodeStatus.ReadOnly)
                        && !predicate.apply(NodeStatus.Removing)));
  }

  @Test
  public void testRemovingDataNodesAreExcludedFromCandidates() throws Exception {
    when(procedureManager.getRemovingDataNodeIds())
        .thenReturn(new HashSet<>(Collections.singletonList(REMOVING_DATA_NODE_ID)));
    // The mocked status filter returns both nodes; the in-progress removal must drop the second.
    when(nodeManager.filterDataNodeThroughStatus(
            ArgumentMatchers.<Function<NodeStatus, Boolean>>any()))
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

  @Test
  public void testPreferredDataNodeIsPinnedForDataRegions() throws Exception {
    mockDataNodeCandidates(1, 2, 3, 4, 5);
    when(partitionManager.getAllReplicaSets(TConsensusGroupType.DataRegion))
        .thenReturn(new ArrayList<>());
    when(partitionManager.getAllReplicaSets(eq("db1"), eq(TConsensusGroupType.DataRegion)))
        .thenReturn(new ArrayList<>());
    when(clusterSchemaManager.getReplicationFactor(eq("db1"), eq(TConsensusGroupType.DataRegion)))
        .thenReturn(3);
    when(partitionManager.generateNextRegionGroupId()).thenReturn(0, 1, 2);

    final CreateRegionGroupsPlan plan =
        regionBalancer.genRegionGroupsAllocationPlan(
            Collections.singletonMap("db1", 3), TConsensusGroupType.DataRegion, 4);

    final List<TRegionReplicaSet> regionGroups = plan.getRegionGroupMap().get("db1");
    Assert.assertEquals(3, regionGroups.size());
    for (final TRegionReplicaSet regionGroup : regionGroups) {
      Assert.assertEquals(3, regionGroup.getDataNodeLocationsSize());
      // The preferred DataNode is pinned as the first replica
      Assert.assertEquals(4, regionGroup.getDataNodeLocations().get(0).getDataNodeId());
      // The remaining replicas are still chosen by the allocator, without duplicates
      Assert.assertEquals(
          3,
          regionGroup.getDataNodeLocations().stream()
              .map(location -> location.getDataNodeId())
              .collect(Collectors.toSet())
              .size());
    }
  }

  @Test
  public void testPreferredDataNodeOutsideCandidatesIsIgnored() throws Exception {
    mockDataNodeCandidates(1, 2, 3);
    when(partitionManager.getAllReplicaSets(TConsensusGroupType.DataRegion))
        .thenReturn(new ArrayList<>());
    when(partitionManager.getAllReplicaSets(eq("db1"), eq(TConsensusGroupType.DataRegion)))
        .thenReturn(new ArrayList<>());
    when(clusterSchemaManager.getReplicationFactor(eq("db1"), eq(TConsensusGroupType.DataRegion)))
        .thenReturn(3);
    when(partitionManager.generateNextRegionGroupId()).thenReturn(0);

    // The preferred DataNode is not a placement candidate, so the allocation must still succeed
    // without placing a replica on it
    final CreateRegionGroupsPlan plan =
        regionBalancer.genRegionGroupsAllocationPlan(
            Collections.singletonMap("db1", 1), TConsensusGroupType.DataRegion, 9);
    final TRegionReplicaSet regionGroup = plan.getRegionGroupMap().get("db1").get(0);
    Assert.assertEquals(3, regionGroup.getDataNodeLocationsSize());
    Assert.assertEquals(
        new HashSet<>(Arrays.asList(1, 2, 3)),
        regionGroup.getDataNodeLocations().stream()
            .map(location -> location.getDataNodeId())
            .collect(Collectors.toSet()));
  }

  @Test
  public void testPreferredDataNodeThatIsNotRunningIsIgnored() throws Exception {
    // DataNode 4 is stopped. It stays a Region candidate -- RegionGroups are still created while
    // too few DataNodes are online -- but it must never be pinned: with replication factor 1 the
    // new RegionGroup would otherwise be created without any working replica.
    when(procedureManager.getRemovingDataNodeIds()).thenReturn(Collections.emptySet());
    when(nodeManager.filterDataNodeThroughStatus(
            ArgumentMatchers.<Function<NodeStatus, Boolean>>any()))
        .thenAnswer(
            invocation -> {
              final Function<NodeStatus, Boolean> predicate = invocation.getArgument(0);
              final boolean runningOnly = !predicate.apply(NodeStatus.Stopped);
              final List<Integer> dataNodeIds =
                  runningOnly ? Arrays.asList(1, 2, 3, 5) : Arrays.asList(1, 2, 3, 4, 5);
              return dataNodeIds.stream()
                  .map(
                      dataNodeId ->
                          new TDataNodeConfiguration()
                              .setLocation(new TDataNodeLocation().setDataNodeId(dataNodeId)))
                  .collect(Collectors.toList());
            });
    when(partitionManager.getAllReplicaSets(TConsensusGroupType.DataRegion))
        .thenReturn(new ArrayList<>());
    when(partitionManager.getAllReplicaSets(eq("db1"), eq(TConsensusGroupType.DataRegion)))
        .thenReturn(new ArrayList<>());
    when(clusterSchemaManager.getReplicationFactor(eq("db1"), eq(TConsensusGroupType.DataRegion)))
        .thenReturn(1);

    final int allotment = 20;
    final AtomicInteger nextRegionGroupId = new AtomicInteger();
    when(partitionManager.generateNextRegionGroupId())
        .thenAnswer(invocation -> nextRegionGroupId.getAndIncrement());

    final CreateRegionGroupsPlan plan =
        regionBalancer.genRegionGroupsAllocationPlan(
            Collections.singletonMap("db1", allotment), TConsensusGroupType.DataRegion, 4);

    final List<TRegionReplicaSet> regionGroups = plan.getRegionGroupMap().get("db1");
    Assert.assertEquals(allotment, regionGroups.size());
    // With replication factor 1 the preference would select DataNode 4 for every RegionGroup if it
    // were applied, so at least one RegionGroup must land elsewhere
    final long pinnedCount =
        regionGroups.stream()
            .filter(regionGroup -> regionGroup.getDataNodeLocations().get(0).getDataNodeId() == 4)
            .count();
    Assert.assertTrue(
        "The preference of a stopped DataNode must be dropped, but all RegionGroups were pinned",
        pinnedCount < allotment);
  }

  @Test
  public void testPreferredDataNodeIsIgnoredForSchemaRegions() throws Exception {
    mockDataNodeCandidates(1, 2, 3, 4, 5);
    when(partitionManager.getAllReplicaSets(TConsensusGroupType.SchemaRegion))
        .thenReturn(new ArrayList<>());
    when(partitionManager.getAllReplicaSets(eq("db1"), eq(TConsensusGroupType.SchemaRegion)))
        .thenReturn(new ArrayList<>());
    when(clusterSchemaManager.getReplicationFactor(eq("db1"), eq(TConsensusGroupType.SchemaRegion)))
        .thenReturn(1);
    when(partitionManager.generateNextRegionGroupId()).thenReturn(0);

    // With replication factor 1 the preference would always select DataNode 5 if it were applied.
    // SchemaRegions must ignore it, so the selected DataNode varies with the random tie-breaker.
    final Set<Integer> selectedDataNodes = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      final CreateRegionGroupsPlan plan =
          regionBalancer.genRegionGroupsAllocationPlan(
              Collections.singletonMap("db1", 1), TConsensusGroupType.SchemaRegion, 5);
      final TRegionReplicaSet regionGroup = plan.getRegionGroupMap().get("db1").get(0);
      Assert.assertEquals(1, regionGroup.getDataNodeLocationsSize());
      selectedDataNodes.add(regionGroup.getDataNodeLocations().get(0).getDataNodeId());
    }
    Assert.assertTrue(
        "SchemaRegion allocation should ignore the DataRegion preference: " + selectedDataNodes,
        selectedDataNodes.size() > 1);
  }

  private void mockDataNodeCandidates(int... dataNodeIds) {
    when(procedureManager.getRemovingDataNodeIds()).thenReturn(Collections.emptySet());
    when(nodeManager.filterDataNodeThroughStatus(
            ArgumentMatchers.<Function<NodeStatus, Boolean>>any()))
        .thenReturn(
            Arrays.stream(dataNodeIds)
                .mapToObj(
                    dataNodeId ->
                        new TDataNodeConfiguration()
                            .setLocation(new TDataNodeLocation().setDataNodeId(dataNodeId)))
                .collect(Collectors.toList()));
  }
}
