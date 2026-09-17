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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.rpc.thrift.TLoadBalanceReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ConfigManager#findRegionsMissingStatistics}, which guards LOAD BALANCE
 * against running with incomplete RegionGroup statistics. The statistics map is built from the load
 * cache while the regions to balance come from the partition table, so the two can diverge
 * transiently (fresh region before its first heartbeat, or right after a ConfigNode-Leader switch
 * clears and repopulates the cache). When that happens LOAD BALANCE must abort instead of feeding
 * default (disk=0) statistics into the balancer, which would corrupt migration decisions.
 */
public class ConfigManagerLoadBalanceStatisticsTest {

  @Test
  public void loadBalanceSelectsOnlyRunningAndDiskFullReadOnlyNodes() {
    ConfigManager configManager = mock(ConfigManager.class, CALLS_REAL_METHODS);
    LoadManager loadManager = mock(LoadManager.class);
    LoadCache loadCache = mock(LoadCache.class);
    NodeManager nodeManager = mock(NodeManager.class);
    PartitionManager partitionManager = mock(PartitionManager.class);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getPartitionManager()).thenReturn(partitionManager);
    when(loadManager.getLoadCache()).thenReturn(loadCache);

    Map<Integer, NodeStatistics> nodeStatistics =
        Map.of(
            1, new NodeStatistics(0, NodeStatus.Running, null, 0),
            2, new NodeStatistics(0, NodeStatus.ReadOnly, NodeStatus.DISK_FULL, 0),
            3, new NodeStatistics(0, NodeStatus.ReadOnly, null, 0),
            4, new NodeStatistics(0, NodeStatus.ReadOnly, "", 0),
            5, new NodeStatistics(0, NodeStatus.ReadOnly, "Manual", 0),
            6, new NodeStatistics(0, NodeStatus.ReadOnly, "SystemError", 0),
            7, new NodeStatistics(0, NodeStatus.Unknown, NodeStatus.DISK_FULL, 0),
            8, new NodeStatistics(0, NodeStatus.Removing, NodeStatus.DISK_FULL, 0),
            9, new NodeStatistics(0, NodeStatus.ReadOnly, "DiskFullOther", 0),
            10, new NodeStatistics(0, NodeStatus.Running, "PreviousReason", 0));
    when(loadCache.getCurrentDataNodeStatisticsMap()).thenReturn(nodeStatistics);
    // Node 11 is registered but has no heartbeat statistics yet.
    List<TDataNodeConfiguration> registeredNodes =
        IntStream.rangeClosed(1, 11)
            .mapToObj(
                id ->
                    new TDataNodeConfiguration()
                        .setLocation(new TDataNodeLocation().setDataNodeId(id)))
            .collect(Collectors.toList());
    when(nodeManager.getRegisteredDataNodes()).thenReturn(registeredNodes);
    Map<Integer, TDataNodeConfiguration> expectedNodes =
        Map.of(1, registeredNodes.get(0), 2, registeredNodes.get(1), 10, registeredNodes.get(9));

    List<TRegionReplicaSet> dataRegions = Collections.singletonList(dataRegion(1));
    List<TRegionReplicaSet> schemaRegions = Collections.singletonList(schemaRegion(2));
    Map<TConsensusGroupId, RegionGroupStatistics> statistics =
        statisticsFor(Arrays.asList(dataRegions.get(0), schemaRegions.get(0)));
    when(loadCache.getCurrentRegionGroupStatisticsMap()).thenReturn(statistics);
    when(partitionManager.getAllReplicaSets(TConsensusGroupType.DataRegion))
        .thenReturn(dataRegions);
    when(partitionManager.getAllReplicaSets(TConsensusGroupType.SchemaRegion))
        .thenReturn(schemaRegions);
    when(loadManager.autoBalanceRegionReplicasDistribution(
            anyMap(), anyMap(), anyList(), anyInt(), any()))
        .thenAnswer(
            invocation -> {
              List<TRegionReplicaSet> regions = invocation.getArgument(2);
              return regions.stream()
                  .collect(Collectors.toMap(TRegionReplicaSet::getRegionId, region -> region));
            });

    for (Model model : Arrays.asList(Model.TREE, Model.TABLE)) {
      when(partitionManager.getRegionDatabase(any()))
          .thenReturn(model == Model.TREE ? "root.test" : "test");
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          configManager.loadBalance(new TLoadBalanceReq().setModel(model)).getCode());
      // DiskFull makes a node a valid source, not a valid target. All other excluded nodes and an
      // unregistered target must be rejected too.
      for (int id : new int[] {2, 3, 4, 5, 6, 7, 8, 9, 11, 12}) {
        Assert.assertEquals(
            TSStatusCode.DATANODE_NOT_EXIST.getStatusCode(),
            configManager
                .loadBalance(
                    new TLoadBalanceReq()
                        .setModel(model)
                        .setTargetNodeIds(Collections.singletonList(id)))
                .getCode());
      }
    }
    verify(loadManager, times(2))
        .autoBalanceRegionReplicasDistribution(
            eq(expectedNodes), eq(statistics), eq(dataRegions), eq(1), isNull());
    verify(loadManager, times(2))
        .autoBalanceRegionReplicasDistribution(
            eq(expectedNodes), eq(statistics), eq(schemaRegions), eq(1), isNull());
  }

  private static TRegionReplicaSet dataRegion(int id) {
    return region(TConsensusGroupType.DataRegion, id);
  }

  private static TRegionReplicaSet schemaRegion(int id) {
    return region(TConsensusGroupType.SchemaRegion, id);
  }

  private static TRegionReplicaSet region(TConsensusGroupType type, int id) {
    TRegionReplicaSet regionReplicaSet = new TRegionReplicaSet();
    regionReplicaSet.setRegionId(new TConsensusGroupId(type, id));
    regionReplicaSet.addToDataNodeLocations(new TDataNodeLocation().setDataNodeId(1));
    return regionReplicaSet;
  }

  private static Map<TConsensusGroupId, RegionGroupStatistics> statisticsFor(
      List<TRegionReplicaSet> regions) {
    Map<TConsensusGroupId, RegionGroupStatistics> statisticsMap = new TreeMap<>();
    for (TRegionReplicaSet region : regions) {
      RegionGroupStatistics statistics =
          RegionGroupStatistics.generateDefaultRegionGroupStatistics();
      statistics.setDiskUsage(100);
      statisticsMap.put(region.getRegionId(), statistics);
    }
    return statisticsMap;
  }

  @Test
  public void allStatisticsPresentReturnsEmpty() {
    List<TRegionReplicaSet> regions = Arrays.asList(dataRegion(0), dataRegion(1), dataRegion(2));
    Map<TConsensusGroupId, RegionGroupStatistics> statisticsMap = statisticsFor(regions);

    Assert.assertTrue(ConfigManager.findRegionsMissingStatistics(regions, statisticsMap).isEmpty());
  }

  @Test
  public void missingStatisticsAreReported() {
    List<TRegionReplicaSet> regions = Arrays.asList(dataRegion(0), dataRegion(1), dataRegion(2));
    // Only region 0 has statistics; regions 1 and 2 are absent from the cache.
    Map<TConsensusGroupId, RegionGroupStatistics> statisticsMap =
        statisticsFor(Collections.singletonList(dataRegion(0)));

    List<TConsensusGroupId> missing =
        ConfigManager.findRegionsMissingStatistics(regions, statisticsMap);

    Assert.assertEquals(2, missing.size());
    Assert.assertTrue(missing.contains(new TConsensusGroupId(TConsensusGroupType.DataRegion, 1)));
    Assert.assertTrue(missing.contains(new TConsensusGroupId(TConsensusGroupType.DataRegion, 2)));
    Assert.assertFalse(missing.contains(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0)));
  }

  @Test
  public void emptyRegionListReturnsEmpty() {
    Assert.assertTrue(
        ConfigManager.findRegionsMissingStatistics(Collections.emptyList(), Collections.emptyMap())
            .isEmpty());
  }

  /**
   * A disk usage of 0 is a valid (possibly empty) RegionGroup, NOT a missing one. The guard keys on
   * presence in the map, so a region with an explicit 0 entry must not be reported as missing.
   */
  @Test
  public void zeroDiskUsageIsNotMissing() {
    List<TRegionReplicaSet> regions = Collections.singletonList(dataRegion(0));
    Map<TConsensusGroupId, RegionGroupStatistics> statisticsMap = new TreeMap<>();
    statisticsMap.put(
        regions.get(0).getRegionId(),
        RegionGroupStatistics.generateDefaultRegionGroupStatistics()); // diskUsage defaults to 0

    Assert.assertTrue(ConfigManager.findRegionsMissingStatistics(regions, statisticsMap).isEmpty());
  }

  /**
   * DataRegion and SchemaRegion ids never collide (they carry distinct {@link
   * TConsensusGroupType}s), so a SchemaRegion's statistics cannot satisfy a DataRegion's lookup
   * even when their numeric ids match. This mirrors how {@code loadBalance} checks both region
   * types against the same statistics map.
   */
  @Test
  public void schemaAndDataRegionsAreCheckedIndependently() {
    List<TRegionReplicaSet> dataRegions = Arrays.asList(dataRegion(0), dataRegion(1));
    List<TRegionReplicaSet> schemaRegions = Arrays.asList(schemaRegion(0), schemaRegion(1));

    // Only DataRegion statistics are present; SchemaRegion 0/1 share numeric ids but must still be
    // reported as missing because their type differs.
    Map<TConsensusGroupId, RegionGroupStatistics> statisticsMap = statisticsFor(dataRegions);

    Assert.assertTrue(
        ConfigManager.findRegionsMissingStatistics(dataRegions, statisticsMap).isEmpty());

    List<TConsensusGroupId> allMissing = new ArrayList<>();
    allMissing.addAll(ConfigManager.findRegionsMissingStatistics(dataRegions, statisticsMap));
    allMissing.addAll(ConfigManager.findRegionsMissingStatistics(schemaRegions, statisticsMap));

    Assert.assertEquals(2, allMissing.size());
    Assert.assertTrue(
        allMissing.contains(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0)));
    Assert.assertTrue(
        allMissing.contains(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1)));
  }
}
