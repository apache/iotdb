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

package org.apache.iotdb.confignode.manager.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionGroupStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.confignode.manager.partition.RegionGroupStatus;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LoadManagerTest {
  private static LoadManager LOAD_MANAGER;
  private static LoadCache LOAD_CACHE;
  private static FakeSubscriber FAKE_SUBSCRIBER;
  private static Semaphore NODE_SEMAPHORE;
  private static Semaphore REGION_GROUP_SEMAPHORE;
  private static Semaphore CONSENSUS_GROUP_SEMAPHORE;

  @BeforeClass
  public static void setUp() throws IOException {
    IManager CONFIG_MANAGER = new ConfigManager();
    LOAD_MANAGER = CONFIG_MANAGER.getLoadManager();
    LOAD_CACHE = LOAD_MANAGER.getLoadCache();
  }

  @Before
  public void renewFakeSubscriber() {
    NODE_SEMAPHORE = new Semaphore(0);
    REGION_GROUP_SEMAPHORE = new Semaphore(0);
    CONSENSUS_GROUP_SEMAPHORE = new Semaphore(0);
    FAKE_SUBSCRIBER =
        new FakeSubscriber(NODE_SEMAPHORE, REGION_GROUP_SEMAPHORE, CONSENSUS_GROUP_SEMAPHORE);
    LOAD_MANAGER.getEventService().getEventPublisher().register(FAKE_SUBSCRIBER);
  }

  @Test
  public void testNodeCache() throws InterruptedException {
    // Create 1 ConfigNode and 1 DataNode heartbeat cache
    LOAD_CACHE.createNodeHeartbeatCache(NodeType.ConfigNode, 0);
    LOAD_CACHE.createNodeHeartbeatCache(NodeType.DataNode, 1);

    // Default NodeStatus is Unknown
    Assert.assertEquals(NodeStatus.Unknown, LOAD_CACHE.getNodeStatus(0));
    Assert.assertEquals(NodeStatus.Unknown, LOAD_CACHE.getNodeStatus(1));

    // Simulate update to Running status
    LOAD_CACHE.cacheConfigNodeHeartbeatSample(0, new NodeHeartbeatSample(NodeStatus.Running));
    LOAD_CACHE.cacheDataNodeHeartbeatSample(1, new NodeHeartbeatSample(NodeStatus.Running));
    LOAD_CACHE.updateNodeStatistics(false);
    LOAD_MANAGER.getEventService().checkAndBroadcastNodeStatisticsChangeEventIfNecessary();
    NODE_SEMAPHORE.acquire();
    Assert.assertEquals(NodeStatus.Running, LOAD_CACHE.getNodeStatus(0));
    Assert.assertEquals(NodeStatus.Running, LOAD_CACHE.getNodeStatus(1));
    Map<Integer, Pair<NodeStatistics, NodeStatistics>> differentNodeStatisticsMap =
        FAKE_SUBSCRIBER.getDifferentNodeStatisticsMap();
    Assert.assertEquals(
        new Pair<>(null, new NodeStatistics(NodeStatus.Running)),
        differentNodeStatisticsMap.get(0));
    Assert.assertEquals(
        new Pair<>(null, new NodeStatistics(NodeStatus.Running)),
        differentNodeStatisticsMap.get(1));

    // Force update to Removing status
    LOAD_MANAGER.forceUpdateNodeCache(
        NodeType.DataNode, 1, new NodeHeartbeatSample(NodeStatus.Removing));
    NODE_SEMAPHORE.acquire();
    Assert.assertEquals(NodeStatus.Removing, LOAD_CACHE.getNodeStatus(1));
    differentNodeStatisticsMap = FAKE_SUBSCRIBER.getDifferentNodeStatisticsMap();
    // Only DataNode 1 is updated
    Assert.assertEquals(1, differentNodeStatisticsMap.size());
    Assert.assertEquals(
        new Pair<>(new NodeStatistics(NodeStatus.Running), new NodeStatistics(NodeStatus.Removing)),
        differentNodeStatisticsMap.get(1));

    // Removing status can't be updated to any other status automatically
    LOAD_CACHE.cacheDataNodeHeartbeatSample(1, new NodeHeartbeatSample(NodeStatus.ReadOnly));
    LOAD_CACHE.updateNodeStatistics(false);
    LOAD_MANAGER.getEventService().checkAndBroadcastNodeStatisticsChangeEventIfNecessary();
    Assert.assertEquals(NodeStatus.Removing, LOAD_CACHE.getNodeStatus(1));

    // Remove NodeCache
    LOAD_MANAGER.removeNodeCache(1);
    NODE_SEMAPHORE.acquire();
    differentNodeStatisticsMap = FAKE_SUBSCRIBER.getDifferentNodeStatisticsMap();
    // Only DataNode 1 is updated
    Assert.assertEquals(1, differentNodeStatisticsMap.size());
    Assert.assertEquals(
        new Pair<>(new NodeStatistics(NodeStatus.Removing), null),
        differentNodeStatisticsMap.get(1));
  }

  @Test
  public void testRegionGroupCache() throws InterruptedException {
    // Create 1 RegionGroup heartbeat cache
    TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 0);
    Set<Integer> dataNodeIds = Stream.of(0, 1, 2).collect(Collectors.toSet());
    LOAD_CACHE.createRegionGroupHeartbeatCache("root.db", regionGroupId, dataNodeIds);

    // Default RegionGroupStatus is Disabled and RegionStatus are Unknown
    Assert.assertEquals(RegionGroupStatus.Disabled, LOAD_CACHE.getRegionGroupStatus(regionGroupId));
    dataNodeIds.forEach(
        dataNodeId ->
            Assert.assertEquals(
                RegionStatus.Unknown, LOAD_CACHE.getRegionStatus(regionGroupId, dataNodeId)));

    // Simulate update Regions to Running status
    dataNodeIds.forEach(
        dataNodeId ->
            LOAD_CACHE.cacheRegionHeartbeatSample(
                regionGroupId, dataNodeId, new RegionHeartbeatSample(RegionStatus.Running), false));
    LOAD_CACHE.updateRegionGroupStatistics();
    LOAD_MANAGER.getEventService().checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary();
    REGION_GROUP_SEMAPHORE.acquire();
    Assert.assertEquals(RegionGroupStatus.Running, LOAD_CACHE.getRegionGroupStatus(regionGroupId));
    dataNodeIds.forEach(
        dataNodeId ->
            Assert.assertEquals(
                RegionStatus.Running, LOAD_CACHE.getRegionStatus(regionGroupId, dataNodeId)));
    Map<TConsensusGroupId, Pair<RegionGroupStatistics, RegionGroupStatistics>>
        differentRegionGroupStatisticsMap = FAKE_SUBSCRIBER.getDifferentRegionGroupStatisticsMap();
    Map<Integer, RegionStatistics> allRunningRegionStatisticsMap =
        dataNodeIds.stream()
            .collect(
                Collectors.toMap(
                    dataNodeId -> dataNodeId,
                    dataNodeId -> new RegionStatistics(RegionStatus.Running)));
    Assert.assertEquals(
        new Pair<>(
            null,
            new RegionGroupStatistics(RegionGroupStatus.Running, allRunningRegionStatisticsMap)),
        differentRegionGroupStatisticsMap.get(regionGroupId));

    // Simulate Region migration from [0, 1, 2] to [1, 2, 3]
    int removeDataNodeId = 0;
    LOAD_MANAGER.forceUpdateRegionCache(regionGroupId, removeDataNodeId, RegionStatus.Removing);
    REGION_GROUP_SEMAPHORE.acquire();
    // Mark Region 0 as Removing
    Assert.assertEquals(
        RegionStatus.Removing, LOAD_CACHE.getRegionStatus(regionGroupId, removeDataNodeId));
    differentRegionGroupStatisticsMap = FAKE_SUBSCRIBER.getDifferentRegionGroupStatisticsMap();
    Map<Integer, RegionStatistics> oneRemovingRegionStatisticsMap =
        new TreeMap<>(allRunningRegionStatisticsMap);
    oneRemovingRegionStatisticsMap.replace(
        removeDataNodeId, new RegionStatistics(RegionStatus.Removing));
    Assert.assertEquals(
        new Pair<>(
            new RegionGroupStatistics(RegionGroupStatus.Running, allRunningRegionStatisticsMap),
            new RegionGroupStatistics(RegionGroupStatus.Available, oneRemovingRegionStatisticsMap)),
        differentRegionGroupStatisticsMap.get(regionGroupId));
    // Add and mark Region 3 as Adding
    int addDataNodeId = 3;
    LOAD_CACHE.createRegionCache(regionGroupId, addDataNodeId);
    LOAD_MANAGER.forceUpdateRegionCache(regionGroupId, addDataNodeId, RegionStatus.Adding);
    REGION_GROUP_SEMAPHORE.acquire();
    Assert.assertEquals(
        RegionStatus.Adding, LOAD_CACHE.getRegionStatus(regionGroupId, addDataNodeId));
    differentRegionGroupStatisticsMap = FAKE_SUBSCRIBER.getDifferentRegionGroupStatisticsMap();
    Map<Integer, RegionStatistics> oneAddingRegionStatisticsMap =
        new TreeMap<>(oneRemovingRegionStatisticsMap);
    oneAddingRegionStatisticsMap.put(addDataNodeId, new RegionStatistics(RegionStatus.Adding));
    Assert.assertEquals(
        new Pair<>(
            new RegionGroupStatistics(RegionGroupStatus.Available, oneRemovingRegionStatisticsMap),
            new RegionGroupStatistics(RegionGroupStatus.Available, oneAddingRegionStatisticsMap)),
        differentRegionGroupStatisticsMap.get(regionGroupId));
    // Both Region 0 and 3 can't be updated
    LOAD_CACHE.cacheRegionHeartbeatSample(
        regionGroupId, removeDataNodeId, new RegionHeartbeatSample(RegionStatus.Unknown), false);
    LOAD_CACHE.cacheRegionHeartbeatSample(
        regionGroupId, addDataNodeId, new RegionHeartbeatSample(RegionStatus.ReadOnly), false);
    LOAD_CACHE.updateRegionGroupStatistics();
    LOAD_MANAGER.getEventService().checkAndBroadcastRegionGroupStatisticsChangeEventIfNecessary();
    Assert.assertEquals(
        RegionStatus.Removing, LOAD_CACHE.getRegionStatus(regionGroupId, removeDataNodeId));
    Assert.assertEquals(
        RegionStatus.Adding, LOAD_CACHE.getRegionStatus(regionGroupId, addDataNodeId));
    // Adding process completed
    LOAD_MANAGER.forceUpdateRegionCache(regionGroupId, addDataNodeId, RegionStatus.Running);
    REGION_GROUP_SEMAPHORE.acquire();
    Assert.assertEquals(
        RegionStatus.Running, LOAD_CACHE.getRegionStatus(regionGroupId, addDataNodeId));
    differentRegionGroupStatisticsMap = FAKE_SUBSCRIBER.getDifferentRegionGroupStatisticsMap();
    oneRemovingRegionStatisticsMap.put(addDataNodeId, new RegionStatistics(RegionStatus.Running));
    Assert.assertEquals(
        new Pair<>(
            new RegionGroupStatistics(RegionGroupStatus.Available, oneAddingRegionStatisticsMap),
            new RegionGroupStatistics(RegionGroupStatus.Available, oneRemovingRegionStatisticsMap)),
        differentRegionGroupStatisticsMap.get(regionGroupId));
    // Removing process completed
    LOAD_MANAGER.removeRegionCache(regionGroupId, removeDataNodeId);
    REGION_GROUP_SEMAPHORE.acquire();
    differentRegionGroupStatisticsMap = FAKE_SUBSCRIBER.getDifferentRegionGroupStatisticsMap();
    allRunningRegionStatisticsMap.remove(removeDataNodeId);
    allRunningRegionStatisticsMap.put(addDataNodeId, new RegionStatistics(RegionStatus.Running));
    Assert.assertEquals(
        new Pair<>(
            new RegionGroupStatistics(RegionGroupStatus.Available, oneRemovingRegionStatisticsMap),
            new RegionGroupStatistics(RegionGroupStatus.Running, allRunningRegionStatisticsMap)),
        differentRegionGroupStatisticsMap.get(regionGroupId));
  }

  @Test
  public void testConsensusGroupCache() throws InterruptedException {
    // Create 1 Consensus heartbeat cache
    TConsensusGroupId regionGroupId = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1);
    Set<Integer> dataNodeIds = Stream.of(0, 1, 2).collect(Collectors.toSet());
    LOAD_CACHE.createRegionGroupHeartbeatCache("root.db", regionGroupId, dataNodeIds);

    // Default ConsensusGroupStatus is leaderId == -1
    Assert.assertEquals(-1, (int) LOAD_CACHE.getRegionLeaderMap().get(regionGroupId));

    // Simulate select leaderId == 1
    int originLeaderId = 1;
    LOAD_CACHE.cacheConsensusSample(
        regionGroupId, new ConsensusGroupHeartbeatSample(originLeaderId));
    LOAD_CACHE.updateConsensusGroupStatistics();
    LOAD_MANAGER
        .getEventService()
        .checkAndBroadcastConsensusGroupStatisticsChangeEventIfNecessary();
    CONSENSUS_GROUP_SEMAPHORE.acquire();
    Map<TConsensusGroupId, Pair<ConsensusGroupStatistics, ConsensusGroupStatistics>>
        differentConsensusGroupStatisticsMap =
            FAKE_SUBSCRIBER.getDifferentConsensusGroupStatisticsMap();
    Assert.assertEquals(
        new Pair<>(null, new ConsensusGroupStatistics(originLeaderId)),
        differentConsensusGroupStatisticsMap.get(regionGroupId));

    // Force update leader to 2
    int newLeaderId = 2;
    LOAD_MANAGER.forceUpdateConsensusGroupCache(
        Collections.singletonMap(regionGroupId, new ConsensusGroupHeartbeatSample(newLeaderId)));
    CONSENSUS_GROUP_SEMAPHORE.acquire();
    differentConsensusGroupStatisticsMap =
        FAKE_SUBSCRIBER.getDifferentConsensusGroupStatisticsMap();
    Assert.assertEquals(
        new Pair<>(
            new ConsensusGroupStatistics(originLeaderId),
            new ConsensusGroupStatistics(newLeaderId)),
        differentConsensusGroupStatisticsMap.get(regionGroupId));

    // Remove ConsensusGroupCache
    LOAD_MANAGER.removeRegionGroupRelatedCache(regionGroupId);
    CONSENSUS_GROUP_SEMAPHORE.acquire();
    differentConsensusGroupStatisticsMap =
        FAKE_SUBSCRIBER.getDifferentConsensusGroupStatisticsMap();
    // Only SchemaRegionGroup 1 is updated
    Assert.assertEquals(1, differentConsensusGroupStatisticsMap.size());
    Assert.assertEquals(
        new Pair<>(new ConsensusGroupStatistics(newLeaderId), null),
        differentConsensusGroupStatisticsMap.get(regionGroupId));
  }
}
