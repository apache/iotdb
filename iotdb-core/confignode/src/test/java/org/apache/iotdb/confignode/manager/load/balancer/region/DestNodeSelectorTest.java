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

package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DestNodeSelectorTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(DestNodeSelectorTest.class);

  private static final IDestNodeSelector SELECTOR = new PartiteGraphPlacementDestNodeSelector();
  private static final IRegionGroupAllocator ALLOCATOR = new GreedyCopySetRegionGroupAllocator();

  private static final TDataNodeLocation REMOVE_DATANODE_LOCATION =
      new TDataNodeLocation().setDataNodeId(5);
  private static final int TEST_DATA_NODE_NUM = 5;
  private static final int DATA_REGION_PER_DATA_NODE = 4;
  private static final int DATA_REPLICATION_FACTOR = 2;

  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new HashMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new HashMap<>();

  @BeforeClass
  public static void setUp() {
    // Construct TEST_DATA_NODE_NUM DataNodes
    for (int i = 1; i <= TEST_DATA_NODE_NUM; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, Math.random());
    }
  }

  @Test
  public void testSelectDestNode() {
    final int dataRegionGroupNum =
        DATA_REGION_PER_DATA_NODE * TEST_DATA_NODE_NUM / DATA_REPLICATION_FACTOR;

    List<TRegionReplicaSet> allocateResult = new ArrayList<>();
    for (int index = 0; index < dataRegionGroupNum; index++) {
      allocateResult.add(
          ALLOCATOR.generateOptimalRegionReplicasDistribution(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              allocateResult,
              allocateResult,
              DATA_REPLICATION_FACTOR,
              new TConsensusGroupId(TConsensusGroupType.DataRegion, index)));
    }
    List<TRegionReplicaSet> migratedReplicas =
        allocateResult.stream()
            .filter(
                replicaSet -> replicaSet.getDataNodeLocations().contains(REMOVE_DATANODE_LOCATION))
            .collect(Collectors.toList());

    AVAILABLE_DATA_NODE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());
    FREE_SPACE_MAP.remove(REMOVE_DATANODE_LOCATION.getDataNodeId());

    List<TRegionReplicaSet> remainReplicas = new ArrayList<>();

    for (TRegionReplicaSet replicaSet : migratedReplicas) {
      List<TDataNodeLocation> dataNodeLocations = replicaSet.getDataNodeLocations();
      allocateResult.remove(replicaSet);
      dataNodeLocations.remove(REMOVE_DATANODE_LOCATION);
      replicaSet.setDataNodeLocations(dataNodeLocations);
      allocateResult.add(replicaSet);
      remainReplicas.add(replicaSet);
    }

    Set<Integer> selectedNodeIds = new HashSet<>();

    for (TRegionReplicaSet remainReplicaSet : remainReplicas) {
      TDataNodeConfiguration selectedNode =
          SELECTOR.selectDestDataNode(
              AVAILABLE_DATA_NODE_MAP,
              FREE_SPACE_MAP,
              allocateResult,
              allocateResult,
              DATA_REPLICATION_FACTOR,
              remainReplicaSet.regionId,
              remainReplicaSet);
      LOGGER.info(
          "Selected DataNode {} for Region {}",
          selectedNode.getLocation().getDataNodeId(),
          remainReplicaSet.regionId);
      allocateResult.remove(remainReplicaSet);
      List<TDataNodeLocation> dataNodeLocations = remainReplicaSet.getDataNodeLocations();
      dataNodeLocations.add(selectedNode.getLocation());
      remainReplicaSet.setDataNodeLocations(dataNodeLocations);
      allocateResult.add(remainReplicaSet);
      selectedNodeIds.add(selectedNode.getLocation().getDataNodeId());
    }

    Assert.assertEquals(TEST_DATA_NODE_NUM - 1, selectedNodeIds.size());
  }
}
