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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.NodeManager;
import org.apache.iotdb.confignode.manager.PartitionManager;
import org.apache.iotdb.confignode.manager.load.balancer.router.IRouter;
import org.apache.iotdb.confignode.manager.load.balancer.router.LazyGreedyRouter;
import org.apache.iotdb.confignode.manager.load.balancer.router.LeaderRouter;
import org.apache.iotdb.confignode.manager.load.balancer.router.LoadScoreGreedyRouter;
import org.apache.iotdb.consensus.ConsensusFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The RouteBalancer plays the role of load information collector since different routing policy
 * need different load information.
 */
public class RouteBalancer {

  public static final String LEADER_POLICY = "leader";
  public static final String GREEDY_POLICY = "greedy";

  private final IManager configManager;

  private final LazyGreedyRouter lazyGreedyRouter;

  public RouteBalancer(IManager configManager) {
    this.configManager = configManager;
    this.lazyGreedyRouter = new LazyGreedyRouter();
  }

  public Map<TConsensusGroupId, TRegionReplicaSet> genLatestRegionRouteMap(
      List<TRegionReplicaSet> regionReplicaSets) {
    List<TRegionReplicaSet> schemaRegionGroups = new ArrayList<>();
    List<TRegionReplicaSet> dataRegionGroups = new ArrayList<>();

    regionReplicaSets.forEach(
        regionReplicaSet -> {
          switch (regionReplicaSet.getRegionId().getType()) {
            case SchemaRegion:
              schemaRegionGroups.add(regionReplicaSet);
              break;
            case DataRegion:
              dataRegionGroups.add(regionReplicaSet);
              break;
          }
        });

    // Generate SchemaRegionRouteMap
    Map<TConsensusGroupId, TRegionReplicaSet> result =
        genRouter(TConsensusGroupType.SchemaRegion).genLatestRegionRouteMap(schemaRegionGroups);
    // Generate DataRegionRouteMap
    result.putAll(
        genRouter(TConsensusGroupType.DataRegion).genLatestRegionRouteMap(dataRegionGroups));
    return result;
  }

  private IRouter genRouter(TConsensusGroupType groupType) {
    String policy = ConfigNodeDescriptor.getInstance().getConf().getRoutingPolicy();
    switch (groupType) {
      case SchemaRegion:
        if (LEADER_POLICY.equals(policy)) {
          return new LeaderRouter(
              getPartitionManager().getAllLeadership(), getNodeManager().getAllLoadScores());
        } else {
          return new LoadScoreGreedyRouter(getNodeManager().getAllLoadScores());
        }
      case DataRegion:
      default:
        if (ConfigNodeDescriptor.getInstance()
            .getConf()
            .getDataRegionConsensusProtocolClass()
            .equals(ConsensusFactory.MultiLeaderConsensus)) {
          // Latent router for MultiLeader consensus protocol
          lazyGreedyRouter.updateDisabledDataNodes(
              getNodeManager()
                  .filterDataNodeThroughStatus(
                      NodeStatus.Unknown, NodeStatus.Removing, NodeStatus.ReadOnly));
          return lazyGreedyRouter;
        } else if (LEADER_POLICY.equals(policy)) {
          return new LeaderRouter(
              getPartitionManager().getAllLeadership(), getNodeManager().getAllLoadScores());
        } else {
          return new LoadScoreGreedyRouter(getNodeManager().getAllLoadScores());
        }
    }
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  public Map<TConsensusGroupId, TRegionReplicaSet> getRouteMap() {
    return lazyGreedyRouter.getRouteMap();
  }
}
