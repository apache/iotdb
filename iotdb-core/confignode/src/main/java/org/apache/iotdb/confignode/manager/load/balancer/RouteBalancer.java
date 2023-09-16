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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.GreedyLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.ILeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.MinCostFlowLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.GreedyPriorityBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.LeaderPriorityBalancer;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * The RouteBalancer will maintain cluster RegionRouteMap, which contains:
 *
 * <p>1. regionLeaderMap, record the leader for each RegionGroup
 *
 * <p>2. regionPriorityMap, record the priority for read/write requests in each RegionGroup
 */
public class RouteBalancer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RouteBalancer.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private static final String SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      CONF.getSchemaRegionConsensusProtocolClass();
  private static final String DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      CONF.getDataRegionConsensusProtocolClass();

  private static final boolean IS_ENABLE_AUTO_LEADER_BALANCE_FOR_DATA_REGION =
      (CONF.isEnableAutoLeaderBalanceForRatisConsensus()
              && ConsensusFactory.RATIS_CONSENSUS.equals(DATA_REGION_CONSENSUS_PROTOCOL_CLASS))
          || (CONF.isEnableAutoLeaderBalanceForIoTConsensus()
              && ConsensusFactory.IOT_CONSENSUS.equals(DATA_REGION_CONSENSUS_PROTOCOL_CLASS));
  private static final boolean IS_ENABLE_AUTO_LEADER_BALANCE_FOR_SCHEMA_REGION =
      (CONF.isEnableAutoLeaderBalanceForRatisConsensus()
              && ConsensusFactory.RATIS_CONSENSUS.equals(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS))
          || (CONF.isEnableAutoLeaderBalanceForIoTConsensus()
              && ConsensusFactory.IOT_CONSENSUS.equals(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS));

  private final IManager configManager;

  /** RegionRouteMap */
  // For generating optimal RegionLeaderMap
  private final ILeaderBalancer leaderBalancer;
  // For generating optimal RegionPriorityMap
  private final IPriorityBalancer priorityRouter;

  public RouteBalancer(IManager configManager) {
    this.configManager = configManager;

    switch (CONF.getLeaderDistributionPolicy()) {
      case ILeaderBalancer.GREEDY_POLICY:
        this.leaderBalancer = new GreedyLeaderBalancer();
        break;
      case ILeaderBalancer.MIN_COST_FLOW_POLICY:
      default:
        this.leaderBalancer = new MinCostFlowLeaderBalancer();
        break;
    }

    switch (CONF.getRoutePriorityPolicy()) {
      case IPriorityBalancer.GREEDY_POLICY:
        this.priorityRouter = new GreedyPriorityBalancer();
        break;
      case IPriorityBalancer.LEADER_POLICY:
      default:
        this.priorityRouter = new LeaderPriorityBalancer();
        break;
    }
  }

  /**
   * Balance cluster RegionGroup leader distribution through configured algorithm
   *
   * @return Map<RegionGroupId, Pair<old leader index, new leader index>>
   */
  public Map<TConsensusGroupId, Pair<Integer, Integer>> balanceRegionLeader() {
    Map<TConsensusGroupId, Pair<Integer, Integer>> differentRegionLeaderMap =
        new ConcurrentHashMap<>();
    if (IS_ENABLE_AUTO_LEADER_BALANCE_FOR_SCHEMA_REGION) {
      differentRegionLeaderMap.putAll(balanceRegionLeader(TConsensusGroupType.SchemaRegion));
    }
    if (IS_ENABLE_AUTO_LEADER_BALANCE_FOR_DATA_REGION) {
      differentRegionLeaderMap.putAll(balanceRegionLeader(TConsensusGroupType.DataRegion));
    }

    return differentRegionLeaderMap;
  }

  private Map<TConsensusGroupId, Pair<Integer, Integer>> balanceRegionLeader(
      TConsensusGroupType regionGroupType) {
    Map<TConsensusGroupId, Pair<Integer, Integer>> differentRegionLeaderMap =
        new ConcurrentHashMap<>();

    // Collect the latest data and generate the optimal leader distribution
    Map<TConsensusGroupId, Integer> currentLeaderMap = getLoadManager().getRegionLeaderMap();
    Map<TConsensusGroupId, Integer> optimalLeaderMap =
        leaderBalancer.generateOptimalLeaderDistribution(
            getPartitionManager().getAllReplicaSetsMap(regionGroupType),
            currentLeaderMap,
            getNodeManager()
                .filterDataNodeThroughStatus(
                    NodeStatus.Unknown, NodeStatus.ReadOnly, NodeStatus.Removing)
                .stream()
                .map(TDataNodeConfiguration::getLocation)
                .map(TDataNodeLocation::getDataNodeId)
                .collect(Collectors.toSet()));

    // Transfer leader to the optimal distribution
    AtomicInteger requestId = new AtomicInteger(0);
    AsyncClientHandler<TRegionLeaderChangeReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CHANGE_REGION_LEADER);
    optimalLeaderMap.forEach(
        (regionGroupId, newLeaderId) -> {
          if (newLeaderId != -1 && !newLeaderId.equals(currentLeaderMap.get(regionGroupId))) {
            String consensusProtocolClass;
            switch (regionGroupId.getType()) {
              case SchemaRegion:
                consensusProtocolClass = SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS;
                break;
              case DataRegion:
              default:
                consensusProtocolClass = DATA_REGION_CONSENSUS_PROTOCOL_CLASS;
                break;
            }
            LOGGER.info(
                "[LeaderBalancer] Try to change the leader of Region: {} to DataNode: {} ",
                regionGroupId,
                newLeaderId);
            changeRegionLeader(
                consensusProtocolClass,
                requestId,
                clientHandler,
                regionGroupId,
                getNodeManager().getRegisteredDataNode(newLeaderId).getLocation());
            differentRegionLeaderMap.put(
                regionGroupId, new Pair<>(currentLeaderMap.get(regionGroupId), newLeaderId));
          }
        });
    if (requestId.get() > 0) {
      // Don't retry ChangeLeader request
      AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler, 1);
    }
    return differentRegionLeaderMap;
  }

  private void changeRegionLeader(
      String consensusProtocolClass,
      AtomicInteger requestId,
      AsyncClientHandler<TRegionLeaderChangeReq, TSStatus> clientHandler,
      TConsensusGroupId regionGroupId,
      TDataNodeLocation newLeader) {
    switch (consensusProtocolClass) {
      case ConsensusFactory.IOT_CONSENSUS:
        // For IoTConsensus protocol, change RegionRouteMap is enough.
        // And the result will be broadcast by Cluster-LoadStatistics-Service soon.
        getLoadManager().forceUpdateRegionLeader(regionGroupId, newLeader.getDataNodeId());
        break;
      case ConsensusFactory.RATIS_CONSENSUS:
      default:
        // For ratis protocol, the ConfigNode-leader will send a changeLeaderRequest to the new
        // leader.
        // And the RegionRouteMap will be updated by Cluster-Heartbeat-Service later if change
        // leader success.
        TRegionLeaderChangeReq regionLeaderChangeReq =
            new TRegionLeaderChangeReq(regionGroupId, newLeader);
        int requestIndex = requestId.getAndIncrement();
        clientHandler.putRequest(requestIndex, regionLeaderChangeReq);
        clientHandler.putDataNodeLocation(requestIndex, newLeader);
        break;
    }
  }

  /**
   * Balance cluster RegionGroup route priority through configured algorithm
   *
   * @return Map<RegionGroupId, Pair<old route priority, new route priority>>
   */
  public Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>>
      balanceRegionPriority() {

    Map<TConsensusGroupId, TRegionReplicaSet> currentPriorityMap =
        getLoadManager().getRegionPriorityMap();
    Map<TConsensusGroupId, Integer> regionLeaderMap = getLoadManager().getRegionLeaderMap();
    Map<Integer, Long> dataNodeLoadScoreMap = getLoadManager().getAllDataNodeLoadScores();

    // Balancing region priority in each SchemaRegionGroup
    Map<TConsensusGroupId, TRegionReplicaSet> optimalRegionPriorityMap =
        priorityRouter.generateOptimalRoutePriority(
            getPartitionManager().getAllReplicaSets(TConsensusGroupType.SchemaRegion),
            regionLeaderMap,
            dataNodeLoadScoreMap);
    // Balancing region priority in each DataRegionGroup
    optimalRegionPriorityMap.putAll(
        priorityRouter.generateOptimalRoutePriority(
            getPartitionManager().getAllReplicaSets(TConsensusGroupType.DataRegion),
            regionLeaderMap,
            dataNodeLoadScoreMap));

    Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>> differentRegionPriorityMap =
        new ConcurrentHashMap<>();
    for (Map.Entry<TConsensusGroupId, TRegionReplicaSet> regionPriorityEntry :
        optimalRegionPriorityMap.entrySet()) {
      TConsensusGroupId regionGroupId = regionPriorityEntry.getKey();
      TRegionReplicaSet optimalRegionPriority = regionPriorityEntry.getValue();
      if (!optimalRegionPriority.equals(currentPriorityMap.get(regionGroupId))) {
        differentRegionPriorityMap.put(
            regionGroupId,
            new Pair<>(currentPriorityMap.get(regionGroupId), optimalRegionPriority));
        getLoadManager().forceUpdateRegionPriority(regionGroupId, optimalRegionPriority);
      }
    }
    return differentRegionPriorityMap;
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }
}
