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
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.GreedyLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.ILeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.MinCostFlowLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.GreedyPriorityBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.IPriorityBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.priority.LeaderPriorityBalancer;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.subscriber.ConsensusGroupStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.IClusterStatusSubscriber;
import org.apache.iotdb.confignode.manager.load.subscriber.NodeStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.load.subscriber.RegionGroupStatisticsChangeEvent;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeResp;
import org.apache.iotdb.mpp.rpc.thrift.TRegionRouteReq;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/** The RouteBalancer guides the cluster RegionGroups' leader distribution and routing priority. */
public class RouteBalancer implements IClusterStatusSubscriber {

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
              && ConsensusFactory.IOT_CONSENSUS.equals(DATA_REGION_CONSENSUS_PROTOCOL_CLASS))
          // The simple consensus protocol will always automatically designate itself as the leader
          || ConsensusFactory.SIMPLE_CONSENSUS.equals(DATA_REGION_CONSENSUS_PROTOCOL_CLASS);
  private static final boolean IS_ENABLE_AUTO_LEADER_BALANCE_FOR_SCHEMA_REGION =
      (CONF.isEnableAutoLeaderBalanceForRatisConsensus()
              && ConsensusFactory.RATIS_CONSENSUS.equals(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS))
          || (CONF.isEnableAutoLeaderBalanceForIoTConsensus()
              && ConsensusFactory.IOT_CONSENSUS.equals(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS))
          // The simple consensus protocol will always automatically designate itself as the leader
          || ConsensusFactory.SIMPLE_CONSENSUS.equals(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS);
  private static final long REGION_PRIORITY_WAITING_TIMEOUT =
      Math.max(
          ProcedureManager.PROCEDURE_WAIT_TIME_OUT - TimeUnit.SECONDS.toMillis(2),
          TimeUnit.SECONDS.toMillis(10));
  private static final long WAIT_PRIORITY_INTERVAL = 10;

  private final IManager configManager;
  // For generating optimal Region leader distribution
  private final ILeaderBalancer leaderBalancer;
  // For generating optimal cluster Region routing priority
  private final IPriorityBalancer priorityRouter;

  private final ReentrantReadWriteLock priorityMapLock;
  // Map<RegionGroupId, Region priority>
  // The client requests are preferentially routed to the Region with the lowest index in the
  // TRegionReplicaSet
  private final Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap;

  // The interval of retrying to balance ratis leader after the last failed time
  private static final long BALANCE_RATIS_LEADER_FAILED_INTERVAL_IN_NS = 60 * 1000L * 1000L * 1000L;
  private final Map<TConsensusGroupId, Long> lastFailedTimeForLeaderBalance;

  public RouteBalancer(IManager configManager) {
    this.configManager = configManager;
    this.priorityMapLock = new ReentrantReadWriteLock();
    this.regionPriorityMap = new TreeMap<>();
    this.lastFailedTimeForLeaderBalance = new TreeMap<>();

    switch (CONF.getLeaderDistributionPolicy()) {
      case ILeaderBalancer.GREEDY_POLICY:
        this.leaderBalancer = new GreedyLeaderBalancer();
        break;
      case ILeaderBalancer.CFD_POLICY:
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

  /** Balance cluster RegionGroup leader distribution through configured algorithm. */
  private synchronized void balanceRegionLeader() {
    if (IS_ENABLE_AUTO_LEADER_BALANCE_FOR_SCHEMA_REGION) {
      balanceRegionLeader(TConsensusGroupType.SchemaRegion, SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS);
    }
    if (IS_ENABLE_AUTO_LEADER_BALANCE_FOR_DATA_REGION) {
      balanceRegionLeader(TConsensusGroupType.DataRegion, DATA_REGION_CONSENSUS_PROTOCOL_CLASS);
    }
  }

  private void balanceRegionLeader(
      TConsensusGroupType regionGroupType, String consensusProtocolClass) {
    // Collect the latest data and generate the optimal leader distribution
    Map<TConsensusGroupId, Integer> currentLeaderMap = getLoadManager().getRegionLeaderMap();
    Map<TConsensusGroupId, Integer> optimalLeaderMap =
        leaderBalancer.generateOptimalLeaderDistribution(
            getPartitionManager().getAllRegionGroupIdMap(regionGroupType),
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
    long currentTime = System.nanoTime();
    AtomicInteger requestId = new AtomicInteger(0);
    AsyncClientHandler<TRegionLeaderChangeReq, TRegionLeaderChangeResp> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CHANGE_REGION_LEADER);
    Map<TConsensusGroupId, ConsensusGroupHeartbeatSample> successTransferMap = new TreeMap<>();
    optimalLeaderMap.forEach(
        (regionGroupId, newLeaderId) -> {
          if (ConsensusFactory.RATIS_CONSENSUS.equals(consensusProtocolClass)
              && currentTime - lastFailedTimeForLeaderBalance.getOrDefault(regionGroupId, 0L)
                  <= BALANCE_RATIS_LEADER_FAILED_INTERVAL_IN_NS) {
            return;
          }

          if (newLeaderId != -1 && !newLeaderId.equals(currentLeaderMap.get(regionGroupId))) {
            LOGGER.info(
                "[LeaderBalancer] Try to change the leader of Region: {} to DataNode: {} ",
                regionGroupId,
                newLeaderId);
            switch (consensusProtocolClass) {
              case ConsensusFactory.IOT_CONSENSUS:
              case ConsensusFactory.SIMPLE_CONSENSUS:
                // For IoTConsensus or SimpleConsensus protocol, change RegionRouteMap is enough
                successTransferMap.put(
                    regionGroupId, new ConsensusGroupHeartbeatSample(currentTime, newLeaderId));
                break;
              case ConsensusFactory.RATIS_CONSENSUS:
              default:
                // For ratis protocol, the ConfigNode-leader will send a changeLeaderRequest to the
                // new
                // leader.
                // And the RegionRouteMap will be updated by Cluster-Heartbeat-Service later if
                // change
                // leader success.
                // Force update region leader for ratis consensus when replication factor is 1.
                if (TConsensusGroupType.SchemaRegion.equals(regionGroupType)
                    && CONF.getSchemaReplicationFactor() == 1) {
                  successTransferMap.put(
                      regionGroupId, new ConsensusGroupHeartbeatSample(0, newLeaderId));
                } else if (TConsensusGroupType.DataRegion.equals(regionGroupType)
                    && CONF.getDataReplicationFactor() == 1) {
                  successTransferMap.put(
                      regionGroupId, new ConsensusGroupHeartbeatSample(0, newLeaderId));
                } else {
                  TDataNodeLocation newLeader =
                      getNodeManager().getRegisteredDataNode(newLeaderId).getLocation();
                  TRegionLeaderChangeReq regionLeaderChangeReq =
                      new TRegionLeaderChangeReq(regionGroupId, newLeader);
                  int requestIndex = requestId.getAndIncrement();
                  clientHandler.putRequest(requestIndex, regionLeaderChangeReq);
                  clientHandler.putDataNodeLocation(requestIndex, newLeader);
                }
                break;
            }
          }
        });
    if (requestId.get() > 0) {
      // Don't retry ChangeLeader request
      AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNode(clientHandler);
      for (int i = 0; i < requestId.get(); i++) {
        if (clientHandler.getResponseMap().get(i).getStatus().getCode()
            == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          successTransferMap.put(
              clientHandler.getRequest(i).getRegionId(),
              new ConsensusGroupHeartbeatSample(
                  clientHandler.getResponseMap().get(i).getConsensusLogicalTimestamp(),
                  clientHandler.getRequest(i).getNewLeaderNode().getDataNodeId()));
        } else {
          lastFailedTimeForLeaderBalance.put(
              clientHandler.getRequest(i).getRegionId(), currentTime);
          LOGGER.error(
              "[LeaderBalancer] Failed to change the leader of Region: {} to DataNode: {}",
              clientHandler.getRequest(i).getRegionId(),
              clientHandler.getRequest(i).getNewLeaderNode().getDataNodeId());
        }
      }
    }
    getLoadManager().forceUpdateConsensusGroupCache(successTransferMap);
  }

  /** Balance cluster RegionGroup route priority through configured algorithm. */
  private synchronized void balanceRegionPriority() {
    priorityMapLock.writeLock().lock();
    AtomicBoolean needBroadcast = new AtomicBoolean(false);
    Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>> differentPriorityMap =
        new TreeMap<>();
    try {
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

      optimalRegionPriorityMap.forEach(
          (regionGroupId, optimalRegionPriority) -> {
            TRegionReplicaSet currentRegionPriority = regionPriorityMap.get(regionGroupId);
            if (!optimalRegionPriority.equals(currentRegionPriority)) {
              differentPriorityMap.put(
                  regionGroupId, new Pair<>(currentRegionPriority, optimalRegionPriority));
              regionPriorityMap.put(regionGroupId, optimalRegionPriority);
              needBroadcast.set(true);
            }
          });
    } finally {
      priorityMapLock.writeLock().unlock();
    }

    if (needBroadcast.get()) {
      recordRegionPriorityMap(differentPriorityMap);
      broadcastLatestRegionPriorityMap();
    }
  }

  private void broadcastLatestRegionPriorityMap() {
    // Broadcast the RegionRouteMap to all DataNodes except the unknown ones
    Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        getNodeManager()
            .filterDataNodeThroughStatus(
                NodeStatus.Running, NodeStatus.Removing, NodeStatus.ReadOnly)
            .stream()
            .map(TDataNodeConfiguration::getLocation)
            .collect(Collectors.toMap(TDataNodeLocation::getDataNodeId, location -> location));

    long broadcastTime = System.currentTimeMillis();
    Map<TConsensusGroupId, TRegionReplicaSet> tmpPriorityMap = getRegionPriorityMap();
    AsyncClientHandler<TRegionRouteReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(
            DataNodeRequestType.UPDATE_REGION_ROUTE_MAP,
            new TRegionRouteReq(broadcastTime, tmpPriorityMap),
            dataNodeLocationMap);
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
  }

  private void recordRegionPriorityMap(
      Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>> differentPriorityMap) {
    LOGGER.info("[RegionPriority] RegionPriorityMap: ");
    for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>>
        regionPriorityEntry : differentPriorityMap.entrySet()) {
      if (!Objects.equals(
          regionPriorityEntry.getValue().getRight(), regionPriorityEntry.getValue().getLeft())) {
        try {
          LOGGER.info(
              "[RegionPriority]\t {}: {}->{}",
              regionPriorityEntry.getKey(),
              regionPriorityEntry.getValue().getLeft() == null
                  ? "null"
                  : regionPriorityEntry.getValue().getLeft().getDataNodeLocations().stream()
                      .map(TDataNodeLocation::getDataNodeId)
                      .collect(Collectors.toList()),
              regionPriorityEntry.getValue().getRight().getDataNodeLocations().stream()
                  .map(TDataNodeLocation::getDataNodeId)
                  .collect(Collectors.toList()));
        } catch (Exception e) {
          LOGGER.error("Unexpected exception", e);
        }
      }
    }
  }

  /** @return Map<RegionGroupId, RegionPriority> */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRegionPriorityMap() {
    priorityMapLock.readLock().lock();
    try {
      return new TreeMap<>(regionPriorityMap);
    } finally {
      priorityMapLock.readLock().unlock();
    }
  }

  public void removeRegionPriority(TConsensusGroupId regionGroupId) {
    priorityMapLock.writeLock().lock();
    try {
      regionPriorityMap.remove(regionGroupId);
    } finally {
      priorityMapLock.writeLock().unlock();
    }
  }

  public void clearRegionPriority() {
    priorityMapLock.writeLock().lock();
    try {
      regionPriorityMap.clear();
    } finally {
      priorityMapLock.writeLock().unlock();
    }
  }

  /**
   * Wait for the specified RegionGroups to finish routing priority calculation.
   *
   * @param regionGroupIds Specified RegionGroupIds
   */
  public void waitForPriorityUpdate(List<TConsensusGroupId> regionGroupIds) {
    long startTime = System.currentTimeMillis();
    LOGGER.info(
        "[RegionPriority] Wait for Region priority update of RegionGroups: {}", regionGroupIds);
    while (System.currentTimeMillis() - startTime <= REGION_PRIORITY_WAITING_TIMEOUT) {
      AtomicBoolean allRegionPriorityCalculated = new AtomicBoolean(true);
      priorityMapLock.readLock().lock();
      try {
        regionGroupIds.forEach(
            regionGroupId -> {
              if (!regionPriorityMap.containsKey(regionGroupId)) {
                allRegionPriorityCalculated.set(false);
              }
            });
      } finally {
        priorityMapLock.readLock().unlock();
      }
      if (allRegionPriorityCalculated.get()) {
        LOGGER.info(
            "[RegionPriority] The routing priority of RegionGroups: {} is calculated.",
            regionGroupIds);
        return;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(WAIT_PRIORITY_INTERVAL);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Interrupt when wait for calculating Region priority", e);
        return;
      }
    }

    LOGGER.warn(
        "[RegionPriority] The routing priority of RegionGroups: {} is not determined after 10 heartbeat interval. Some function might fail.",
        regionGroupIds);
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

  @Override
  public void onNodeStatisticsChanged(NodeStatisticsChangeEvent event) {
    balanceRegionLeader();
  }

  @Override
  public void onRegionGroupStatisticsChanged(RegionGroupStatisticsChangeEvent event) {
    balanceRegionLeader();
  }

  @Override
  public void onConsensusGroupStatisticsChanged(ConsensusGroupStatisticsChangeEvent event) {
    balanceRegionLeader();
    balanceRegionPriority();
  }
}
