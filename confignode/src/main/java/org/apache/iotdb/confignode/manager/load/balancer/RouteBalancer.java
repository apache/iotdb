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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.balancer.router.IRouter;
import org.apache.iotdb.confignode.manager.load.balancer.router.LeaderRouter;
import org.apache.iotdb.confignode.manager.load.balancer.router.LoadScoreGreedyRouter;
import org.apache.iotdb.confignode.manager.load.balancer.router.RegionRouteMap;
import org.apache.iotdb.confignode.manager.load.balancer.router.mcf.MCFLeaderBalancer;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final boolean ENABLE_LEADER_BALANCING = CONF.isEnableLeaderBalancing();
  private static final String SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS =
      CONF.getSchemaRegionConsensusProtocolClass();
  private static final String DATA_REGION_CONSENSUS_PROTOCOL_CLASS =
      CONF.getDataRegionConsensusProtocolClass();
  private static final boolean isMultiLeader =
      ConsensusFactory.MULTI_LEADER_CONSENSUS.equals(
          ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass());

  public static final String LEADER_POLICY = "leader";
  public static final String GREEDY_POLICY = "greedy";

  private final IManager configManager;

  // Key: RegionGroupId
  // Value: Pair<Timestamp, LeaderDataNodeId>, where
  // the left value stands for sampling timestamp
  // and the right value stands for the index of DataNode that leader resides.
  private final Map<TConsensusGroupId, Pair<Long, Integer>> leaderCache;

  /** RegionRouteMap */
  private final RegionRouteMap regionRouteMap;
  // For generating optimal RegionRouteMap
  private final IRouter router;

  /** Leader Balancing service */
  private Future<?> currentLeaderBalancingFuture;

  private final ScheduledExecutorService leaderBalancingExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Cluster-LeaderBalancing-Service");
  private final Object scheduleMonitor = new Object();

  public RouteBalancer(IManager configManager) {
    this.configManager = configManager;

    this.leaderCache = new ConcurrentHashMap<>();
    this.regionRouteMap = new RegionRouteMap();
    switch (ConfigNodeDescriptor.getInstance().getConf().getRoutingPolicy()) {
      case GREEDY_POLICY:
        this.router = new LoadScoreGreedyRouter();
        break;
      case LEADER_POLICY:
      default:
        this.router = new LeaderRouter();
        break;
    }
  }

  /**
   * Cache the newest leaderHeartbeatSample
   *
   * @param regionGroupId Corresponding RegionGroup's index
   * @param leaderSample <Sample timestamp, leaderDataNodeId>, The newest HeartbeatSample
   */
  public void cacheLeaderSample(TConsensusGroupId regionGroupId, Pair<Long, Integer> leaderSample) {
    if (TConsensusGroupType.DataRegion.equals(regionGroupId.getType()) && isMultiLeader) {
      // The leadership of multi-leader consensus protocol is decided by ConfigNode-leader
      return;
    }

    leaderCache.putIfAbsent(regionGroupId, leaderSample);
    synchronized (leaderCache.get(regionGroupId)) {
      if (leaderCache.get(regionGroupId).getLeft() < leaderSample.getLeft()) {
        leaderCache.replace(regionGroupId, leaderSample);
      }
    }
  }

  /**
   * Invoking periodically to update the RegionRouteMap
   *
   * @return True if the RegionRouteMap has changed, false otherwise
   */
  public boolean updateRegionRouteMap() {
    synchronized (regionRouteMap) {
      return updateRegionLeaderMap() | updateRegionPriorityMap();
    }
  }

  private boolean updateRegionLeaderMap() {
    AtomicBoolean isLeaderChanged = new AtomicBoolean(false);
    leaderCache.forEach(
        (regionGroupId, leadershipSample) -> {
          if (TConsensusGroupType.DataRegion.equals(regionGroupId.getType()) && isMultiLeader) {
            // Ignore MultiLeader consensus protocol
            return;
          }

          if (leadershipSample.getRight() != regionRouteMap.getLeader(regionGroupId)) {
            // Update leader
            regionRouteMap.setLeader(regionGroupId, leadershipSample.getRight());
            isLeaderChanged.set(true);
          }
        });
    return isLeaderChanged.get();
  }

  private boolean updateRegionPriorityMap() {
    Map<TConsensusGroupId, Integer> regionLeaderMap = regionRouteMap.getRegionLeaderMap();
    Map<Integer, Long> dataNodeLoadScoreMap = getNodeManager().getAllLoadScores();

    // Balancing region priority in each SchemaRegionGroup
    Map<TConsensusGroupId, TRegionReplicaSet> latestRegionPriorityMap =
        router.getLatestRegionRouteMap(
            getPartitionManager().getAllReplicaSets(TConsensusGroupType.SchemaRegion),
            regionLeaderMap,
            dataNodeLoadScoreMap);
    // Balancing region priority in each DataRegionGroup
    latestRegionPriorityMap.putAll(
        router.getLatestRegionRouteMap(
            getPartitionManager().getAllReplicaSets(TConsensusGroupType.DataRegion),
            regionLeaderMap,
            dataNodeLoadScoreMap));

    if (!latestRegionPriorityMap.equals(regionRouteMap.getRegionPriorityMap())) {
      regionRouteMap.setRegionPriorityMap(latestRegionPriorityMap);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Select leader for the specified RegionGroup greedily. The selected leader will be the DataNode
   * that currently has the fewest leaders
   *
   * @param regionGroupId The specified RegionGroup
   * @param dataNodeIds The indices of DataNodes where the RegionReplicas reside
   */
  public void greedySelectLeader(TConsensusGroupId regionGroupId, List<Integer> dataNodeIds) {
    synchronized (regionRouteMap) {
      // Map<DataNodeId, The number of leaders>
      Map<Integer, AtomicInteger> leaderCounter = new HashMap<>();
      regionRouteMap
          .getRegionLeaderMap()
          .forEach(
              (consensusGroupId, leaderId) -> {
                if (TConsensusGroupType.DataRegion.equals(consensusGroupId.getType())) {
                  leaderCounter
                      .computeIfAbsent(leaderId, empty -> new AtomicInteger(0))
                      .getAndIncrement();
                }
              });

      int newLeaderId = -1;
      int minCount = Integer.MAX_VALUE;
      AtomicInteger zero = new AtomicInteger(0);
      for (int dataNodeId : dataNodeIds) {
        int leaderCount = leaderCounter.getOrDefault(dataNodeId, zero).get();
        if (leaderCount < minCount) {
          newLeaderId = dataNodeId;
          minCount = leaderCount;
        }
      }
      regionRouteMap.setLeader(regionGroupId, newLeaderId);
    }
  }

  /** Start the route balancing service */
  public void startRouteBalancingService() {
    if (ENABLE_LEADER_BALANCING) {
      synchronized (scheduleMonitor) {
        if (currentLeaderBalancingFuture == null) {
          currentLeaderBalancingFuture =
              ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                  leaderBalancingExecutor,
                  this::balancingRegionLeader,
                  0,
                  // Execute route balancing service in every 5 loops of heartbeat service
                  NodeManager.HEARTBEAT_INTERVAL * 5,
                  TimeUnit.MILLISECONDS);
          LOGGER.info("Route-Balancing service is started successfully.");
        }
      }
    }
  }

  /** Stop the route balancing service */
  public void stopRouteBalancingService() {
    if (ENABLE_LEADER_BALANCING) {
      synchronized (scheduleMonitor) {
        if (currentLeaderBalancingFuture != null) {
          currentLeaderBalancingFuture.cancel(false);
          currentLeaderBalancingFuture = null;
          leaderCache.clear();
          regionRouteMap.clear();
          LOGGER.info("Route-Balancing service is stopped successfully.");
        }
      }
    }
  }

  private void balancingRegionLeader() {
    balancingRegionLeader(TConsensusGroupType.SchemaRegion);
    balancingRegionLeader(TConsensusGroupType.DataRegion);
  }

  private void balancingRegionLeader(TConsensusGroupType regionGroupType) {
    // Collect latest data to generate leaderBalancer
    MCFLeaderBalancer leaderBalancer =
        new MCFLeaderBalancer(
            getPartitionManager().getAllReplicaSetsMap(regionGroupType),
            regionRouteMap.getRegionLeaderMap(),
            getNodeManager()
                .filterDataNodeThroughStatus(
                    NodeStatus.Unknown, NodeStatus.ReadOnly, NodeStatus.Removing)
                .stream()
                .map(TDataNodeConfiguration::getLocation)
                .map(TDataNodeLocation::getDataNodeId)
                .collect(Collectors.toSet()));

    // Calculate the optimal leader distribution
    Map<TConsensusGroupId, Integer> leaderDistribution =
        leaderBalancer.generateOptimalLeaderDistribution();

    // Transfer leader to the optimal distribution
    AtomicInteger requestId = new AtomicInteger(0);
    AsyncClientHandler<TRegionLeaderChangeReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.CHANGE_REGION_LEADER);
    leaderDistribution.forEach(
        (regionGroupId, newLeaderId) -> {
          if (newLeaderId != regionRouteMap.getLeader(regionGroupId)) {
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
                consensusProtocolClass, requestId, clientHandler, regionGroupId, newLeaderId);
          }
        });
    if (requestId.get() > 0) {
      AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
    }
  }

  public void changeLeaderForMultiLeaderConsensus(
      TConsensusGroupId regionGroupId, int newLeaderId) {
    regionRouteMap.setLeader(regionGroupId, newLeaderId);
  }

  private void changeRegionLeader(
      String consensusProtocolClass,
      AtomicInteger requestId,
      AsyncClientHandler<TRegionLeaderChangeReq, TSStatus> clientHandler,
      TConsensusGroupId regionGroupId,
      int newLeaderId) {
    switch (consensusProtocolClass) {
      case ConsensusFactory.MULTI_LEADER_CONSENSUS:
        // For multi-leader protocol, change RegionRouteMap is enough.
        // And the result will be broadcast by Cluster-LoadStatistics-Service soon.
        regionRouteMap.setLeader(regionGroupId, newLeaderId);
        break;
      case ConsensusFactory.RATIS_CONSENSUS:
      default:
        // For ratis protocol, the ConfigNode-leader will send a changeLeaderRequest to the new
        // leader.
        // And the RegionRouteMap will be updated by Cluster-Heartbeat-Service later if change
        // leader success.
        TRegionLeaderChangeReq regionLeaderChangeReq =
            new TRegionLeaderChangeReq(
                regionGroupId, getNodeManager().getRegisteredDataNode(newLeaderId).getLocation());
        clientHandler.putRequest(requestId.getAndIncrement(), regionLeaderChangeReq);
    }
  }

  /** Initialize the regionRouteMap when the ConfigNode-Leader is switched */
  public void initRegionRouteMap() {
    synchronized (regionRouteMap) {
      regionRouteMap.clear();
      if (isMultiLeader) {
        // Greedily pick leader for all existed DataRegionGroups
        List<TRegionReplicaSet> dataRegionGroups =
            getPartitionManager().getAllReplicaSets(TConsensusGroupType.DataRegion);
        for (TRegionReplicaSet dataRegionGroup : dataRegionGroups) {
          greedySelectLeader(
              dataRegionGroup.getRegionId(),
              dataRegionGroup.getDataNodeLocations().stream()
                  .map(TDataNodeLocation::getDataNodeId)
                  .collect(Collectors.toList()));
        }
      }
      updateRegionRouteMap();
    }
  }

  public Map<TConsensusGroupId, Integer> getLatestRegionLeaderMap() {
    return regionRouteMap.getRegionLeaderMap();
  }

  public Map<TConsensusGroupId, TRegionReplicaSet> getLatestRegionPriorityMap() {
    return regionRouteMap.getRegionPriorityMap();
  }

  public RegionRouteMap getLatestRegionRouteMap() {
    return regionRouteMap;
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }
}
