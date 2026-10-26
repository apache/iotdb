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
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.AbstractLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.CostFlowSelectionLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.GreedyLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.HashLeaderBalancer;
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

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
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
          || ConsensusFactory.IOT_CONSENSUS.equals(DATA_REGION_CONSENSUS_PROTOCOL_CLASS)
          || ConsensusFactory.IOT_CONSENSUS_V2.equals(DATA_REGION_CONSENSUS_PROTOCOL_CLASS)
          // The simple consensus protocol will always automatically designate itself as the leader
          || ConsensusFactory.SIMPLE_CONSENSUS.equals(DATA_REGION_CONSENSUS_PROTOCOL_CLASS);
  private static final boolean IS_ENABLE_AUTO_LEADER_BALANCE_FOR_SCHEMA_REGION =
      (CONF.isEnableAutoLeaderBalanceForRatisConsensus()
              && ConsensusFactory.RATIS_CONSENSUS.equals(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS))
          || ConsensusFactory.IOT_CONSENSUS.equals(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS)
          // The simple consensus protocol will always automatically designate itself as the leader
          || ConsensusFactory.SIMPLE_CONSENSUS.equals(SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS);
  private static final long REGION_PRIORITY_WAITING_TIMEOUT =
      Math.max(
          ProcedureManager.PROCEDURE_WAIT_TIME_OUT - TimeUnit.SECONDS.toMillis(2),
          TimeUnit.SECONDS.toMillis(10));
  private static final long WAIT_PRIORITY_INTERVAL = 10;
  private static final long RATIS_CHANGE_LEADER_RPC_TIMEOUT_IN_MS = TimeUnit.SECONDS.toMillis(10);

  private final IManager configManager;
  // For serializing and generating optimal Region leader distribution by RegionGroup type
  private final LeaderBalanceContext schemaRegionLeaderBalanceContext;
  private final LeaderBalanceContext dataRegionLeaderBalanceContext;
  // For generating optimal cluster Region routing priority
  private final IPriorityBalancer priorityRouter;

  private final ReentrantReadWriteLock priorityMapLock;
  // Map<RegionGroupId, Region priority>
  // The client requests are preferentially routed to the Region with the lowest index in the
  // TRegionReplicaSet
  private final Map<TConsensusGroupId, TRegionReplicaSet> regionPriorityMap;

  // The interval of retrying to balance ratis leader after the last failed time
  private static final long BALANCE_RATIS_LEADER_FAILED_INTERVAL_IN_NS = 20 * 1000L * 1000L * 1000L;
  private final Map<TConsensusGroupId, Long> lastFailedTimeForLeaderBalance;

  public RouteBalancer(IManager configManager) {
    this.configManager = configManager;
    this.priorityMapLock = new ReentrantReadWriteLock();
    this.regionPriorityMap = new TreeMap<>();
    this.lastFailedTimeForLeaderBalance = new ConcurrentHashMap<>();

    this.schemaRegionLeaderBalanceContext =
        new LeaderBalanceContext(
            TConsensusGroupType.SchemaRegion,
            new ReentrantLock(),
            createLeaderBalancer(),
            SCHEMA_REGION_CONSENSUS_PROTOCOL_CLASS);
    this.dataRegionLeaderBalanceContext =
        new LeaderBalanceContext(
            TConsensusGroupType.DataRegion,
            new ReentrantLock(),
            createLeaderBalancer(),
            DATA_REGION_CONSENSUS_PROTOCOL_CLASS);

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

  /** Result of DataRegion leader balance used by the required post-balance actions. */
  private static class DataRegionLeaderBalanceResult {

    private final Map<TConsensusGroupId, Integer> dataRegion2OldLeaderMap;
    private final Set<TConsensusGroupId> balancedDataRegionSet;
    private final Map<Integer, List<String>> balancedOldLeaderId2RegionMap;

    private DataRegionLeaderBalanceResult(
        Map<TConsensusGroupId, Integer> dataRegion2OldLeaderMap,
        Set<TConsensusGroupId> balancedDataRegionSet,
        Map<Integer, List<String>> balancedOldLeaderId2RegionMap) {
      this.dataRegion2OldLeaderMap = dataRegion2OldLeaderMap;
      this.balancedDataRegionSet = balancedDataRegionSet;
      this.balancedOldLeaderId2RegionMap = balancedOldLeaderId2RegionMap;
    }

    private static DataRegionLeaderBalanceResult empty() {
      return new DataRegionLeaderBalanceResult(
          Collections.emptyMap(), Collections.emptySet(), Collections.emptyMap());
    }

    private boolean hasBalancedDataRegion() {
      return !balancedDataRegionSet.isEmpty();
    }
  }

  /** Immutable state for one RegionGroup type's leader balance round. */
  private static class LeaderBalanceContext {

    private final TConsensusGroupType regionGroupType;
    private final ReentrantLock leaderBalanceLock;
    private final AbstractLeaderBalancer leaderBalancer;
    private final String consensusProtocolClass;

    private LeaderBalanceContext(
        TConsensusGroupType regionGroupType,
        ReentrantLock leaderBalanceLock,
        AbstractLeaderBalancer leaderBalancer,
        String consensusProtocolClass) {
      this.regionGroupType = regionGroupType;
      this.leaderBalanceLock = leaderBalanceLock;
      this.leaderBalancer = leaderBalancer;
      this.consensusProtocolClass = consensusProtocolClass;
    }
  }

  /** Mutable accumulator for one leader balance round's transfer requests and cache updates. */
  private static class LeaderTransferContext {

    private final long currentTime;
    private int requestId;
    private final DataNodeAsyncRequestContext<TRegionLeaderChangeReq, TRegionLeaderChangeResp>
        clientHandler;
    private final Map<TConsensusGroupId, ConsensusGroupHeartbeatSample> successTransferMap;
    private final Map<Integer, List<String>> balancedOldLeaderId2RegionMap;

    private LeaderTransferContext(long currentTime) {
      this.currentTime = currentTime;
      this.requestId = 0;
      this.clientHandler =
          new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.CHANGE_REGION_LEADER);
      this.successTransferMap = new TreeMap<>();
      this.balancedOldLeaderId2RegionMap = new HashMap<>();
    }

    private void putSuccessTransfer(TConsensusGroupId regionGroupId, long timestamp, int leaderId) {
      successTransferMap.put(regionGroupId, new ConsensusGroupHeartbeatSample(timestamp, leaderId));
    }

    private void putRatisTransferRequest(
        TConsensusGroupId regionGroupId, TDataNodeLocation newLeader) {
      TRegionLeaderChangeReq regionLeaderChangeReq =
          new TRegionLeaderChangeReq(regionGroupId, newLeader);
      clientHandler.putRequest(requestId, regionLeaderChangeReq);
      clientHandler.putNodeLocation(requestId, newLeader);
      requestId++;
    }

    private boolean hasRatisTransferRequest() {
      return requestId > 0;
    }
  }

  /** Create a leader balancer instance according to the configured leader distribution policy. */
  private AbstractLeaderBalancer createLeaderBalancer() {
    switch (CONF.getLeaderDistributionPolicy()) {
      case AbstractLeaderBalancer.GREEDY_POLICY:
        return new GreedyLeaderBalancer();
      case AbstractLeaderBalancer.HASH_POLICY:
        return new HashLeaderBalancer();
      case AbstractLeaderBalancer.CFS_POLICY:
      default:
        return new CostFlowSelectionLeaderBalancer();
    }
  }

  /**
   * Balance leaders for all enabled RegionGroup types.
   *
   * <p>If both SchemaRegion and DataRegion leader balance are enabled, the two types are balanced
   * concurrently and serialized only by their own type-specific locks.
   */
  private DataRegionLeaderBalanceResult balanceAllEnabledRegionLeaders() {
    return balanceSelectedRegionLeaders(
        IS_ENABLE_AUTO_LEADER_BALANCE_FOR_SCHEMA_REGION,
        IS_ENABLE_AUTO_LEADER_BALANCE_FOR_DATA_REGION);
  }

  /**
   * Balance leaders only for the RegionGroup types included in the given change event.
   *
   * <p>This avoids letting an unrelated SchemaRegion change delay DataRegion leader selection, and
   * vice versa.
   */
  private DataRegionLeaderBalanceResult balanceRegionLeadersForChangedGroups(
      Set<TConsensusGroupId> regionGroupIds) {
    final boolean shouldBalanceSchemaRegion =
        IS_ENABLE_AUTO_LEADER_BALANCE_FOR_SCHEMA_REGION
            && containsRegionType(regionGroupIds, TConsensusGroupType.SchemaRegion);
    final boolean shouldBalanceDataRegion =
        IS_ENABLE_AUTO_LEADER_BALANCE_FOR_DATA_REGION
            && containsRegionType(regionGroupIds, TConsensusGroupType.DataRegion);
    return balanceSelectedRegionLeaders(shouldBalanceSchemaRegion, shouldBalanceDataRegion);
  }

  /** Return whether the given RegionGroup set contains a RegionGroup of the specified type. */
  private boolean containsRegionType(
      Set<TConsensusGroupId> regionGroupIds, TConsensusGroupType regionGroupType) {
    return regionGroupIds.stream()
        .anyMatch(regionGroupId -> regionGroupType.equals(regionGroupId.getType()));
  }

  /**
   * Balance the selected RegionGroup types.
   *
   * <p>When both types are selected, DataRegion balance runs in the current thread while
   * SchemaRegion balance runs asynchronously. The method still waits for both rounds to finish
   * before returning.
   */
  private DataRegionLeaderBalanceResult balanceSelectedRegionLeaders(
      boolean shouldBalanceSchemaRegion, boolean shouldBalanceDataRegion) {
    if (shouldBalanceSchemaRegion && shouldBalanceDataRegion) {
      return balanceSchemaAndDataRegionLeaders();
    }
    if (shouldBalanceSchemaRegion) {
      balanceRegionLeaders(schemaRegionLeaderBalanceContext);
    }
    return shouldBalanceDataRegion
        ? balanceRegionLeaders(dataRegionLeaderBalanceContext)
        : DataRegionLeaderBalanceResult.empty();
  }

  /**
   * Balance SchemaRegion and DataRegion leaders in parallel while preserving serialization inside
   * each RegionGroup type.
   */
  private DataRegionLeaderBalanceResult balanceSchemaAndDataRegionLeaders() {
    CompletableFuture<Void> schemaRegionLeaderBalanceFuture =
        CompletableFuture.runAsync(() -> balanceRegionLeaders(schemaRegionLeaderBalanceContext));
    try {
      return balanceRegionLeaders(dataRegionLeaderBalanceContext);
    } finally {
      schemaRegionLeaderBalanceFuture.join();
    }
  }

  /** Balance leaders of one RegionGroup type under its dedicated serialization lock. */
  private DataRegionLeaderBalanceResult balanceRegionLeaders(
      LeaderBalanceContext leaderBalanceContext) {
    leaderBalanceContext.leaderBalanceLock.lock();
    try {
      return balanceRegionLeadersInLock(leaderBalanceContext);
    } finally {
      leaderBalanceContext.leaderBalanceLock.unlock();
    }
  }

  /**
   * Generate the optimal leader distribution of one RegionGroup type and apply the required leader
   * transfers.
   */
  private DataRegionLeaderBalanceResult balanceRegionLeadersInLock(
      LeaderBalanceContext leaderBalanceContext) {
    // Collect the latest data and generate the optimal leader distribution
    Map<TConsensusGroupId, Integer> currentLeaderMap =
        getLoadManager().getLoadCache().getRegionLeaderMap(leaderBalanceContext.regionGroupType);
    Map<TConsensusGroupId, Integer> optimalLeaderMap =
        leaderBalanceContext.leaderBalancer.generateOptimalLeaderDistribution(
            getLoadManager()
                .getLoadCache()
                .getCurrentDatabaseRegionGroupMap(leaderBalanceContext.regionGroupType),
            getLoadManager()
                .getLoadCache()
                .getCurrentRegionLocationMap(leaderBalanceContext.regionGroupType),
            currentLeaderMap,
            getLoadManager().getLoadCache().getCurrentDataNodeStatisticsMap(),
            getLoadManager()
                .getLoadCache()
                .getCurrentRegionStatisticsMap(leaderBalanceContext.regionGroupType));

    LeaderTransferContext leaderTransferContext = new LeaderTransferContext(System.nanoTime());
    optimalLeaderMap.forEach(
        (regionGroupId, newLeaderId) ->
            collectLeaderTransfer(
                leaderBalanceContext.regionGroupType,
                leaderBalanceContext.consensusProtocolClass,
                currentLeaderMap,
                leaderTransferContext,
                regionGroupId,
                newLeaderId));

    sendRatisLeaderTransferRequests(leaderTransferContext);
    getLoadManager().forceUpdateConsensusGroupCache(leaderTransferContext.successTransferMap);

    return TConsensusGroupType.DataRegion.equals(leaderBalanceContext.regionGroupType)
        ? new DataRegionLeaderBalanceResult(
            new HashMap<>(currentLeaderMap),
            new HashSet<>(leaderTransferContext.successTransferMap.keySet()),
            leaderTransferContext.balancedOldLeaderId2RegionMap)
        : DataRegionLeaderBalanceResult.empty();
  }

  /** Collect one leader transfer into either an immediate cache update or a Ratis transfer RPC. */
  private void collectLeaderTransfer(
      TConsensusGroupType regionGroupType,
      String consensusProtocolClass,
      Map<TConsensusGroupId, Integer> currentLeaderMap,
      LeaderTransferContext leaderTransferContext,
      TConsensusGroupId regionGroupId,
      Integer newLeaderId) {
    if (shouldSkipRatisLeaderTransferAfterFailure(
        consensusProtocolClass, regionGroupId, leaderTransferContext.currentTime)) {
      return;
    }

    int oldLeaderId = currentLeaderMap.get(regionGroupId);
    if (newLeaderId == -1 || newLeaderId.equals(oldLeaderId)) {
      return;
    }

    LOGGER.info(
        ManagerMessages.LEADERBALANCER_TRY_TO_CHANGE_THE_LEADER_OF_REGION_TO_DATANODE,
        regionGroupId,
        newLeaderId);
    switch (consensusProtocolClass) {
      case ConsensusFactory.IOT_CONSENSUS:
      case ConsensusFactory.SIMPLE_CONSENSUS:
        // For IoTConsensus or SimpleConsensus protocol, changing RegionRouteMap is enough.
        leaderTransferContext.putSuccessTransfer(
            regionGroupId, leaderTransferContext.currentTime, newLeaderId);
        break;
      case ConsensusFactory.IOT_CONSENSUS_V2:
        leaderTransferContext.putSuccessTransfer(
            regionGroupId, leaderTransferContext.currentTime, newLeaderId);
        recordOldLeaderForFlush(
            leaderTransferContext.balancedOldLeaderId2RegionMap, regionGroupId, oldLeaderId);
        break;
      case ConsensusFactory.RATIS_CONSENSUS:
      default:
        collectRatisLeaderTransfer(
            regionGroupType, leaderTransferContext, regionGroupId, newLeaderId);
        break;
    }
  }

  /** Return whether a Ratis leader transfer should be skipped because it recently failed. */
  private boolean shouldSkipRatisLeaderTransferAfterFailure(
      String consensusProtocolClass, TConsensusGroupId regionGroupId, long currentTime) {
    return ConsensusFactory.RATIS_CONSENSUS.equals(consensusProtocolClass)
        && currentTime - lastFailedTimeForLeaderBalance.getOrDefault(regionGroupId, 0L)
            <= BALANCE_RATIS_LEADER_FAILED_INTERVAL_IN_NS;
  }

  /** Prepare a Ratis leader transfer, or force-update cache directly for single-replica groups. */
  private void collectRatisLeaderTransfer(
      TConsensusGroupType regionGroupType,
      LeaderTransferContext leaderTransferContext,
      TConsensusGroupId regionGroupId,
      int newLeaderId) {
    // The RegionRouteMap will be updated later by heartbeat if the transfer succeeds.
    if (isSingleReplicaRegionGroup(regionGroupType)) {
      leaderTransferContext.putSuccessTransfer(regionGroupId, 0, newLeaderId);
      return;
    }

    TDataNodeLocation newLeader = getNodeManager().getRegisteredDataNode(newLeaderId).getLocation();
    leaderTransferContext.putRatisTransferRequest(regionGroupId, newLeader);
  }

  /** Return whether the RegionGroup type has only one replica and therefore no transfer RPC. */
  private boolean isSingleReplicaRegionGroup(TConsensusGroupType regionGroupType) {
    return (TConsensusGroupType.SchemaRegion.equals(regionGroupType)
            && CONF.getSchemaReplicationFactor() == 1)
        || (TConsensusGroupType.DataRegion.equals(regionGroupType)
            && CONF.getDataReplicationFactor() == 1);
  }

  /**
   * Record the old DataRegion leader so IoTConsensusV2 can flush it after a successful transfer.
   */
  private void recordOldLeaderForFlush(
      Map<Integer, List<String>> balancedOldLeaderId2RegionMap,
      TConsensusGroupId regionGroupId,
      int oldLeaderId) {
    if (oldLeaderId != -1) {
      balancedOldLeaderId2RegionMap
          .computeIfAbsent(oldLeaderId, ignored -> new ArrayList<>())
          .add(String.valueOf(regionGroupId.getId()));
    }
  }

  /** Send collected Ratis leader transfer RPCs and record successful transfers in cache samples. */
  private void sendRatisLeaderTransferRequests(LeaderTransferContext leaderTransferContext) {
    if (!leaderTransferContext.hasRatisTransferRequest()) {
      return;
    }

    // Don't retry ChangeLeader request.
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(
            leaderTransferContext.clientHandler, 1, RATIS_CHANGE_LEADER_RPC_TIMEOUT_IN_MS);
    for (int i = 0; i < leaderTransferContext.requestId; i++) {
      TRegionLeaderChangeReq request = leaderTransferContext.clientHandler.getRequest(i);
      TRegionLeaderChangeResp response =
          leaderTransferContext.clientHandler.getResponseMap().get(i);
      if (response != null
          && response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        leaderTransferContext.putSuccessTransfer(
            request.getRegionId(),
            response.getConsensusLogicalTimestamp(),
            request.getNewLeaderNode().getDataNodeId());
      } else {
        lastFailedTimeForLeaderBalance.put(request.getRegionId(), System.nanoTime());
        LOGGER.error(
            ManagerMessages.LEADERBALANCER_FAILED_TO_CHANGE_THE_LEADER_OF_REGION_TO_DATANODE,
            request.getRegionId(),
            request.getNewLeaderNode().getDataNodeId());
      }
    }
  }

  /** Invalidate schema cache on old DataRegion leaders after successful leader transfers. */
  private void invalidateSchemaCacheOfOldLeaders(
      DataRegionLeaderBalanceResult leaderBalanceResult) {
    if (!IS_ENABLE_AUTO_LEADER_BALANCE_FOR_DATA_REGION
        || !leaderBalanceResult.hasBalancedDataRegion()) {
      return;
    }

    final DataNodeAsyncRequestContext<String, TSStatus> invalidateSchemaCacheRequestHandler =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.INVALIDATE_LAST_CACHE);
    int requestIndex = 0;
    for (Map.Entry<TConsensusGroupId, Integer> entry :
        leaderBalanceResult.dataRegion2OldLeaderMap.entrySet()) {
      if (!leaderBalanceResult.balancedDataRegionSet.contains(entry.getKey())) {
        continue;
      }
      requestIndex =
          addInvalidateSchemaCacheRequest(
              invalidateSchemaCacheRequestHandler, requestIndex, entry.getKey(), entry.getValue());
    }
    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequest(invalidateSchemaCacheRequestHandler);
  }

  /** Add one invalidate-schema-cache request for the old leader of a balanced DataRegion. */
  private int addInvalidateSchemaCacheRequest(
      DataNodeAsyncRequestContext<String, TSStatus> invalidateSchemaCacheRequestHandler,
      int requestIndex,
      TConsensusGroupId consensusGroupId,
      Integer dataNodeId) {
    if (dataNodeId == null || dataNodeId == -1) {
      return requestIndex;
    }

    TDataNodeConfiguration dataNodeConfiguration =
        getNodeManager().getRegisteredDataNode(dataNodeId);
    if (dataNodeConfiguration == null || dataNodeConfiguration.getLocation() == null) {
      LOGGER.warn(ManagerMessages.DATANODELOCATION_IS_NULL_DATANODEID, dataNodeId);
      return requestIndex;
    }

    invalidateSchemaCacheRequestHandler.putNodeLocation(
        requestIndex, dataNodeConfiguration.getLocation());
    invalidateSchemaCacheRequestHandler.putRequest(
        requestIndex, getPartitionManager().getRegionDatabase(consensusGroupId));
    return requestIndex + 1;
  }

  /** Flush old DataRegion leaders after IoTConsensusV2 leader transfers. */
  private void flushOldLeaderIfIoTV2(DataRegionLeaderBalanceResult leaderBalanceResult) {
    if (!IS_ENABLE_AUTO_LEADER_BALANCE_FOR_DATA_REGION
        || !Objects.equals(DATA_REGION_CONSENSUS_PROTOCOL_CLASS, ConsensusFactory.IOT_CONSENSUS_V2)
        || leaderBalanceResult.balancedOldLeaderId2RegionMap.isEmpty()) {
      return;
    }

    leaderBalanceResult.balancedOldLeaderId2RegionMap.forEach(
        (oldLeaderId, regionGroupIds) -> {
          TDataNodeConfiguration configuration =
              getNodeManager().getRegisteredDataNode(oldLeaderId);
          Map<Integer, TDataNodeLocation> oldLeaderDataNodeLocation = new HashMap<>();
          oldLeaderDataNodeLocation.put(
              configuration.getLocation().dataNodeId, configuration.getLocation());

          TFlushReq flushReq = new TFlushReq();
          flushReq.setRegionIds(regionGroupIds);
          // Do our best to flush. If flush failed, never retry
          TSStatus result = configManager.flushOnSpecificDN(flushReq, oldLeaderDataNodeLocation);
          if (result.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            LOGGER.info(
                ManagerMessages
                    .IOTCONSENSUSV2_LEADER_CHANGED_SUCCESSFULLY_FLUSH_OLD_LEADER_FOR_REGION,
                oldLeaderId,
                regionGroupIds);
          } else {
            LOGGER.info(
                ManagerMessages.IOTCONSENSUSV2_LEADER_CHANGED_FAILED_TO_FLUSH_OLD_LEADER_FOR_REGION,
                oldLeaderId,
                regionGroupIds);
          }
        });
  }

  /** Execute follow-up actions required after DataRegion leader balance. */
  private void handleBalanceAction(DataRegionLeaderBalanceResult leaderBalanceResult) {
    invalidateSchemaCacheOfOldLeaders(leaderBalanceResult);
    flushOldLeaderIfIoTV2(leaderBalanceResult);
  }

  /** Balance leaders and routing priority immediately, then run DataRegion follow-up actions. */
  public void balanceRegionLeaderAndPriority() {
    DataRegionLeaderBalanceResult leaderBalanceResult = balanceAllEnabledRegionLeaders();
    balanceRegionPriority();
    handleBalanceAction(leaderBalanceResult);
  }

  /** Balance cluster RegionGroup route priority through configured algorithm. */
  private synchronized void balanceRegionPriority() {
    priorityMapLock.writeLock().lock();
    AtomicBoolean needBroadcast = new AtomicBoolean(false);
    Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>> differentPriorityMap =
        new TreeMap<>();
    try {
      Map<TConsensusGroupId, Integer> regionLeaderMap = getLoadManager().getRegionLeaderMap();

      // Balancing region priority in each SchemaRegionGroup
      Map<TConsensusGroupId, TRegionReplicaSet> optimalRegionPriorityMap =
          priorityRouter.generateOptimalRoutePriority(
              getPartitionManager().getAllReplicaSets(TConsensusGroupType.SchemaRegion),
              regionLeaderMap);
      // Balancing region priority in each DataRegionGroup
      optimalRegionPriorityMap.putAll(
          priorityRouter.generateOptimalRoutePriority(
              getPartitionManager().getAllReplicaSets(TConsensusGroupType.DataRegion),
              regionLeaderMap));

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

  /** Broadcast the latest Region route priority map to all available DataNodes. */
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
    DataNodeAsyncRequestContext<TRegionRouteReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.UPDATE_REGION_ROUTE_MAP,
            new TRegionRouteReq(broadcastTime, tmpPriorityMap),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
  }

  /** Record Region route priority changes for later diagnosis. */
  private void recordRegionPriorityMap(
      Map<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>> differentPriorityMap) {
    LOGGER.info(ManagerMessages.REGIONPRIORITY_REGIONPRIORITYMAP);
    for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, TRegionReplicaSet>>
        regionPriorityEntry : differentPriorityMap.entrySet()) {
      if (!Objects.equals(
          regionPriorityEntry.getValue().getRight(), regionPriorityEntry.getValue().getLeft())) {
        LOGGER.info(
            ManagerMessages.REGIONPRIORITY,
            regionPriorityEntry.getKey(),
            regionPriorityEntry.getValue().getLeft() == null
                ? "null"
                : regionPriorityEntry.getValue().getLeft().getDataNodeLocations().stream()
                    .map(TDataNodeLocation::getDataNodeId)
                    .collect(Collectors.toList()),
            regionPriorityEntry.getValue().getRight().getDataNodeLocations().stream()
                .map(TDataNodeLocation::getDataNodeId)
                .collect(Collectors.toList()));
      }
    }
  }

  /**
   * Return a snapshot of the current Region route priority map.
   *
   * @return Map<RegionGroupId, RegionPriority>
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRegionPriorityMap() {
    priorityMapLock.readLock().lock();
    try {
      return new TreeMap<>(regionPriorityMap);
    } finally {
      priorityMapLock.readLock().unlock();
    }
  }

  /** Remove one RegionGroup's route priority from the local cache. */
  public void removeRegionPriority(TConsensusGroupId regionGroupId) {
    priorityMapLock.writeLock().lock();
    try {
      regionPriorityMap.remove(regionGroupId);
    } finally {
      priorityMapLock.writeLock().unlock();
    }
  }

  /** Clear all cached Region route priorities. */
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
        ManagerMessages.REGIONPRIORITY_WAIT_FOR_REGION_PRIORITY_UPDATE_OF_REGIONGROUPS,
        regionGroupIds);
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
            ManagerMessages.REGIONPRIORITY_THE_ROUTING_PRIORITY_OF_REGIONGROUPS_IS_CALCULATED,
            regionGroupIds);
        return;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(WAIT_PRIORITY_INTERVAL);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(ManagerMessages.INTERRUPT_WHEN_WAIT_FOR_CALCULATING_REGION_PRIORITY, e);
        return;
      }
    }

    LOGGER.warn(
        ManagerMessages.REGIONPRIORITY_THE_ROUTING_PRIORITY_OF_REGIONGROUPS_IS_NOT_DETERMINED_AFTER,
        regionGroupIds);
  }

  /** Return the NodeManager facade used by route balance operations. */
  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  /** Return the PartitionManager facade used by route balance operations. */
  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  /** Return the LoadManager facade used by route balance operations. */
  private LoadManager getLoadManager() {
    return configManager.getLoadManager();
  }

  /** Trigger leader balance when DataNode-level load statistics change. */
  @Override
  public void onNodeStatisticsChanged(NodeStatisticsChangeEvent event) {
    handleBalanceAction(balanceAllEnabledRegionLeaders());
  }

  /**
   * Trigger leader balance only for RegionGroup types present in RegionGroup statistics changes.
   */
  @Override
  public void onRegionGroupStatisticsChanged(RegionGroupStatisticsChangeEvent event) {
    handleBalanceAction(
        balanceRegionLeadersForChangedGroups(
            event.getDifferentRegionGroupStatisticsMap().keySet()));
  }

  /** Trigger leader balance and route priority update after consensus leader statistics change. */
  @Override
  public void onConsensusGroupStatisticsChanged(ConsensusGroupStatisticsChangeEvent event) {
    DataRegionLeaderBalanceResult leaderBalanceResult =
        balanceRegionLeadersForChangedGroups(
            event.getDifferentConsensusGroupStatisticsMap().keySet());
    balanceRegionPriority();
    handleBalanceAction(leaderBalanceResult);
  }
}
