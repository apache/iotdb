1/*
1 * Licensed to the Apache Software Foundation (ASF) under one
1 * or more contributor license agreements.  See the NOTICE file
1 * distributed with this work for additional information
1 * regarding copyright ownership.  The ASF licenses this file
1 * to you under the Apache License, Version 2.0 (the
1 * "License"); you may not use this file except in compliance
1 * with the License.  You may obtain a copy of the License at
1 *
1 *     http://www.apache.org/licenses/LICENSE-2.0
1 *
1 * Unless required by applicable law or agreed to in writing,
1 * software distributed under the License is distributed on an
1 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
1 * KIND, either express or implied.  See the License for the
1 * specific language governing permissions and limitations
1 * under the License.
1 */
1package org.apache.iotdb.confignode.procedure.env;
1
1import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
1import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
1import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
1import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
1import org.apache.iotdb.common.rpc.thrift.TEndPoint;
1import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
1import org.apache.iotdb.common.rpc.thrift.TRegionMigrateFailedType;
1import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
1import org.apache.iotdb.common.rpc.thrift.TSStatus;
1import org.apache.iotdb.commons.client.ClientPoolFactory;
1import org.apache.iotdb.commons.client.IClientManager;
1import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
1import org.apache.iotdb.commons.cluster.NodeStatus;
1import org.apache.iotdb.commons.cluster.RegionStatus;
1import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
1import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
1import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
1import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
1import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
1import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
1import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
1import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
1import org.apache.iotdb.confignode.consensus.request.write.partition.AddRegionLocationPlan;
1import org.apache.iotdb.confignode.consensus.request.write.partition.RemoveRegionLocationPlan;
1import org.apache.iotdb.confignode.manager.ConfigManager;
1import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;
1import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
1import org.apache.iotdb.mpp.rpc.thrift.TCreatePeerReq;
1import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
1import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeResp;
1import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
1import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
1import org.apache.iotdb.rpc.TSStatusCode;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.util.ArrayList;
1import java.util.Collections;
1import java.util.HashMap;
1import java.util.List;
1import java.util.Map;
1import java.util.Optional;
1import java.util.concurrent.TimeUnit;
1import java.util.stream.Collectors;
1
1import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REGION_MIGRATE_PROCESS;
1import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS;
1import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS_V2;
1import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;
1
1public class RegionMaintainHandler {
1
1  private static final Logger LOGGER = LoggerFactory.getLogger(RegionMaintainHandler.class);
1
1  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
1
1  private final ConfigManager configManager;
1
1  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> dataNodeClientManager;
1
1  public RegionMaintainHandler(ConfigManager configManager) {
1    this.configManager = configManager;
1    dataNodeClientManager =
1        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
1            .createClientManager(
1                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
1  }
1
1  public static String getIdWithRpcEndpoint(TDataNodeLocation location) {
1    return String.format(
1        "[dataNodeId: %s, clientRpcEndPoint: %s]",
1        location.getDataNodeId(), location.getClientRpcEndPoint());
1  }
1
1  public static String simplifiedLocation(TDataNodeLocation dataNodeLocation) {
1    return dataNodeLocation.getDataNodeId() + "@" + dataNodeLocation.getInternalEndPoint().getIp();
1  }
1
1  /**
1   * Find dest data node.
1   *
1   * @param regionId region id
1   * @return dest data node location
1   */
1  public TDataNodeLocation findDestDataNode(TConsensusGroupId regionId) {
1    TSStatus status;
1    List<TDataNodeLocation> regionReplicaNodes = findRegionLocations(regionId);
1    if (regionReplicaNodes.isEmpty()) {
1      LOGGER.warn("Cannot find region replica nodes, region: {}", regionId);
1      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
1      status.setMessage("Cannot find region replica nodes, region: " + regionId);
1      return null;
1    }
1
1    Optional<TDataNodeLocation> newNode = pickNewReplicaNodeForRegion(regionReplicaNodes);
1    if (!newNode.isPresent()) {
1      LOGGER.warn("No enough Data node to migrate region: {}", regionId);
1      return null;
1    }
1    return newNode.get();
1  }
1
1  /**
1   * Create a new RegionReplica and build the ConsensusGroup on the destined DataNode
1   *
1   * <p>createNewRegionPeer should be invoked on a DataNode that doesn't contain any peer of the
1   * specific ConsensusGroup, in order to avoid there exists one DataNode who has more than one
1   * RegionReplica.
1   *
1   * @param regionId The given ConsensusGroup
1   * @param destDataNode The destined DataNode where the new peer will be created
1   * @return status
1   */
1  public TSStatus createNewRegionPeer(TConsensusGroupId regionId, TDataNodeLocation destDataNode) {
1    TSStatus status;
1    List<TDataNodeLocation> regionReplicaNodes = findRegionLocations(regionId);
1    if (regionReplicaNodes.isEmpty()) {
1      LOGGER.warn(
1          "{}, Cannot find region replica nodes in createPeer, regionId: {}",
1          REGION_MIGRATE_PROCESS,
1          regionId);
1      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
1      status.setMessage("Not find region replica nodes in createPeer, regionId: " + regionId);
1      return status;
1    }
1
1    List<TDataNodeLocation> currentPeerNodes;
1    if (TConsensusGroupType.DataRegion.equals(regionId.getType())
1        && (IOT_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())
1            || IOT_CONSENSUS_V2.equals(CONF.getDataRegionConsensusProtocolClass()))) {
1      // parameter of createPeer for MultiLeader should be all peers
1      currentPeerNodes = new ArrayList<>(regionReplicaNodes);
1      currentPeerNodes.add(destDataNode);
1    } else {
1      // parameter of createPeer for Ratis can be empty
1      currentPeerNodes = Collections.emptyList();
1    }
1
1    String database = configManager.getPartitionManager().getRegionDatabase(regionId);
1    TCreatePeerReq req = new TCreatePeerReq(regionId, currentPeerNodes, database);
1
1    status =
1        (TSStatus)
1            SyncDataNodeClientPool.getInstance()
1                .sendSyncRequestToDataNodeWithRetry(
1                    destDataNode.getInternalEndPoint(),
1                    req,
1                    CnToDnSyncRequestType.CREATE_NEW_REGION_PEER);
1
1    if (isSucceed(status)) {
1      LOGGER.info(
1          "{}, Send action createNewRegionPeer finished, regionId: {}, newPeerDataNodeId: {}",
1          REGION_MIGRATE_PROCESS,
1          regionId,
1          getIdWithRpcEndpoint(destDataNode));
1    } else {
1      LOGGER.error(
1          "{}, Send action createNewRegionPeer error, regionId: {}, newPeerDataNodeId: {}, result: {}",
1          REGION_MIGRATE_PROCESS,
1          regionId,
1          getIdWithRpcEndpoint(destDataNode),
1          status);
1    }
1
1    return status;
1  }
1
1  /**
1   * Order the specific ConsensusGroup to add peer for the new RegionReplica.
1   *
1   * <p>The add peer interface could be invoked at any DataNode who contains one of the
1   * RegionReplica of the specified ConsensusGroup except the new one
1   *
1   * @param destDataNode The DataNodeLocation where the new RegionReplica is created
1   * @param regionId region id
1   * @return TSStatus
1   */
1  public TSStatus submitAddRegionPeerTask(
1      long procedureId,
1      TDataNodeLocation destDataNode,
1      TConsensusGroupId regionId,
1      TDataNodeLocation coordinator) {
1    TSStatus status;
1
1    // Send addRegionPeer request to the selected DataNode,
1    // destDataNode is where the new RegionReplica is created
1    TMaintainPeerReq maintainPeerReq = new TMaintainPeerReq(regionId, destDataNode, procedureId);
1    status =
1        (TSStatus)
1            SyncDataNodeClientPool.getInstance()
1                .sendSyncRequestToDataNodeWithRetry(
1                    coordinator.getInternalEndPoint(),
1                    maintainPeerReq,
1                    CnToDnSyncRequestType.ADD_REGION_PEER);
1    LOGGER.info(
1        "{}, Send action addRegionPeer finished, regionId: {}, rpcDataNode: {},  destDataNode: {}, status: {}",
1        REGION_MIGRATE_PROCESS,
1        regionId,
1        getIdWithRpcEndpoint(coordinator),
1        getIdWithRpcEndpoint(destDataNode),
1        status);
1    return status;
1  }
1
1  /**
1   * Order the specific ConsensusGroup to remove peer for the old RegionReplica.
1   *
1   * <p>The remove peer interface could be invoked at any DataNode who contains one of the
1   * RegionReplica of the specified ConsensusGroup except the origin one
1   *
1   * @param originalDataNode The DataNodeLocation who contains the original RegionReplica
1   * @param regionId region id
1   * @return TSStatus
1   */
1  public TSStatus submitRemoveRegionPeerTask(
1      long procedureId,
1      TDataNodeLocation originalDataNode,
1      TConsensusGroupId regionId,
1      TDataNodeLocation coordinator) {
1    TSStatus status;
1
1    // Send removeRegionPeer request to the rpcClientDataNode
1    TMaintainPeerReq maintainPeerReq =
1        new TMaintainPeerReq(regionId, originalDataNode, procedureId);
1    status =
1        (TSStatus)
1            SyncDataNodeClientPool.getInstance()
1                .sendSyncRequestToDataNodeWithRetry(
1                    coordinator.getInternalEndPoint(),
1                    maintainPeerReq,
1                    CnToDnSyncRequestType.REMOVE_REGION_PEER);
1    LOGGER.info(
1        "{}, Send action removeRegionPeer finished, regionId: {}, rpcDataNode: {}",
1        REGION_MIGRATE_PROCESS,
1        regionId,
1        getIdWithRpcEndpoint(coordinator));
1    return status;
1  }
1
1  /**
1   * Delete a Region peer in the given ConsensusGroup and all of its data on the specified DataNode
1   *
1   * <p>If the originalDataNode is down, we should delete local data and do other cleanup works
1   * manually.
1   *
1   * @param originalDataNode The DataNodeLocation who contains the original RegionReplica
1   * @param regionId region id
1   * @return TSStatus
1   */
1  public TSStatus submitDeleteOldRegionPeerTask(
1      long procedureId, TDataNodeLocation originalDataNode, TConsensusGroupId regionId) {
1
1    TSStatus status;
1    TMaintainPeerReq maintainPeerReq =
1        new TMaintainPeerReq(regionId, originalDataNode, procedureId);
1
1    status =
1        configManager.getLoadManager().getNodeStatus(originalDataNode.getDataNodeId())
1                == NodeStatus.Unknown
1            ? (TSStatus)
1                SyncDataNodeClientPool.getInstance()
1                    .sendSyncRequestToDataNodeWithGivenRetry(
1                        originalDataNode.getInternalEndPoint(),
1                        maintainPeerReq,
1                        CnToDnSyncRequestType.DELETE_OLD_REGION_PEER,
1                        1)
1            : (TSStatus)
1                SyncDataNodeClientPool.getInstance()
1                    .sendSyncRequestToDataNodeWithRetry(
1                        originalDataNode.getInternalEndPoint(),
1                        maintainPeerReq,
1                        CnToDnSyncRequestType.DELETE_OLD_REGION_PEER);
1    LOGGER.info(
1        "{}, Send action deleteOldRegionPeer finished, regionId: {}, dataNodeId: {}",
1        REGION_MIGRATE_PROCESS,
1        regionId,
1        originalDataNode.getInternalEndPoint());
1    return status;
1  }
1
1  public Map<Integer, TSStatus> resetPeerList(
1      TConsensusGroupId regionId,
1      List<TDataNodeLocation> correctDataNodeLocations,
1      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
1    DataNodeAsyncRequestContext<TResetPeerListReq, TSStatus> clientHandler =
1        new DataNodeAsyncRequestContext<>(
1            CnToDnAsyncRequestType.RESET_PEER_LIST,
1            new TResetPeerListReq(regionId, correctDataNodeLocations),
1            dataNodeLocationMap);
1    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
1    return clientHandler.getResponseMap();
1  }
1
1  // TODO: will use 'procedure yield' to refactor later
1  public TRegionMigrateResult waitTaskFinish(long taskId, TDataNodeLocation dataNodeLocation) {
1    final long MAX_DISCONNECTION_TOLERATE_MS = 600_000;
1    final long INITIAL_DISCONNECTION_TOLERATE_MS = 60_000;
1    long startTime = System.nanoTime();
1    long lastReportTime = System.nanoTime();
1    while (true) {
1      try (SyncDataNodeInternalServiceClient dataNodeClient =
1          dataNodeClientManager.borrowClient(dataNodeLocation.getInternalEndPoint())) {
1        TRegionMigrateResult report = dataNodeClient.getRegionMaintainResult(taskId);
1        lastReportTime = System.nanoTime();
1        if (report.getTaskStatus() != TRegionMaintainTaskStatus.PROCESSING) {
1          return report;
1        }
1      } catch (Exception ignore) {
1
1      }
1      long waitTime =
1          Math.min(
1              INITIAL_DISCONNECTION_TOLERATE_MS
1                  + TimeUnit.NANOSECONDS.toMillis(lastReportTime - startTime) / 60,
1              MAX_DISCONNECTION_TOLERATE_MS);
1      long disconnectionTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastReportTime);
1      if (disconnectionTime > waitTime) {
1        LOGGER.warn(
1            "{} task {} cannot get task report from DataNode {}, last report time is {} ago",
1            REGION_MIGRATE_PROCESS,
1            taskId,
1            dataNodeLocation,
1            CommonDateTimeUtils.convertMillisecondToDurationStr(
1                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastReportTime)));
1        TRegionMigrateResult report = new TRegionMigrateResult();
1        report.setTaskStatus(TRegionMaintainTaskStatus.FAIL);
1        report.setFailedNodeAndReason(new HashMap<>());
1        report.getFailedNodeAndReason().put(dataNodeLocation, TRegionMigrateFailedType.Disconnect);
1        return report;
1      }
1      try {
1        TimeUnit.SECONDS.sleep(1);
1      } catch (InterruptedException ignore) {
1        Thread.currentThread().interrupt();
1        return new TRegionMigrateResult(TRegionMaintainTaskStatus.PROCESSING);
1      }
1    }
1  }
1
1  public void addRegionLocation(TConsensusGroupId regionId, TDataNodeLocation newLocation) {
1    AddRegionLocationPlan req = new AddRegionLocationPlan(regionId, newLocation);
1    TSStatus status = configManager.getPartitionManager().addRegionLocation(req);
1    LOGGER.info(
1        "AddRegionLocation finished, add region {} to {}, result is {}",
1        regionId,
1        getIdWithRpcEndpoint(newLocation),
1        status);
1    configManager
1        .getLoadManager()
1        .getLoadCache()
1        .createRegionCache(regionId, newLocation.getDataNodeId());
1  }
1
1  public void forceUpdateRegionCache(
1      TConsensusGroupId regionId, TDataNodeLocation newLocation, RegionStatus regionStatus) {
1    configManager
1        .getLoadManager()
1        .forceUpdateRegionCache(regionId, newLocation.getDataNodeId(), regionStatus);
1  }
1
1  public void removeRegionLocation(
1      TConsensusGroupId regionId, TDataNodeLocation deprecatedLocation) {
1    RemoveRegionLocationPlan req = new RemoveRegionLocationPlan(regionId, deprecatedLocation);
1    TSStatus status = configManager.getPartitionManager().removeRegionLocation(req);
1    LOGGER.info(
1        "RemoveRegionLocation remove region {} from DataNode {}, result is {}",
1        regionId,
1        getIdWithRpcEndpoint(deprecatedLocation),
1        status);
1    configManager.getLoadManager().removeRegionCache(regionId, deprecatedLocation.getDataNodeId());
1    configManager.getLoadManager().getRouteBalancer().balanceRegionLeaderAndPriority();
1  }
1
1  /**
1   * Find all DataNodes which contains the given regionId
1   *
1   * @param regionId region id
1   * @return DataNode locations
1   */
1  public List<TDataNodeLocation> findRegionLocations(TConsensusGroupId regionId) {
1    Optional<TRegionReplicaSet> regionReplicaSet = getRegionReplicaSet(regionId);
1    if (regionReplicaSet.isPresent()) {
1      return regionReplicaSet.get().getDataNodeLocations();
1    }
1    return Collections.emptyList();
1  }
1
1  public Optional<TRegionReplicaSet> getRegionReplicaSet(TConsensusGroupId regionId) {
1    return configManager.getPartitionManager().getAllReplicaSets().stream()
1        .filter(rg -> rg.regionId.equals(regionId))
1        .findAny();
1  }
1
1  public String getRegionReplicaSetString(TConsensusGroupId regionId) {
1    Optional<TRegionReplicaSet> regionReplicaSet = getRegionReplicaSet(regionId);
1    if (!regionReplicaSet.isPresent()) {
1      return "UNKNOWN!";
1    }
1    StringBuilder result = new StringBuilder(regionReplicaSet.get().getRegionId() + ": {");
1    for (TDataNodeLocation dataNodeLocation : regionReplicaSet.get().getDataNodeLocations()) {
1      result.append(simplifiedLocation(dataNodeLocation)).append(", ");
1    }
1    result.append("}");
1    return result.toString();
1  }
1
1  private Optional<TDataNodeLocation> pickNewReplicaNodeForRegion(
1      List<TDataNodeLocation> regionReplicaNodes) {
1    List<TDataNodeConfiguration> dataNodeConfigurations =
1        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running);
1    // Randomly selected to ensure a basic load balancing
1    Collections.shuffle(dataNodeConfigurations);
1    return dataNodeConfigurations.stream()
1        .map(TDataNodeConfiguration::getLocation)
1        .filter(e -> !regionReplicaNodes.contains(e))
1        .findAny();
1  }
1
1  public boolean isSucceed(TSStatus status) {
1    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
1  }
1
1  public boolean isFailed(TSStatus status) {
1    return !isSucceed(status);
1  }
1
1  /**
1   * Change the leader of given Region.
1   *
1   * <p>For IOT_CONSENSUS, using `changeLeaderForIoTConsensus` method to change the regionLeaderMap
1   * maintained in ConfigNode.
1   *
1   * <p>For RATIS_CONSENSUS, invoking `changeRegionLeader` DataNode RPC method to change the leader.
1   *
1   * @param regionId The region to be migrated
1   * @param originalDataNode The DataNode where the region locates
1   */
1  public void transferRegionLeader(
1      TConsensusGroupId regionId, TDataNodeLocation originalDataNode, TDataNodeLocation coodinator)
1      throws ProcedureException, InterruptedException {
1    // find new leader
1    Optional<TDataNodeLocation> newLeaderNode = Optional.empty();
1    List<TDataNodeLocation> excludeDataNode = new ArrayList<>();
1    excludeDataNode.add(originalDataNode);
1    excludeDataNode.add(coodinator);
1    newLeaderNode = filterDataNodeWithOtherRegionReplica(regionId, excludeDataNode);
1    if (!newLeaderNode.isPresent()) {
1      // If we have no choice, we use it
1      newLeaderNode = Optional.of(coodinator);
1    }
1    // ratis needs DataNode to do election by itself
1    long timestamp = System.nanoTime();
1    if (TConsensusGroupType.SchemaRegion.equals(regionId.getType())
1        || TConsensusGroupType.DataRegion.equals(regionId.getType())
1            && RATIS_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())) {
1      final int MAX_RETRY_TIME = 10;
1      int retryTime = 0;
1      long sleepTime =
1          (CONF.getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs()
1                  + CONF.getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs())
1              / 2;
1      Integer leaderId = configManager.getLoadManager().getRegionLeaderMap().get(regionId);
1
1      if (leaderId != -1) {
1        // The migrated node is not leader, so we don't need to transfer temporarily
1        if (originalDataNode.getDataNodeId() != leaderId) {
1          return;
1        }
1      }
1      while (true) {
1        TRegionLeaderChangeResp resp =
1            SyncDataNodeClientPool.getInstance()
1                .changeRegionLeader(
1                    regionId, originalDataNode.getInternalEndPoint(), newLeaderNode.get());
1        if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
1          timestamp = resp.getConsensusLogicalTimestamp();
1          break;
1        }
1        if (retryTime++ > MAX_RETRY_TIME) {
1          LOGGER.warn("[RemoveRegion] Ratis transfer leader fail, but procedure will continue.");
1          return;
1        }
1        LOGGER.warn(
1            "Call changeRegionLeader fail for the {} time, will sleep {} ms", retryTime, sleepTime);
1        Thread.sleep(sleepTime);
1      }
1    }
1
1    configManager
1        .getLoadManager()
1        .forceUpdateConsensusGroupCache(
1            Collections.singletonMap(
1                regionId,
1                new ConsensusGroupHeartbeatSample(timestamp, newLeaderNode.get().getDataNodeId())));
1    configManager.getLoadManager().getRouteBalancer().balanceRegionLeaderAndPriority();
1
1    LOGGER.info(
1        "{}, Change region leader finished, regionId: {}, newLeaderNode: {}",
1        REGION_MIGRATE_PROCESS,
1        regionId,
1        newLeaderNode);
1  }
1
1  /**
1   * Filter a DataNode who contains other RegionReplica excepts the given one.
1   *
1   * <p>Choose the RUNNING status datanode firstly, if no RUNNING status datanode match the
1   * condition, then we choose the REMOVING status datanode.
1   *
1   * <p>`addRegionPeer`, `removeRegionPeer` and `changeRegionLeader` invoke this method.
1   *
1   * @param regionId The specific RegionId
1   * @param filterLocation The DataNodeLocation that should be filtered
1   * @return A DataNodeLocation that contains other RegionReplica and different from the
1   *     filterLocation
1   */
1  public Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
1      TConsensusGroupId regionId, TDataNodeLocation filterLocation) {
1    List<TDataNodeLocation> filterLocations = Collections.singletonList(filterLocation);
1    return filterDataNodeWithOtherRegionReplica(regionId, filterLocations);
1  }
1
1  public Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
1      TConsensusGroupId regionId, List<TDataNodeLocation> filterLocations) {
1    return filterDataNodeWithOtherRegionReplica(
1        regionId, filterLocations, NodeStatus.Running, NodeStatus.ReadOnly);
1  }
1
1  public Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
1      TConsensusGroupId regionId, TDataNodeLocation filterLocation, NodeStatus... allowingStatus) {
1    List<TDataNodeLocation> excludeLocations = Collections.singletonList(filterLocation);
1    return filterDataNodeWithOtherRegionReplica(regionId, excludeLocations, allowingStatus);
1  }
1
1  public Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
1      TConsensusGroupId regionId,
1      List<TDataNodeLocation> excludeLocations,
1      NodeStatus... allowingStatus) {
1    List<TDataNodeLocation> regionLocations = findRegionLocations(regionId);
1    if (regionLocations.isEmpty()) {
1      LOGGER.warn("Cannot find DataNodes contain the given region: {}", regionId);
1      return Optional.empty();
1    }
1
1    // Choosing the RUNNING DataNodes to execute firstly
1    // If all DataNodes are not RUNNING, then choose the REMOVING DataNodes secondly
1    List<TDataNodeLocation> aliveDataNodes =
1        configManager.getNodeManager().filterDataNodeThroughStatus(allowingStatus).stream()
1            .map(TDataNodeConfiguration::getLocation)
1            .collect(Collectors.toList());
1    final int leaderId = configManager.getLoadManager().getRegionLeaderMap().get(regionId);
1    Collections.shuffle(aliveDataNodes);
1    Optional<TDataNodeLocation> bestChoice = Optional.empty();
1    for (TDataNodeLocation aliveDataNode : aliveDataNodes) {
1      if (regionLocations.contains(aliveDataNode) && !excludeLocations.contains(aliveDataNode)) {
1        if (leaderId == aliveDataNode.getDataNodeId()) {
1          bestChoice = Optional.of(aliveDataNode);
1          break;
1        } else if (!bestChoice.isPresent()) {
1          bestChoice = Optional.of(aliveDataNode);
1        }
1      }
1    }
1    return bestChoice;
1  }
1}
1