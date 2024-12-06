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
package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TRegionMigrateFailedType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.client.sync.CnToDnSyncRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.partition.AddRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.RemoveRegionLocationPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.cache.consensus.ConsensusGroupHeartbeatSample;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeResp;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REGION_MIGRATE_PROCESS;
import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS;
import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS_V2;
import static org.apache.iotdb.consensus.ConsensusFactory.RATIS_CONSENSUS;

public class RegionMaintainHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegionMaintainHandler.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private final ConfigManager configManager;

  /** region migrate lock */
  private final LockQueue regionMigrateLock = new LockQueue();

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> dataNodeClientManager;

  public RegionMaintainHandler(ConfigManager configManager) {
    this.configManager = configManager;
    dataNodeClientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
  }

  public static String getIdWithRpcEndpoint(TDataNodeLocation location) {
    return String.format(
        "[dataNodeId: %s, clientRpcEndPoint: %s]",
        location.getDataNodeId(), location.getClientRpcEndPoint());
  }

  /**
   * Find dest data node.
   *
   * @param regionId region id
   * @return dest data node location
   */
  public TDataNodeLocation findDestDataNode(TConsensusGroupId regionId) {
    TSStatus status;
    List<TDataNodeLocation> regionReplicaNodes = findRegionLocations(regionId);
    if (regionReplicaNodes.isEmpty()) {
      LOGGER.warn("Cannot find region replica nodes, region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("Cannot find region replica nodes, region: " + regionId);
      return null;
    }

    Optional<TDataNodeLocation> newNode = pickNewReplicaNodeForRegion(regionReplicaNodes);
    if (!newNode.isPresent()) {
      LOGGER.warn("No enough Data node to migrate region: {}", regionId);
      return null;
    }
    return newNode.get();
  }

  /**
   * Create a new RegionReplica and build the ConsensusGroup on the destined DataNode
   *
   * <p>createNewRegionPeer should be invoked on a DataNode that doesn't contain any peer of the
   * specific ConsensusGroup, in order to avoid there exists one DataNode who has more than one
   * RegionReplica.
   *
   * @param regionId The given ConsensusGroup
   * @param destDataNode The destined DataNode where the new peer will be created
   * @return status
   */
  public TSStatus createNewRegionPeer(TConsensusGroupId regionId, TDataNodeLocation destDataNode) {
    TSStatus status;
    List<TDataNodeLocation> regionReplicaNodes = findRegionLocations(regionId);
    if (regionReplicaNodes.isEmpty()) {
      LOGGER.warn(
          "{}, Cannot find region replica nodes in createPeer, regionId: {}",
          REGION_MIGRATE_PROCESS,
          regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("Not find region replica nodes in createPeer, regionId: " + regionId);
      return status;
    }

    List<TDataNodeLocation> currentPeerNodes;
    if (TConsensusGroupType.DataRegion.equals(regionId.getType())
        && (IOT_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())
            || IOT_CONSENSUS_V2.equals(CONF.getDataRegionConsensusProtocolClass()))) {
      // parameter of createPeer for MultiLeader should be all peers
      currentPeerNodes = new ArrayList<>(regionReplicaNodes);
      currentPeerNodes.add(destDataNode);
    } else {
      // parameter of createPeer for Ratis can be empty
      currentPeerNodes = Collections.emptyList();
    }

    String database = configManager.getPartitionManager().getRegionDatabase(regionId);
    TCreatePeerReq req = new TCreatePeerReq(regionId, currentPeerNodes, database);

    status =
        (TSStatus)
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithRetry(
                    destDataNode.getInternalEndPoint(),
                    req,
                    CnToDnSyncRequestType.CREATE_NEW_REGION_PEER);

    if (isSucceed(status)) {
      LOGGER.info(
          "{}, Send action createNewRegionPeer finished, regionId: {}, newPeerDataNodeId: {}",
          REGION_MIGRATE_PROCESS,
          regionId,
          getIdWithRpcEndpoint(destDataNode));
    } else {
      LOGGER.error(
          "{}, Send action createNewRegionPeer error, regionId: {}, newPeerDataNodeId: {}, result: {}",
          REGION_MIGRATE_PROCESS,
          regionId,
          getIdWithRpcEndpoint(destDataNode),
          status);
    }

    return status;
  }

  /**
   * Order the specific ConsensusGroup to add peer for the new RegionReplica.
   *
   * <p>The add peer interface could be invoked at any DataNode who contains one of the
   * RegionReplica of the specified ConsensusGroup except the new one
   *
   * @param destDataNode The DataNodeLocation where the new RegionReplica is created
   * @param regionId region id
   * @return TSStatus
   */
  public TSStatus submitAddRegionPeerTask(
      long procedureId,
      TDataNodeLocation destDataNode,
      TConsensusGroupId regionId,
      TDataNodeLocation coordinator) {
    TSStatus status;

    // Send addRegionPeer request to the selected DataNode,
    // destDataNode is where the new RegionReplica is created
    TMaintainPeerReq maintainPeerReq = new TMaintainPeerReq(regionId, destDataNode, procedureId);
    status =
        (TSStatus)
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithRetry(
                    coordinator.getInternalEndPoint(),
                    maintainPeerReq,
                    CnToDnSyncRequestType.ADD_REGION_PEER);
    LOGGER.info(
        "{}, Send action addRegionPeer finished, regionId: {}, rpcDataNode: {},  destDataNode: {}, status: {}",
        REGION_MIGRATE_PROCESS,
        regionId,
        getIdWithRpcEndpoint(coordinator),
        getIdWithRpcEndpoint(destDataNode),
        status);
    return status;
  }

  /**
   * Order the specific ConsensusGroup to remove peer for the old RegionReplica.
   *
   * <p>The remove peer interface could be invoked at any DataNode who contains one of the
   * RegionReplica of the specified ConsensusGroup except the origin one
   *
   * @param originalDataNode The DataNodeLocation who contains the original RegionReplica
   * @param regionId region id
   * @return TSStatus
   */
  public TSStatus submitRemoveRegionPeerTask(
      long procedureId,
      TDataNodeLocation originalDataNode,
      TConsensusGroupId regionId,
      TDataNodeLocation coordinator) {
    TSStatus status;

    // Send removeRegionPeer request to the rpcClientDataNode
    TMaintainPeerReq maintainPeerReq =
        new TMaintainPeerReq(regionId, originalDataNode, procedureId);
    status =
        (TSStatus)
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithRetry(
                    coordinator.getInternalEndPoint(),
                    maintainPeerReq,
                    CnToDnSyncRequestType.REMOVE_REGION_PEER);
    LOGGER.info(
        "{}, Send action removeRegionPeer finished, regionId: {}, rpcDataNode: {}",
        REGION_MIGRATE_PROCESS,
        regionId,
        getIdWithRpcEndpoint(coordinator));
    return status;
  }

  /**
   * Delete a Region peer in the given ConsensusGroup and all of its data on the specified DataNode
   *
   * <p>If the originalDataNode is down, we should delete local data and do other cleanup works
   * manually.
   *
   * @param originalDataNode The DataNodeLocation who contains the original RegionReplica
   * @param regionId region id
   * @return TSStatus
   */
  public TSStatus submitDeleteOldRegionPeerTask(
      long procedureId, TDataNodeLocation originalDataNode, TConsensusGroupId regionId) {

    TSStatus status;
    TMaintainPeerReq maintainPeerReq =
        new TMaintainPeerReq(regionId, originalDataNode, procedureId);

    status =
        configManager.getLoadManager().getNodeStatus(originalDataNode.getDataNodeId())
                == NodeStatus.Unknown
            ? (TSStatus)
                SyncDataNodeClientPool.getInstance()
                    .sendSyncRequestToDataNodeWithGivenRetry(
                        originalDataNode.getInternalEndPoint(),
                        maintainPeerReq,
                        CnToDnSyncRequestType.DELETE_OLD_REGION_PEER,
                        1)
            : (TSStatus)
                SyncDataNodeClientPool.getInstance()
                    .sendSyncRequestToDataNodeWithRetry(
                        originalDataNode.getInternalEndPoint(),
                        maintainPeerReq,
                        CnToDnSyncRequestType.DELETE_OLD_REGION_PEER);
    LOGGER.info(
        "{}, Send action deleteOldRegionPeer finished, regionId: {}, dataNodeId: {}",
        REGION_MIGRATE_PROCESS,
        regionId,
        originalDataNode.getInternalEndPoint());
    return status;
  }

  public Map<Integer, TSStatus> resetPeerList(
      TConsensusGroupId regionId,
      List<TDataNodeLocation> correctDataNodeLocations,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap) {
    DataNodeAsyncRequestContext<TResetPeerListReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.RESET_PEER_LIST,
            new TResetPeerListReq(regionId, correctDataNodeLocations),
            dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseMap();
  }

  // TODO: will use 'procedure yield' to refactor later
  public TRegionMigrateResult waitTaskFinish(long taskId, TDataNodeLocation dataNodeLocation) {
    final long MAX_DISCONNECTION_TOLERATE_MS = 600_000;
    final long INITIAL_DISCONNECTION_TOLERATE_MS = 60_000;
    long startTime = System.nanoTime();
    long lastReportTime = System.nanoTime();
    while (true) {
      try (SyncDataNodeInternalServiceClient dataNodeClient =
          dataNodeClientManager.borrowClient(dataNodeLocation.getInternalEndPoint())) {
        TRegionMigrateResult report = dataNodeClient.getRegionMaintainResult(taskId);
        lastReportTime = System.nanoTime();
        if (report.getTaskStatus() != TRegionMaintainTaskStatus.PROCESSING) {
          return report;
        }
      } catch (Exception ignore) {

      }
      long waitTime =
          Math.min(
              INITIAL_DISCONNECTION_TOLERATE_MS
                  + TimeUnit.NANOSECONDS.toMillis(lastReportTime - startTime) / 60,
              MAX_DISCONNECTION_TOLERATE_MS);
      long disconnectionTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastReportTime);
      if (disconnectionTime > waitTime) {
        LOGGER.warn(
            "{} task {} cannot get task report from DataNode {}, last report time is {} ago",
            REGION_MIGRATE_PROCESS,
            taskId,
            dataNodeLocation,
            CommonDateTimeUtils.convertMillisecondToDurationStr(
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastReportTime)));
        TRegionMigrateResult report = new TRegionMigrateResult();
        report.setTaskStatus(TRegionMaintainTaskStatus.FAIL);
        report.setFailedNodeAndReason(new HashMap<>());
        report.getFailedNodeAndReason().put(dataNodeLocation, TRegionMigrateFailedType.Disconnect);
        return report;
      }
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
        return new TRegionMigrateResult(TRegionMaintainTaskStatus.PROCESSING);
      }
    }
  }

  public void addRegionLocation(TConsensusGroupId regionId, TDataNodeLocation newLocation) {
    AddRegionLocationPlan req = new AddRegionLocationPlan(regionId, newLocation);
    TSStatus status = configManager.getPartitionManager().addRegionLocation(req);
    LOGGER.info(
        "AddRegionLocation finished, add region {} to {}, result is {}",
        regionId,
        getIdWithRpcEndpoint(newLocation),
        status);
    configManager
        .getLoadManager()
        .getLoadCache()
        .createRegionCache(regionId, newLocation.getDataNodeId());
  }

  public void forceUpdateRegionCache(
      TConsensusGroupId regionId, TDataNodeLocation newLocation, RegionStatus regionStatus) {
    configManager
        .getLoadManager()
        .forceUpdateRegionCache(regionId, newLocation.getDataNodeId(), regionStatus);
  }

  public void removeRegionLocation(
      TConsensusGroupId regionId, TDataNodeLocation deprecatedLocation) {
    RemoveRegionLocationPlan req = new RemoveRegionLocationPlan(regionId, deprecatedLocation);
    TSStatus status = configManager.getPartitionManager().removeRegionLocation(req);
    LOGGER.info(
        "RemoveRegionLocation remove region {} from DataNode {}, result is {}",
        regionId,
        getIdWithRpcEndpoint(deprecatedLocation),
        status);
    configManager.getLoadManager().removeRegionCache(regionId, deprecatedLocation.getDataNodeId());
    configManager.getLoadManager().getRouteBalancer().balanceRegionLeaderAndPriority();
  }

  /**
   * Find all DataNodes which contains the given regionId
   *
   * @param regionId region id
   * @return DataNode locations
   */
  public List<TDataNodeLocation> findRegionLocations(TConsensusGroupId regionId) {
    Optional<TRegionReplicaSet> regionReplicaSet =
        configManager.getPartitionManager().getAllReplicaSets().stream()
            .filter(rg -> rg.regionId.equals(regionId))
            .findAny();
    if (regionReplicaSet.isPresent()) {
      return regionReplicaSet.get().getDataNodeLocations();
    }

    return Collections.emptyList();
  }

  private Optional<TDataNodeLocation> pickNewReplicaNodeForRegion(
      List<TDataNodeLocation> regionReplicaNodes) {
    List<TDataNodeConfiguration> dataNodeConfigurations =
        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running);
    // Randomly selected to ensure a basic load balancing
    Collections.shuffle(dataNodeConfigurations);
    return dataNodeConfigurations.stream()
        .map(TDataNodeConfiguration::getLocation)
        .filter(e -> !regionReplicaNodes.contains(e))
        .findAny();
  }

  public boolean isSucceed(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  public boolean isFailed(TSStatus status) {
    return !isSucceed(status);
  }

  public LockQueue getRegionMigrateLock() {
    return regionMigrateLock;
  }

  /**
   * Change the leader of given Region.
   *
   * <p>For IOT_CONSENSUS, using `changeLeaderForIoTConsensus` method to change the regionLeaderMap
   * maintained in ConfigNode.
   *
   * <p>For RATIS_CONSENSUS, invoking `changeRegionLeader` DataNode RPC method to change the leader.
   *
   * @param regionId The region to be migrated
   * @param originalDataNode The DataNode where the region locates
   */
  public void transferRegionLeader(
      TConsensusGroupId regionId, TDataNodeLocation originalDataNode, TDataNodeLocation coodinator)
      throws ProcedureException, InterruptedException {
    // find new leader
    Optional<TDataNodeLocation> newLeaderNode = Optional.empty();
    List<TDataNodeLocation> excludeDataNode = new ArrayList<>();
    excludeDataNode.add(originalDataNode);
    excludeDataNode.add(coodinator);
    newLeaderNode = filterDataNodeWithOtherRegionReplica(regionId, excludeDataNode);
    if (!newLeaderNode.isPresent()) {
      // If we have no choice, we use it
      newLeaderNode = Optional.of(coodinator);
    }
    // ratis needs DataNode to do election by itself
    long timestamp = System.nanoTime();
    if (TConsensusGroupType.SchemaRegion.equals(regionId.getType())
        || TConsensusGroupType.DataRegion.equals(regionId.getType())
            && RATIS_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())) {
      final int MAX_RETRY_TIME = 10;
      int retryTime = 0;
      long sleepTime =
          (CONF.getSchemaRegionRatisRpcLeaderElectionTimeoutMaxMs()
                  + CONF.getSchemaRegionRatisRpcLeaderElectionTimeoutMinMs())
              / 2;
      Integer leaderId = configManager.getLoadManager().getRegionLeaderMap().get(regionId);

      if (leaderId != -1) {
        // The migrated node is not leader, so we don't need to transfer temporarily
        if (originalDataNode.getDataNodeId() != leaderId) {
          return;
        }
      }
      while (true) {
        TRegionLeaderChangeResp resp =
            SyncDataNodeClientPool.getInstance()
                .changeRegionLeader(
                    regionId, originalDataNode.getInternalEndPoint(), newLeaderNode.get());
        if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          timestamp = resp.getConsensusLogicalTimestamp();
          break;
        }
        if (retryTime++ > MAX_RETRY_TIME) {
          LOGGER.warn("[RemoveRegion] Ratis transfer leader fail, but procedure will continue.");
          return;
        }
        LOGGER.warn(
            "Call changeRegionLeader fail for the {} time, will sleep {} ms", retryTime, sleepTime);
        Thread.sleep(sleepTime);
      }
    }

    configManager
        .getLoadManager()
        .forceUpdateConsensusGroupCache(
            Collections.singletonMap(
                regionId,
                new ConsensusGroupHeartbeatSample(timestamp, newLeaderNode.get().getDataNodeId())));
    configManager.getLoadManager().getRouteBalancer().balanceRegionLeaderAndPriority();

    LOGGER.info(
        "{}, Change region leader finished, regionId: {}, newLeaderNode: {}",
        REGION_MIGRATE_PROCESS,
        regionId,
        newLeaderNode);
  }

  /**
   * Filter a DataNode who contains other RegionReplica excepts the given one.
   *
   * <p>Choose the RUNNING status datanode firstly, if no RUNNING status datanode match the
   * condition, then we choose the REMOVING status datanode.
   *
   * <p>`addRegionPeer`, `removeRegionPeer` and `changeRegionLeader` invoke this method.
   *
   * @param regionId The specific RegionId
   * @param filterLocation The DataNodeLocation that should be filtered
   * @return A DataNodeLocation that contains other RegionReplica and different from the
   *     filterLocation
   */
  public Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
      TConsensusGroupId regionId, TDataNodeLocation filterLocation) {
    List<TDataNodeLocation> filterLocations = Collections.singletonList(filterLocation);
    return filterDataNodeWithOtherRegionReplica(regionId, filterLocations);
  }

  public Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
      TConsensusGroupId regionId, List<TDataNodeLocation> filterLocations) {
    return filterDataNodeWithOtherRegionReplica(
        regionId, filterLocations, NodeStatus.Running, NodeStatus.ReadOnly);
  }

  public Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
      TConsensusGroupId regionId, TDataNodeLocation filterLocation, NodeStatus... allowingStatus) {
    List<TDataNodeLocation> excludeLocations = Collections.singletonList(filterLocation);
    return filterDataNodeWithOtherRegionReplica(regionId, excludeLocations, allowingStatus);
  }

  public Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
      TConsensusGroupId regionId,
      List<TDataNodeLocation> excludeLocations,
      NodeStatus... allowingStatus) {
    List<TDataNodeLocation> regionLocations = findRegionLocations(regionId);
    if (regionLocations.isEmpty()) {
      LOGGER.warn("Cannot find DataNodes contain the given region: {}", regionId);
      return Optional.empty();
    }

    // Choosing the RUNNING DataNodes to execute firstly
    // If all DataNodes are not RUNNING, then choose the REMOVING DataNodes secondly
    List<TDataNodeLocation> aliveDataNodes =
        configManager.getNodeManager().filterDataNodeThroughStatus(allowingStatus).stream()
            .map(TDataNodeConfiguration::getLocation)
            .collect(Collectors.toList());
    Collections.shuffle(aliveDataNodes);
    for (TDataNodeLocation aliveDataNode : aliveDataNodes) {
      if (regionLocations.contains(aliveDataNode) && !excludeLocations.contains(aliveDataNode)) {
        return Optional.of(aliveDataNode);
      }
    }
    return Optional.empty();
  }
}
