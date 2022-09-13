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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.sync.datanode.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.response.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.NodeInfo;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.scheduler.LockQueue;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TDisableDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DataNodeRemoveHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeRemoveHandler.class);

  private final ConfigManager configManager;

  /** region migrate lock */
  private final LockQueue regionMigrateLock = new LockQueue();

  public DataNodeRemoveHandler(ConfigManager configManager) {
    this.configManager = configManager;
  }

  /**
   * Get all consensus group id in this node
   *
   * @param dataNodeLocation data node location
   * @return group id list
   */
  public List<TConsensusGroupId> getDataNodeRegionIds(TDataNodeLocation dataNodeLocation) {
    return configManager.getPartitionManager().getAllReplicaSets().stream()
        .filter(
            rg ->
                rg.getDataNodeLocations().contains(dataNodeLocation)
                    && rg.regionId.getType() != TConsensusGroupType.PartitionRegion)
        .map(TRegionReplicaSet::getRegionId)
        .collect(Collectors.toList());
  }

  /**
   * broadcast these datanode in RemoveDataNodeReq are disabled, so they will not accept read/write
   * request
   *
   * @param disabledDataNode TDataNodeLocation
   */
  public TSStatus broadcastDisableDataNode(TDataNodeLocation disabledDataNode) {
    LOGGER.info(
        "DataNodeRemoveService start send disable the Data Node to cluster, {}", disabledDataNode);
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    List<TEndPoint> otherOnlineDataNodes =
        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
            .map(TDataNodeConfiguration::getLocation)
            .filter(loc -> !loc.equals(disabledDataNode))
            .map(TDataNodeLocation::getInternalEndPoint)
            .collect(Collectors.toList());

    for (TEndPoint server : otherOnlineDataNodes) {
      TDisableDataNodeReq disableReq = new TDisableDataNodeReq(disabledDataNode);
      status =
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  server, disableReq, DataNodeRequestType.DISABLE_DATA_NODE);
      if (!isSucceed(status)) {
        return status;
      }
    }
    LOGGER.info(
        "DataNodeRemoveService finished send disable the Data Node to cluster, {}",
        disabledDataNode);
    status.setMessage("Succeed disable the Data Node from cluster");
    return status;
  }

  /**
   * Find dest data node
   *
   * @param regionId region id
   * @return dest data node location
   */
  public TDataNodeLocation findDestDataNode(TConsensusGroupId regionId) {
    TSStatus status;
    List<TDataNodeLocation> regionReplicaNodes = findRegionReplicaNodes(regionId);
    if (regionReplicaNodes.isEmpty()) {
      LOGGER.warn("Not find region replica nodes, region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("not find region replica nodes, region: " + regionId);
      return null;
    }

    Optional<TDataNodeLocation> newNode = pickNewReplicaNodeForRegion(regionReplicaNodes);
    if (!newNode.isPresent()) {
      LOGGER.warn("No enough Data node to migrate region: {}", regionId);
    }
    return newNode.get();
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
  public TSStatus addRegionPeer(TDataNodeLocation destDataNode, TConsensusGroupId regionId) {
    TSStatus status;

    // Here we pick the DataNode who contains one of the RegionReplica of the specified
    // ConsensusGroup except the new one
    // in order to notify the origin ConsensusGroup that another peer is created and demand to join
    Optional<TDataNodeLocation> selectedDataNode =
        filterDataNodeWithOtherRegionReplica(regionId, destDataNode);
    if (!selectedDataNode.isPresent()) {
      LOGGER.warn(
          "There are no other DataNodes could be selected to perform the add peer process, please check RegionGroup: {} by SQL: show regions",
          regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "There are no other DataNodes could be selected to perform the add peer process, please check by SQL: show regions");
      return status;
    }

    // Send addRegionPeer request to the selected DataNode
    TMaintainPeerReq maintainPeerReq = new TMaintainPeerReq(regionId, selectedDataNode.get());
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                selectedDataNode.get().getInternalEndPoint(),
                maintainPeerReq,
                DataNodeRequestType.ADD_REGION_PEER);
    LOGGER.info(
        "Send region {} add peer action to {}, wait it finished",
        regionId,
        selectedDataNode.get().getInternalEndPoint());
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
  public TSStatus removeRegionPeer(TDataNodeLocation originalDataNode, TConsensusGroupId regionId) {
    TSStatus status;

    // Here we pick the DataNode who contains one of the RegionReplica of the specified
    // ConsensusGroup except the origin one
    // in order to notify the new ConsensusGroup that the origin peer should secede now
    Optional<TDataNodeLocation> selectedDataNode =
        filterDataNodeWithOtherRegionReplica(regionId, originalDataNode);
    if (!selectedDataNode.isPresent()) {
      LOGGER.warn(
          "There are no other DataNodes could be selected to perform the remove peer process, please check RegionGroup: {} by SQL: show regions",
          regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "There are no other DataNodes could be selected to perform the remove peer process, please check by SQL: show regions");
      return status;
    }

    // Send addRegionPeer request to the selected DataNode
    TMaintainPeerReq maintainPeerReq = new TMaintainPeerReq(regionId, selectedDataNode.get());
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                selectedDataNode.get().getInternalEndPoint(),
                maintainPeerReq,
                DataNodeRequestType.REMOVE_REGION_PEER);
    LOGGER.info(
        "Send region {} remove peer to {}, wait it finished",
        regionId,
        selectedDataNode.get().getInternalEndPoint());
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
  public TSStatus deleteOldRegionPeer(
      TDataNodeLocation originalDataNode, TConsensusGroupId regionId) {
    TSStatus status;
    TMaintainPeerReq maintainPeerReq = new TMaintainPeerReq(regionId, originalDataNode);
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                originalDataNode.getInternalEndPoint(),
                maintainPeerReq,
                DataNodeRequestType.DELETE_OLD_REGION_PEER);
    LOGGER.info(
        "Send region {} delete peer action to {}, wait it finished",
        regionId,
        originalDataNode.getInternalEndPoint());
    return status;
  }

  /**
   * Update region location cache
   *
   * @param regionId region id
   * @param originalDataNode old location data node
   * @param destDataNode dest data node
   */
  public void updateRegionLocationCache(
      TConsensusGroupId regionId,
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode) {
    LOGGER.info(
        "start to update region {} location from {} to {} when it migrate succeed",
        regionId,
        originalDataNode.getInternalEndPoint().getIp(),
        destDataNode.getInternalEndPoint().getIp());
    UpdateRegionLocationPlan req =
        new UpdateRegionLocationPlan(regionId, originalDataNode, destDataNode);
    TSStatus status = configManager.getPartitionManager().updateRegionLocation(req);
    LOGGER.info(
        "update region {} location finished, result:{}, old:{}, new:{}",
        regionId,
        status,
        originalDataNode.getInternalEndPoint().getIp(),
        destDataNode.getInternalEndPoint().getIp());
    // Broadcast the latest RegionRouteMap when Region migration finished
    configManager.getLoadManager().broadcastLatestRegionRouteMap();
  }

  /**
   * Find region replication Nodes
   *
   * @param regionId region id
   * @return data node location
   */
  public List<TDataNodeLocation> findRegionReplicaNodes(TConsensusGroupId regionId) {
    List<TRegionReplicaSet> regionReplicaSets =
        configManager.getPartitionManager().getAllReplicaSets().stream()
            .filter(rg -> rg.regionId.equals(regionId))
            .collect(Collectors.toList());
    if (regionReplicaSets.isEmpty()) {
      LOGGER.warn("not find TRegionReplica for region: {}", regionId);
      return Collections.emptyList();
    }

    return regionReplicaSets.get(0).getDataNodeLocations();
  }

  private Optional<TDataNodeLocation> pickNewReplicaNodeForRegion(
      List<TDataNodeLocation> regionReplicaNodes) {
    return configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
        .map(TDataNodeConfiguration::getLocation)
        .filter(e -> !regionReplicaNodes.contains(e))
        .findAny();
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
    List<TDataNodeLocation> regionReplicaNodes = findRegionReplicaNodes(regionId);
    if (regionReplicaNodes.isEmpty()) {
      LOGGER.warn("Not find region replica nodes in createPeer, regionId: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("Not find region replica nodes in createPeer, regionId: " + regionId);
      return status;
    }

    List<TDataNodeLocation> currentPeerNodes = new ArrayList<>(regionReplicaNodes);
    currentPeerNodes.add(destDataNode);
    String storageGroup = configManager.getPartitionManager().getRegionStorageGroup(regionId);
    TCreatePeerReq req = new TCreatePeerReq(regionId, currentPeerNodes, storageGroup);
    // TODO replace with real ttl
    req.setTtl(Long.MAX_VALUE);

    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                destDataNode.getInternalEndPoint(),
                req,
                DataNodeRequestType.CREATE_NEW_REGION_PEER);

    LOGGER.info("Send create peer for regionId {} on data node {}", regionId, destDataNode);
    if (isFailed(status)) {
      LOGGER.error(
          "Send create peer for regionId {} on data node {},  result: {}",
          regionId,
          destDataNode,
          status);
    }
    return status;
  }

  private boolean isSucceed(TSStatus status) {
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
  }

  private boolean isFailed(TSStatus status) {
    return !isSucceed(status);
  }

  /**
   * Stop old data node
   *
   * @param dataNode old data node
   * @return status
   * @throws ProcedureException procedure exception
   */
  public TSStatus stopDataNode(TDataNodeLocation dataNode) throws ProcedureException {
    LOGGER.info("begin to stop Data Node {}", dataNode);
    AsyncDataNodeClientPool.getInstance().resetClient(dataNode.getInternalEndPoint());
    TSStatus status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                dataNode.getInternalEndPoint(), dataNode, DataNodeRequestType.STOP_DATA_NODE);
    configManager.getNodeManager().removeNodeCache(dataNode.getDataNodeId());
    LOGGER.info("stop Data Node {} result: {}", dataNode, status);
    return status;
  }

  /**
   * check if the remove datanode request illegal
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS when request is legal.
   */
  public DataNodeToStatusResp checkRemoveDataNodeRequest(RemoveDataNodePlan removeDataNodePlan) {
    DataNodeToStatusResp dataSet = new DataNodeToStatusResp();
    dataSet.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    TSStatus status = checkRegionReplication(removeDataNodePlan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(status);
      return dataSet;
    }

    status = checkDataNodeExist(removeDataNodePlan);
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      dataSet.setStatus(status);
      return dataSet;
    }

    return dataSet;
  }

  /**
   * Check whether all DataNodes to be deleted exist in the cluster
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS if all DataNodes to be deleted exist in the cluster, DATANODE_NOT_EXIST
   *     otherwise
   */
  private TSStatus checkDataNodeExist(RemoveDataNodePlan removeDataNodePlan) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

    List<TDataNodeLocation> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes().stream()
            .map(TDataNodeConfiguration::getLocation)
            .collect(Collectors.toList());
    boolean hasNotExistNode =
        removeDataNodePlan.getDataNodeLocations().stream()
            .anyMatch(loc -> !allDataNodes.contains(loc));
    if (hasNotExistNode) {
      status.setCode(TSStatusCode.DATANODE_NOT_EXIST.getStatusCode());
      status.setMessage("there exist Data Node in request but not in cluster");
    }
    return status;
  }

  /**
   * Check whether the cluster has enough DataNodes to maintain RegionReplicas
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS if the number of DataNodes is enough, LACK_REPLICATION otherwise
   */
  private TSStatus checkRegionReplication(RemoveDataNodePlan removeDataNodePlan) {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    int removedDataNodeSize = removeDataNodePlan.getDataNodeLocations().size();
    int allDataNodeSize = configManager.getNodeManager().getRegisteredDataNodeCount();
    if (allDataNodeSize - removedDataNodeSize < NodeInfo.getMinimumDataNode()) {
      status.setCode(TSStatusCode.LACK_REPLICATION.getStatusCode());
      status.setMessage(
          "lack replication, allow most removed Data Node size : "
              + (allDataNodeSize - NodeInfo.getMinimumDataNode()));
    }
    return status;
  }

  public LockQueue getRegionMigrateLock() {
    return regionMigrateLock;
  }

  /**
   * Remove data node in node info
   *
   * @param tDataNodeLocation data node location
   */
  public void removeDataNodePersistence(TDataNodeLocation tDataNodeLocation) {
    List<TDataNodeLocation> removeDataNodes = new ArrayList<>();
    removeDataNodes.add(tDataNodeLocation);
    configManager.getConsensusManager().write(new RemoveDataNodePlan(removeDataNodes));
  }

  public void changeRegionLeader(TConsensusGroupId regionId, TDataNodeLocation tDataNodeLocation) {
    Optional<TDataNodeLocation> newLeaderNode =
        filterDataNodeWithOtherRegionReplica(regionId, tDataNodeLocation);
    if (newLeaderNode.isPresent()) {
      SyncDataNodeClientPool.getInstance()
          .changeRegionLeader(
              regionId, tDataNodeLocation.getInternalEndPoint(), newLeaderNode.get());
      LOGGER.info(
          "Change region leader finished, region is {}, newLeaderNode is {}",
          regionId,
          newLeaderNode);
    }
  }

  /**
   * Filter a DataNode who contains other RegionReplica excepts the given one
   *
   * @param regionId The specific RegionId
   * @param filterLocation The DataNodeLocation that should be filtered
   * @return A DataNodeLocation that contains other RegionReplica and different from the
   *     filterLocation
   */
  private Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
      TConsensusGroupId regionId, TDataNodeLocation filterLocation) {
    List<TDataNodeLocation> regionReplicaNodes = findRegionReplicaNodes(regionId);
    if (regionReplicaNodes.isEmpty()) {
      LOGGER.warn("Not find region replica nodes, region: {}", regionId);
      return Optional.empty();
    }

    // TODO replace findAny() by select the low load node.
    return regionReplicaNodes.stream().filter(e -> !e.equals(filterLocation)).findAny();
  }
}
