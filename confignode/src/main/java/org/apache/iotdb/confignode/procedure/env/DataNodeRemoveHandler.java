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
import org.apache.iotdb.mpp.rpc.thrift.TAddConsensusGroup;
import org.apache.iotdb.mpp.rpc.thrift.TDisableDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TMigrateRegionReq;
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
   * Send to DataNode, add region peer
   *
   * @param originalDataNode old location data node
   * @param destDataNode dest data node
   * @param regionId region id
   * @return migrate status
   */
  public TSStatus addRegionPeer(
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode,
      TConsensusGroupId regionId) {
    TSStatus status;
    Optional<TDataNodeLocation> otherNode = findNodeOfAnotherReplica(regionId, originalDataNode);
    if (!otherNode.isPresent()) {
      LOGGER.warn(
          "No other Node to change region leader, check by show regions, region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("No other Node to change region leader, check by show regions");
      return status;
    }

    TMigrateRegionReq migrateRegionReq =
        new TMigrateRegionReq(regionId, originalDataNode, destDataNode);
    migrateRegionReq.setNewLeaderNode(otherNode.get());

    // send to otherNode node
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                otherNode.get().getInternalEndPoint(),
                migrateRegionReq,
                DataNodeRequestType.ADD_REGION_PEER);
    LOGGER.info(
        "Send region {} add peer action to {}, wait it finished",
        regionId,
        otherNode.get().getInternalEndPoint());
    return status;
  }

  /**
   * Send to DataNode, remove region
   *
   * @param originalDataNode old location data node
   * @param destDataNode dest data node
   * @param regionId region id
   * @return migrate status
   */
  public TSStatus removeRegionPeer(
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode,
      TConsensusGroupId regionId) {
    TSStatus status;
    Optional<TDataNodeLocation> otherNode = findNodeOfAnotherReplica(regionId, originalDataNode);
    if (!otherNode.isPresent()) {
      LOGGER.warn(
          "No other Node to change region leader, check by show regions, region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("No other Node to change region leader, check by show regions");
      return status;
    }

    TMigrateRegionReq migrateRegionReq =
        new TMigrateRegionReq(regionId, originalDataNode, destDataNode);
    migrateRegionReq.setNewLeaderNode(otherNode.get());

    // send to other node
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                otherNode.get().getInternalEndPoint(),
                migrateRegionReq,
                DataNodeRequestType.REMOVE_REGION_PEER);
    LOGGER.info(
        "Send region {} remove peer to {}, wait it finished",
        regionId,
        otherNode.get().getInternalEndPoint());
    return status;
  }

  /**
   * Send to DataNode, remove region consensus group from originalDataNode node
   *
   * @param originalDataNode old location data node
   * @param destDataNode dest data node
   * @param regionId region id
   * @return migrate status
   */
  public TSStatus removeRegionConsensusGroup(
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode,
      TConsensusGroupId regionId) {
    TSStatus status;
    TMigrateRegionReq migrateRegionReq =
        new TMigrateRegionReq(regionId, originalDataNode, destDataNode);
    migrateRegionReq.setNewLeaderNode(originalDataNode);
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                originalDataNode.getInternalEndPoint(),
                migrateRegionReq,
                DataNodeRequestType.REMOVE_REGION_CONSENSUS_GROUP);
    LOGGER.info(
        "Send region {} remove consensus group action to {}, wait it finished",
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
   * add region Consensus group in new node
   *
   * @param regionId region id
   * @param destDataNode dest data node
   * @return status
   */
  public TSStatus addNewNodeToRegionConsensusGroup(
      TConsensusGroupId regionId, TDataNodeLocation destDataNode) {
    TSStatus status;
    List<TDataNodeLocation> regionReplicaNodes = findRegionReplicaNodes(regionId);
    if (regionReplicaNodes.isEmpty()) {
      LOGGER.warn("Not find region replica nodes, region: {}", regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage("not find region replica nodes, region: " + regionId);
      return status;
    }

    List<TDataNodeLocation> currentPeerNodes = new ArrayList<>(regionReplicaNodes);
    currentPeerNodes.add(destDataNode);
    String storageGroup = configManager.getPartitionManager().getRegionStorageGroup(regionId);
    TAddConsensusGroup req = new TAddConsensusGroup(regionId, currentPeerNodes, storageGroup);
    // TODO replace with real ttl
    req.setTtl(Long.MAX_VALUE);

    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                destDataNode.getInternalEndPoint(),
                req,
                DataNodeRequestType.ADD_REGION_CONSENSUS_GROUP);

    LOGGER.info("send add region {} consensus group to {}", regionId, destDataNode);
    if (isFailed(status)) {
      LOGGER.error(
          "add new node {} to region {} consensus group failed,  result: {}",
          destDataNode,
          regionId,
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
        findNodeOfAnotherReplica(regionId, tDataNodeLocation);
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

  private Optional<TDataNodeLocation> findNodeOfAnotherReplica(
      TConsensusGroupId regionId, TDataNodeLocation tDataNodeLocation) {
    List<TDataNodeLocation> regionReplicaNodes = findRegionReplicaNodes(regionId);
    if (regionReplicaNodes.isEmpty()) {
      LOGGER.warn("Not find region replica nodes, region: {}", regionId);
      return Optional.empty();
    }

    // TODO replace findAny() by select the low load node.
    Optional<TDataNodeLocation> newLeaderNode =
        regionReplicaNodes.stream().filter(e -> !e.equals(tDataNodeLocation)).findAny();
    return newLeaderNode;
  }
}
