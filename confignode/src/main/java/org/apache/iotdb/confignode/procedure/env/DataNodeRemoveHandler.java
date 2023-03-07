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
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.node.heartbeat.BaseNodeCache;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
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

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REGION_MIGRATE_PROCESS;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;
import static org.apache.iotdb.consensus.ConsensusFactory.IOT_CONSENSUS;
import static org.apache.iotdb.consensus.ConsensusFactory.SIMPLE_CONSENSUS;

public class DataNodeRemoveHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataNodeRemoveHandler.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private final ConfigManager configManager;

  /** region migrate lock */
  private final LockQueue regionMigrateLock = new LockQueue();

  public DataNodeRemoveHandler(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public static String getIdWithRpcEndpoint(TDataNodeLocation location) {
    return String.format(
        "[dataNodeId: %s, clientRpcEndPoint: %s]",
        location.getDataNodeId(), location.getClientRpcEndPoint());
  }

  /**
   * Get all consensus group id in this node
   *
   * @param removedDataNode the DataNode to be removed
   * @return group id list to be migrated
   */
  public List<TConsensusGroupId> getMigratedDataNodeRegions(TDataNodeLocation removedDataNode) {
    return configManager.getPartitionManager().getAllReplicaSets().stream()
        .filter(
            replicaSet ->
                replicaSet.getDataNodeLocations().contains(removedDataNode)
                    && replicaSet.regionId.getType() != TConsensusGroupType.ConfigRegion)
        .map(TRegionReplicaSet::getRegionId)
        .collect(Collectors.toList());
  }

  /**
   * broadcast these datanode in RemoveDataNodeReq are disabled, so they will not accept read/write
   * request
   *
   * @param disabledDataNode TDataNodeLocation
   */
  public void broadcastDisableDataNode(TDataNodeLocation disabledDataNode) {
    LOGGER.info(
        "DataNodeRemoveService start broadcastDisableDataNode to cluster, disabledDataNode: {}",
        getIdWithRpcEndpoint(disabledDataNode));

    List<TDataNodeConfiguration> otherOnlineDataNodes =
        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
            .filter(node -> !node.getLocation().equals(disabledDataNode))
            .collect(Collectors.toList());

    for (TDataNodeConfiguration node : otherOnlineDataNodes) {
      TDisableDataNodeReq disableReq = new TDisableDataNodeReq(disabledDataNode);
      TSStatus status =
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  node.getLocation().getInternalEndPoint(),
                  disableReq,
                  DataNodeRequestType.DISABLE_DATA_NODE);
      if (!isSucceed(status)) {
        LOGGER.error(
            "{}, BroadcastDisableDataNode meets error, disabledDataNode: {}, error: {}",
            REMOVE_DATANODE_PROCESS,
            getIdWithRpcEndpoint(disabledDataNode),
            status);
        return;
      }
    }

    LOGGER.info(
        "{}, DataNodeRemoveService finished broadcastDisableDataNode to cluster, disabledDataNode: {}",
        REMOVE_DATANODE_PROCESS,
        getIdWithRpcEndpoint(disabledDataNode));
  }

  /**
   * Find dest data node
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
        && IOT_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())) {
      // parameter of createPeer for MultiLeader should be all peers
      currentPeerNodes = new ArrayList<>(regionReplicaNodes);
      currentPeerNodes.add(destDataNode);
    } else {
      // parameter of createPeer for Ratis can be empty
      currentPeerNodes = Collections.emptyList();
    }

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

    LOGGER.info(
        "{}, Send action createNewRegionPeer finished, regionId: {}, newPeerDataNodeId: {}",
        REGION_MIGRATE_PROCESS,
        regionId,
        getIdWithRpcEndpoint(destDataNode));
    if (isFailed(status)) {
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
  public TSStatus addRegionPeer(TDataNodeLocation destDataNode, TConsensusGroupId regionId) {
    TSStatus status;

    // Here we pick the DataNode who contains one of the RegionReplica of the specified
    // ConsensusGroup except the new one
    // in order to notify the origin ConsensusGroup that another peer is created and demand to join
    Optional<TDataNodeLocation> selectedDataNode =
        filterDataNodeWithOtherRegionReplica(regionId, destDataNode);
    if (!selectedDataNode.isPresent()) {
      LOGGER.warn(
          "{}, There are no other DataNodes could be selected to perform the add peer process, "
              + "please check RegionGroup: {} by show regions sql command",
          REGION_MIGRATE_PROCESS,
          regionId);
      status = new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode());
      status.setMessage(
          "There are no other DataNodes could be selected to perform the add peer process, "
              + "please check by show regions sql command");
      return status;
    }

    // Send addRegionPeer request to the selected DataNode,
    // destDataNode is where the new RegionReplica is created
    TMaintainPeerReq maintainPeerReq = new TMaintainPeerReq(regionId, destDataNode);
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                selectedDataNode.get().getInternalEndPoint(),
                maintainPeerReq,
                DataNodeRequestType.ADD_REGION_PEER);
    LOGGER.info(
        "{}, Send action addRegionPeer finished, regionId: {}, rpcDataNode: {},  destDataNode: {}",
        REGION_MIGRATE_PROCESS,
        regionId,
        getIdWithRpcEndpoint(selectedDataNode.get()),
        getIdWithRpcEndpoint(destDataNode));
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
  public TSStatus removeRegionPeer(
      TDataNodeLocation originalDataNode,
      TDataNodeLocation destDataNode,
      TConsensusGroupId regionId) {
    TSStatus status;

    TDataNodeLocation rpcClientDataNode;

    // Here we pick the DataNode who contains one of the RegionReplica of the specified
    // ConsensusGroup except the origin one
    // in order to notify the new ConsensusGroup that the origin peer should secede now
    // If the selectedDataNode equals null, we choose the destDataNode to execute the method
    Optional<TDataNodeLocation> selectedDataNode =
        filterDataNodeWithOtherRegionReplica(regionId, originalDataNode);
    rpcClientDataNode = selectedDataNode.orElse(destDataNode);

    // Send removeRegionPeer request to the rpcClientDataNode
    TMaintainPeerReq maintainPeerReq = new TMaintainPeerReq(regionId, originalDataNode);
    status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithRetry(
                rpcClientDataNode.getInternalEndPoint(),
                maintainPeerReq,
                DataNodeRequestType.REMOVE_REGION_PEER);
    LOGGER.info(
        "{}, Send action removeRegionPeer finished, regionId: {}, rpcDataNode: {}",
        REGION_MIGRATE_PROCESS,
        regionId,
        getIdWithRpcEndpoint(rpcClientDataNode));
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
        configManager.getNodeManager().getNodeStatusByNodeId(originalDataNode.getDataNodeId())
                == NodeStatus.Unknown
            ? SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithGivenRetry(
                    originalDataNode.getInternalEndPoint(),
                    maintainPeerReq,
                    DataNodeRequestType.DELETE_OLD_REGION_PEER,
                    1)
            : SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithRetry(
                    originalDataNode.getInternalEndPoint(),
                    maintainPeerReq,
                    DataNodeRequestType.DELETE_OLD_REGION_PEER);
    LOGGER.info(
        "{}, Send action deleteOldRegionPeer finished, regionId: {}, dataNodeId: {}",
        REGION_MIGRATE_PROCESS,
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
        "Start to updateRegionLocationCache {} location from {} to {} when it migrate succeed",
        regionId,
        getIdWithRpcEndpoint(originalDataNode),
        getIdWithRpcEndpoint(destDataNode));
    UpdateRegionLocationPlan req =
        new UpdateRegionLocationPlan(regionId, originalDataNode, destDataNode);
    TSStatus status = configManager.getPartitionManager().updateRegionLocation(req);
    LOGGER.info(
        "UpdateRegionLocationCache finished, region:{}, result:{}, old:{}, new:{}",
        regionId,
        status,
        getIdWithRpcEndpoint(originalDataNode),
        getIdWithRpcEndpoint(destDataNode));

    // Broadcast the latest RegionRouteMap when Region migration finished
    configManager.getLoadManager().broadcastLatestRegionRouteMap();
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
    return configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
        .map(TDataNodeConfiguration::getLocation)
        .filter(e -> !regionReplicaNodes.contains(e))
        .findAny();
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
   */
  public void stopDataNode(TDataNodeLocation dataNode) {
    LOGGER.info(
        "{}, Begin to stop DataNode and kill the DataNode process {}",
        REMOVE_DATANODE_PROCESS,
        dataNode);
    TSStatus status =
        SyncDataNodeClientPool.getInstance()
            .sendSyncRequestToDataNodeWithGivenRetry(
                dataNode.getInternalEndPoint(), dataNode, DataNodeRequestType.STOP_DATA_NODE, 2);
    configManager.getNodeManager().removeNodeCache(dataNode.getDataNodeId());
    LOGGER.info(
        "{}, Stop Data Node result: {}, stoppedDataNode: {}",
        REMOVE_DATANODE_PROCESS,
        status,
        dataNode);
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

    TSStatus status = checkClusterProtocol();
    if (isFailed(status)) {
      dataSet.setStatus(status);
      return dataSet;
    }
    status = checkRegionReplication(removeDataNodePlan);
    if (isFailed(status)) {
      dataSet.setStatus(status);
      return dataSet;
    }

    status = checkDataNodeExist(removeDataNodePlan);
    if (isFailed(status)) {
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
    List<TDataNodeLocation> removedDataNodes = removeDataNodePlan.getDataNodeLocations();

    int availableDatanodeSize =
        configManager
            .getNodeManager()
            .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly)
            .size();
    // when the configuration is one replication, it will be failed if the data node is not in
    // running state.
    if (CONF.getSchemaReplicationFactor() == 1 || CONF.getDataReplicationFactor() == 1) {
      for (TDataNodeLocation dataNodeLocation : removedDataNodes) {
        // check whether removed data node is in running state
        BaseNodeCache nodeCache =
            configManager.getNodeManager().getNodeCacheMap().get(dataNodeLocation.getDataNodeId());
        if (!NodeStatus.Running.equals(nodeCache.getNodeStatus())) {
          removedDataNodes.remove(dataNodeLocation);
          LOGGER.error(
              "Failed to remove data node {} because it is not in running and the configuration of cluster is one replication",
              dataNodeLocation);
        }
        if (removedDataNodes.isEmpty()) {
          status.setCode(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
          status.setMessage("Failed to remove all requested data nodes");
          return status;
        }
      }
    }

    int removedDataNodeSize =
        (int)
            removeDataNodePlan.getDataNodeLocations().stream()
                .filter(
                    x ->
                        configManager.getNodeManager().getNodeStatusByNodeId(x.getDataNodeId())
                            != NodeStatus.Unknown)
                .count();
    if (availableDatanodeSize - removedDataNodeSize < NodeInfo.getMinimumDataNode()) {
      status.setCode(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
      status.setMessage(
          String.format(
              "Can't remove datanode due to the limit of replication factor, "
                  + "availableDataNodeSize: %s, maxReplicaFactor: %s, max allowed removed Data Node size is: %s",
              availableDatanodeSize,
              NodeInfo.getMinimumDataNode(),
              (availableDatanodeSize - NodeInfo.getMinimumDataNode())));
    }
    return status;
  }

  public LockQueue getRegionMigrateLock() {
    return regionMigrateLock;
  }

  /**
   * Remove data node in node info
   *
   * @param dataNodeLocation data node location
   */
  public void removeDataNodePersistence(TDataNodeLocation dataNodeLocation) {
    // Remove consensus record
    List<TDataNodeLocation> removeDataNodes = Collections.singletonList(dataNodeLocation);
    configManager.getConsensusManager().write(new RemoveDataNodePlan(removeDataNodes));

    // Remove metrics
    PartitionMetrics.unbindDataNodePartitionMetrics(
        NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint()));
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
   * @param migrateDestDataNode The DataNode where the region is to be migrated
   */
  public void changeRegionLeader(
      TConsensusGroupId regionId,
      TDataNodeLocation originalDataNode,
      TDataNodeLocation migrateDestDataNode) {
    Optional<TDataNodeLocation> newLeaderNode =
        filterDataNodeWithOtherRegionReplica(regionId, originalDataNode);

    if (TConsensusGroupType.DataRegion.equals(regionId.getType())
        && IOT_CONSENSUS.equals(CONF.getDataRegionConsensusProtocolClass())) {
      if (CONF.getDataReplicationFactor() == 1) {
        newLeaderNode = Optional.of(migrateDestDataNode);
      }
      if (newLeaderNode.isPresent()) {
        configManager
            .getLoadManager()
            .getRouteBalancer()
            .changeLeaderForIoTConsensus(regionId, newLeaderNode.get().getDataNodeId());

        LOGGER.info(
            "{}, Change region leader finished for IOT_CONSENSUS, regionId: {}, newLeaderNode: {}",
            REGION_MIGRATE_PROCESS,
            regionId,
            newLeaderNode);
      }

      return;
    }

    if (newLeaderNode.isPresent()) {
      SyncDataNodeClientPool.getInstance()
          .changeRegionLeader(
              regionId, originalDataNode.getInternalEndPoint(), newLeaderNode.get());
      LOGGER.info(
          "{}, Change region leader finished for RATIS_CONSENSUS, regionId: {}, newLeaderNode: {}",
          REGION_MIGRATE_PROCESS,
          regionId,
          newLeaderNode);
    }
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
  private Optional<TDataNodeLocation> filterDataNodeWithOtherRegionReplica(
      TConsensusGroupId regionId, TDataNodeLocation filterLocation) {
    List<TDataNodeLocation> regionLocations = findRegionLocations(regionId);
    if (regionLocations.isEmpty()) {
      LOGGER.warn("Cannot find DataNodes contain the given region: {}", regionId);
      return Optional.empty();
    }

    // Choosing the RUNNING DataNodes to execute firstly
    // If all DataNodes are not RUNNING, then choose the REMOVING DataNodes secondly
    List<TDataNodeLocation> aliveDataNodes =
        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
            .map(TDataNodeConfiguration::getLocation)
            .collect(Collectors.toList());

    aliveDataNodes.addAll(
        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Removing).stream()
            .map(TDataNodeConfiguration::getLocation)
            .collect(Collectors.toList()));

    // TODO return the node which has lowest load.
    for (TDataNodeLocation aliveDataNode : aliveDataNodes) {
      if (regionLocations.contains(aliveDataNode) && !aliveDataNode.equals(filterLocation)) {
        return Optional.of(aliveDataNode);
      }
    }

    return Optional.empty();
  }

  /**
   * Check the protocol of the cluster, standalone is not supported to remove data node currently
   *
   * @return SUCCEED_STATUS if the Cluster is not standalone protocol, REMOVE_DATANODE_FAILED
   *     otherwise
   */
  private TSStatus checkClusterProtocol() {
    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    if (CONF.getDataRegionConsensusProtocolClass().equals(SIMPLE_CONSENSUS)
        || CONF.getSchemaRegionConsensusProtocolClass().equals(SIMPLE_CONSENSUS)) {
      status.setCode(TSStatusCode.REMOVE_DATANODE_ERROR.getStatusCode());
      status.setMessage("SimpleConsensus protocol is not supported to remove data node");
    }
    return status;
  }
}
