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
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.client.CnToDnRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TDisableDataNodeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;
import static org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler.getIdWithRpcEndpoint;
import static org.apache.iotdb.consensus.ConsensusFactory.SIMPLE_CONSENSUS;
import static org.apache.iotdb.db.service.RegionMigrateService.isFailed;
import static org.apache.iotdb.db.service.RegionMigrateService.isSucceed;

public class RemoveDataNodeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveDataNodeManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private final ConfigManager configManager;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> dataNodeClientManager;

  public RemoveDataNodeManager(ConfigManager configManager) {
    this.configManager = configManager;
    dataNodeClientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
  }

  /**
   * Check if the data nodes are sufficient after removing.
   *
   * @param removedDataNodes List<TDataNodeLocation>
   * @return true if the number of DataNodes is enough, false otherwise
   */
  public boolean checkEnoughDataNodeAfterRemoving(List<TDataNodeLocation> removedDataNodes) {
    final int availableDatanodeSize =
        configManager
            .getNodeManager()
            .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly)
            .size();

    int dataNodeNumAfterRemoving = availableDatanodeSize;
    for (TDataNodeLocation removedDatanode : removedDataNodes) {
      if (configManager.getLoadManager().getNodeStatus(removedDatanode.getDataNodeId())
          != NodeStatus.Unknown) {
        dataNodeNumAfterRemoving = availableDatanodeSize - 1;
      }
    }

    return dataNodeNumAfterRemoving >= NodeInfo.getMinimumDataNode();
  }

  /**
   * Marks the given batch of DataNodes with the 'removing' status to prevent read or write requests
   * from being routed to these nodes.
   *
   * @param removedDataNodes the DataNodes to be marked as in 'removing' status
   */
  public void markDataNodesAsRemovingAndBroadcast(List<TDataNodeLocation> removedDataNodes) {
    removedDataNodes.parallelStream().forEach(this::markDataNodeAsRemovingAndBroadcast);
  }

  /**
   * Marks the given DataNode as being in the 'removing' status to prevent read or write requests
   * from being routed to this node.
   *
   * @param dataNodeLocation the DataNode to be marked as in 'removing' status
   */
  public void markDataNodeAsRemovingAndBroadcast(TDataNodeLocation dataNodeLocation) {
    // Send request to update NodeStatus on the DataNode to be removed
    if (configManager.getLoadManager().getNodeStatus(dataNodeLocation.getDataNodeId())
        == NodeStatus.Unknown) {
      SyncDataNodeClientPool.getInstance()
          .sendSyncRequestToDataNodeWithGivenRetry(
              dataNodeLocation.getInternalEndPoint(),
              NodeStatus.Removing.getStatus(),
              CnToDnRequestType.SET_SYSTEM_STATUS,
              1);
    } else {
      SyncDataNodeClientPool.getInstance()
          .sendSyncRequestToDataNodeWithRetry(
              dataNodeLocation.getInternalEndPoint(),
              NodeStatus.Removing.getStatus(),
              CnToDnRequestType.SET_SYSTEM_STATUS);
    }

    long currentTime = System.nanoTime();
    // Force updating NodeStatus to NodeStatus.Removing
    configManager
        .getLoadManager()
        .forceUpdateNodeCache(
            NodeType.DataNode,
            dataNodeLocation.getDataNodeId(),
            new NodeHeartbeatSample(currentTime, NodeStatus.Removing));
    Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>> removingHeartbeatSampleMap =
        new TreeMap<>();
    // Force update RegionStatus to NodeStatus.Removing
    configManager
        .getPartitionManager()
        .getAllReplicaSets(dataNodeLocation.getDataNodeId())
        .forEach(
            replicaSet ->
                removingHeartbeatSampleMap.put(
                    replicaSet.getRegionId(),
                    Collections.singletonMap(
                        dataNodeLocation.getDataNodeId(),
                        new RegionHeartbeatSample(currentTime, RegionStatus.Removing))));
    configManager.getLoadManager().forceUpdateRegionGroupCache(removingHeartbeatSampleMap);
  }

  /**
   * Retrieves all region migration plans for the specified removed DataNodes.
   *
   * @param removedDataNodes the list of DataNodes from which to obtain migration plans
   * @return a list of region migration plans associated with the removed DataNodes
   */
  public List<RegionMigrationPlan> getRegionMigrationPlans(
      List<TDataNodeLocation> removedDataNodes) {
    List<RegionMigrationPlan> regionMigrationPlans = new ArrayList<>();
    for (TDataNodeLocation removedDataNode : removedDataNodes) {
      List<TConsensusGroupId> migratedDataNodeRegions = getMigratedDataNodeRegions(removedDataNode);
      regionMigrationPlans.addAll(
          migratedDataNodeRegions.stream()
              .map(regionId -> RegionMigrationPlan.create(regionId, removedDataNode))
              .collect(Collectors.toList()));
    }
    return regionMigrationPlans;
  }

  /**
   * Broadcasts a batch of disabled DataNodes to notify other components or services.
   *
   * @param removedDataNodes the list of DataNodes to be broadcasted as disabled
   */
  public void broadcastDisableDataNodes(List<TDataNodeLocation> removedDataNodes) {
    for (TDataNodeLocation dataNodeLocation : removedDataNodes) {
      broadcastDisableDataNode(dataNodeLocation);
    }
  }

  /**
   * Broadcasts that the specified DataNode in the RemoveDataNodeReq is disabled, preventing it from
   * accepting read or write requests.
   *
   * @param disabledDataNode the DataNode to be broadcasted as disabled
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
          (TSStatus)
              SyncDataNodeClientPool.getInstance()
                  .sendSyncRequestToDataNodeWithRetry(
                      node.getLocation().getInternalEndPoint(),
                      disableReq,
                      CnToDnRequestType.DISABLE_DATA_NODE);
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
   * Removes a batch of DataNodes from the node information.
   *
   * @param removedDataNodes the list of DataNodeLocations to be removed
   */
  public void removeDataNodePersistence(List<TDataNodeLocation> removedDataNodes) {
    // Remove consensus record
    try {
      configManager.getConsensusManager().write(new RemoveDataNodePlan(removedDataNodes));
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
    }

    // Adjust maxRegionGroupNum
    configManager.getClusterSchemaManager().adjustMaxRegionGroupNum();

    // Remove metrics
    for (TDataNodeLocation dataNodeLocation : removedDataNodes) {
      PartitionMetrics.unbindDataNodePartitionMetricsWhenUpdate(
          MetricService.getInstance(),
          NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint()));
    }
  }

  /**
   * Stops the specified old DataNodes.
   *
   * @param removedDataNodes the list of DataNodeLocations to be stopped
   */
  public void stopDataNodes(List<TDataNodeLocation> removedDataNodes) {
    for (TDataNodeLocation dataNodeLocation : removedDataNodes) {
      stopDataNode(dataNodeLocation);
    }
  }

  /**
   * Stops the specified old DataNode.
   *
   * @param dataNode the old DataNode to be stopped
   */
  public void stopDataNode(TDataNodeLocation dataNode) {
    LOGGER.info(
        "{}, Begin to stop DataNode and kill the DataNode process {}",
        REMOVE_DATANODE_PROCESS,
        dataNode);
    TSStatus status =
        (TSStatus)
            SyncDataNodeClientPool.getInstance()
                .sendSyncRequestToDataNodeWithGivenRetry(
                    dataNode.getInternalEndPoint(), dataNode, CnToDnRequestType.STOP_DATA_NODE, 2);
    configManager.getLoadManager().removeNodeCache(dataNode.getDataNodeId());
    LOGGER.info(
        "{}, Stop Data Node result: {}, stoppedDataNode: {}",
        REMOVE_DATANODE_PROCESS,
        status,
        dataNode);
  }

  /**
   * Checks if the RemoveDataNode request is valid.
   *
   * @param removeDataNodePlan the RemoveDataNodeReq to be validated
   * @return SUCCEED_STATUS if the request is valid
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

    status = checkAllowRemoveDataNodes(removeDataNodePlan);
    if (isFailed(status)) {
      dataSet.setStatus(status);
      return dataSet;
    }

    return dataSet;
  }

  /**
   * Checks the cluster protocol. Removing a DataNode is not supported in standalone mode.
   *
   * @return SUCCEED_STATUS if the cluster is not in standalone mode, REMOVE_DATANODE_FAILED
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

  /**
   * Checks whether the cluster has enough DataNodes to maintain the required number of
   * RegionReplicas.
   *
   * @param removeDataNodePlan the RemoveDataNodeReq to be evaluated
   * @return SUCCEED_STATUS if the number of DataNodes is sufficient, LACK_REPLICATION otherwise
   */
  public TSStatus checkRegionReplication(RemoveDataNodePlan removeDataNodePlan) {
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
        if (!NodeStatus.Running.equals(
            configManager.getLoadManager().getNodeStatus(dataNodeLocation.getDataNodeId()))) {
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
                        configManager.getLoadManager().getNodeStatus(x.getDataNodeId())
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

  /**
   * Checks whether all DataNodes specified for deletion exist in the cluster.
   *
   * @param removeDataNodePlan the RemoveDataNodeReq containing the DataNodes to be checked
   * @return SUCCEED_STATUS if all specified DataNodes exist in the cluster, DATANODE_NOT_EXIST
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
   * Checks if it is allowed to remove the specified DataNodes from the cluster.
   *
   * @param removeDataNodePlan the RemoveDataNodeReq to be evaluated
   * @return SUCCEED_STATUS if the request is valid, otherwise an appropriate error status
   */
  public TSStatus checkAllowRemoveDataNodes(RemoveDataNodePlan removeDataNodePlan) {
    return configManager
        .getProcedureManager()
        .checkRemoveDataNodes(removeDataNodePlan.getDataNodeLocations());
  }

  /**
   * Retrieves all consensus group IDs from the specified removed DataNodes.
   *
   * @param removedDataNodes the list of removed DataNodes
   * @return a set of TConsensusGroupId representing the consensus groups associated with the
   *     removed DataNodes
   */
  public Set<TConsensusGroupId> getRemovedDataNodesRegionSet(
      List<TDataNodeLocation> removedDataNodes) {
    return removedDataNodes.stream()
        .map(this::getMigratedDataNodeRegions)
        .flatMap(List::stream)
        .collect(Collectors.toSet());
  }

  /**
   * Retrieves all consensus group IDs from the specified DataNode.
   *
   * @param removedDataNode the DataNode to be removed
   * @return a list of group IDs that need to be migrated
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
}
