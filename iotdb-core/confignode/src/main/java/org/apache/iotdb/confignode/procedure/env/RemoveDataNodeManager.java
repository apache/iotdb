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
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.client.CnToDnRequestType;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TDisableDataNodeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

    status = checkAllowRemoveDataNodes(removeDataNodePlan);
    if (isFailed(status)) {
      dataSet.setStatus(status);
      return dataSet;
    }

    return dataSet;
  }

  /**
   * check if allow to remove the DataNodes from the cluster
   *
   * @param removeDataNodePlan RemoveDataNodeReq
   * @return SUCCEED_STATUS when request is legal.
   */
  public TSStatus checkAllowRemoveDataNodes(RemoveDataNodePlan removeDataNodePlan) {
    return configManager
        .getProcedureManager()
        .checkRemoveDataNodes(removeDataNodePlan.getDataNodeLocations());
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
   * Get all region migration plans in removedDataNodes
   *
   * @param removedDataNodes List<TDataNodeLocation>
   * @return List<RegionMigrationPlan>
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
   * Get all consensus group id in removedDataNodes
   *
   * @param removedDataNodes List
   * @return Set<TConsensusGroupId>
   */
  public Set<TConsensusGroupId> getRemovedDataNodesRegionSet(
      List<TDataNodeLocation> removedDataNodes) {
    return removedDataNodes.stream()
        .map(this::getMigratedDataNodeRegions)
        .flatMap(List::stream)
        .collect(Collectors.toSet());
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
   * broadcast a batch of disabled DataNodes
   *
   * @param removedDataNodes List<TDataNodeLocation>
   */
  public void broadcastDisableDataNodes(List<TDataNodeLocation> removedDataNodes) {
    for (TDataNodeLocation dataNodeLocation : removedDataNodes) {
      broadcastDisableDataNode(dataNodeLocation);
    }
  }

  /**
   * broadcast the datanode in RemoveDataNodeReq are disabled, so they will not accept read/write
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
   * Remove a batch of DataNodes in node info
   *
   * @param removedDataNodes List of DataNodeLocation
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
   * Stop old data nodes
   *
   * @param removedDataNodes List of DataNodeLocation
   */
  public void stopDataNodes(List<TDataNodeLocation> removedDataNodes) {
    for (TDataNodeLocation dataNodeLocation : removedDataNodes) {
      stopDataNode(dataNodeLocation);
    }
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
}
