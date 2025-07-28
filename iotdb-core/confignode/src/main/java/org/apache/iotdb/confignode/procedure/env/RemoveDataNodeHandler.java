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
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyCopySetRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TCleanDataNodeCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;
import static org.apache.iotdb.consensus.ConsensusFactory.SIMPLE_CONSENSUS;
import static org.apache.iotdb.db.service.RegionMigrateService.isFailed;
import static org.apache.iotdb.db.service.RegionMigrateService.isSucceed;

public class RemoveDataNodeHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveDataNodeHandler.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private final ConfigManager configManager;

  private final IRegionGroupAllocator regionGroupAllocator;

  public RemoveDataNodeHandler(ConfigManager configManager) {
    this.configManager = configManager;

    switch (ConfigNodeDescriptor.getInstance().getConf().getRegionGroupAllocatePolicy()) {
      case GREEDY:
        this.regionGroupAllocator = new GreedyCopySetRegionGroupAllocator();
        break;
      case PGR:
        this.regionGroupAllocator = new GreedyCopySetRegionGroupAllocator();
        break;
      case GCR:
      default:
        this.regionGroupAllocator = new GreedyCopySetRegionGroupAllocator();
    }
  }

  /**
   * Check if the data nodes are sufficient after removing.
   *
   * @param removedDataNodes List<TDataNodeLocation>
   * @return true if the number of DataNodes is enough, false otherwise
   */
  public boolean checkEnoughDataNodeAfterRemoving(List<TDataNodeLocation> removedDataNodes) {
    int availableDatanodeSize =
        configManager
            .getNodeManager()
            .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly)
            .size();

    int removedDataNodeSize =
        (int)
            removedDataNodes.stream()
                .filter(
                    x ->
                        configManager.getLoadManager().getNodeStatus(x.getDataNodeId())
                            != NodeStatus.Unknown)
                .count();

    return availableDatanodeSize - removedDataNodeSize >= NodeInfo.getMinimumDataNode();
  }

  /**
   * Changes the status of a batch of specified DataNodes to the given status. This is done to
   * prevent the DataNodes from receiving read or write requests when they are being removed or are
   * in a restricted state.
   *
   * @param removedDataNodes the locations of the DataNodes whose statuses need to be changed
   * @param nodeStatusMap a map containing the new status to assign to each DataNode (e.g.,
   *     Removing, Running, etc.)
   */
  public void changeDataNodeStatus(
      List<TDataNodeLocation> removedDataNodes, Map<Integer, NodeStatus> nodeStatusMap) {
    LOGGER.info(
        "{}, Begin to change DataNode status, nodeStatusMap: {}",
        REMOVE_DATANODE_PROCESS,
        nodeStatusMap);

    DataNodeAsyncRequestContext<String, TSStatus> changeDataNodeStatusContext =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.SET_SYSTEM_STATUS);

    for (TDataNodeLocation dataNode : removedDataNodes) {
      changeDataNodeStatusContext.putRequest(
          dataNode.getDataNodeId(), nodeStatusMap.get(dataNode.getDataNodeId()).getStatus());
      changeDataNodeStatusContext.putNodeLocation(dataNode.getDataNodeId(), dataNode);
    }

    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithRetry(changeDataNodeStatusContext);

    for (Map.Entry<Integer, TSStatus> entry :
        changeDataNodeStatusContext.getResponseMap().entrySet()) {
      int dataNodeId = entry.getKey();
      NodeStatus nodeStatus = nodeStatusMap.get(dataNodeId);
      RegionStatus regionStatus = RegionStatus.valueOf(nodeStatus.getStatus());

      if (!isSucceed(entry.getValue())) {
        LOGGER.error(
            "{}, Failed to change DataNode status, dataNodeId={}, nodeStatus={}",
            REMOVE_DATANODE_PROCESS,
            dataNodeId,
            nodeStatus);
        continue;
      }

      // Force updating NodeStatus
      long currentTime = System.nanoTime();
      configManager
          .getLoadManager()
          .forceUpdateNodeCache(
              NodeType.DataNode, dataNodeId, new NodeHeartbeatSample(currentTime, nodeStatus));

      LOGGER.info(
          "{}, Force update NodeCache: dataNodeId={}, nodeStatus={}, currentTime={}",
          REMOVE_DATANODE_PROCESS,
          dataNodeId,
          nodeStatus,
          currentTime);

      // Force update RegionStatus
      if (regionStatus != RegionStatus.Removing) {
        Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>> heartbeatSampleMap =
            new TreeMap<>();
        configManager
            .getPartitionManager()
            .getAllReplicaSets(dataNodeId)
            .forEach(
                replicaSet ->
                    heartbeatSampleMap.put(
                        replicaSet.getRegionId(),
                        Collections.singletonMap(
                            dataNodeId, new RegionHeartbeatSample(currentTime, regionStatus))));
        configManager.getLoadManager().forceUpdateRegionGroupCache(heartbeatSampleMap);
      }
    }
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
   * Retrieves all region migration plans for the specified removed DataNodes and selects the
   * destination.
   *
   * @param removedDataNodes the list of DataNodes from which to obtain migration plans
   * @return a list of region migration plans associated with the removed DataNodes
   */
  public List<RegionMigrationPlan> selectedRegionMigrationPlans(
      List<TDataNodeLocation> removedDataNodes) {

    Set<Integer> removedDataNodesSet = new HashSet<>();
    for (TDataNodeLocation removedDataNode : removedDataNodes) {
      removedDataNodesSet.add(removedDataNode.dataNodeId);
    }

    final List<TDataNodeConfiguration> availableDataNodes =
        configManager
            .getNodeManager()
            .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.Unknown)
            .stream()
            .filter(node -> !removedDataNodesSet.contains(node.getLocation().getDataNodeId()))
            .collect(Collectors.toList());

    List<RegionMigrationPlan> regionMigrationPlans = new ArrayList<>();

    regionMigrationPlans.addAll(
        selectMigrationPlans(availableDataNodes, TConsensusGroupType.DataRegion, removedDataNodes));

    regionMigrationPlans.addAll(
        selectMigrationPlans(
            availableDataNodes, TConsensusGroupType.SchemaRegion, removedDataNodes));

    return regionMigrationPlans;
  }

  public List<RegionMigrationPlan> selectMigrationPlans(
      List<TDataNodeConfiguration> availableDataNodes,
      TConsensusGroupType consensusGroupType,
      List<TDataNodeLocation> removedDataNodes) {

    // Retrieve all allocated replica sets for the given consensus group type
    List<TRegionReplicaSet> allocatedReplicaSets =
        configManager.getPartitionManager().getAllReplicaSets(consensusGroupType);

    // Step 1: Identify affected replica sets and record the removed DataNode for each replica set
    Map<TConsensusGroupId, TDataNodeLocation> removedNodeMap = new HashMap<>();
    Set<TRegionReplicaSet> affectedReplicaSets =
        identifyAffectedReplicaSets(allocatedReplicaSets, removedDataNodes, removedNodeMap);

    // Step 2: Update affected replica sets by removing the removed DataNode
    updateReplicaSets(allocatedReplicaSets, affectedReplicaSets, removedNodeMap);

    // Build a mapping of available DataNodes and their free disk space (computed only once)
    Map<Integer, TDataNodeConfiguration> availableDataNodeMap =
        buildAvailableDataNodeMap(availableDataNodes);
    Map<Integer, Double> freeDiskSpaceMap = buildFreeDiskSpaceMap(availableDataNodes);

    // Step 3: For each affected replica set, select a new destination DataNode and create a
    // migration plan
    List<RegionMigrationPlan> migrationPlans = new ArrayList<>();

    Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap = new HashMap<>();
    Map<TConsensusGroupId, String> regionDatabaseMap = new HashMap<>();
    Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap = new HashMap<>();

    for (TRegionReplicaSet replicaSet : affectedReplicaSets) {
      remainReplicasMap.put(replicaSet.getRegionId(), replicaSet);
      String database =
          configManager.getPartitionManager().getRegionDatabase(replicaSet.getRegionId());
      List<TRegionReplicaSet> databaseAllocatedReplicaSets =
          configManager.getPartitionManager().getAllReplicaSets(database, consensusGroupType);
      regionDatabaseMap.put(replicaSet.getRegionId(), database);
      databaseAllocatedRegionGroupMap.put(database, databaseAllocatedReplicaSets);
    }

    Map<TConsensusGroupId, TDataNodeConfiguration> result =
        regionGroupAllocator.removeNodeReplicaSelect(
            availableDataNodeMap,
            freeDiskSpaceMap,
            allocatedReplicaSets,
            regionDatabaseMap,
            databaseAllocatedRegionGroupMap,
            remainReplicasMap);

    for (TConsensusGroupId regionId : result.keySet()) {

      TDataNodeConfiguration selectedNode = result.get(regionId);
      LOGGER.info(
          "Selected DataNode {} for Region {}",
          selectedNode.getLocation().getDataNodeId(),
          regionId);

      // Create the migration plan
      RegionMigrationPlan plan = RegionMigrationPlan.create(regionId, removedNodeMap.get(regionId));
      plan.setToDataNode(selectedNode.getLocation());
      migrationPlans.add(plan);
    }
    return migrationPlans;
  }

  /**
   * Identifies affected replica sets from allocatedReplicaSets that contain any DataNode in
   * removedDataNodes, and records the removed DataNode for each replica set.
   */
  private Set<TRegionReplicaSet> identifyAffectedReplicaSets(
      List<TRegionReplicaSet> allocatedReplicaSets,
      List<TDataNodeLocation> removedDataNodes,
      Map<TConsensusGroupId, TDataNodeLocation> removedNodeMap) {

    Set<TRegionReplicaSet> affectedReplicaSets = new HashSet<>();
    // Create a copy of allocatedReplicaSets to avoid concurrent modifications
    List<TRegionReplicaSet> allocatedCopy = new ArrayList<>(allocatedReplicaSets);

    for (TDataNodeLocation removedNode : removedDataNodes) {
      allocatedCopy.stream()
          .filter(replicaSet -> replicaSet.getDataNodeLocations().contains(removedNode))
          .forEach(
              replicaSet -> {
                removedNodeMap.put(replicaSet.getRegionId(), removedNode);
                affectedReplicaSets.add(replicaSet);
              });
    }
    return affectedReplicaSets;
  }

  /**
   * Updates each affected replica set by removing the removed DataNode from its list. The
   * allocatedReplicaSets list is updated accordingly.
   */
  private void updateReplicaSets(
      List<TRegionReplicaSet> allocatedReplicaSets,
      Set<TRegionReplicaSet> affectedReplicaSets,
      Map<TConsensusGroupId, TDataNodeLocation> removedNodeMap) {
    for (TRegionReplicaSet replicaSet : affectedReplicaSets) {
      // Remove the replica set, update its node list, then re-add it
      allocatedReplicaSets.remove(replicaSet);
      replicaSet.getDataNodeLocations().remove(removedNodeMap.get(replicaSet.getRegionId()));
      allocatedReplicaSets.add(replicaSet);
    }
  }

  /**
   * Constructs a mapping from DataNodeId to TDataNodeConfiguration from the available DataNodes.
   */
  private Map<Integer, TDataNodeConfiguration> buildAvailableDataNodeMap(
      List<TDataNodeConfiguration> availableDataNodes) {
    return availableDataNodes.stream()
        .collect(
            Collectors.toMap(
                dataNode -> dataNode.getLocation().getDataNodeId(), Function.identity()));
  }

  /** Constructs a mapping of free disk space for each DataNode. */
  private Map<Integer, Double> buildFreeDiskSpaceMap(
      List<TDataNodeConfiguration> availableDataNodes) {
    Map<Integer, Double> freeDiskSpaceMap = new HashMap<>(availableDataNodes.size());
    availableDataNodes.forEach(
        dataNode ->
            freeDiskSpaceMap.put(
                dataNode.getLocation().getDataNodeId(),
                configManager
                    .getLoadManager()
                    .getFreeDiskSpace(dataNode.getLocation().getDataNodeId())));
    return freeDiskSpaceMap;
  }

  /**
   * Broadcasts DataNodes' status change, preventing disabled DataNodes from accepting read or write
   * requests.
   *
   * @param dataNodes the list of DataNodes that require broadcast status changes
   */
  public void broadcastDataNodeStatusChange(List<TDataNodeLocation> dataNodes) {
    String dataNodesString =
        dataNodes.stream()
            .map(RegionMaintainHandler::getIdWithRpcEndpoint)
            .collect(Collectors.joining(", "));
    LOGGER.info(
        "{}, BroadcastDataNodeStatusChange start, dataNode: {}",
        REMOVE_DATANODE_PROCESS,
        dataNodesString);

    List<TDataNodeConfiguration> otherOnlineDataNodes =
        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
            .filter(node -> !dataNodes.contains(node.getLocation()))
            .collect(Collectors.toList());

    DataNodeAsyncRequestContext<TCleanDataNodeCacheReq, TSStatus> cleanDataNodeCacheContext =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.CLEAN_DATA_NODE_CACHE);

    for (TDataNodeConfiguration node : otherOnlineDataNodes) {
      TCleanDataNodeCacheReq disableReq = new TCleanDataNodeCacheReq(dataNodes);
      cleanDataNodeCacheContext.putRequest(node.getLocation().getDataNodeId(), disableReq);
      cleanDataNodeCacheContext.putNodeLocation(
          node.getLocation().getDataNodeId(), node.getLocation());
    }

    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithRetry(cleanDataNodeCacheContext);

    for (Map.Entry<Integer, TSStatus> entry :
        cleanDataNodeCacheContext.getResponseMap().entrySet()) {
      if (!isSucceed(entry.getValue())) {
        LOGGER.error(
            "{}, BroadcastDataNodeStatusChange meets error, status change dataNodes: {}, error datanode: {}",
            REMOVE_DATANODE_PROCESS,
            dataNodesString,
            entry.getValue());
        return;
      }
    }

    LOGGER.info(
        "{}, BroadcastDataNodeStatusChange finished, dataNode: {}",
        REMOVE_DATANODE_PROCESS,
        dataNodesString);
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

    LOGGER.info(
        "{}, Begin to stop DataNodes and kill the DataNode process: {}",
        REMOVE_DATANODE_PROCESS,
        removedDataNodes);

    DataNodeAsyncRequestContext<TDataNodeLocation, TSStatus> stopDataNodesContext =
        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.STOP_AND_CLEAR_DATA_NODE);

    for (TDataNodeLocation dataNode : removedDataNodes) {
      stopDataNodesContext.putRequest(dataNode.getDataNodeId(), dataNode);
      stopDataNodesContext.putNodeLocation(dataNode.getDataNodeId(), dataNode);
    }

    CnToDnInternalServiceAsyncRequestManager.getInstance()
        .sendAsyncRequestWithRetry(stopDataNodesContext);

    for (Map.Entry<Integer, TSStatus> entry : stopDataNodesContext.getResponseMap().entrySet()) {
      int dataNodeId = entry.getKey();
      configManager.getLoadManager().removeNodeCache(dataNodeId);
      if (!isSucceed(entry.getValue())) {
        LOGGER.error(
            "{}, Stop Data Node meets error, error datanode: {}",
            REMOVE_DATANODE_PROCESS,
            entry.getValue());
      } else {
        LOGGER.info("{}, Stop Data Node {} success.", REMOVE_DATANODE_PROCESS, dataNodeId);
      }
    }
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
        .filter(replicaSet -> replicaSet.getDataNodeLocations().contains(removedDataNode))
        .map(TRegionReplicaSet::getRegionId)
        .collect(Collectors.toList());
  }

  /**
   * Retrieves all DataNodes related to the specified DataNode.
   *
   * @param removedDataNode the DataNode to be removed
   * @return a set of TDataNodeLocation representing the DataNodes associated with the specified
   *     DataNode
   */
  public Set<TDataNodeLocation> getRelatedDataNodeLocations(TDataNodeLocation removedDataNode) {
    return configManager.getPartitionManager().getAllReplicaSets().stream()
        .filter(replicaSet -> replicaSet.getDataNodeLocations().contains(removedDataNode))
        .flatMap(replicaSet -> replicaSet.getDataNodeLocations().stream())
        .collect(Collectors.toSet());
  }
}
