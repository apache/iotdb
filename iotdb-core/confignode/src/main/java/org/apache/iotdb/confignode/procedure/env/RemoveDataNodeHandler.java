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
1
1package org.apache.iotdb.confignode.procedure.env;
1
1import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
1import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
1import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
1import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
1import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
1import org.apache.iotdb.common.rpc.thrift.TSStatus;
1import org.apache.iotdb.commons.cluster.NodeStatus;
1import org.apache.iotdb.commons.cluster.NodeType;
1import org.apache.iotdb.commons.cluster.RegionStatus;
1import org.apache.iotdb.commons.service.metric.MetricService;
1import org.apache.iotdb.commons.utils.NodeUrlUtils;
1import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
1import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
1import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
1import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
1import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
1import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
1import org.apache.iotdb.confignode.consensus.response.datanode.DataNodeToStatusResp;
1import org.apache.iotdb.confignode.manager.ConfigManager;
1import org.apache.iotdb.confignode.manager.load.balancer.region.GreedyCopySetRegionGroupAllocator;
1import org.apache.iotdb.confignode.manager.load.balancer.region.IRegionGroupAllocator;
1import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
1import org.apache.iotdb.confignode.manager.load.cache.region.RegionHeartbeatSample;
1import org.apache.iotdb.confignode.manager.partition.PartitionMetrics;
1import org.apache.iotdb.confignode.persistence.node.NodeInfo;
1import org.apache.iotdb.confignode.procedure.impl.region.RegionMigrationPlan;
1import org.apache.iotdb.consensus.exception.ConsensusException;
1import org.apache.iotdb.mpp.rpc.thrift.TCleanDataNodeCacheReq;
1import org.apache.iotdb.rpc.TSStatusCode;
1
1import org.slf4j.Logger;
1import org.slf4j.LoggerFactory;
1
1import java.util.ArrayList;
1import java.util.Collections;
1import java.util.HashMap;
1import java.util.HashSet;
1import java.util.List;
1import java.util.Map;
1import java.util.Set;
1import java.util.TreeMap;
1import java.util.function.Function;
1import java.util.stream.Collectors;
1
1import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.REMOVE_DATANODE_PROCESS;
1import static org.apache.iotdb.consensus.ConsensusFactory.SIMPLE_CONSENSUS;
1import static org.apache.iotdb.db.service.RegionMigrateService.isFailed;
1import static org.apache.iotdb.db.service.RegionMigrateService.isSucceed;
1
1public class RemoveDataNodeHandler {
1
1  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveDataNodeHandler.class);
1
1  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
1
1  private final ConfigManager configManager;
1
1  private final IRegionGroupAllocator regionGroupAllocator;
1
1  public RemoveDataNodeHandler(ConfigManager configManager) {
1    this.configManager = configManager;
1
1    switch (ConfigNodeDescriptor.getInstance().getConf().getRegionGroupAllocatePolicy()) {
1      case GREEDY:
1        this.regionGroupAllocator = new GreedyCopySetRegionGroupAllocator();
1        break;
1      case PGR:
1        this.regionGroupAllocator = new GreedyCopySetRegionGroupAllocator();
1        break;
1      case GCR:
1      default:
1        this.regionGroupAllocator = new GreedyCopySetRegionGroupAllocator();
1    }
1  }
1
1  /**
1   * Check if the data nodes are sufficient after removing.
1   *
1   * @param removedDataNodes List<TDataNodeLocation>
1   * @return true if the number of DataNodes is enough, false otherwise
1   */
1  public boolean checkEnoughDataNodeAfterRemoving(List<TDataNodeLocation> removedDataNodes) {
1    int availableDatanodeSize =
1        configManager
1            .getNodeManager()
1            .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly)
1            .size();
1
1    int removedDataNodeSize =
1        (int)
1            removedDataNodes.stream()
1                .filter(
1                    x ->
1                        configManager.getLoadManager().getNodeStatus(x.getDataNodeId())
1                            != NodeStatus.Unknown)
1                .count();
1
1    return availableDatanodeSize - removedDataNodeSize >= NodeInfo.getMinimumDataNode();
1  }
1
1  /**
1   * Changes the status of a batch of specified DataNodes to the given status. This is done to
1   * prevent the DataNodes from receiving read or write requests when they are being removed or are
1   * in a restricted state.
1   *
1   * @param removedDataNodes the locations of the DataNodes whose statuses need to be changed
1   * @param nodeStatusMap a map containing the new status to assign to each DataNode (e.g.,
1   *     Removing, Running, etc.)
1   */
1  public void changeDataNodeStatus(
1      List<TDataNodeLocation> removedDataNodes, Map<Integer, NodeStatus> nodeStatusMap) {
1    LOGGER.info(
1        "{}, Begin to change DataNode status, nodeStatusMap: {}",
1        REMOVE_DATANODE_PROCESS,
1        nodeStatusMap);
1
1    DataNodeAsyncRequestContext<String, TSStatus> changeDataNodeStatusContext =
1        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.SET_SYSTEM_STATUS);
1
1    for (TDataNodeLocation dataNode : removedDataNodes) {
1      changeDataNodeStatusContext.putRequest(
1          dataNode.getDataNodeId(), nodeStatusMap.get(dataNode.getDataNodeId()).getStatus());
1      changeDataNodeStatusContext.putNodeLocation(dataNode.getDataNodeId(), dataNode);
1    }
1
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestWithRetry(changeDataNodeStatusContext);
1
1    for (Map.Entry<Integer, TSStatus> entry :
1        changeDataNodeStatusContext.getResponseMap().entrySet()) {
1      int dataNodeId = entry.getKey();
1      NodeStatus nodeStatus = nodeStatusMap.get(dataNodeId);
1      RegionStatus regionStatus = RegionStatus.valueOf(nodeStatus.getStatus());
1
1      if (!isSucceed(entry.getValue())) {
1        LOGGER.error(
1            "{}, Failed to change DataNode status, dataNodeId={}, nodeStatus={}",
1            REMOVE_DATANODE_PROCESS,
1            dataNodeId,
1            nodeStatus);
1        continue;
1      }
1
1      // Force updating NodeStatus
1      long currentTime = System.nanoTime();
1      configManager
1          .getLoadManager()
1          .forceUpdateNodeCache(
1              NodeType.DataNode, dataNodeId, new NodeHeartbeatSample(currentTime, nodeStatus));
1
1      LOGGER.info(
1          "{}, Force update NodeCache: dataNodeId={}, nodeStatus={}, currentTime={}",
1          REMOVE_DATANODE_PROCESS,
1          dataNodeId,
1          nodeStatus,
1          currentTime);
1
1      // Force update RegionStatus
1      if (regionStatus != RegionStatus.Removing) {
1        Map<TConsensusGroupId, Map<Integer, RegionHeartbeatSample>> heartbeatSampleMap =
1            new TreeMap<>();
1        configManager
1            .getPartitionManager()
1            .getAllReplicaSets(dataNodeId)
1            .forEach(
1                replicaSet ->
1                    heartbeatSampleMap.put(
1                        replicaSet.getRegionId(),
1                        Collections.singletonMap(
1                            dataNodeId, new RegionHeartbeatSample(currentTime, regionStatus))));
1        configManager.getLoadManager().forceUpdateRegionGroupCache(heartbeatSampleMap);
1      }
1    }
1  }
1
1  /**
1   * Retrieves all region migration plans for the specified removed DataNodes.
1   *
1   * @param removedDataNodes the list of DataNodes from which to obtain migration plans
1   * @return a list of region migration plans associated with the removed DataNodes
1   */
1  public List<RegionMigrationPlan> getRegionMigrationPlans(
1      List<TDataNodeLocation> removedDataNodes) {
1    List<RegionMigrationPlan> regionMigrationPlans = new ArrayList<>();
1    for (TDataNodeLocation removedDataNode : removedDataNodes) {
1      List<TConsensusGroupId> migratedDataNodeRegions = getMigratedDataNodeRegions(removedDataNode);
1      regionMigrationPlans.addAll(
1          migratedDataNodeRegions.stream()
1              .map(regionId -> RegionMigrationPlan.create(regionId, removedDataNode))
1              .collect(Collectors.toList()));
1    }
1    return regionMigrationPlans;
1  }
1
1  /**
1   * Retrieves all region migration plans for the specified removed DataNodes and selects the
1   * destination.
1   *
1   * @param removedDataNodes the list of DataNodes from which to obtain migration plans
1   * @return a list of region migration plans associated with the removed DataNodes
1   */
1  public List<RegionMigrationPlan> selectedRegionMigrationPlans(
1      List<TDataNodeLocation> removedDataNodes) {
1
1    Set<Integer> removedDataNodesSet = new HashSet<>();
1    for (TDataNodeLocation removedDataNode : removedDataNodes) {
1      removedDataNodesSet.add(removedDataNode.dataNodeId);
1    }
1
1    final List<TDataNodeConfiguration> availableDataNodes =
1        configManager
1            .getNodeManager()
1            .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.Unknown)
1            .stream()
1            .filter(node -> !removedDataNodesSet.contains(node.getLocation().getDataNodeId()))
1            .collect(Collectors.toList());
1
1    List<RegionMigrationPlan> regionMigrationPlans = new ArrayList<>();
1
1    regionMigrationPlans.addAll(
1        selectMigrationPlans(availableDataNodes, TConsensusGroupType.DataRegion, removedDataNodes));
1
1    regionMigrationPlans.addAll(
1        selectMigrationPlans(
1            availableDataNodes, TConsensusGroupType.SchemaRegion, removedDataNodes));
1
1    return regionMigrationPlans;
1  }
1
1  public List<RegionMigrationPlan> selectMigrationPlans(
1      List<TDataNodeConfiguration> availableDataNodes,
1      TConsensusGroupType consensusGroupType,
1      List<TDataNodeLocation> removedDataNodes) {
1
1    // Retrieve all allocated replica sets for the given consensus group type
1    List<TRegionReplicaSet> allocatedReplicaSets =
1        configManager.getPartitionManager().getAllReplicaSets(consensusGroupType);
1
1    // Step 1: Identify affected replica sets and record the removed DataNode for each replica set
1    Map<TConsensusGroupId, TDataNodeLocation> removedNodeMap = new HashMap<>();
1    Set<TRegionReplicaSet> affectedReplicaSets =
1        identifyAffectedReplicaSets(allocatedReplicaSets, removedDataNodes, removedNodeMap);
1
1    // Step 2: Update affected replica sets by removing the removed DataNode
1    updateReplicaSets(allocatedReplicaSets, affectedReplicaSets, removedNodeMap);
1
1    // Build a mapping of available DataNodes and their free disk space (computed only once)
1    Map<Integer, TDataNodeConfiguration> availableDataNodeMap =
1        buildAvailableDataNodeMap(availableDataNodes);
1    Map<Integer, Double> freeDiskSpaceMap = buildFreeDiskSpaceMap(availableDataNodes);
1
1    // Step 3: For each affected replica set, select a new destination DataNode and create a
1    // migration plan
1    List<RegionMigrationPlan> migrationPlans = new ArrayList<>();
1
1    Map<TConsensusGroupId, TRegionReplicaSet> remainReplicasMap = new HashMap<>();
1    Map<TConsensusGroupId, String> regionDatabaseMap = new HashMap<>();
1    Map<String, List<TRegionReplicaSet>> databaseAllocatedRegionGroupMap = new HashMap<>();
1
1    for (TRegionReplicaSet replicaSet : affectedReplicaSets) {
1      remainReplicasMap.put(replicaSet.getRegionId(), replicaSet);
1      String database =
1          configManager.getPartitionManager().getRegionDatabase(replicaSet.getRegionId());
1      List<TRegionReplicaSet> databaseAllocatedReplicaSets =
1          configManager.getPartitionManager().getAllReplicaSets(database, consensusGroupType);
1      regionDatabaseMap.put(replicaSet.getRegionId(), database);
1      databaseAllocatedRegionGroupMap.put(database, databaseAllocatedReplicaSets);
1    }
1
1    Map<TConsensusGroupId, TDataNodeConfiguration> result =
1        regionGroupAllocator.removeNodeReplicaSelect(
1            availableDataNodeMap,
1            freeDiskSpaceMap,
1            allocatedReplicaSets,
1            regionDatabaseMap,
1            databaseAllocatedRegionGroupMap,
1            remainReplicasMap);
1
1    for (TConsensusGroupId regionId : result.keySet()) {
1
1      TDataNodeConfiguration selectedNode = result.get(regionId);
1      LOGGER.info(
1          "Selected DataNode {} for Region {}",
1          selectedNode.getLocation().getDataNodeId(),
1          regionId);
1
1      // Create the migration plan
1      RegionMigrationPlan plan = RegionMigrationPlan.create(regionId, removedNodeMap.get(regionId));
1      plan.setToDataNode(selectedNode.getLocation());
1      migrationPlans.add(plan);
1    }
1    return migrationPlans;
1  }
1
1  /**
1   * Identifies affected replica sets from allocatedReplicaSets that contain any DataNode in
1   * removedDataNodes, and records the removed DataNode for each replica set.
1   */
1  private Set<TRegionReplicaSet> identifyAffectedReplicaSets(
1      List<TRegionReplicaSet> allocatedReplicaSets,
1      List<TDataNodeLocation> removedDataNodes,
1      Map<TConsensusGroupId, TDataNodeLocation> removedNodeMap) {
1
1    Set<TRegionReplicaSet> affectedReplicaSets = new HashSet<>();
1    // Create a copy of allocatedReplicaSets to avoid concurrent modifications
1    List<TRegionReplicaSet> allocatedCopy = new ArrayList<>(allocatedReplicaSets);
1
1    for (TDataNodeLocation removedNode : removedDataNodes) {
1      allocatedCopy.stream()
1          .filter(replicaSet -> replicaSet.getDataNodeLocations().contains(removedNode))
1          .forEach(
1              replicaSet -> {
1                removedNodeMap.put(replicaSet.getRegionId(), removedNode);
1                affectedReplicaSets.add(replicaSet);
1              });
1    }
1    return affectedReplicaSets;
1  }
1
1  /**
1   * Updates each affected replica set by removing the removed DataNode from its list. The
1   * allocatedReplicaSets list is updated accordingly.
1   */
1  private void updateReplicaSets(
1      List<TRegionReplicaSet> allocatedReplicaSets,
1      Set<TRegionReplicaSet> affectedReplicaSets,
1      Map<TConsensusGroupId, TDataNodeLocation> removedNodeMap) {
1    for (TRegionReplicaSet replicaSet : affectedReplicaSets) {
1      // Remove the replica set, update its node list, then re-add it
1      allocatedReplicaSets.remove(replicaSet);
1      replicaSet.getDataNodeLocations().remove(removedNodeMap.get(replicaSet.getRegionId()));
1      allocatedReplicaSets.add(replicaSet);
1    }
1  }
1
1  /**
1   * Constructs a mapping from DataNodeId to TDataNodeConfiguration from the available DataNodes.
1   */
1  private Map<Integer, TDataNodeConfiguration> buildAvailableDataNodeMap(
1      List<TDataNodeConfiguration> availableDataNodes) {
1    return availableDataNodes.stream()
1        .collect(
1            Collectors.toMap(
1                dataNode -> dataNode.getLocation().getDataNodeId(), Function.identity()));
1  }
1
1  /** Constructs a mapping of free disk space for each DataNode. */
1  private Map<Integer, Double> buildFreeDiskSpaceMap(
1      List<TDataNodeConfiguration> availableDataNodes) {
1    Map<Integer, Double> freeDiskSpaceMap = new HashMap<>(availableDataNodes.size());
1    availableDataNodes.forEach(
1        dataNode ->
1            freeDiskSpaceMap.put(
1                dataNode.getLocation().getDataNodeId(),
1                configManager
1                    .getLoadManager()
1                    .getFreeDiskSpace(dataNode.getLocation().getDataNodeId())));
1    return freeDiskSpaceMap;
1  }
1
1  /**
1   * Broadcasts DataNodes' status change, preventing disabled DataNodes from accepting read or write
1   * requests.
1   *
1   * @param dataNodes the list of DataNodes that require broadcast status changes
1   */
1  public void broadcastDataNodeStatusChange(List<TDataNodeLocation> dataNodes) {
1    String dataNodesString =
1        dataNodes.stream()
1            .map(RegionMaintainHandler::getIdWithRpcEndpoint)
1            .collect(Collectors.joining(", "));
1    LOGGER.info(
1        "{}, BroadcastDataNodeStatusChange start, dataNode: {}",
1        REMOVE_DATANODE_PROCESS,
1        dataNodesString);
1
1    List<TDataNodeConfiguration> otherOnlineDataNodes =
1        configManager.getNodeManager().filterDataNodeThroughStatus(NodeStatus.Running).stream()
1            .filter(node -> !dataNodes.contains(node.getLocation()))
1            .collect(Collectors.toList());
1
1    DataNodeAsyncRequestContext<TCleanDataNodeCacheReq, TSStatus> cleanDataNodeCacheContext =
1        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.CLEAN_DATA_NODE_CACHE);
1
1    for (TDataNodeConfiguration node : otherOnlineDataNodes) {
1      TCleanDataNodeCacheReq disableReq = new TCleanDataNodeCacheReq(dataNodes);
1      cleanDataNodeCacheContext.putRequest(node.getLocation().getDataNodeId(), disableReq);
1      cleanDataNodeCacheContext.putNodeLocation(
1          node.getLocation().getDataNodeId(), node.getLocation());
1    }
1
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestWithRetry(cleanDataNodeCacheContext);
1
1    for (Map.Entry<Integer, TSStatus> entry :
1        cleanDataNodeCacheContext.getResponseMap().entrySet()) {
1      if (!isSucceed(entry.getValue())) {
1        LOGGER.error(
1            "{}, BroadcastDataNodeStatusChange meets error, status change dataNodes: {}, error datanode: {}",
1            REMOVE_DATANODE_PROCESS,
1            dataNodesString,
1            entry.getValue());
1        return;
1      }
1    }
1
1    LOGGER.info(
1        "{}, BroadcastDataNodeStatusChange finished, dataNode: {}",
1        REMOVE_DATANODE_PROCESS,
1        dataNodesString);
1  }
1
1  /**
1   * Removes a batch of DataNodes from the node information.
1   *
1   * @param removedDataNodes the list of DataNodeLocations to be removed
1   */
1  public void removeDataNodePersistence(List<TDataNodeLocation> removedDataNodes) {
1    // Remove consensus record
1    try {
1      configManager.getConsensusManager().write(new RemoveDataNodePlan(removedDataNodes));
1    } catch (ConsensusException e) {
1      LOGGER.warn("Failed in the write API executing the consensus layer due to: ", e);
1    }
1
1    // Adjust maxRegionGroupNum
1    configManager.getClusterSchemaManager().adjustMaxRegionGroupNum();
1
1    // Remove metrics
1    for (TDataNodeLocation dataNodeLocation : removedDataNodes) {
1      PartitionMetrics.unbindDataNodePartitionMetricsWhenUpdate(
1          MetricService.getInstance(),
1          NodeUrlUtils.convertTEndPointUrl(dataNodeLocation.getClientRpcEndPoint()));
1    }
1  }
1
1  /**
1   * Stops the specified old DataNodes.
1   *
1   * @param removedDataNodes the list of DataNodeLocations to be stopped
1   */
1  public void stopDataNodes(List<TDataNodeLocation> removedDataNodes) {
1
1    LOGGER.info(
1        "{}, Begin to stop DataNodes and kill the DataNode process: {}",
1        REMOVE_DATANODE_PROCESS,
1        removedDataNodes);
1
1    DataNodeAsyncRequestContext<TDataNodeLocation, TSStatus> stopDataNodesContext =
1        new DataNodeAsyncRequestContext<>(CnToDnAsyncRequestType.STOP_AND_CLEAR_DATA_NODE);
1
1    for (TDataNodeLocation dataNode : removedDataNodes) {
1      stopDataNodesContext.putRequest(dataNode.getDataNodeId(), dataNode);
1      stopDataNodesContext.putNodeLocation(dataNode.getDataNodeId(), dataNode);
1    }
1
1    CnToDnInternalServiceAsyncRequestManager.getInstance()
1        .sendAsyncRequestWithRetry(stopDataNodesContext);
1
1    for (Map.Entry<Integer, TSStatus> entry : stopDataNodesContext.getResponseMap().entrySet()) {
1      int dataNodeId = entry.getKey();
1      configManager.getLoadManager().removeNodeCache(dataNodeId);
1      if (!isSucceed(entry.getValue())) {
1        LOGGER.error(
1            "{}, Stop Data Node meets error, error datanode: {}",
1            REMOVE_DATANODE_PROCESS,
1            entry.getValue());
1      } else {
1        LOGGER.info("{}, Stop Data Node {} success.", REMOVE_DATANODE_PROCESS, dataNodeId);
1      }
1    }
1  }
1
1  /**
1   * Checks if the RemoveDataNode request is valid.
1   *
1   * @param removeDataNodePlan the RemoveDataNodeReq to be validated
1   * @return SUCCEED_STATUS if the request is valid
1   */
1  public DataNodeToStatusResp checkRemoveDataNodeRequest(RemoveDataNodePlan removeDataNodePlan) {
1    DataNodeToStatusResp dataSet = new DataNodeToStatusResp();
1    dataSet.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
1
1    TSStatus status = checkClusterProtocol();
1    if (isFailed(status)) {
1      dataSet.setStatus(status);
1      return dataSet;
1    }
1    status = checkRegionReplication(removeDataNodePlan);
1    if (isFailed(status)) {
1      dataSet.setStatus(status);
1      return dataSet;
1    }
1
1    status = checkDataNodeExist(removeDataNodePlan);
1    if (isFailed(status)) {
1      dataSet.setStatus(status);
1      return dataSet;
1    }
1
1    status = checkAllowRemoveDataNodes(removeDataNodePlan);
1    if (isFailed(status)) {
1      dataSet.setStatus(status);
1      return dataSet;
1    }
1
1    return dataSet;
1  }
1
1  /**
1   * Checks the cluster protocol. Removing a DataNode is not supported in standalone mode.
1   *
1   * @return SUCCEED_STATUS if the cluster is not in standalone mode, REMOVE_DATANODE_FAILED
1   *     otherwise
1   */
1  private TSStatus checkClusterProtocol() {
1    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
1    if (CONF.getDataRegionConsensusProtocolClass().equals(SIMPLE_CONSENSUS)
1        || CONF.getSchemaRegionConsensusProtocolClass().equals(SIMPLE_CONSENSUS)) {
1      status.setCode(TSStatusCode.REMOVE_DATANODE_ERROR.getStatusCode());
1      status.setMessage("SimpleConsensus protocol is not supported to remove data node");
1    }
1    return status;
1  }
1
1  /**
1   * Checks whether the cluster has enough DataNodes to maintain the required number of
1   * RegionReplicas.
1   *
1   * @param removeDataNodePlan the RemoveDataNodeReq to be evaluated
1   * @return SUCCEED_STATUS if the number of DataNodes is sufficient, LACK_REPLICATION otherwise
1   */
1  public TSStatus checkRegionReplication(RemoveDataNodePlan removeDataNodePlan) {
1    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
1    List<TDataNodeLocation> removedDataNodes = removeDataNodePlan.getDataNodeLocations();
1
1    int availableDatanodeSize =
1        configManager
1            .getNodeManager()
1            .filterDataNodeThroughStatus(NodeStatus.Running, NodeStatus.ReadOnly)
1            .size();
1    // when the configuration is one replication, it will be failed if the data node is not in
1    // running state.
1    if (CONF.getSchemaReplicationFactor() == 1 || CONF.getDataReplicationFactor() == 1) {
1      for (TDataNodeLocation dataNodeLocation : removedDataNodes) {
1        // check whether removed data node is in running state
1        if (!NodeStatus.Running.equals(
1            configManager.getLoadManager().getNodeStatus(dataNodeLocation.getDataNodeId()))) {
1          removedDataNodes.remove(dataNodeLocation);
1          LOGGER.error(
1              "Failed to remove data node {} because it is not in running and the configuration of cluster is one replication",
1              dataNodeLocation);
1        }
1        if (removedDataNodes.isEmpty()) {
1          status.setCode(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
1          status.setMessage("Failed to remove all requested data nodes");
1          return status;
1        }
1      }
1    }
1
1    int removedDataNodeSize =
1        (int)
1            removeDataNodePlan.getDataNodeLocations().stream()
1                .filter(
1                    x ->
1                        configManager.getLoadManager().getNodeStatus(x.getDataNodeId())
1                            != NodeStatus.Unknown)
1                .count();
1    if (availableDatanodeSize - removedDataNodeSize < NodeInfo.getMinimumDataNode()) {
1      status.setCode(TSStatusCode.NO_ENOUGH_DATANODE.getStatusCode());
1      status.setMessage(
1          String.format(
1              "Can't remove datanode due to the limit of replication factor, "
1                  + "availableDataNodeSize: %s, maxReplicaFactor: %s, max allowed removed Data Node size is: %s",
1              availableDatanodeSize,
1              NodeInfo.getMinimumDataNode(),
1              (availableDatanodeSize - NodeInfo.getMinimumDataNode())));
1    }
1    return status;
1  }
1
1  /**
1   * Checks whether all DataNodes specified for deletion exist in the cluster.
1   *
1   * @param removeDataNodePlan the RemoveDataNodeReq containing the DataNodes to be checked
1   * @return SUCCEED_STATUS if all specified DataNodes exist in the cluster, DATANODE_NOT_EXIST
1   *     otherwise
1   */
1  private TSStatus checkDataNodeExist(RemoveDataNodePlan removeDataNodePlan) {
1    TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
1
1    List<TDataNodeLocation> allDataNodes =
1        configManager.getNodeManager().getRegisteredDataNodes().stream()
1            .map(TDataNodeConfiguration::getLocation)
1            .collect(Collectors.toList());
1    boolean hasNotExistNode =
1        removeDataNodePlan.getDataNodeLocations().stream()
1            .anyMatch(loc -> !allDataNodes.contains(loc));
1    if (hasNotExistNode) {
1      status.setCode(TSStatusCode.DATANODE_NOT_EXIST.getStatusCode());
1      status.setMessage("there exist Data Node in request but not in cluster");
1    }
1    return status;
1  }
1
1  /**
1   * Checks if it is allowed to remove the specified DataNodes from the cluster.
1   *
1   * @param removeDataNodePlan the RemoveDataNodeReq to be evaluated
1   * @return SUCCEED_STATUS if the request is valid, otherwise an appropriate error status
1   */
1  public TSStatus checkAllowRemoveDataNodes(RemoveDataNodePlan removeDataNodePlan) {
1    return configManager
1        .getProcedureManager()
1        .checkRemoveDataNodes(removeDataNodePlan.getDataNodeLocations());
1  }
1
1  /**
1   * Retrieves all consensus group IDs from the specified removed DataNodes.
1   *
1   * @param removedDataNodes the list of removed DataNodes
1   * @return a set of TConsensusGroupId representing the consensus groups associated with the
1   *     removed DataNodes
1   */
1  public Set<TConsensusGroupId> getRemovedDataNodesRegionSet(
1      List<TDataNodeLocation> removedDataNodes) {
1    return removedDataNodes.stream()
1        .map(this::getMigratedDataNodeRegions)
1        .flatMap(List::stream)
1        .collect(Collectors.toSet());
1  }
1
1  /**
1   * Retrieves all consensus group IDs from the specified DataNode.
1   *
1   * @param removedDataNode the DataNode to be removed
1   * @return a list of group IDs that need to be migrated
1   */
1  public List<TConsensusGroupId> getMigratedDataNodeRegions(TDataNodeLocation removedDataNode) {
1    return configManager.getPartitionManager().getAllReplicaSets().stream()
1        .filter(replicaSet -> replicaSet.getDataNodeLocations().contains(removedDataNode))
1        .map(TRegionReplicaSet::getRegionId)
1        .collect(Collectors.toList());
1  }
1
1  /**
1   * Retrieves all DataNodes related to the specified DataNode.
1   *
1   * @param removedDataNode the DataNode to be removed
1   * @return a set of TDataNodeLocation representing the DataNodes associated with the specified
1   *     DataNode
1   */
1  public Set<TDataNodeLocation> getRelatedDataNodeLocations(TDataNodeLocation removedDataNode) {
1    return configManager.getPartitionManager().getAllReplicaSets().stream()
1        .filter(replicaSet -> replicaSet.getDataNodeLocations().contains(removedDataNode))
1        .flatMap(replicaSet -> replicaSet.getDataNodeLocations().stream())
1        .collect(Collectors.toSet());
1  }
1}
1