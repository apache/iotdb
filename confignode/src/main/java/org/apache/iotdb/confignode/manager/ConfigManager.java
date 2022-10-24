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

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.utils.AuthUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.CreatePipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.sync.DropPipeSinkPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeConfigurationResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeRegisterResp;
import org.apache.iotdb.confignode.consensus.response.DataNodeToStatusResp;
import org.apache.iotdb.confignode.consensus.response.DataPartitionResp;
import org.apache.iotdb.confignode.consensus.response.PermissionInfoResp;
import org.apache.iotdb.confignode.consensus.response.RegionInfoListResp;
import org.apache.iotdb.confignode.consensus.response.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.consensus.response.SchemaPartitionResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.consensus.response.TemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.statemachine.PartitionRegionStateMachine;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.persistence.executor.ConfigPlanExecutor;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.sync.ClusterSyncInfo;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TCreateTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TDeactivateSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteTimeSeriesReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropTriggerReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetLocationForTriggerResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetPipeSinkResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetRegionIdResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetSeriesSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTimeSlotListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerJarReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerJarResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionMigrateResultReportReq;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetSchemaTemplateReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowConfigNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowDataNodesResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/** Entry of all management, AssignPartitionManager,AssignRegionManager. */
public class ConfigManager implements IManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigManager.class);

  /** Manage PartitionTable read/write requests through the ConsensusLayer */
  private ConsensusManager consensusManager;

  /** Manage cluster node */
  private final NodeManager nodeManager;

  /** Manage cluster schema */
  private final ClusterSchemaManager clusterSchemaManager;

  /** Manage cluster regions and partitions */
  private final PartitionManager partitionManager;

  /** Manage cluster authorization */
  private final PermissionManager permissionManager;

  private final LoadManager loadManager;

  /** Manage procedure */
  private final ProcedureManager procedureManager;

  /** UDF */
  private final UDFManager udfManager;

  /** Manage Trigger */
  private final TriggerManager triggerManager;
  /** Sync */
  private final SyncManager syncManager;

  private final PartitionRegionStateMachine stateMachine;

  public ConfigManager() throws IOException {
    // Build the persistence module
    NodeInfo nodeInfo = new NodeInfo();
    ClusterSchemaInfo clusterSchemaInfo = new ClusterSchemaInfo();
    PartitionInfo partitionInfo = new PartitionInfo();
    AuthorInfo authorInfo = new AuthorInfo();
    ProcedureInfo procedureInfo = new ProcedureInfo();
    UDFInfo udfInfo = new UDFInfo();
    TriggerInfo triggerInfo = new TriggerInfo();
    ClusterSyncInfo syncInfo = new ClusterSyncInfo();

    // Build state machine and executor
    ConfigPlanExecutor executor =
        new ConfigPlanExecutor(
            nodeInfo,
            clusterSchemaInfo,
            partitionInfo,
            authorInfo,
            procedureInfo,
            udfInfo,
            triggerInfo,
            syncInfo);
    this.stateMachine = new PartitionRegionStateMachine(this, executor);

    // Build the manager module
    this.nodeManager = new NodeManager(this, nodeInfo);
    this.clusterSchemaManager = new ClusterSchemaManager(this, clusterSchemaInfo);
    this.partitionManager = new PartitionManager(this, partitionInfo);
    this.permissionManager = new PermissionManager(this, authorInfo);
    this.procedureManager = new ProcedureManager(this, procedureInfo);
    this.udfManager = new UDFManager(this, udfInfo);
    this.triggerManager = new TriggerManager(this, triggerInfo);
    this.loadManager = new LoadManager(this);
    this.syncManager = new SyncManager(this, syncInfo);
  }

  public void initConsensusManager() throws IOException {
    this.consensusManager = new ConsensusManager(this, this.stateMachine);
  }

  public void close() throws IOException {
    consensusManager.close();
    partitionManager.getRegionMaintainer().shutdown();
    procedureManager.shiftExecutor(false);
  }

  @Override
  public DataSet registerDataNode(RegisterDataNodePlan registerDataNodePlan) {
    TSStatus status = confirmLeader();
    DataNodeRegisterResp dataSet;
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      triggerManager.getTriggerInfo().acquireTriggerTableLock();
      try {
        dataSet = (DataNodeRegisterResp) nodeManager.registerDataNode(registerDataNodePlan);
        dataSet.setTemplateInfo(clusterSchemaManager.getAllTemplateSetInfo());
        dataSet.setTriggerInformation(
            triggerManager.getTriggerTable(false).getAllTriggerInformation());
      } finally {
        triggerManager.getTriggerInfo().releaseTriggerTableLock();
      }
    } else {
      dataSet = new DataNodeRegisterResp();
      dataSet.setStatus(status);
      dataSet.setConfigNodeList(nodeManager.getRegisteredConfigNodes());
    }
    return dataSet;
  }

  @Override
  public DataSet removeDataNode(RemoveDataNodePlan removeDataNodePlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return nodeManager.removeDataNode(removeDataNodePlan);
    } else {
      DataNodeToStatusResp dataSet = new DataNodeToStatusResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      procedureManager.reportRegionMigrateResult(req);
    }
    return status;
  }

  @Override
  public DataSet getDataNodeConfiguration(
      GetDataNodeConfigurationPlan getDataNodeConfigurationPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return nodeManager.getDataNodeConfiguration(getDataNodeConfigurationPlan);
    } else {
      DataNodeConfigurationResp dataSet = new DataNodeConfigurationResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TShowClusterResp showCluster() {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      List<TConfigNodeLocation> configNodeLocations = getNodeManager().getRegisteredConfigNodes();
      configNodeLocations.sort(Comparator.comparingInt(TConfigNodeLocation::getConfigNodeId));
      List<TDataNodeLocation> dataNodeInfoLocations =
          getNodeManager().getRegisteredDataNodes().stream()
              .map(TDataNodeConfiguration::getLocation)
              .sorted(Comparator.comparingInt(TDataNodeLocation::getDataNodeId))
              .collect(Collectors.toList());
      Map<Integer, String> nodeStatus = new HashMap<>();
      getNodeManager()
          .getNodeCacheMap()
          .forEach(
              (nodeId, heartbeatCache) ->
                  nodeStatus.put(nodeId, heartbeatCache.getNodeStatusWithReason()));
      return new TShowClusterResp(status, configNodeLocations, dataNodeInfoLocations, nodeStatus);
    } else {
      return new TShowClusterResp(status, new ArrayList<>(), new ArrayList<>(), new HashMap<>());
    }
  }

  @Override
  public TSStatus setTTL(SetTTLPlan setTTLPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setTTL(setTTLPlan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setSchemaReplicationFactor(
      SetSchemaReplicationFactorPlan setSchemaReplicationFactorPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setSchemaReplicationFactor(setSchemaReplicationFactorPlan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setDataReplicationFactor(
      SetDataReplicationFactorPlan setDataReplicationFactorPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setDataReplicationFactor(setDataReplicationFactorPlan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus setTimePartitionInterval(
      SetTimePartitionIntervalPlan setTimePartitionIntervalPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setTimePartitionInterval(setTimePartitionIntervalPlan);
    } else {
      return status;
    }
  }

  @Override
  public DataSet countMatchedStorageGroups(CountStorageGroupPlan countStorageGroupPlan) {
    TSStatus status = confirmLeader();
    CountStorageGroupResp result = new CountStorageGroupResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.countMatchedStorageGroups(countStorageGroupPlan);
    } else {
      result.setStatus(status);
    }
    return result;
  }

  @Override
  public DataSet getMatchedStorageGroupSchemas(GetStorageGroupPlan getStorageGroupReq) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getMatchedStorageGroupSchema(getStorageGroupReq);
    } else {
      StorageGroupSchemaResp dataSet = new StorageGroupSchemaResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TSStatus setStorageGroup(SetStorageGroupPlan setStorageGroupPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setStorageGroup(setStorageGroupPlan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus deleteStorageGroups(List<String> deletedPaths) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // remove wild
      Map<String, TStorageGroupSchema> deleteStorageSchemaMap =
          getClusterSchemaManager().getMatchedStorageGroupSchemasByName(deletedPaths);
      if (deleteStorageSchemaMap.isEmpty()) {
        return RpcUtils.getStatus(
            TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode(),
            String.format("Path %s does not exist", Arrays.toString(deletedPaths.toArray())));
      }
      ArrayList<TStorageGroupSchema> parsedDeleteStorageGroups =
          new ArrayList<>(deleteStorageSchemaMap.values());
      return procedureManager.deleteStorageGroups(parsedDeleteStorageGroups);
    } else {
      return status;
    }
  }

  private List<TSeriesPartitionSlot> calculateRelatedSlot(
      PartialPath path, PartialPath storageGroup) {
    // The path contains `**`
    if (path.getFullPath().contains(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
      return new ArrayList<>();
    }
    // path doesn't contain * so the size of innerPathList should be 1
    PartialPath innerPath = path.alterPrefixPath(storageGroup).get(0);
    // The innerPath contains `*` and the only `*` is not in last level
    if (innerPath.getDevice().contains(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)) {
      return new ArrayList<>();
    }
    return Collections.singletonList(
        getPartitionManager().getSeriesPartitionSlot(innerPath.getDevice()));
  }

  @Override
  public TSchemaPartitionTableResp getSchemaPartition(PathPatternTree patternTree) {
    // Construct empty response
    TSchemaPartitionTableResp resp = new TSchemaPartitionTableResp();

    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setStatus(status);
    }

    // Build GetSchemaPartitionPlan
    Map<String, Set<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    List<PartialPath> relatedPaths = patternTree.getAllPathPatterns();
    List<String> allStorageGroups = getClusterSchemaManager().getStorageGroupNames();
    Map<String, Boolean> scanAllRegions = new HashMap<>();
    for (PartialPath path : relatedPaths) {
      for (String storageGroup : allStorageGroups) {
        try {
          PartialPath storageGroupPath = new PartialPath(storageGroup);
          if (path.overlapWith(storageGroupPath.concatNode(MULTI_LEVEL_PATH_WILDCARD))
              && !scanAllRegions.containsKey(storageGroup)) {
            List<TSeriesPartitionSlot> relatedSlot = calculateRelatedSlot(path, storageGroupPath);
            if (relatedSlot.isEmpty()) {
              scanAllRegions.put(storageGroup, true);
              partitionSlotsMap.put(storageGroup, new HashSet<>());
            } else {
              partitionSlotsMap
                  .computeIfAbsent(storageGroup, k -> new HashSet<>())
                  .addAll(relatedSlot);
            }
          }
        } catch (IllegalPathException e) {
          // this line won't be reached in general
          throw new RuntimeException(e);
        }
      }
    }

    // Return empty resp if the partitionSlotsMap is empty
    if (partitionSlotsMap.isEmpty()) {
      return resp.setStatus(StatusUtils.OK).setSchemaPartitionTable(new HashMap<>());
    }

    GetSchemaPartitionPlan getSchemaPartitionPlan =
        new GetSchemaPartitionPlan(
            partitionSlotsMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> new ArrayList<>(e.getValue()))));
    SchemaPartitionResp queryResult =
        (SchemaPartitionResp) partitionManager.getSchemaPartition(getSchemaPartitionPlan);
    resp = queryResult.convertToRpcSchemaPartitionTableResp();

    // TODO: Delete or hide this LOGGER before officially release.
    LOGGER.info("GetSchemaPartition receive paths: {}, return: {}", relatedPaths, resp);

    return resp;
  }

  @Override
  public TSchemaPartitionTableResp getOrCreateSchemaPartition(PathPatternTree patternTree) {
    // Construct empty response
    TSchemaPartitionTableResp resp = new TSchemaPartitionTableResp();

    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setStatus(status);
    }

    List<String> devicePaths = patternTree.getAllDevicePatterns();
    List<String> storageGroups = getClusterSchemaManager().getStorageGroupNames();

    // Build GetOrCreateSchemaPartitionPlan
    Map<String, List<TSeriesPartitionSlot>> partitionSlotsMap = new HashMap<>();
    for (String devicePath : devicePaths) {
      if (!devicePath.contains("*")) {
        // Only check devicePaths that without "*"
        for (String storageGroup : storageGroups) {
          if (PathUtils.isStartWith(devicePath, storageGroup)) {
            partitionSlotsMap
                .computeIfAbsent(storageGroup, key -> new ArrayList<>())
                .add(getPartitionManager().getSeriesPartitionSlot(devicePath));
            break;
          }
        }
      }
    }
    GetOrCreateSchemaPartitionPlan getOrCreateSchemaPartitionPlan =
        new GetOrCreateSchemaPartitionPlan(partitionSlotsMap);

    SchemaPartitionResp queryResult =
        partitionManager.getOrCreateSchemaPartition(getOrCreateSchemaPartitionPlan);
    resp = queryResult.convertToRpcSchemaPartitionTableResp();

    // TODO: Delete or hide this LOGGER before officially release.
    LOGGER.info(
        "GetOrCreateSchemaPartition receive devicePaths: {}, return TSchemaPartitionResp: {}",
        devicePaths,
        resp);

    return resp;
  }

  @Override
  public TSchemaNodeManagementResp getNodePathsPartition(PartialPath partialPath, Integer level) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      GetNodePathsPartitionPlan getNodePathsPartitionPlan = new GetNodePathsPartitionPlan();
      getNodePathsPartitionPlan.setPartialPath(partialPath);
      if (null != level) {
        getNodePathsPartitionPlan.setLevel(level);
      }
      SchemaNodeManagementResp resp =
          partitionManager.getNodePathsPartition(getNodePathsPartitionPlan);
      TSchemaNodeManagementResp result =
          resp.convertToRpcSchemaNodeManagementPartitionResp(
              getLoadManager().getLatestRegionRouteMap());

      // TODO: Delete or hide this LOGGER before officially release.
      LOGGER.info(
          "getNodePathsPartition receive devicePaths: {}, level: {}, return TSchemaNodeManagementResp: {}",
          partialPath,
          level,
          result);

      return result;
    } else {
      return new TSchemaNodeManagementResp().setStatus(status);
    }
  }

  @Override
  public TDataPartitionTableResp getDataPartition(GetDataPartitionPlan getDataPartitionPlan) {
    // Construct empty response
    TDataPartitionTableResp resp = new TDataPartitionTableResp();

    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setStatus(status);
    }
    DataPartitionResp queryResult =
        (DataPartitionResp) partitionManager.getDataPartition(getDataPartitionPlan);

    resp = queryResult.convertToTDataPartitionTableResp();

    // TODO: Delete or hide this LOGGER before officially release.
    LOGGER.info(
        "GetDataPartition interface receive PartitionSlotsMap: {}, return: {}",
        getDataPartitionPlan.getPartitionSlotsMap(),
        resp);

    return resp;
  }

  @Override
  public TDataPartitionTableResp getOrCreateDataPartition(
      GetOrCreateDataPartitionPlan getOrCreateDataPartitionReq) {
    // Construct empty response
    TDataPartitionTableResp resp = new TDataPartitionTableResp();

    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setStatus(status);
    }

    DataPartitionResp queryResult =
        partitionManager.getOrCreateDataPartition(getOrCreateDataPartitionReq);

    resp = queryResult.convertToTDataPartitionTableResp();

    // TODO: Delete or hide this LOGGER before officially release.
    LOGGER.info(
        "GetOrCreateDataPartition success. receive PartitionSlotsMap: {}, return: {}",
        getOrCreateDataPartitionReq.getPartitionSlotsMap(),
        resp);

    return resp;
  }

  private TSStatus confirmLeader() {
    return getConsensusManager().confirmLeader();
  }

  @Override
  public NodeManager getNodeManager() {
    return nodeManager;
  }

  @Override
  public ClusterSchemaManager getClusterSchemaManager() {
    return clusterSchemaManager;
  }

  @Override
  public ConsensusManager getConsensusManager() {
    return consensusManager;
  }

  @Override
  public PartitionManager getPartitionManager() {
    return partitionManager;
  }

  @Override
  public LoadManager getLoadManager() {
    return loadManager;
  }

  @Override
  public TriggerManager getTriggerManager() {
    return triggerManager;
  }

  @Override
  public SyncManager getSyncManager() {
    return syncManager;
  }

  @Override
  public TSStatus operatePermission(AuthorPlan authorPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.operatePermission(authorPlan);
    } else {
      return status;
    }
  }

  @Override
  public DataSet queryPermission(AuthorPlan authorPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.queryPermission(authorPlan);
    } else {
      PermissionInfoResp dataSet = new PermissionInfoResp();
      dataSet.setStatus(status);
      return dataSet;
    }
  }

  @Override
  public TPermissionInfoResp login(String username, String password) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.login(username, password);
    } else {
      TPermissionInfoResp resp = AuthUtils.generateEmptyPermissionInfoResp();
      resp.setStatus(status);
      return resp;
    }
  }

  @Override
  public TPermissionInfoResp checkUserPrivileges(
      String username, List<String> paths, int permission) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return permissionManager.checkUserPrivileges(username, paths, permission);
    } else {
      TPermissionInfoResp resp = AuthUtils.generateEmptyPermissionInfoResp();
      resp.setStatus(status);
      return resp;
    }
  }

  @Override
  public TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req) {
    return nodeManager.registerConfigNode(req);
  }

  public TSStatus checkConfigNodeGlobalConfig(TConfigNodeRegisterReq req) {
    final String errorPrefix = "Reject register, please ensure that the parameter ";
    final String errorSuffix = " is consistent with the Seed-ConfigNode.";

    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    TSStatus errorStatus = new TSStatus(TSStatusCode.ERROR_GLOBAL_CONFIG.getStatusCode());
    if (!req.getDataRegionConsensusProtocolClass()
        .equals(conf.getDataRegionConsensusProtocolClass())) {
      return errorStatus.setMessage(
          errorPrefix + "data_region_consensus_protocol_class" + errorSuffix);
    }
    if (!req.getSchemaRegionConsensusProtocolClass()
        .equals(conf.getSchemaRegionConsensusProtocolClass())) {
      return errorStatus.setMessage(
          errorPrefix + "schema_region_consensus_protocol_class" + errorSuffix);
    }
    if (req.getSeriesPartitionSlotNum() != conf.getSeriesPartitionSlotNum()) {
      return errorStatus.setMessage(errorPrefix + "series_partition_slot_num" + errorSuffix);
    }
    if (!req.getSeriesPartitionExecutorClass().equals(conf.getSeriesPartitionExecutorClass())) {
      return errorStatus.setMessage(errorPrefix + "series_partition_executor_class" + errorSuffix);
    }
    if (req.getDefaultTTL() != CommonDescriptor.getInstance().getConfig().getDefaultTTL()) {
      return errorStatus.setMessage(errorPrefix + "default_ttl" + errorSuffix);
    }
    if (req.getTimePartitionInterval() != conf.getTimePartitionInterval()) {
      return errorStatus.setMessage(
          errorPrefix + "time_partition_interval_for_routing" + errorSuffix);
    }
    if (req.getSchemaReplicationFactor() != conf.getSchemaReplicationFactor()) {
      return errorStatus.setMessage(errorPrefix + "schema_replication_factor" + errorSuffix);
    }
    if (req.getSchemaRegionPerDataNode() != conf.getSchemaRegionPerDataNode()) {
      return errorStatus.setMessage(errorPrefix + "schema_region_per_data_node" + errorSuffix);
    }
    if (req.getDataReplicationFactor() != conf.getDataReplicationFactor()) {
      return errorStatus.setMessage(errorPrefix + "data_replication_factor" + errorSuffix);
    }
    if (req.getDataRegionPerProcessor() != conf.getDataRegionPerProcessor()) {
      return errorStatus.setMessage(errorPrefix + "data_region_per_processor" + errorSuffix);
    }
    if (!req.getReadConsistencyLevel().equals(conf.getReadConsistencyLevel())) {
      return errorStatus.setMessage(errorPrefix + "read_consistency_level" + errorSuffix);
    }
    if (req.getDiskSpaceWarningThreshold()
        != CommonDescriptor.getInstance().getConfig().getDiskSpaceWarningThreshold()) {
      return errorStatus.setMessage(errorPrefix + "disk_space_warning_threshold" + errorSuffix);
    }
    return null;
  }

  @Override
  public TSStatus createPeerForConsensusGroup(List<TConfigNodeLocation> configNodeLocations) {
    for (int i = 0; i < 30; i++) {
      try {
        if (consensusManager == null) {
          Thread.sleep(1000);
        } else {
          consensusManager.createPeerForConsensusGroup(configNodeLocations);
          return StatusUtils.OK;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Unexpected interruption during retry creating peer for consensus group");
      } catch (Exception e) {
        LOGGER.error("Failed to create peer for consensus group", e);
        break;
      }
    }
    return StatusUtils.INTERNAL_ERROR;
  }

  @Override
  public TSStatus removeConfigNode(RemoveConfigNodePlan removeConfigNodePlan) {
    TSStatus status = confirmLeader();

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      status = nodeManager.checkConfigNodeBeforeRemove(removeConfigNodePlan);
      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        procedureManager.removeConfigNode(removeConfigNodePlan);
      }
    }

    return status;
  }

  @Override
  public TSStatus createFunction(String udfName, String className, List<String> uris) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? udfManager.createFunction(udfName, className, uris)
        : status;
  }

  @Override
  public TSStatus dropFunction(String udfName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? udfManager.dropFunction(udfName)
        : status;
  }

  @Override
  public TSStatus createTrigger(TCreateTriggerReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.createTrigger(req)
        : status;
  }

  @Override
  public TSStatus dropTrigger(TDropTriggerReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.dropTrigger(req)
        : status;
  }

  @Override
  public TGetTriggerTableResp getTriggerTable() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.getTriggerTable(false)
        : new TGetTriggerTableResp(status, Collections.emptyList());
  }

  @Override
  public TGetTriggerTableResp getStatefulTriggerTable() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.getTriggerTable(true)
        : new TGetTriggerTableResp(status, Collections.emptyList());
  }

  @Override
  public TGetLocationForTriggerResp getLocationOfStatefulTrigger(String triggerName) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.getLocationOfStatefulTrigger(triggerName)
        : new TGetLocationForTriggerResp(status);
  }

  @Override
  public TGetTriggerJarResp getTriggerJar(TGetTriggerJarReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? triggerManager.getTriggerJar(req)
        : new TGetTriggerJarResp(status, Collections.emptyList());
  }

  @Override
  public TSStatus merge() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.merge())
        : status;
  }

  @Override
  public TSStatus flush(TFlushReq req) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.flush(req))
        : status;
  }

  @Override
  public TSStatus clearCache() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.clearCache())
        : status;
  }

  @Override
  public TSStatus loadConfiguration() {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.loadConfiguration())
        : status;
  }

  @Override
  public TSStatus setSystemStatus(String systemStatus) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? RpcUtils.squashResponseStatusList(nodeManager.setSystemStatus(systemStatus))
        : status;
  }

  @Override
  public TRegionRouteMapResp getLatestRegionRouteMap() {
    TSStatus status = confirmLeader();
    TRegionRouteMapResp resp = new TRegionRouteMapResp(status);

    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      resp.setTimestamp(System.currentTimeMillis());
      resp.setRegionRouteMap(getLoadManager().getLatestRegionRouteMap());
    }

    return resp;
  }

  @Override
  public UDFManager getUDFManager() {
    return udfManager;
  }

  @Override
  public RegionInfoListResp showRegion(GetRegionInfoListPlan getRegionInfoListPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return partitionManager.getRegionInfoList(getRegionInfoListPlan);
    } else {
      RegionInfoListResp regionResp = new RegionInfoListResp();
      regionResp.setStatus(status);
      return regionResp;
    }
  }

  @Override
  public TShowDataNodesResp showDataNodes() {
    TSStatus status = confirmLeader();
    TShowDataNodesResp resp = new TShowDataNodesResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setDataNodesInfoList(nodeManager.getRegisteredDataNodeInfoList())
          .setStatus(StatusUtils.OK);
    } else {
      return resp.setStatus(status);
    }
  }

  @Override
  public TShowConfigNodesResp showConfigNodes() {
    TSStatus status = confirmLeader();
    TShowConfigNodesResp resp = new TShowConfigNodesResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.setConfigNodesInfoList(nodeManager.getRegisteredConfigNodeInfoList())
          .setStatus(StatusUtils.OK);
    } else {
      return resp.setStatus(status);
    }
  }

  @Override
  public TShowStorageGroupResp showStorageGroup(GetStorageGroupPlan getStorageGroupPlan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return getClusterSchemaManager().showStorageGroup(getStorageGroupPlan);
    } else {
      return new TShowStorageGroupResp().setStatus(status);
    }
  }

  @Override
  public ProcedureManager getProcedureManager() {
    return procedureManager;
  }

  /**
   * @param storageGroups the storage groups to check
   * @return List of PartialPath the storage groups that not exist
   */
  public List<PartialPath> checkStorageGroupExist(List<PartialPath> storageGroups) {
    List<PartialPath> noExistSg = new ArrayList<>();
    if (storageGroups == null) {
      return noExistSg;
    }
    for (PartialPath storageGroup : storageGroups) {
      if (!clusterSchemaManager.getStorageGroupNames().contains(storageGroup.toString())) {
        noExistSg.add(storageGroup);
      }
    }
    return noExistSg;
  }

  @Override
  public void addMetrics() {
    partitionManager.addMetrics();
    nodeManager.addMetrics();
  }

  @Override
  public TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      CreateSchemaTemplatePlan createSchemaTemplatePlan =
          new CreateSchemaTemplatePlan(req.getSerializedTemplate());
      return clusterSchemaManager.createTemplate(createSchemaTemplatePlan);
    } else {
      return status;
    }
  }

  @Override
  public TGetAllTemplatesResp getAllTemplates() {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getAllTemplates();
    } else {
      return new TGetAllTemplatesResp().setStatus(status);
    }
  }

  @Override
  public TGetTemplateResp getTemplate(String req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getTemplate(req);
    } else {
      return new TGetTemplateResp().setStatus(status);
    }
  }

  @Override
  public TSStatus setSchemaTemplate(TSetSchemaTemplateReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.setSchemaTemplate(req.getName(), req.getPath());
    } else {
      return status;
    }
  }

  @Override
  public TGetPathsSetTemplatesResp getPathsSetTemplate(String req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return clusterSchemaManager.getPathsSetTemplate(req);
    } else {
      return new TGetPathsSetTemplatesResp(status);
    }
  }

  @Override
  public TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    }

    PathPatternTree patternTree =
        PathPatternTree.deserialize(ByteBuffer.wrap(req.getPathPatternTree()));

    List<PartialPath> patternList = patternTree.getAllPathPatterns();
    TemplateSetInfoResp templateSetInfoResp = clusterSchemaManager.getTemplateSetInfo(patternList);
    if (templateSetInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return templateSetInfoResp.getStatus();
    }

    Map<PartialPath, List<Template>> templateSetInfo = templateSetInfoResp.getPatternTemplateMap();
    if (templateSetInfo.isEmpty()) {
      return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
    }

    if (!req.getTemplateName().equals(ONE_LEVEL_PATH_WILDCARD)) {
      Map<PartialPath, List<Template>> filteredTemplateSetInfo = new HashMap<>();
      for (Map.Entry<PartialPath, List<Template>> entry : templateSetInfo.entrySet()) {
        for (Template template : entry.getValue()) {
          if (template.getName().equals(req.getTemplateName())) {
            filteredTemplateSetInfo.put(entry.getKey(), Collections.singletonList(template));
            break;
          }
        }
      }

      if (filteredTemplateSetInfo.isEmpty()) {
        return RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS);
      }

      templateSetInfo = filteredTemplateSetInfo;
    }

    return procedureManager.deactivateTemplate(req.getQueryId(), templateSetInfo);
  }

  @Override
  public TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.deleteTimeSeries(req);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus createPipeSink(CreatePipeSinkPlan plan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return syncManager.createPipeSink(plan);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus dropPipeSink(DropPipeSinkPlan plan) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return syncManager.dropPipeSink(plan);
    } else {
      return status;
    }
  }

  @Override
  public TGetPipeSinkResp getPipeSink(TGetPipeSinkReq req) {
    TSStatus status = confirmLeader();
    TGetPipeSinkResp resp = new TGetPipeSinkResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return syncManager.getPipeSink(req.getPipeSinkName());
    } else {
      return resp.setStatus(status);
    }
  }

  @Override
  public TSStatus createPipe(TCreatePipeReq req) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.createPipe(req);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus startPipe(String pipeName) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.startPipe(pipeName);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus stopPipe(String pipeName) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.stopPipe(pipeName);
    } else {
      return status;
    }
  }

  @Override
  public TSStatus dropPipe(String pipeName) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return procedureManager.dropPipe(pipeName);
    } else {
      return status;
    }
  }

  @Override
  public TShowPipeResp showPipe(TShowPipeReq req) {
    TSStatus status = confirmLeader();
    TShowPipeResp resp = new TShowPipeResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return syncManager.showPipe(req.getPipeName());
    } else {
      return resp.setStatus(status);
    }
  }

  @Override
  public TGetRegionIdResp getRegionId(GetRegionIdPlan plan) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? partitionManager.getRegionId(plan).convertToRpcGetRegionIdResp()
        : new TGetRegionIdResp(status);
  }

  @Override
  public TGetTimeSlotListResp getTimeSlotList(GetTimeSlotListPlan plan) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? partitionManager.getTimeSlotList(plan).convertToRpcGetTimeSlotListResp()
        : new TGetTimeSlotListResp(status);
  }

  @Override
  public TGetSeriesSlotListResp getSeriesSlotList(GetSeriesSlotListPlan plan) {
    TSStatus status = confirmLeader();
    return status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        ? partitionManager.getSeriesSlotList(plan).convertToRpcGetSeriesSlotListResp()
        : new TGetSeriesSlotListResp(status);
  }

  /** Get all related schemaRegion which may contains the timeSeries matched by given patternTree */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRelatedSchemaRegionGroup(
      PathPatternTree patternTree) {
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        getSchemaPartition(patternTree).getSchemaPartitionTable();

    List<TRegionReplicaSet> allRegionReplicaSets = getPartitionManager().getAllReplicaSets();
    Set<TConsensusGroupId> groupIdSet =
        schemaPartitionTable.values().stream()
            .flatMap(m -> m.values().stream())
            .collect(Collectors.toSet());
    Map<TConsensusGroupId, TRegionReplicaSet> filteredRegionReplicaSets = new HashMap<>();
    for (TRegionReplicaSet regionReplicaSet : allRegionReplicaSets) {
      if (groupIdSet.contains(regionReplicaSet.getRegionId())) {
        filteredRegionReplicaSets.put(regionReplicaSet.getRegionId(), regionReplicaSet);
      }
    }
    return filteredRegionReplicaSets;
  }

  /**
   * Get all related dataRegion which may contains the data of specific timeseries matched by given
   * patternTree
   */
  public Map<TConsensusGroupId, TRegionReplicaSet> getRelatedDataRegionGroup(
      PathPatternTree patternTree) {
    // Get all storage groups and slots by getting schema partition
    Map<String, Map<TSeriesPartitionSlot, TConsensusGroupId>> schemaPartitionTable =
        getSchemaPartition(patternTree).getSchemaPartitionTable();

    // Construct request for getting data partition
    Map<String, Map<TSeriesPartitionSlot, List<TTimePartitionSlot>>> partitionSlotsMap =
        new HashMap<>();
    schemaPartitionTable.forEach(
        (key, value) -> {
          Map<TSeriesPartitionSlot, List<TTimePartitionSlot>> slotListMap = new HashMap<>();
          value.keySet().forEach(slot -> slotListMap.put(slot, Collections.emptyList()));
          partitionSlotsMap.put(key, slotListMap);
        });

    // Get all data partitions
    GetDataPartitionPlan getDataPartitionPlan = new GetDataPartitionPlan(partitionSlotsMap);
    Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
        dataPartitionTable = getDataPartition(getDataPartitionPlan).getDataPartitionTable();

    // Get all region replicaset of target data partitions
    List<TRegionReplicaSet> allRegionReplicaSets = getPartitionManager().getAllReplicaSets();
    Set<TConsensusGroupId> groupIdSet =
        dataPartitionTable.values().stream()
            .flatMap(
                tSeriesPartitionSlotMapMap ->
                    tSeriesPartitionSlotMapMap.values().stream()
                        .flatMap(
                            tTimePartitionSlotListMap ->
                                tTimePartitionSlotListMap.values().stream()
                                    .flatMap(Collection::stream)))
            .collect(Collectors.toSet());
    Map<TConsensusGroupId, TRegionReplicaSet> filteredRegionReplicaSets = new HashMap<>();
    for (TRegionReplicaSet regionReplicaSet : allRegionReplicaSets) {
      if (groupIdSet.contains(regionReplicaSet.getRegionId())) {
        filteredRegionReplicaSets.put(regionReplicaSet.getRegionId(), regionReplicaSet);
      }
    }
    return filteredRegionReplicaSets;
  }

  public TConfigNodeLocation getConfigNodeLocation(int nodeId) {
    List<TConfigNodeLocation> configNodeLocations = this.nodeManager.getRegisteredConfigNodes();
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      if (configNodeLocation.getConfigNodeId() == nodeId) {
        return configNodeLocation;
      }
    }
    return null;
  }

  public TSStatus transfer(List<TDataNodeLocation> newUnknownDataList) {
    Map<Integer, TDataNodeLocation> runningDataNodeLocationMap = new HashMap<>();
    nodeManager
        .filterDataNodeThroughStatus(NodeStatus.Running)
        .forEach(
            dataNodeConfiguration ->
                runningDataNodeLocationMap.put(
                    dataNodeConfiguration.getLocation().getDataNodeId(),
                    dataNodeConfiguration.getLocation()));
    if (runningDataNodeLocationMap.isEmpty()) {
      // no running DataNode, will not transfer and print log
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }

    newUnknownDataList.forEach(
        dataNodeLocation -> runningDataNodeLocationMap.remove(dataNodeLocation.getDataNodeId()));

    LOGGER.info("start Transfer of {}", newUnknownDataList);
    // transfer trigger
    TSStatus transferResult =
        triggerManager.transferTrigger(newUnknownDataList, runningDataNodeLocationMap);
    if (transferResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn("Fail to transfer because {}, will retry", transferResult.getMessage());
    }

    return transferResult;
  }
}
