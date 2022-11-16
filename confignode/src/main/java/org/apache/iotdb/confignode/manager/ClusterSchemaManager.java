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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.client.sync.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.storagegroup.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.storagegroup.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.AdjustMaxRegionGroupNumPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.storagegroup.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.RollbackPreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.PathInfoResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.consensus.response.TemplateInfoResp;
import org.apache.iotdb.confignode.consensus.response.TemplateSetInfoResp;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.partition.PartitionManager;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupInfo;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUtil;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** The ClusterSchemaManager Manages cluster schema read and write requests. */
public class ClusterSchemaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaManager.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final int LEAST_SCHEMA_REGION_GROUP_NUM = CONF.getLeastSchemaRegionGroupNum();
  private static final double SCHEMA_REGION_PER_DATA_NODE = CONF.getSchemaRegionPerDataNode();
  private static final int LEAST_DATA_REGION_GROUP_NUM = CONF.getLeastDataRegionGroupNum();
  private static final double DATA_REGION_PER_PROCESSOR = CONF.getDataRegionPerProcessor();

  private final IManager configManager;
  private final ClusterSchemaInfo clusterSchemaInfo;

  public ClusterSchemaManager(IManager configManager, ClusterSchemaInfo clusterSchemaInfo) {
    this.configManager = configManager;
    this.clusterSchemaInfo = clusterSchemaInfo;
  }

  // ======================================================
  // Consensus read/write interfaces
  // ======================================================

  /**
   * Set StorageGroup
   *
   * @return SUCCESS_STATUS if the StorageGroup is set successfully. STORAGE_GROUP_ALREADY_EXISTS if
   *     the StorageGroup is already set. PERSISTENCE_FAILURE if fail to set StorageGroup in
   *     MTreeAboveSG.
   */
  public TSStatus setStorageGroup(SetStorageGroupPlan setStorageGroupPlan) {
    TSStatus result;
    try {
      clusterSchemaInfo.checkContainsStorageGroup(setStorageGroupPlan.getSchema().getName());
    } catch (MetadataException metadataException) {
      // Reject if StorageGroup already set
      if (metadataException instanceof IllegalPathException) {
        result = new TSStatus(TSStatusCode.PATH_ILLEGAL.getStatusCode());
      } else {
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode());
      }
      result.setMessage(metadataException.getMessage());
      return result;
    }

    // Cache StorageGroupSchema
    result = getConsensusManager().write(setStorageGroupPlan).getStatus();

    // Adjust the maximum RegionGroup number of each StorageGroup
    adjustMaxRegionGroupNum();

    return result;
  }

  public TSStatus deleteStorageGroup(DeleteStorageGroupPlan deleteStorageGroupPlan) {
    // Adjust the maximum RegionGroup number of each StorageGroup
    adjustMaxRegionGroupNum();
    return getConsensusManager().write(deleteStorageGroupPlan).getStatus();
  }

  /**
   * Count StorageGroups by specific path pattern
   *
   * @return CountStorageGroupResp
   */
  public DataSet countMatchedStorageGroups(CountStorageGroupPlan countStorageGroupPlan) {
    return getConsensusManager().read(countStorageGroupPlan).getDataset();
  }

  /**
   * Get StorageGroupSchemas by specific path pattern
   *
   * @return StorageGroupSchemaDataSet
   */
  public DataSet getMatchedStorageGroupSchema(GetStorageGroupPlan getStorageGroupPlan) {
    return getConsensusManager().read(getStorageGroupPlan).getDataset();
  }

  /** Only used in cluster tool show StorageGroup */
  public TShowStorageGroupResp showStorageGroup(GetStorageGroupPlan getStorageGroupPlan) {
    StorageGroupSchemaResp storageGroupSchemaResp =
        (StorageGroupSchemaResp) getMatchedStorageGroupSchema(getStorageGroupPlan);
    if (storageGroupSchemaResp.getStatus().getCode()
        != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // Return immediately if some StorageGroups doesn't exist
      return new TShowStorageGroupResp().setStatus(storageGroupSchemaResp.getStatus());
    }

    Map<String, TStorageGroupInfo> infoMap = new ConcurrentHashMap<>();
    for (TStorageGroupSchema storageGroupSchema : storageGroupSchemaResp.getSchemaMap().values()) {
      String name = storageGroupSchema.getName();
      TStorageGroupInfo storageGroupInfo = new TStorageGroupInfo();
      storageGroupInfo.setName(name);
      storageGroupInfo.setTTL(storageGroupSchema.getTTL());
      storageGroupInfo.setSchemaReplicationFactor(storageGroupSchema.getSchemaReplicationFactor());
      storageGroupInfo.setDataReplicationFactor(storageGroupSchema.getDataReplicationFactor());
      storageGroupInfo.setTimePartitionInterval(storageGroupSchema.getTimePartitionInterval());

      try {
        storageGroupInfo.setSchemaRegionNum(
            getPartitionManager().getRegionCount(name, TConsensusGroupType.SchemaRegion));
        storageGroupInfo.setDataRegionNum(
            getPartitionManager().getRegionCount(name, TConsensusGroupType.DataRegion));
      } catch (StorageGroupNotExistsException e) {
        // Return immediately if some StorageGroups doesn't exist
        return new TShowStorageGroupResp()
            .setStatus(
                new TSStatus(TSStatusCode.STORAGE_GROUP_NOT_EXIST.getStatusCode())
                    .setMessage(e.getMessage()));
      }

      infoMap.put(name, storageGroupInfo);
    }

    return new TShowStorageGroupResp().setStorageGroupInfoMap(infoMap).setStatus(StatusUtils.OK);
  }

  /**
   * Update TTL for the specific StorageGroup or all databases in a path
   *
   * @param setTTLPlan setTTLPlan
   * @return SUCCESS_STATUS if successfully update the TTL, STORAGE_GROUP_NOT_EXIST if the path
   *     doesn't exist
   */
  public TSStatus setTTL(SetTTLPlan setTTLPlan) {

    Map<String, TStorageGroupSchema> storageSchemaMap =
        clusterSchemaInfo.getMatchedStorageGroupSchemasByOneName(
            setTTLPlan.getStorageGroupPathPattern());

    if (storageSchemaMap.isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.STORAGE_GROUP_NOT_EXIST,
          "Path [" + new PartialPath(setTTLPlan.getStorageGroupPathPattern()) + "] does not exist");
    }

    // Map<DataNodeId, TDataNodeLocation>
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    // Map<DataNodeId, StorageGroupPatterns>
    Map<Integer, List<String>> dnlToSgMap = new ConcurrentHashMap<>();
    for (String storageGroup : storageSchemaMap.keySet()) {
      // Get related DataNodes
      Set<TDataNodeLocation> dataNodeLocations =
          getPartitionManager()
              .getStorageGroupRelatedDataNodes(storageGroup, TConsensusGroupType.DataRegion);

      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        dataNodeLocationMap.putIfAbsent(dataNodeLocation.getDataNodeId(), dataNodeLocation);
        dnlToSgMap
            .computeIfAbsent(dataNodeLocation.getDataNodeId(), empty -> new ArrayList<>())
            .add(storageGroup);
      }
    }

    AsyncClientHandler<TSetTTLReq, TSStatus> clientHandler =
        new AsyncClientHandler<>(DataNodeRequestType.SET_TTL);
    dnlToSgMap
        .keySet()
        .forEach(
            dataNodeId -> {
              TSetTTLReq setTTLReq =
                  new TSetTTLReq(dnlToSgMap.get(dataNodeId), setTTLPlan.getTTL());
              clientHandler.putRequest(dataNodeId, setTTLReq);
              clientHandler.putDataNodeLocation(dataNodeId, dataNodeLocationMap.get(dataNodeId));
            });
    // TODO: Check response
    AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);

    return getConsensusManager().write(setTTLPlan).getStatus();
  }

  public TSStatus setSchemaReplicationFactor(
      SetSchemaReplicationFactorPlan setSchemaReplicationFactorPlan) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setSchemaReplicationFactorPlan).getStatus();
  }

  public TSStatus setDataReplicationFactor(
      SetDataReplicationFactorPlan setDataReplicationFactorPlan) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setDataReplicationFactorPlan).getStatus();
  }

  public TSStatus setTimePartitionInterval(
      SetTimePartitionIntervalPlan setTimePartitionIntervalPlan) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setTimePartitionIntervalPlan).getStatus();
  }

  /**
   * Only leader use this interface. Adjust the maxSchemaRegionGroupNum and maxDataRegionGroupNum of
   * each StorageGroup bases on existing cluster resources
   */
  public synchronized void adjustMaxRegionGroupNum() {
    // Get all StorageGroupSchemas
    Map<String, TStorageGroupSchema> storageGroupSchemaMap =
        getMatchedStorageGroupSchemasByName(getStorageGroupNames());
    int dataNodeNum = getNodeManager().getRegisteredDataNodeCount();
    int totalCpuCoreNum = getNodeManager().getTotalCpuCoreCount();
    int storageGroupNum = storageGroupSchemaMap.size();

    AdjustMaxRegionGroupNumPlan adjustMaxRegionGroupNumPlan = new AdjustMaxRegionGroupNumPlan();
    for (TStorageGroupSchema storageGroupSchema : storageGroupSchemaMap.values()) {
      try {
        // Adjust maxSchemaRegionGroupNum for each StorageGroup.
        // All StorageGroups share the DataNodes equally.
        // Allocated SchemaRegionGroups are not shrunk.
        int allocatedSchemaRegionGroupCount =
            getPartitionManager()
                .getRegionCount(storageGroupSchema.getName(), TConsensusGroupType.SchemaRegion);
        int maxSchemaRegionGroupNum =
            Math.max(
                // The least number of DataRegionGroup of each StorageGroup is specified
                // by parameter least_data_region_group_num, which is currently unconfigurable.
                LEAST_SCHEMA_REGION_GROUP_NUM,
                Math.max(
                    // The maxSchemaRegionGroupNum of the current StorageGroup is expected to be
                    // (SCHEMA_REGION_PER_DATA_NODE * registerDataNodeNum) /
                    // (createdStorageGroupNum * schemaReplicationFactor)
                    (int)
                        (SCHEMA_REGION_PER_DATA_NODE
                            * dataNodeNum
                            / (double)
                                (storageGroupNum
                                    * storageGroupSchema.getSchemaReplicationFactor())),
                    allocatedSchemaRegionGroupCount));

        // Adjust maxDataRegionGroupNum for each StorageGroup.
        // All StorageGroups divide the total cpu cores equally.
        // Allocated DataRegionGroups are not shrunk.
        int allocatedDataRegionGroupCount =
            getPartitionManager()
                .getRegionCount(storageGroupSchema.getName(), TConsensusGroupType.DataRegion);
        int maxDataRegionGroupNum =
            Math.max(
                // The least number of DataRegionGroup of each StorageGroup is specified
                // by parameter least_data_region_group_num.
                LEAST_DATA_REGION_GROUP_NUM,
                Math.max(
                    // The maxDataRegionGroupNum of the current StorageGroup is expected to be
                    // (DATA_REGION_PER_PROCESSOR * totalCpuCoreNum) /
                    // (createdStorageGroupNum * dataReplicationFactor)
                    (int)
                        (DATA_REGION_PER_PROCESSOR
                            * totalCpuCoreNum
                            / (double)
                                (storageGroupNum * storageGroupSchema.getDataReplicationFactor())),
                    allocatedDataRegionGroupCount));

        adjustMaxRegionGroupNumPlan.putEntry(
            storageGroupSchema.getName(),
            new Pair<>(maxSchemaRegionGroupNum, maxDataRegionGroupNum));
      } catch (StorageGroupNotExistsException e) {
        LOGGER.warn("Adjust maxRegionGroupNum failed because StorageGroup doesn't exist", e);
      }
    }
    getConsensusManager().write(adjustMaxRegionGroupNumPlan);
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Only leader use this interface.
   *
   * @param storageGroup StorageGroupName
   * @return The specific StorageGroupSchema
   * @throws StorageGroupNotExistsException When the specific StorageGroup doesn't exist
   */
  public TStorageGroupSchema getStorageGroupSchemaByName(String storageGroup)
      throws StorageGroupNotExistsException {
    return clusterSchemaInfo.getMatchedStorageGroupSchemaByName(storageGroup);
  }

  /**
   * Only leader use this interface
   *
   * @param storageGroup StorageGroupName
   * @param consensusGroupType SchemaRegion for SchemaReplicationFactor and DataRegion for
   *     DataReplicationFactor
   * @return SchemaReplicationFactor or DataReplicationFactor
   * @throws StorageGroupNotExistsException When the specific StorageGroup doesn't exist
   */
  public int getReplicationFactor(String storageGroup, TConsensusGroupType consensusGroupType)
      throws StorageGroupNotExistsException {
    TStorageGroupSchema storageGroupSchema = getStorageGroupSchemaByName(storageGroup);
    return consensusGroupType == TConsensusGroupType.SchemaRegion
        ? storageGroupSchema.getSchemaReplicationFactor()
        : storageGroupSchema.getDataReplicationFactor();
  }

  /**
   * Only leader use this interface.
   *
   * @param rawPathList List<StorageGroupName>
   * @return the matched StorageGroupSchemas
   */
  public Map<String, TStorageGroupSchema> getMatchedStorageGroupSchemasByName(
      List<String> rawPathList) {
    return clusterSchemaInfo.getMatchedStorageGroupSchemasByName(rawPathList);
  }

  /**
   * Only leader use this interface.
   *
   * @return List<StorageGroupName>, all StorageGroups' name
   */
  public List<String> getStorageGroupNames() {
    return clusterSchemaInfo.getStorageGroupNames();
  }

  /**
   * Only leader use this interface. Get the maxRegionGroupNum of specific StorageGroup.
   *
   * @param storageGroup StorageGroupName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return maxSchemaRegionGroupNum or maxDataRegionGroupNum
   */
  public int getMaxRegionGroupNum(String storageGroup, TConsensusGroupType consensusGroupType) {
    return clusterSchemaInfo.getMaxRegionGroupNum(storageGroup, consensusGroupType);
  }

  /**
   * create schema template
   *
   * @param createSchemaTemplatePlan CreateSchemaTemplatePlan
   * @return TSStatus
   */
  public TSStatus createTemplate(CreateSchemaTemplatePlan createSchemaTemplatePlan) {
    return getConsensusManager().write(createSchemaTemplatePlan).getStatus();
  }

  /**
   * show schema template
   *
   * @return TGetAllTemplatesResp
   */
  public TGetAllTemplatesResp getAllTemplates() {
    GetAllSchemaTemplatePlan getAllSchemaTemplatePlan = new GetAllSchemaTemplatePlan();
    TemplateInfoResp templateResp =
        (TemplateInfoResp) getConsensusManager().read(getAllSchemaTemplatePlan).getDataset();
    TGetAllTemplatesResp resp = new TGetAllTemplatesResp();
    resp.setStatus(templateResp.getStatus());
    if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (templateResp.getTemplateList() != null) {
        List<ByteBuffer> list = new ArrayList<>();
        templateResp.getTemplateList().forEach(template -> list.add(template.serialize()));
        resp.setTemplateList(list);
      }
    }
    return resp;
  }

  /** show nodes in schema template */
  public TGetTemplateResp getTemplate(String req) {
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(req);
    TemplateInfoResp templateResp =
        (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan).getDataset();
    TGetTemplateResp resp = new TGetTemplateResp();
    if (templateResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (templateResp.getTemplateList() != null && !templateResp.getTemplateList().isEmpty()) {
        ByteBuffer byteBuffer = templateResp.getTemplateList().get(0).serialize();
        resp.setTemplate(byteBuffer);
      }
    }
    resp.setStatus(templateResp.getStatus());
    return resp;
  }

  /** mount template */
  public synchronized TSStatus setSchemaTemplate(String templateName, String path) {
    // check whether the template can be set on given path
    CheckTemplateSettablePlan checkTemplateSettablePlan =
        new CheckTemplateSettablePlan(templateName, path);
    TemplateInfoResp resp =
        (TemplateInfoResp) getConsensusManager().read(checkTemplateSettablePlan).getDataset();
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return resp.getStatus();
    }

    Template template = resp.getTemplateList().get(0);

    // prepare req
    TUpdateTemplateReq req = new TUpdateTemplateReq();
    req.setType(TemplateInternalRPCUpdateType.ADD_TEMPLATE_SET_INFO.toByte());
    req.setTemplateInfo(TemplateInternalRPCUtil.generateAddTemplateSetInfoBytes(template, path));

    // sync template set info to all dataNodes
    TSStatus status;
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes();
    for (TDataNodeConfiguration dataNodeInfo : allDataNodes) {
      status =
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  dataNodeInfo.getLocation().getInternalEndPoint(),
                  req,
                  DataNodeRequestType.UPDATE_TEMPLATE);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // roll back the synced cache on dataNodes
        return status.setSubStatus(rollbackTemplateSetInfoSync(template.getId(), path));
      }
    }

    // execute set operation on configNode
    SetSchemaTemplatePlan setSchemaTemplatePlan = new SetSchemaTemplatePlan(templateName, path);
    status = getConsensusManager().write(setSchemaTemplatePlan).getStatus();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return status;
    } else {
      // roll back the synced cache on dataNodes
      return status.setSubStatus(rollbackTemplateSetInfoSync(template.getId(), path));
    }
  }

  private List<TSStatus> rollbackTemplateSetInfoSync(int templateId, String path) {
    // construct the rollbackReq
    TUpdateTemplateReq rollbackReq = new TUpdateTemplateReq();
    rollbackReq.setType(TemplateInternalRPCUpdateType.INVALIDATE_TEMPLATE_SET_INFO.toByte());
    rollbackReq.setTemplateInfo(
        TemplateInternalRPCUtil.generateInvalidateTemplateSetInfoBytes(templateId, path));

    // get all dataNodes
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes();

    // send rollbackReq
    TSStatus status;
    List<TSStatus> failedRollbackStatusList = new ArrayList<>();
    for (TDataNodeConfiguration dataNodeInfo : allDataNodes) {
      status =
          SyncDataNodeClientPool.getInstance()
              .sendSyncRequestToDataNodeWithRetry(
                  dataNodeInfo.getLocation().getInternalEndPoint(),
                  rollbackReq,
                  DataNodeRequestType.UPDATE_TEMPLATE);
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        failedRollbackStatusList.add(status);
      }
    }
    return failedRollbackStatusList;
  }

  /** show path set template xx */
  public TGetPathsSetTemplatesResp getPathsSetTemplate(String templateName) {
    GetPathsSetTemplatePlan getPathsSetTemplatePlan = new GetPathsSetTemplatePlan(templateName);
    PathInfoResp pathInfoResp =
        (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan).getDataset();
    if (pathInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      TGetPathsSetTemplatesResp resp = new TGetPathsSetTemplatesResp();
      resp.setStatus(RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS));
      resp.setPathList(pathInfoResp.getPathList());
      return resp;
    } else {
      return new TGetPathsSetTemplatesResp(pathInfoResp.getStatus());
    }
  }

  /**
   * get all template set info to sync to all dataNodes, the pre unset template info won't be taken
   */
  public byte[] getAllTemplateSetInfo() {
    AllTemplateSetInfoResp resp =
        (AllTemplateSetInfoResp)
            getConsensusManager().read(new GetAllTemplateSetInfoPlan()).getDataset();
    return resp.getTemplateInfo();
  }

  public TemplateSetInfoResp getTemplateSetInfo(List<PartialPath> patternList) {
    return (TemplateSetInfoResp)
        getConsensusManager().read(new GetTemplateSetInfoPlan(patternList)).getDataset();
  }

  public Pair<TSStatus, Template> checkIsTemplateSetOnPath(String templateName, String path) {
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(templateName);
    TemplateInfoResp templateResp =
        (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan).getDataset();
    if (templateResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (templateResp.getTemplateList() == null || templateResp.getTemplateList().isEmpty()) {
        return new Pair<>(
            RpcUtils.getStatus(
                TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode(),
                String.format("Undefined template name: %s", templateName)),
            null);
      }
    } else {
      return new Pair<>(templateResp.getStatus(), null);
    }

    GetPathsSetTemplatePlan getPathsSetTemplatePlan = new GetPathsSetTemplatePlan(templateName);
    PathInfoResp pathInfoResp =
        (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan).getDataset();
    if (pathInfoResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      List<String> templateSetPathList = pathInfoResp.getPathList();
      if (templateSetPathList == null
          || templateSetPathList.isEmpty()
          || !pathInfoResp.getPathList().contains(path)) {
        return new Pair<>(
            RpcUtils.getStatus(
                TSStatusCode.NO_TEMPLATE_ON_MNODE.getStatusCode(),
                String.format("No template on %s", path)),
            null);
      } else {
        return new Pair<>(templateResp.getStatus(), templateResp.getTemplateList().get(0));
      }
    } else {
      return new Pair<>(pathInfoResp.getStatus(), null);
    }
  }

  public TSStatus preUnsetSchemaTemplate(int templateId, PartialPath path) {
    return getConsensusManager()
        .write(new PreUnsetSchemaTemplatePlan(templateId, path))
        .getStatus();
  }

  public TSStatus rollbackPreUnsetSchemaTemplate(int templateId, PartialPath path) {
    return getConsensusManager()
        .write(new RollbackPreUnsetSchemaTemplatePlan(templateId, path))
        .getStatus();
  }

  public TSStatus unsetSchemaTemplateInBlackList(int templateId, PartialPath path) {
    return getConsensusManager().write(new UnsetSchemaTemplatePlan(templateId, path)).getStatus();
  }

  public synchronized TSStatus dropSchemaTemplate(String templateName) {

    // check template existence
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan(templateName);
    TemplateInfoResp templateInfoResp =
        (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan).getDataset();
    if (templateInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return templateInfoResp.getStatus();
    } else if (templateInfoResp.getTemplateList() == null
        || templateInfoResp.getTemplateList().isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.UNDEFINED_TEMPLATE.getStatusCode(),
          String.format("Undefined template name: %s", templateName));
    }

    // check is template set on some path, block all template set operation
    GetPathsSetTemplatePlan getPathsSetTemplatePlan = new GetPathsSetTemplatePlan(templateName);
    PathInfoResp pathInfoResp =
        (PathInfoResp) getConsensusManager().read(getPathsSetTemplatePlan).getDataset();
    if (pathInfoResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return pathInfoResp.getStatus();
    } else if (pathInfoResp.getPathList() != null && !pathInfoResp.getPathList().isEmpty()) {
      return RpcUtils.getStatus(
          TSStatusCode.METADATA_ERROR.getStatusCode(),
          String.format(
              "Template [%s] has been set on MTree, cannot be dropped now.", templateName));
    }

    // execute drop template
    return getConsensusManager().write(new DropSchemaTemplatePlan(templateName)).getStatus();
  }

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private PartitionManager getPartitionManager() {
    return configManager.getPartitionManager();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
