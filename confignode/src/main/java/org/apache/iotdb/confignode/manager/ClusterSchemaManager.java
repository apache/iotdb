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
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.sync.datanode.SyncDataNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetAllTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.AdjustMaxRegionGroupCountPlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.response.AllTemplateSetInfoResp;
import org.apache.iotdb.confignode.consensus.response.PathInfoResp;
import org.apache.iotdb.confignode.consensus.response.TemplateInfoResp;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.metadata.template.TemplateInternalRPCUpdateType;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** The ClusterSchemaManager Manages cluster schema read and write requests. */
public class ClusterSchemaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaManager.class);

  private static final double schemaRegionPerDataNode =
      ConfigNodeDescriptor.getInstance().getConf().getSchemaRegionPerDataNode();
  private static final double dataRegionPerProcessor =
      ConfigNodeDescriptor.getInstance().getConf().getDataRegionPerProcessor();

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
    adjustMaxRegionGroupCount();

    return result;
  }

  public TSStatus deleteStorageGroup(DeleteStorageGroupPlan deleteStorageGroupPlan) {
    // Adjust the maximum RegionGroup number of each StorageGroup
    adjustMaxRegionGroupCount();
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

  /**
   * Update TTL for the specific StorageGroup
   *
   * @param setTTLPlan setTTLPlan
   * @return SUCCESS_STATUS if successfully update the TTL, STORAGE_GROUP_NOT_EXIST if the specific
   *     StorageGroup doesn't exist
   */
  public TSStatus setTTL(SetTTLPlan setTTLPlan) {

    if (!getStorageGroupNames().contains(setTTLPlan.getStorageGroup())) {
      return RpcUtils.getStatus(
          TSStatusCode.STORAGE_GROUP_NOT_EXIST,
          "storageGroup " + setTTLPlan.getStorageGroup() + " does not exist");
    }
    Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
    Set<TDataNodeLocation> dataNodeLocations =
        getPartitionManager()
            .getStorageGroupRelatedDataNodes(
                setTTLPlan.getStorageGroup(), TConsensusGroupType.DataRegion);
    dataNodeLocations.forEach(
        dataNodeLocation ->
            dataNodeLocationMap.put(dataNodeLocation.getDataNodeId(), dataNodeLocation));
    if (dataNodeLocations.size() > 0) {
      // TODO: Use procedure to protect SetTTL on DataNodes
      AsyncDataNodeClientPool.getInstance()
          .sendAsyncRequestToDataNodeWithRetry(
              new TSetTTLReq(setTTLPlan.getStorageGroup(), setTTLPlan.getTTL()),
              dataNodeLocationMap,
              DataNodeRequestType.SET_TTL,
              null);
    }

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
   * Only leader use this interface. Adjust the maxSchemaRegionGroupCount and
   * maxDataRegionGroupCount of each StorageGroup bases on existing cluster resources
   */
  public synchronized void adjustMaxRegionGroupCount() {
    // Get all StorageGroupSchemas
    Map<String, TStorageGroupSchema> storageGroupSchemaMap =
        getMatchedStorageGroupSchemasByName(getStorageGroupNames());
    int dataNodeNum = getNodeManager().getRegisteredDataNodeCount();
    int totalCpuCoreNum = getNodeManager().getTotalCpuCoreCount();
    int storageGroupNum = storageGroupSchemaMap.size();

    AdjustMaxRegionGroupCountPlan adjustMaxRegionGroupCountPlan =
        new AdjustMaxRegionGroupCountPlan();
    for (TStorageGroupSchema storageGroupSchema : storageGroupSchemaMap.values()) {
      try {
        // Adjust maxSchemaRegionGroupCount.
        // All StorageGroups share the DataNodes equally.
        // Allocated SchemaRegionGroups are not shrunk.
        int allocatedSchemaRegionGroupCount =
            getPartitionManager()
                .getRegionCount(storageGroupSchema.getName(), TConsensusGroupType.SchemaRegion);
        int maxSchemaRegionGroupCount =
            Math.max(
                1,
                Math.max(
                    (int)
                        (schemaRegionPerDataNode
                            * dataNodeNum
                            / (double)
                                (storageGroupNum
                                    * storageGroupSchema.getSchemaReplicationFactor())),
                    allocatedSchemaRegionGroupCount));

        // Adjust maxDataRegionGroupCount.
        // All StorageGroups divide one-third of the total cpu cores equally.
        // Allocated DataRegionGroups are not shrunk.
        int allocatedDataRegionGroupCount =
            getPartitionManager()
                .getRegionCount(storageGroupSchema.getName(), TConsensusGroupType.DataRegion);
        int maxDataRegionGroupCount =
            Math.max(
                2,
                Math.max(
                    (int)
                        (dataRegionPerProcessor
                            * totalCpuCoreNum
                            / (double)
                                (storageGroupNum * storageGroupSchema.getDataReplicationFactor())),
                    allocatedDataRegionGroupCount));

        adjustMaxRegionGroupCountPlan.putEntry(
            storageGroupSchema.getName(),
            new Pair<>(maxSchemaRegionGroupCount, maxDataRegionGroupCount));
      } catch (StorageGroupNotExistsException e) {
        LOGGER.warn("Adjust maxRegionGroupCount failed because StorageGroup doesn't exist", e);
      }
    }
    getConsensusManager().write(adjustMaxRegionGroupCountPlan);
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
   * @return List<StorageGroupName>, all storageGroups' name
   */
  public List<String> getStorageGroupNames() {
    return clusterSchemaInfo.getStorageGroupNames();
  }

  /**
   * Only leader use this interface. Get the maxRegionGroupCount of specific StorageGroup.
   *
   * @param storageGroup StorageGroupName
   * @param consensusGroupType SchemaRegion or DataRegion
   * @return maxSchemaRegionGroupCount or maxDataRegionGroupCount
   */
  public int getMaxRegionGroupCount(String storageGroup, TConsensusGroupType consensusGroupType) {
    return clusterSchemaInfo.getMaxRegionGroupCount(storageGroup, consensusGroupType);
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
        List<ByteBuffer> list = new ArrayList<ByteBuffer>();
        templateResp
            .getTemplateList()
            .forEach(
                template -> {
                  list.add(template.serialize());
                });
        resp.setTemplateList(list);
      }
    }
    return resp;
  }

  /**
   * show nodes in schema template
   *
   * @param req
   * @return
   */
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

  /**
   * mount template
   *
   * @param templateName
   * @param path
   * @return
   */
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

    // prepare template data and req
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(1, outputStream);
      template.serialize(outputStream);
      ReadWriteIOUtils.write(1, outputStream);
      ReadWriteIOUtils.write(path, outputStream);
    } catch (IOException ignored) {
    }
    TUpdateTemplateReq req = new TUpdateTemplateReq();
    req.setType(TemplateInternalRPCUpdateType.ADD_TEMPLATE_SET_INFO.toByte());
    req.setTemplateInfo(outputStream.toByteArray());

    // sync template set info to all dataNodes
    TSStatus status;
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes(-1);
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
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(templateId, outputStream);
      ReadWriteIOUtils.write(path, outputStream);
    } catch (IOException ignored) {

    }
    rollbackReq.setTemplateInfo(outputStream.toByteArray());

    // get all dataNodes
    List<TDataNodeConfiguration> allDataNodes =
        configManager.getNodeManager().getRegisteredDataNodes(-1);

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

  /**
   * show path set template xx
   *
   * @param templateName
   * @return
   */
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

  public byte[] getAllTemplateSetInfo() {
    AllTemplateSetInfoResp resp =
        (AllTemplateSetInfoResp)
            getConsensusManager().read(new GetAllTemplateSetInfoPlan()).getDataset();
    return resp.getTemplateInfo();
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
