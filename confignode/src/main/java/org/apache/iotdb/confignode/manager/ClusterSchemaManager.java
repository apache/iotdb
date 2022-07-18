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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.datanode.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AbstractRetryHandler;
import org.apache.iotdb.confignode.client.async.handlers.SetTTLHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.read.GetNodesInSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.AdjustMaxRegionGroupCountPlan;
import org.apache.iotdb.confignode.consensus.request.write.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.response.PathInfoResp;
import org.apache.iotdb.confignode.consensus.response.TemplateInfoResp;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetPathsSetTemplatesResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTemplateResp;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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

    Set<TDataNodeLocation> dataNodeLocations =
        getPartitionManager()
            .getStorageGroupRelatedDataNodes(
                setTTLPlan.getStorageGroup(), TConsensusGroupType.DataRegion);
    if (dataNodeLocations.size() > 0) {
      CountDownLatch countDownLatch = new CountDownLatch(dataNodeLocations.size());
      Map<Integer, AbstractRetryHandler> handler = new HashMap<>();
      Map<Integer, TDataNodeLocation> dataNodeLocationMap = new ConcurrentHashMap<>();
      AtomicInteger index = new AtomicInteger();
      // TODO: Use procedure to protect SetTTL on DataNodes
      for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
        handler.put(
            index.get(),
            new SetTTLHandler(
                countDownLatch,
                DataNodeRequestType.SET_TTL,
                dataNodeLocation,
                dataNodeLocationMap,
                index.get()));
        dataNodeLocationMap.put(index.getAndIncrement(), dataNodeLocation);
      }
      AsyncDataNodeClientPool.getInstance()
          .sendAsyncRequestToDataNodeWithRetry(
              new TSetTTLReq(setTTLPlan.getStorageGroup(), setTTLPlan.getTTL()),
              handler,
              dataNodeLocationMap);
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
    GetSchemaTemplatePlan getSchemaTemplatePlan = new GetSchemaTemplatePlan();
    TemplateInfoResp templateResp =
        (TemplateInfoResp) getConsensusManager().read(getSchemaTemplatePlan).getDataset();
    TGetAllTemplatesResp resp = new TGetAllTemplatesResp();
    resp.setStatus(templateResp.getStatus());
    if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (templateResp.getTemplateList() != null) {
        List<ByteBuffer> list = new ArrayList<ByteBuffer>();
        templateResp.getTemplateList().stream()
            .forEach(
                item -> {
                  try {
                    list.add(Template.template2ByteBuffer(item));
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
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
    GetNodesInSchemaTemplatePlan getNodesInSchemaTemplatePlan =
        new GetNodesInSchemaTemplatePlan(req);
    TemplateInfoResp templateResp =
        (TemplateInfoResp) getConsensusManager().read(getNodesInSchemaTemplatePlan).getDataset();
    TGetTemplateResp resp = new TGetTemplateResp();
    try {
      if (templateResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if (templateResp.getTemplateList() != null && !templateResp.getTemplateList().isEmpty()) {
          ByteBuffer byteBuffer =
              Template.template2ByteBuffer(templateResp.getTemplateList().get(0));
          resp.setTemplate(byteBuffer);
        }
      }
      resp.setStatus(templateResp.getStatus());
    } catch (IOException e) {
      resp.setStatus(new TSStatus(TSStatusCode.TEMPLATE_IMCOMPATIBLE.getStatusCode()));
    }
    return resp;
  }

  /**
   * mount template
   *
   * @param templateName
   * @param path
   * @return
   */
  public TSStatus setSchemaTemplate(String templateName, String path) {
    SetSchemaTemplatePlan setSchemaTemplatePlan = new SetSchemaTemplatePlan(templateName, path);
    return getConsensusManager().write(setSchemaTemplatePlan).getStatus();
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
