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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.AdjustMaxRegionGroupCountReq;
import org.apache.iotdb.confignode.consensus.request.write.DeleteStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.exception.StorageGroupNotExistsException;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/** The ClusterSchemaManager Manages cluster schema read and write requests. */
public class ClusterSchemaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaManager.class);

  private final Manager configManager;
  private final ClusterSchemaInfo clusterSchemaInfo;

  public ClusterSchemaManager(Manager configManager, ClusterSchemaInfo clusterSchemaInfo) {
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
  public TSStatus setStorageGroup(SetStorageGroupReq setStorageGroupReq) {
    TSStatus result;
    try {
      clusterSchemaInfo.checkContainsStorageGroup(setStorageGroupReq.getSchema().getName());
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
    result = getConsensusManager().write(setStorageGroupReq).getStatus();

    // Adjust the maximum RegionGroup number of each StorageGroup
    adjustMaxRegionGroupCount();

    return result;
  }

  public TSStatus deleteStorageGroup(DeleteStorageGroupReq deleteStorageGroupReq) {
    return getConsensusManager().write(deleteStorageGroupReq).getStatus();
  }

  /**
   * Count StorageGroups by specific path pattern
   *
   * @return CountStorageGroupResp
   */
  public DataSet countMatchedStorageGroups(
    CountStorageGroupReq countStorageGroupReq) {
    return getConsensusManager().read(countStorageGroupReq).getDataset();
  }

  /**
   * Get StorageGroupSchemas by specific path pattern
   *
   * @return StorageGroupSchemaDataSet
   */
  public DataSet getMatchedStorageGroupSchema(
    GetStorageGroupReq getStorageGroupReq) {
    return getConsensusManager().read(getStorageGroupReq).getDataset();
  }

  public TSStatus setTTL(SetTTLReq setTTLReq) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setTTLReq).getStatus();
  }

  public TSStatus setSchemaReplicationFactor(
    SetSchemaReplicationFactorReq setSchemaReplicationFactorReq) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setSchemaReplicationFactorReq).getStatus();
  }

  public TSStatus setDataReplicationFactor(
    SetDataReplicationFactorReq setDataReplicationFactorReq) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setDataReplicationFactorReq).getStatus();
  }

  public TSStatus setTimePartitionInterval(
    SetTimePartitionIntervalReq setTimePartitionIntervalReq) {
    // TODO: Inform DataNodes
    return getConsensusManager().write(setTimePartitionIntervalReq).getStatus();
  }

  // ======================================================
  // Leader scheduling interfaces
  // ======================================================

  /**
   * Only leader use this interface.
   * Adjust the maxSchemaRegionGroupCount and maxDataRegionGroupCount of each StorageGroup
   * bases on existing cluster resources
   */
  public void adjustMaxRegionGroupCount() {
    int dataNodeNum = getNodeManager().getOnlineDataNodeCount();
    int totalCpuCoreNum = getNodeManager().getTotalCpuCoreCount();
    Map<String, TStorageGroupSchema> storageGroupSchemaMap = getMatchedStorageGroupSchemasByName(getStorageGroupNames());
    int storageGroupNum = storageGroupSchemaMap.size();

    AdjustMaxRegionGroupCountReq adjustMaxRegionGroupCountReq = new AdjustMaxRegionGroupCountReq();
    for (TStorageGroupSchema storageGroupSchema : storageGroupSchemaMap.values()) {
      // Adjust maxSchemaRegionGroupCount.
      // All StorageGroups share the DataNodes equally.
      int maxSchemaRegionGroupCount = Math.max(1, dataNodeNum /
        (storageGroupNum * storageGroupSchema.getSchemaReplicationFactor()));
      // Adjust maxDataRegionGroupCount.
      // All StorageGroups divide one-third of the total cpu cores equally.
      int maxDataRegionGroupCount = Math.max(2, totalCpuCoreNum / (3 * storageGroupNum * storageGroupSchema.getDataReplicationFactor()));

      adjustMaxRegionGroupCountReq.putEntry(storageGroupSchema.getName(), new Pair<>(maxSchemaRegionGroupCount, maxDataRegionGroupCount));
    }
    getConsensusManager().write(adjustMaxRegionGroupCountReq);
  }

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

  private NodeManager getNodeManager() {
    return configManager.getNodeManager();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
