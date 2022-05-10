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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.CreateRegionsReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.consensus.response.CountStorageGroupResp;
import org.apache.iotdb.confignode.consensus.response.StorageGroupSchemaResp;
import org.apache.iotdb.confignode.persistence.ClusterSchemaInfo;
import org.apache.iotdb.confignode.persistence.PartitionInfo;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** The ClusterSchemaManager Manages cluster schema read and write requests. */
public class ClusterSchemaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterSchemaManager.class);

  private static final ClusterSchemaInfo clusterSchemaInfo = ClusterSchemaInfo.getInstance();

  private final Manager configManager;

  public ClusterSchemaManager(Manager configManager) {
    this.configManager = configManager;
  }

  /**
   * Set StorageGroup
   *
   * @return SUCCESS_STATUS if the StorageGroup is set successfully.
   *         STORAGE_GROUP_ALREADY_EXISTS if the StorageGroup is already set.
   *         PERSISTENCE_FAILURE if fail to set StorageGroup in MTreeAboveSG.
   */
  public TSStatus setStorageGroup(SetStorageGroupReq setStorageGroupReq) {
    TSStatus result;
      if (clusterSchemaInfo.containsStorageGroup(setStorageGroupReq.getSchema().getName())) {
        // Reject if StorageGroup already set
        result = new TSStatus(TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode());
        result.setMessage(
            String.format(
                "StorageGroup %s is already set.", setStorageGroupReq.getSchema().getName()));
      } else {
        // Persist StorageGroupSchema
        result = getConsensusManager().write(setStorageGroupReq).getStatus();
      }
    return result;
  }

  /**
   * Get the SchemaRegionGroupIds or DataRegionGroupIds from the specific StorageGroup
   *
   * @param storageGroup StorageGroupName
   * @param type SchemaRegion or DataRegion
   * @return All SchemaRegionGroupIds when type is SchemaRegion, and all DataRegionGroupIds when
   *     type is DataRegion
   */
  public List<TConsensusGroupId> getRegionGroupIds(String storageGroup, TConsensusGroupType type) {
    return clusterSchemaInfo.getRegionGroupIds(storageGroup, type);
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

  /**
   * Count StorageGroups by specific path pattern
   *
   * @return CountStorageGroupResp
   */
  public CountStorageGroupResp countMatchedStorageGroups(
      CountStorageGroupReq countStorageGroupReq) {
    ConsensusReadResponse readResponse = getConsensusManager().read(countStorageGroupReq);
    return (CountStorageGroupResp) readResponse.getDataset();
  }

  /**
   * Get StorageGroupSchemas by specific path pattern
   *
   * @return StorageGroupSchemaDataSet
   */
  public StorageGroupSchemaResp getMatchedStorageGroupSchema(
      GetStorageGroupReq getStorageGroupReq) {
    ConsensusReadResponse readResponse = getConsensusManager().read(getStorageGroupReq);
    return (StorageGroupSchemaResp) readResponse.getDataset();
  }

  public List<String> getStorageGroupNames() {
    return clusterSchemaInfo.getStorageGroupNames();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }
}
