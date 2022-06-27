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
import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorReq;
import org.apache.iotdb.confignode.consensus.request.read.CountStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.read.GetConfigNodeConfigurationReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataNodeInfoReq;
import org.apache.iotdb.confignode.consensus.request.read.GetDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetOrCreateDataPartitionReq;
import org.apache.iotdb.confignode.consensus.request.read.GetRegionInfoListReq;
import org.apache.iotdb.confignode.consensus.request.read.GetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.RegisterDataNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.RemoveConfigNodeReq;
import org.apache.iotdb.confignode.consensus.request.write.SetDataReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetSchemaReplicationFactorReq;
import org.apache.iotdb.confignode.consensus.request.write.SetStorageGroupReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTTLReq;
import org.apache.iotdb.confignode.consensus.request.write.SetTimePartitionIntervalReq;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaNodeManagementResp;
import org.apache.iotdb.confignode.rpc.thrift.TSchemaPartitionResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;

import java.util.List;

/**
 * a subset of services provided by {@ConfigManager}. For use internally only, passed to Managers,
 * services.
 */
public interface IManager {

  /**
   * if a service stop
   *
   * @return true if service stopped
   */
  boolean isStopped();

  /**
   * Get DataManager
   *
   * @return DataNodeManager instance
   */
  NodeManager getNodeManager();

  /**
   * Get ConsensusManager
   *
   * @return ConsensusManager instance
   */
  ConsensusManager getConsensusManager();

  /**
   * Get ClusterSchemaManager
   *
   * @return ClusterSchemaManager instance
   */
  ClusterSchemaManager getClusterSchemaManager();

  /**
   * Get PartitionManager
   *
   * @return PartitionManager instance
   */
  PartitionManager getPartitionManager();

  /**
   * Get LoadManager
   *
   * @return LoadManager instance
   */
  LoadManager getLoadManager();

  /**
   * Get UDFManager
   *
   * @return UDFManager instance
   */
  UDFManager getUDFManager();

  /**
   * Register DataNode
   *
   * @return DataNodeConfigurationDataSet
   */
  DataSet registerDataNode(RegisterDataNodeReq registerDataNodeReq);

  /**
   * Get DataNode info
   *
   * @return DataNodesInfoDataSet
   */
  DataSet getDataNodeInfo(GetDataNodeInfoReq getDataNodeInfoReq);

  TSStatus setTTL(SetTTLReq configRequest);

  TSStatus setSchemaReplicationFactor(SetSchemaReplicationFactorReq configRequest);

  TSStatus setDataReplicationFactor(SetDataReplicationFactorReq configRequest);

  TSStatus setTimePartitionInterval(SetTimePartitionIntervalReq configRequest);

  /**
   * Count StorageGroups
   *
   * @return The number of matched StorageGroups
   */
  DataSet countMatchedStorageGroups(CountStorageGroupReq countStorageGroupReq);

  /**
   * Get StorageGroupSchemas
   *
   * @return StorageGroupSchemaDataSet
   */
  DataSet getMatchedStorageGroupSchemas(GetStorageGroupReq getOrCountStorageGroupReq);

  /**
   * Set StorageGroup
   *
   * @return status
   */
  TSStatus setStorageGroup(SetStorageGroupReq setStorageGroupReq);

  /**
   * Delete StorageGroups
   *
   * @param deletedPaths List<StringPattern>
   * @return status
   */
  TSStatus deleteStorageGroups(List<String> deletedPaths);

  /**
   * Get SchemaPartition
   *
   * @return TSchemaPartitionResp
   */
  TSchemaPartitionResp getSchemaPartition(PathPatternTree patternTree);

  /**
   * Get or create SchemaPartition
   *
   * @return TSchemaPartitionResp
   */
  TSchemaPartitionResp getOrCreateSchemaPartition(PathPatternTree patternTree);

  /**
   * create SchemaNodeManagementPartition for child paths node management
   *
   * @return TSchemaNodeManagementResp
   */
  TSchemaNodeManagementResp getNodePathsPartition(PartialPath partialPath, Integer level);

  /**
   * Get DataPartition
   *
   * @return TDataPartitionResp
   */
  TDataPartitionResp getDataPartition(GetDataPartitionReq getDataPartitionReq);

  /**
   * Get or create DataPartition
   *
   * @return TDataPartitionResp
   */
  TDataPartitionResp getOrCreateDataPartition(
      GetOrCreateDataPartitionReq getOrCreateDataPartitionReq);

  /**
   * Operate Permission
   *
   * @return status
   */
  TSStatus operatePermission(AuthorReq authorReq);

  /**
   * Query Permission
   *
   * @return PermissionInfoDataSet
   */
  DataSet queryPermission(AuthorReq authorReq);

  /** login */
  TPermissionInfoResp login(String username, String password);

  /** Check User Privileges */
  TPermissionInfoResp checkUserPrivileges(String username, List<String> paths, int permission);

  /**
   * Get ConfigNode Configuration
   *
   * @return TConfigNodeConfigrationResp
   */
  DataSet getConfigNodeConfiguration(GetConfigNodeConfigurationReq req);

  /**
   * Register ConfigNode when it is first startup
   *
   * @return TConfigNodeRegisterResp
   */
  TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req);

  /**
   * Add Consensus Group in new node.
   *
   * @return status
   */
  TSStatus addConsensusGroup(List<TConfigNodeLocation> configNodeLocations);

  /**
   * Remove ConfigNode
   *
   * @return status
   */
  TSStatus removeConfigNode(RemoveConfigNodeReq removeConfigNodeReq);

  TSStatus createFunction(String udfName, String className, List<String> uris);

  TSStatus dropFunction(String udfName);

  TSStatus flush(TFlushReq req);

  void addMetrics();

  /** Show (data/schema) regions */
  DataSet showRegion(GetRegionInfoListReq getRegionsinfoReq);
}
