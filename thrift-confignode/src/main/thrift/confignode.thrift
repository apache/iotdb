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

include "common.thrift"
namespace java org.apache.iotdb.confignode.rpc.thrift
namespace py iotdb.thrift.confignode

// DataNode
struct TDataNodeRegisterReq {
  1: required common.TDataNodeConfiguration dataNodeConfiguration
  // Map<StorageGroupName, TStorageGroupSchema>
  // DataNode can use statusMap to report its status to the ConfigNode when restart
  2: optional map<string, TStorageGroupSchema> statusMap
}

struct TDataNodeRegisterResp {
  1: required common.TSStatus status
  2: required list<common.TConfigNodeLocation> configNodeList
  3: optional i32 dataNodeId
  4: optional TGlobalConfig globalConfig
  5: optional binary templateInfo
}

struct TGlobalConfig {
  1: required string dataRegionConsensusProtocolClass
  2: required string schemaRegionConsensusProtocolClass
  3: required i32 seriesPartitionSlotNum
  4: required string seriesPartitionExecutorClass
  5: required i64 timePartitionInterval
  6: required string readConsistencyLevel
}

struct TDataNodeRemoveReq {
  1: required list<common.TDataNodeLocation> dataNodeLocations
}

struct TDataNodeRemoveResp {
  1: required common.TSStatus status
  2: optional map<common.TDataNodeLocation, common.TSStatus> nodeToStatus
}

struct TRegionMigrateResultReportReq {
  1: required common.TConsensusGroupId regionId
  2: required common.TSStatus migrateResult
  3: optional map<common.TDataNodeLocation, common.TRegionMigrateFailedType> failedNodeAndReason
}

struct TDataNodeConfigurationResp {
  1: required common.TSStatus status
  // map<DataNodeId, DataNodeConfiguration>
  2: optional map<i32, common.TDataNodeConfiguration> dataNodeConfigurationMap
}

// StorageGroup
struct TSetStorageGroupReq {
  1: required TStorageGroupSchema storageGroup
}

struct TDeleteStorageGroupReq {
  1: required string prefixPath
}

struct TDeleteStorageGroupsReq {
  1: required list<string> prefixPathList
}

struct TSetSchemaReplicationFactorReq {
  1: required string storageGroup
  2: required i32 schemaReplicationFactor
}

struct TSetDataReplicationFactorReq {
  1: required string storageGroup
  2: required i32 dataReplicationFactor
}

struct TSetTimePartitionIntervalReq {
  1: required string storageGroup
  2: required i64 timePartitionInterval
}

struct TCountStorageGroupResp {
  1: required common.TSStatus status
  2: optional i32 count
}

struct TStorageGroupSchemaResp {
  1: required common.TSStatus status
  // map<string, StorageGroupMessage>
  2: optional map<string, TStorageGroupSchema> storageGroupSchemaMap
}

struct TStorageGroupSchema {
  1: required string name
  2: optional i64 TTL
  3: optional i32 schemaReplicationFactor
  4: optional i32 dataReplicationFactor
  5: optional i64 timePartitionInterval
  6: optional i32 maxSchemaRegionGroupCount
  7: optional i32 maxDataRegionGroupCount
}

// Schema
struct TSchemaPartitionReq {
  1: required binary pathPatternTree
}

// TODO: Replace this by TSchemaPartitionTableResp
struct TSchemaPartitionResp {
  1: required common.TSStatus status
  // map<StorageGroupName, map<TSeriesPartitionSlot, TRegionReplicaSet>>
  2: optional map<string, map<common.TSeriesPartitionSlot, common.TRegionReplicaSet>> schemaRegionMap
}

struct TSchemaPartitionTableResp {
  1: required common.TSStatus status
  // map<StorageGroupName, map<TSeriesPartitionSlot, TConsensusGroupId>>
  2: optional map<string, map<common.TSeriesPartitionSlot, common.TConsensusGroupId>> schemaPartitionTable
}

// Node Management
struct TSchemaNodeManagementReq {
  1: required binary pathPatternTree
  2: optional i32 level
}

struct TSchemaNodeManagementResp {
  1: required common.TSStatus status
  // map<StorageGroupName, map<TSeriesPartitionSlot, TRegionReplicaSet>>
  2: optional map<string, map<common.TSeriesPartitionSlot, common.TRegionReplicaSet>> schemaRegionMap
  3: optional set<common.TSchemaNode> matchedNode
}

// Data
struct TDataPartitionReq {
  // map<StorageGroupName, map<TSeriesPartitionSlot, list<TTimePartitionSlot>>>
  1: required map<string, map<common.TSeriesPartitionSlot, list<common.TTimePartitionSlot>>> partitionSlotsMap
}

// TODO: Replace this by TDataPartitionTableResp
struct TDataPartitionResp {
  1: required common.TSStatus status
  // map<StorageGroupName, map<TSeriesPartitionSlot, map<TTimePartitionSlot, list<TRegionReplicaSet>>>>
  2: optional map<string, map<common.TSeriesPartitionSlot, map<common.TTimePartitionSlot, list<common.TRegionReplicaSet>>>> dataPartitionMap
}

struct TDataPartitionTableResp {
  1: required common.TSStatus status
  // map<StorageGroupName, map<TSeriesPartitionSlot, map<TTimePartitionSlot, list<TConsensusGroupId>>>>
  2: optional map<string, map<common.TSeriesPartitionSlot, map<common.TTimePartitionSlot, list<common.TConsensusGroupId>>>> dataPartitionTable
}

// Authorize
struct TAuthorizerReq {
  1: required i32 authorType
  2: required string userName
  3: required string roleName
  4: required string password
  5: required string newPassword
  6: required set<i32> permissions
  7: required list<string> nodeNameList
}

struct TAuthorizerResp {
  1: required common.TSStatus status
  2: optional map<string, list<string>> authorizerInfo
}

struct TUserResp {
  1: required string username
  2: required string password
  3: required list<string> privilegeList
  4: required list<string> roleList
}

struct TRoleResp {
  1: required string roleName
  2: required list<string> privilegeList
}

struct TPermissionInfoResp {
  1: required common.TSStatus status
  2: optional TUserResp userInfo
  3: optional map<string, TRoleResp> roleInfo
}

struct TLoginReq {
  1: required string userrname
  2: required string password
}

struct TCheckUserPrivilegesReq {
  1: required string username;
  2: required list<string> paths
  3: required i32 permission
}

// ConfigNode
struct TConfigNodeRegisterReq {
  1: required common.TConfigNodeLocation configNodeLocation
  // The Non-Seed-ConfigNode must ensure that the following
  // fields are consistent with the Seed-ConfigNode
  2: required string dataRegionConsensusProtocolClass
  3: required string schemaRegionConsensusProtocolClass
  4: required i32 seriesPartitionSlotNum
  5: required string seriesPartitionExecutorClass
  6: required i64 defaultTTL
  7: required i64 timePartitionInterval
  8: required i32 schemaReplicationFactor
  9: required double schemaRegionPerDataNode
  10: required i32 dataReplicationFactor
  11: required double dataRegionPerProcessor
  12: required string readConsistencyLevel
}

struct TAddConsensusGroupReq {
  1: required list<common.TConfigNodeLocation> configNodeList
}

// UDF
struct TCreateFunctionReq {
  1: required string udfName
  2: required string className
  3: required list<string> uris
}

struct TDropFunctionReq {
  1: required string udfName
}

// Show cluster
struct TShowClusterResp {
  1: required common.TSStatus status
  2: required list<common.TConfigNodeLocation> configNodeList
  3: required list<common.TDataNodeLocation> dataNodeList
  4: required map<i32, string> nodeStatus
}

// Show datanodes
struct TDataNodeInfo {
  1: required i32 dataNodeId
  2: required string status
  3: required string rpcAddresss
  4: required i32 rpcPort
  5: required i32 dataRegionNum
  6: required i32 schemaRegionNum
}

struct TShowDataNodesResp {
  1: required common.TSStatus status
  2: optional list<TDataNodeInfo> dataNodesInfoList
}

// Show confignodes
struct TConfigNodeInfo {
  1: required i32 configNodeId
  2: required string status
  3: required string internalAddress
  4: required i32 internalPort
}

struct TShowConfigNodesResp {
  1: required common.TSStatus status
  2: optional list<TConfigNodeInfo> configNodesInfoList
}

// Show regions
struct TShowRegionReq {
  1: optional common.TConsensusGroupType consensusGroupType;
  2: optional list<string> storageGroups
}

struct TRegionInfo {
  1: required common.TConsensusGroupId consensusGroupId
  2: required string storageGroup
  3: required i32 dataNodeId
  4: required string clientRpcIp
  5: required i32 clientRpcPort
  6: required i64 seriesSlots
  7: required i64 timeSlots
  8: optional string status
  9: optional string roleType
}

struct TShowRegionResp {
  1: required common.TSStatus status
  2: optional list<TRegionInfo> regionInfoList;
}

// Routing
struct TRegionRouteMapResp {
  1: required common.TSStatus status
  // For version stamp
  2: optional i64 timestamp
  // The routing policy of read/write requests for each RegionGroup is based on the order in the TRegionReplicaSet.
  // The replica with higher sorting result in TRegionReplicaSet will have higher priority.
  3: optional map<common.TConsensusGroupId, common.TRegionReplicaSet> regionRouteMap
}

// Template
struct TCreateSchemaTemplateReq {
  1: required string name
  2: required binary serializedTemplate
}

struct TGetAllTemplatesResp {
  1: required common.TSStatus status
  2: optional list<binary> templateList
}

struct TGetTemplateResp {
  1: required common.TSStatus status
  2: optional binary template
}

struct TSetSchemaTemplateReq {
  1: required string name
  2: required string path
}
struct TGetPathsSetTemplatesResp {
  1: required common.TSStatus status
  2: optional list<string> pathList
}

service IConfigNodeRPCService {

  // ======================================================
  // DataNode
  // ======================================================

  /**
   * Register a new DataNode into the cluster
   *
   * @return SUCCESS_STATUS if the new DataNode registered successfully
   *         DATANODE_ALREADY_REGISTERED if the DataNode already registered
   */
  TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req)

  /**
   * Generate a set of DataNodeRemoveProcedure to remove some specific DataNodes from the cluster
   *
   * @return SUCCESS_STATUS if the DataNodeRemoveProcedure submitted successfully
   *         LACK_REPLICATION if the number of DataNodes will be too small to maintain
   *                          RegionReplicas after remove these DataNodes
   *         DATANODE_NOT_EXIST if one of the DataNodes in the TDataNodeRemoveReq doesn't exist in the cluster
   *         NODE_DELETE_FAILED_ERROR if failed to submit the DataNodeRemoveProcedure
   */
  TDataNodeRemoveResp removeDataNode(TDataNodeRemoveReq req)

  /**
   * Get one or more DataNodes' configuration
   *
   * @param dataNodeId, the specific DataNode's index
   * @return The specific DataNode's configuration if the DataNode exists,
   *         or all DataNodes' configuration if dataNodeId is -1
   */
  TDataNodeConfigurationResp getDataNodeConfiguration(i32 dataNodeId)

  /** Report region migration complete */
  common.TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req)

  // ======================================================
  // StorageGroup
  // ======================================================

  /**
   * Set a new StorageGroup, all fields in TStorageGroupSchema can be customized
   * while the undefined fields will automatically use default values
   *
   * @return SUCCESS_STATUS if the new StorageGroup set successfully
   *         PATH_ILLEGAL if the new StorageGroup's name is illegal
   *         STORAGE_GROUP_ALREADY_EXISTS if the StorageGroup already exist
   */
  common.TSStatus setStorageGroup(TSetStorageGroupReq req)

  /**
   * Generate a DeleteStorageGroupProcedure to delete a specific StorageGroup
   *
   * @return SUCCESS_STATUS if the DeleteStorageGroupProcedure submitted successfully
   *         TIMESERIES_NOT_EXIST if the specific StorageGroup doesn't exist
   *         EXECUTE_STATEMENT_ERROR if failed to submit the DeleteStorageGroupProcedure
   */
  common.TSStatus deleteStorageGroup(TDeleteStorageGroupReq req)

  /**
   * Generate a set of DeleteStorageGroupProcedure to delete some specific StorageGroups
   *
   * @return SUCCESS_STATUS if the DeleteStorageGroupProcedure submitted successfully
   *         TIMESERIES_NOT_EXIST if the specific StorageGroup doesn't exist
   *         EXECUTE_STATEMENT_ERROR if failed to submit the DeleteStorageGroupProcedure
   */
  common.TSStatus deleteStorageGroups(TDeleteStorageGroupsReq req)

  /** Update the specific StorageGroup's TTL */
  common.TSStatus setTTL(common.TSetTTLReq req)

  /** Update the specific StorageGroup's SchemaReplicationFactor */
  common.TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req)

  /** Update the specific StorageGroup's DataReplicationFactor */
  common.TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req)

  /** Update the specific StorageGroup's PartitionInterval */
  common.TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req)

  /** Count the matched StorageGroups */
  TCountStorageGroupResp countMatchedStorageGroups(list<string> storageGroupPathPattern)

  /** Get the matched StorageGroups' TStorageGroupSchema */
  TStorageGroupSchemaResp getMatchedStorageGroupSchemas(list<string> storageGroupPathPattern)

  // ======================================================
  // SchemaPartition
  // ======================================================

  // TODO: Replace this by getSchemaPartitionTable
  TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req)

  /**
   * Get SchemaPartitionTable by specific PathPatternTree,
   * the returned SchemaPartitionTable will not contain the unallocated SeriesPartitionSlots
   * See https://apache-iotdb.feishu.cn/docs/doccnqe3PLPEKwsCX1xadXQ2JOg for detailed matching rules
   */
  TSchemaPartitionTableResp getSchemaPartitionTable(TSchemaPartitionReq req)

  // TODO: Replace this by getOrCreateSchemaPartitionTable
  TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)

  /**
   * Get or create SchemaPartitionTable by specific PathPatternTree,
   * the returned SchemaPartitionTable always contains all the SeriesPartitionSlots
   * since the unallocated SeriesPartitionSlots will be allocated by the way
   *
   * @return SUCCESS_STATUS if the SchemaPartitionTable got or created successfully
   *         NOT_ENOUGH_DATA_NODE if the number of cluster DataNodes is not enough for creating new SchemaRegions
   *         STORAGE_GROUP_NOT_EXIST if some StorageGroups don't exist
   */
  TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req)

  // ======================================================
  // Node Management TODO: @MarcosZyk add interface annotation
  // ======================================================

  TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req)

  // ======================================================
  // DataPartition
  // ======================================================

  // TODO: Replace this by getDataPartitionTable
  TDataPartitionResp getDataPartition(TDataPartitionReq req)

  /**
   * Get DataPartitionTable by specific PartitionSlotsMap,
   * the returned DataPartitionTable will not contain the unallocated SeriesPartitionSlots and TimePartitionSlots
   */
  TDataPartitionTableResp getDataPartitionTable(TDataPartitionReq req)

  // TODO: Replace this by getOrCreateDataPartitionTable
  TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req)

  /**
   * Get or create DataPartitionTable by specific PartitionSlotsMap,
   * the returned SchemaPartitionTable always contains all the SeriesPartitionSlots and TimePartitionSlots
   * since the unallocated SeriesPartitionSlots and TimePartitionSlots will be allocated by the way
   *
   * @return SUCCESS_STATUS if the DataPartitionTable got or created successfully
   *         NOT_ENOUGH_DATA_NODE if the number of cluster DataNodes is not enough for creating new DataRegions
   *         STORAGE_GROUP_NOT_EXIST if some StorageGroups don't exist
   */
  TDataPartitionTableResp getOrCreateDataPartitionTable(TDataPartitionReq req)

  // ======================================================
  // Authorize
  // ======================================================

  /**
   * Execute permission write operations such as create user, create role, and grant permission.
   * There is no need to update the cache information of the DataNode for creating users and roles
   *
   * @return SUCCESS_STATUS if the permission write operation is executed successfully
   *         INVALIDATE_PERMISSION_CACHE_ERROR if the update cache of the permission information in the datanode fails
   *         EXECUTE_PERMISSION_EXCEPTION_ERROR if the permission write operation fails, like the user doesn't exist
   *         INTERNAL_SERVER_ERROR if the permission type does not exist
   */
  common.TSStatus operatePermission(TAuthorizerReq req)

  /**
   * Execute permission read operations such as list user
   *
   * @return SUCCESS_STATUS if the permission read operation is executed successfully
   *         ROLE_NOT_EXIST_ERROR if the role does not exist
   *         USER_NOT_EXIST_ERROR if the user does not exist
   *         INTERNAL_SERVER_ERROR if the permission type does not exist
   */
  TAuthorizerResp queryPermission(TAuthorizerReq req)

  /**
   * Authenticate user login
   *
   * @return SUCCESS_STATUS if the user exists and the correct username and password are entered
   *         WRONG_LOGIN_PASSWORD_ERROR if the user enters the wrong username or password
   */
  TPermissionInfoResp login(TLoginReq req)

  /**
   * Permission checking for user operations
   *
   * @return SUCCESS_STATUS if the user has the permission
   *         EXECUTE_PERMISSION_EXCEPTION_ERROR if the seriesPath or the privilege is illegal.
   *         NO_PERMISSION_ERROR if the user does not have this permission
   */
  TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req)

  // ======================================================
  // ConfigNode
  // ======================================================

  /**
   * The Non-Seed-ConfigNode submit a registration request to the ConfigNode-leader when first startup
   *
   * @return SUCCESS_STATUS if the AddConfigNodeProcedure submitted successfully
   *         ERROR_GLOBAL_CONFIG if some global configurations in the Non-Seed-ConfigNode
   *                             are inconsist with the ConfigNode-leader
   */
  common.TSStatus registerConfigNode(TConfigNodeRegisterReq req)

  /** The ConfigNode-leader will guide the Non-Seed-ConfigNode to join the ConsensusGroup when first startup */
  common.TSStatus addConsensusGroup(TAddConsensusGroupReq req)

  /** The ConfigNode-leader will notify the Non-Seed-ConfigNode that the registration success */
  common.TSStatus notifyRegisterSuccess()

  /**
   * Remove the specific ConfigNode from the cluster
   *
   * @return SUCCESS_STATUS if the RemoveConfigNodeProcedure submitted successfully
   *         REMOVE_CONFIGNODE_FAILED if the number of ConfigNode is less than 1
   *                                  or the specific ConfigNode doesn't exist
   *                                  or the specific ConfigNode is leader
   */
  common.TSStatus removeConfigNode(common.TConfigNodeLocation configNodeLocation)

  /**
   * Let the specific ConfigNode remove the ConsensusGroup
   *
   * @return SUCCESS_STATUS if remove ConsensusGroup successfully
   *         REMOVE_CONFIGNODE_FAILED if the specific ConfigNode doesn't exist in the current cluster
   *                                  or Ratis internal failure
   */
  common.TSStatus removeConsensusGroup(common.TConfigNodeLocation configNodeLocation)

  /** Stop the specific ConfigNode */
  common.TSStatus stopConfigNode(common.TConfigNodeLocation configNodeLocation)

  /** The ConfigNode-leader will ping other ConfigNodes periodically */
  i64 getConfigNodeHeartBeat(i64 timestamp)

  // ======================================================
  // UDF
  // ======================================================

  /**
     * Create a function on all online ConfigNodes and DataNodes
     *
     * @return SUCCESS_STATUS if the function was created successfully
     *         EXECUTE_STATEMENT_ERROR if operations on any node failed
     */
  common.TSStatus createFunction(TCreateFunctionReq req)

  /**
     * Remove a function on all online ConfigNodes and DataNodes
     *
     * @return SUCCESS_STATUS if the function was removed successfully
     *         EXECUTE_STATEMENT_ERROR if operations on any node failed
     */
  common.TSStatus dropFunction(TDropFunctionReq req)

  // ======================================================
  // Maintenance Tools
  // ======================================================

  /** Execute Level Compaction and unsequence Compaction task on all DataNodes */
  common.TSStatus merge()

  /** Persist all the data points in the memory table of the storage group to the disk, and seal the data file on all DataNodes */
  common.TSStatus flush(common.TFlushReq req)

  /** Clear the cache of chunk, chunk metadata and timeseries metadata to release the memory footprint on all DataNodes */
  common.TSStatus clearCache()

  /** Load configuration on all DataNodes */
  common.TSStatus loadConfiguration()

  // ======================================================
  // Cluster Tools
  // ======================================================

  /** Show cluster ConfigNodes' and DataNodes' information */
  TShowClusterResp showCluster()

  /** Show cluster DataNodes' information */
  TShowDataNodesResp showDataNodes()

  /** Show cluster ConfigNodes' information */
  TShowConfigNodesResp showConfigNodes()

  /**
   * Show the matched cluster Regions' information
   * See https://apache-iotdb.feishu.cn/docx/doxcnOzmIlaE2MX5tKjmYWuMSRg for detailed matching rules
   */
  TShowRegionResp showRegion(TShowRegionReq req)

  // ======================================================
  // Routing
  // ======================================================

  /** The ConfigNode-leader will generate and return a latest RegionRouteMap */
  TRegionRouteMapResp getLatestRegionRouteMap()

  // ======================================================
  // Template TODO: @MarcosZyk add interface annotation
  // ======================================================

  common.TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req)

  TGetAllTemplatesResp getAllTemplates()

  TGetTemplateResp getTemplate(string req)

  common.TSStatus setSchemaTemplate(TSetSchemaTemplateReq req)

  TGetPathsSetTemplatesResp getPathsSetTemplate(string req)

}

