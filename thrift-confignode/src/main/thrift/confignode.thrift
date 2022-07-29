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

struct TDataNodeRemoveReq {
  1: required list<common.TDataNodeLocation> dataNodeLocations
}

struct TRegionMigrateResultReportReq {
  1: required common.TConsensusGroupId regionId
  2: required common.TSStatus migrateResult
  3: optional map<common.TDataNodeLocation, common.TRegionMigrateFailedType> failedNodeAndReason
}

struct TGlobalConfig {
  1: required string dataRegionConsensusProtocolClass
  2: required string schemaRegionConsensusProtocolClass
  3: required i32 seriesPartitionSlotNum
  4: required string seriesPartitionExecutorClass
  5: required i64 timePartitionInterval
  6: required string readConsistencyLevel
}

struct TDataNodeRegisterResp {
  1: required common.TSStatus status
  2: required list<common.TConfigNodeLocation> configNodeList
  3: optional i32 dataNodeId
  4: optional TGlobalConfig globalConfig
  5: optional binary templateInfo
}

struct TDataNodeRemoveResp {
  1: required common.TSStatus status
  2: optional map<common.TDataNodeLocation, common.TSStatus> nodeToStatus
}
struct TDataNodeConfigurationResp {
  1: required common.TSStatus status
  // map<DataNodeId, DataNodeLocation>
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
  7: required string nodeName
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

struct TConfigNodeRegisterResp {
  1: required common.TSStatus status
  2: optional common.TConsensusGroupId partitionRegionId
  3: optional list<common.TConfigNodeLocation> configNodeList
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

  /* DataNode */

  TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req)

  TDataNodeRemoveResp removeDataNode(TDataNodeRemoveReq req)

  TDataNodeConfigurationResp getDataNodeConfiguration(i32 dataNodeId)

  common.TSStatus reportRegionMigrateResult(TRegionMigrateResultReportReq req)

  /* StorageGroup */

  common.TSStatus setStorageGroup(TSetStorageGroupReq req)

  common.TSStatus deleteStorageGroup(TDeleteStorageGroupReq req)

  common.TSStatus deleteStorageGroups(TDeleteStorageGroupsReq req)

  common.TSStatus setTTL(common.TSetTTLReq req)

  common.TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req)

  common.TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req)

  common.TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req)

  TCountStorageGroupResp countMatchedStorageGroups(list<string> storageGroupPathPattern)

  TStorageGroupSchemaResp getMatchedStorageGroupSchemas(list<string> storageGroupPathPattern)

  /* Schema */

  // TODO: Replace this by getSchemaPartitionTable
  TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req)

  TSchemaPartitionTableResp getSchemaPartitionTable(TSchemaPartitionReq req)

  // TODO: Replace this by getOrCreateSchemaPartitionTable
  TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)

  TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req)

  /* Node Management */

  TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req)

  /* Data */

  // TODO: Replace this by getDataPartitionTable
  TDataPartitionResp getDataPartition(TDataPartitionReq req)

  TDataPartitionTableResp getDataPartitionTable(TDataPartitionReq req)

  // TODO: Replace this by getOrCreateDataPartitionTable
  TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req)

  TDataPartitionTableResp getOrCreateDataPartitionTable(TDataPartitionReq req)

  /* Authorize */

  common.TSStatus operatePermission(TAuthorizerReq req)

  TAuthorizerResp queryPermission(TAuthorizerReq req)

  TPermissionInfoResp login(TLoginReq req)

  TPermissionInfoResp checkUserPrivileges(TCheckUserPrivilegesReq req)

  /* ConfigNode */

  TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req)

  common.TSStatus addConsensusGroup(TConfigNodeRegisterResp req)

  common.TSStatus notifyRegisterSuccess()

  common.TSStatus removeConfigNode(common.TConfigNodeLocation configNodeLocation)

  common.TSStatus removeConsensusGroup(common.TConfigNodeLocation configNodeLocation)

  common.TSStatus stopConfigNode(common.TConfigNodeLocation configNodeLocation)

  /* UDF */

  common.TSStatus createFunction(TCreateFunctionReq req)

  common.TSStatus dropFunction(TDropFunctionReq req)

  /* Flush */

  common.TSStatus flush(common.TFlushReq req)

  /* ClearCache */

  common.TSStatus clearCache(common.TClearCacheReq req)

  /* Cluster Tools */

  TShowClusterResp showCluster()

  TShowDataNodesResp showDataNodes()

  TShowConfigNodesResp showConfigNodes()

  TShowRegionResp showRegion(TShowRegionReq req)

  /* Routing */

  TRegionRouteMapResp getLatestRegionRouteMap()

  /* Get confignode heartbeat */

  i64 getConfigNodeHeartBeat(i64 timestamp)

  /* Template */

  common.TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req)

  TGetAllTemplatesResp getAllTemplates()

  TGetTemplateResp getTemplate(string req)

  common.TSStatus setSchemaTemplate(TSetSchemaTemplateReq req)

  TGetPathsSetTemplatesResp getPathsSetTemplate(string req)

}

