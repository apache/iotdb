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
struct TSystemConfigurationResp {
  1: required common.TSStatus status
  2: optional TGlobalConfig globalConfig
  3: optional TRatisConfig ratisConfig
  4: optional TCQConfig cqConfig
}

struct TGlobalConfig {
  1: required string dataRegionConsensusProtocolClass
  2: required string schemaRegionConsensusProtocolClass
  3: required i32 seriesPartitionSlotNum
  4: required string seriesPartitionExecutorClass
  5: required i64 timePartitionInterval
  6: required string readConsistencyLevel
  7: required double diskSpaceWarningThreshold
}

struct TRatisConfig {
  1: required i64 schemaAppenderBufferSize
  2: required i64 dataAppenderBufferSize

  3: required i64 schemaSnapshotTriggerThreshold
  4: required i64 dataSnapshotTriggerThreshold

  5: required bool schemaLogUnsafeFlushEnable
  6: required bool dataLogUnsafeFlushEnable

  7: required i64 schemaLogSegmentSizeMax
  8: required i64 dataLogSegmentSizeMax

  9: required i64 schemaGrpcFlowControlWindow
  10: required i64 dataGrpcFlowControlWindow

  11: required i64 schemaLeaderElectionTimeoutMin
  12: required i64 dataLeaderElectionTimeoutMin

  13: required i64 schemaLeaderElectionTimeoutMax
  14: required i64 dataLeaderElectionTimeoutMax

  15: required i64 schemaRequestTimeout
  16: required i64 dataRequestTimeout

  17: required i32 schemaMaxRetryAttempts
  18: required i32 dataMaxRetryAttempts
  19: required i64 schemaInitialSleepTime
  20: required i64 dataInitialSleepTime
  21: required i64 schemaMaxSleepTime
  22: required i64 dataMaxSleepTime

  23: required i64 schemaPreserveWhenPurge
  24: required i64 dataPreserveWhenPurge

  25: required i64 firstElectionTimeoutMin
  26: required i64 firstElectionTimeoutMax

  27: required i64 schemaRegionRatisLogMax
  28: required i64 dataRegionRatisLogMax
}

struct TCQConfig {
  1: required i64 cqMinEveryIntervalInMs
}

struct TRuntimeConfiguration {
  1: required binary templateInfo
  2: required list<binary> allTriggerInformation
  3: required list<binary> allUDFInformation
  4: required binary allTTLInformation
}

struct TDataNodeRegisterReq {
  1: required string clusterName
  2: required common.TDataNodeConfiguration dataNodeConfiguration
}

struct TDataNodeRegisterResp {
  1: required common.TSStatus status
  2: required list<common.TConfigNodeLocation> configNodeList
  3: optional i32 dataNodeId
  4: optional TRuntimeConfiguration runtimeConfiguration
}

struct TDataNodeRestartReq {
  1: required string clusterName
  2: required common.TDataNodeConfiguration dataNodeConfiguration
}

struct TDataNodeRestartResp {
  1: required common.TSStatus status
  2: required list<common.TConfigNodeLocation> configNodeList
  3: optional TRuntimeConfiguration runtimeConfiguration
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

struct TSetDataNodeStatusReq {
  1: required common.TDataNodeLocation targetDataNode
  2: required string status
}

// Database
struct TDeleteDatabaseReq {
  1: required string prefixPath
}

struct TDeleteDatabasesReq {
  1: required list<string> prefixPathList
}

struct TSetSchemaReplicationFactorReq {
  1: required string database
  2: required i32 schemaReplicationFactor
}

struct TSetDataReplicationFactorReq {
  1: required string database
  2: required i32 dataReplicationFactor
}

struct TSetTimePartitionIntervalReq {
  1: required string database
  2: required i64 timePartitionInterval
}

struct TCountDatabaseResp {
  1: required common.TSStatus status
  2: optional i32 count
}

struct TDatabaseSchemaResp {
  1: required common.TSStatus status
  // map<string, TDatabaseSchema>
  2: optional map<string, TDatabaseSchema> databaseSchemaMap
}

struct TDatabaseSchema {
  1: required string name
  2: optional i64 TTL
  3: optional i32 schemaReplicationFactor
  4: optional i32 dataReplicationFactor
  5: optional i64 timePartitionInterval
  6: optional i32 minSchemaRegionGroupNum
  7: optional i32 maxSchemaRegionGroupNum
  8: optional i32 minDataRegionGroupNum
  9: optional i32 maxDataRegionGroupNum
}

// Schema
struct TSchemaPartitionReq {
  1: required binary pathPatternTree
}

struct TSchemaPartitionTableResp {
  1: required common.TSStatus status
  // map<DatabaseName, map<TSeriesPartitionSlot, TConsensusGroupId>>
  2: optional map<string, map<common.TSeriesPartitionSlot, common.TConsensusGroupId>> schemaPartitionTable
}

// Node Management
struct TSchemaNodeManagementReq {
  1: required binary pathPatternTree
  2: optional i32 level
}

struct TSchemaNodeManagementResp {
  1: required common.TSStatus status
  // map<DatabaseName, map<TSeriesPartitionSlot, TRegionReplicaSet>>
  2: optional map<string, map<common.TSeriesPartitionSlot, common.TRegionReplicaSet>> schemaRegionMap
  3: optional set<common.TSchemaNode> matchedNode
}

struct TTimeSlotList {
  1: required list<common.TTimePartitionSlot> timePartitionSlots
  2: required bool needLeftAll
  3: required bool needRightAll
}

// Data
struct TDataPartitionReq {
  // map<DatabaseName, map<TSeriesPartitionSlot, TTimePartionSlotList>>
  1: required map<string, map<common.TSeriesPartitionSlot, TTimeSlotList>> partitionSlotsMap
}

struct TDataPartitionTableResp {
  1: required common.TSStatus status
  // map<DatabaseName, map<TSeriesPartitionSlot, map<TTimePartitionSlot, list<TConsensusGroupId>>>>
  2: optional map<string, map<common.TSeriesPartitionSlot, map<common.TTimePartitionSlot, list<common.TConsensusGroupId>>>> dataPartitionTable
}

struct TGetRegionIdReq {
    1: required string database
    2: required common.TConsensusGroupType type
    3: optional common.TSeriesPartitionSlot seriesSlotId
    4: optional string deviceId
    5: optional common.TTimePartitionSlot timeSlotId
    6: optional i64 timeStamp
}

struct TGetRegionIdResp {
    1: required common.TSStatus status
    2: optional list<common.TConsensusGroupId> dataRegionIdList
}

struct TGetTimeSlotListReq {
    1: required string database
    2: required common.TSeriesPartitionSlot seriesSlotId
    3: optional i64 startTime
    4: optional i64 endTime
}

struct TGetTimeSlotListResp {
    1: required common.TSStatus status
    2: optional list<common.TTimePartitionSlot> timeSlotList
}

struct TGetSeriesSlotListReq {
    1: required string database
    2: optional common.TConsensusGroupType type
}

struct TGetSeriesSlotListResp {
    1: required common.TSStatus status
    2: optional list<common.TSeriesPartitionSlot> seriesSlotList
}

struct TMigrateRegionReq {
    1: required i32 regionId
    2: required i32 fromId
    3: required i32 toId
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
  5: required bool isOpenIdUser
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
  1: required string username
  2: required list<string> paths
  3: required i32 permission
}

// ConfigNode

/* These parameters should be consist within the cluster */
struct TClusterParameters {
  1: required string clusterName
  2: required i32 dataReplicationFactor
  3: required i32 schemaReplicationFactor
  4: required string dataRegionConsensusProtocolClass
  5: required string schemaRegionConsensusProtocolClass
  6: required string configNodeConsensusProtocolClass
  7: required i64 timePartitionInterval
  8: required i64 defaultTTL
  9: required string readConsistencyLevel
  10: required double schemaRegionPerDataNode
  11: required double dataRegionPerProcessor
  12: required i32 seriesPartitionSlotNum
  13: required string seriesPartitionExecutorClass
  14: required double diskSpaceWarningThreshold
}

struct TConfigNodeRegisterReq {
  // The Non-Seed-ConfigNode must ensure that the following
  // fields are consistent with the Seed-ConfigNode
  1: required TClusterParameters clusterParameters
  2: required common.TConfigNodeLocation configNodeLocation
}

struct TConfigNodeRegisterResp {
  1: required common.TSStatus status
  2: optional i32 configNodeId
}

struct TConfigNodeRestartReq {
  1: required string clusterName
  2: required common.TConfigNodeLocation configNodeLocation
}

struct TAddConsensusGroupReq {
  1: required list<common.TConfigNodeLocation> configNodeList
}

// UDF
struct TCreateFunctionReq {
  1: required string udfName
  2: required string className
  3: required bool isUsingURI
  4: optional string jarName
  5: optional binary jarFile
  6: optional string jarMD5
}

struct TDropFunctionReq {
  1: required string udfName
}

// Get UDF table from config node
struct TGetUDFTableResp {
  1: required common.TSStatus status
  2: required list<binary> allUDFInformation
}

// Trigger
enum TTriggerState {
  // The intermediate state of Create trigger, the trigger need to create has not yet activated on any DataNodes.
  INACTIVE
  // The successful state of Create trigger, the trigger need to create has activated on some DataNodes.
  ACTIVE
  // The intermediate state of Drop trigger, the cluster is in the process of removing the trigger.
  DROPPING
  // The intermediate state of Transfer trigger, the cluster is in the process of transferring the trigger.
  TRANSFERRING
}

struct TCreateTriggerReq {
  1: required string triggerName
  2: required string className,
  3: required byte triggerEvent,
  4: required byte triggerType
  5: required binary pathPattern,
  6: required map<string, string> attributes,
  7: required i32 failureStrategy
  8: required bool isUsingURI,
  9: optional string jarName,
  10: optional binary jarFile,
  11: optional string jarMD5,
}

struct TDropTriggerReq {
  1: required string triggerName
}

struct TGetLocationForTriggerResp {
  1: required common.TSStatus status
  2: optional common.TDataNodeLocation dataNodeLocation
}

// Get trigger table from config node
struct TGetTriggerTableResp {
  1: required common.TSStatus status
  2: required list<binary> allTriggerInformation
}

// Get jars of the corresponding jarName
struct TGetJarInListReq {
  1: required list<string> jarNameList
}

struct TGetJarInListResp {
  1: required common.TSStatus status
  2: required list<binary> jarList
}

struct TGetDataNodeLocationsResp {
  1: required common.TSStatus status
  2: required list<common.TDataNodeLocation> dataNodeLocationList
}

// Pipe Plugin
struct TCreatePipePluginReq {
  1: required string pluginName
  2: required string className
  3: required string jarName
  4: required binary jarFile
  5: required string jarMD5
}

struct TDropPipePluginReq {
  1: required string pluginName
}

// Get PipePlugin table from config node
struct TGetPipePluginTableResp {
  1: required common.TSStatus status
  2: required list<binary> allPipePluginMeta
}

// Show cluster
struct TShowClusterResp {
  1: required common.TSStatus status
  2: required list<common.TConfigNodeLocation> configNodeList
  3: required list<common.TDataNodeLocation> dataNodeList
  4: required map<i32, string> nodeStatus
}

struct TShowVariablesResp {
  1: required common.TSStatus status
  2: optional TClusterParameters clusterParameters
}

// Show datanodes
struct TDataNodeInfo {
  1: required i32 dataNodeId
  2: required string status
  3: required string rpcAddresss
  4: required i32 rpcPort
  5: required i32 dataRegionNum
  6: required i32 schemaRegionNum
  7: optional i32 cpuCoreNum
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
  5: required string roleType
}

struct TShowConfigNodesResp {
  1: required common.TSStatus status
  2: optional list<TConfigNodeInfo> configNodesInfoList
}

// Show Database
struct TDatabaseInfo {
  1: required string name
  2: required i64 TTL
  3: required i32 schemaReplicationFactor
  4: required i32 dataReplicationFactor
  5: required i64 timePartitionInterval
  6: required i32 schemaRegionNum
  7: required i32 minSchemaRegionNum
  8: required i32 maxSchemaRegionNum
  9: required i32 dataRegionNum
  10: required i32 minDataRegionNum
  11: required i32 maxDataRegionNum
}

struct TShowDatabaseResp {
  1: required common.TSStatus status
  // map<DatabaseName, TDatabaseInfo>
  2: optional map<string, TDatabaseInfo> databaseInfoMap
}

// Show regions
struct TShowRegionReq {
  1: optional common.TConsensusGroupType consensusGroupType;
  2: optional list<string> databases
}

struct TRegionInfo {
  1: required common.TConsensusGroupId consensusGroupId
  2: required string database
  3: required i32 dataNodeId
  4: required string clientRpcIp
  5: required i32 clientRpcPort
  6: required i32 seriesSlots
  7: required i64 timeSlots
  8: optional string status
  9: optional string roleType
  10: optional i64 createTime
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

// SYNC
struct TRecordPipeMessageReq{
  1: required string pipeName
  2: required binary message
}

struct TShowPipeInfo {
  1: required string id
  2: required i64 creationTime
  3: required string state
  4: required string pipeCollector
  5: required string pipeProcessor
  6: required string pipeConnector
  7: required string exceptionMessage
}

struct TGetAllPipeInfoResp{
  1: required common.TSStatus status
  2: optional list<binary> allPipeInfo
}

struct TCreatePipeReq {
    1: required string pipeName
    2: optional map<string, string> collectorAttributes
    3: optional map<string, string> processorAttributes
    4: required map<string, string> connectorAttributes
}

struct TPipeSinkInfo {
  1: required string pipeSinkName
  2: required string pipeSinkType
  3: optional map<string, string> attributes
}

struct TDropPipeSinkReq {
  1: required string pipeSinkName
}

struct TGetPipeSinkReq {
  1: optional string pipeSinkName
}

struct TGetPipeSinkResp {
  1: required common.TSStatus status
  2: required list<TPipeSinkInfo> pipeSinkInfoList
}

struct TShowPipeReq {
  1: optional string pipeName
  2: optional bool whereClause
}

struct TShowPipeResp {
  1: required common.TSStatus status
  2: optional list<TShowPipeInfo> pipeInfoList
}

struct TDeleteTimeSeriesReq{
  1: required string queryId
  2: required binary pathPatternTree
}

// ====================================================
// CQ
// ====================================================
struct TCreateCQReq {
  1: required string cqId,
  2: required i64 everyInterval
  3: required i64 boundaryTime
  4: required i64 startTimeOffset
  5: required i64 endTimeOffset
  6: required byte timeoutPolicy
  7: required string queryBody
  8: required string sql
  9: required string zoneId
  10: required string username
}

struct TDropCQReq {
  1: required string cqId
}

struct TCQEntry {
  1: required string cqId
  2: required string sql
  3: required byte state
}

struct TShowCQResp {
  1: required common.TSStatus status
  2: required list<TCQEntry> cqList
}


struct TDeactivateSchemaTemplateReq{
  1: required string queryId
  2: required binary pathPatternTree
  3: optional string templateName
}

struct TUnsetSchemaTemplateReq{
  1: required string queryId
  2: required string templateName
  3: required string path
}

struct TCreateModelReq {
  1: required string modelId
  2: required common.ModelTask modelTask
  3: required string modelType
  4: required list<string> queryExpressions
  5: optional string queryFilter
  6: required bool isAuto
  7: required map<string, string> modelConfigs
}

struct TDropModelReq {
  1: required string modelId
}

struct TShowModelReq {
  1: optional string modelId
}

struct TShowModelResp {
  1: required common.TSStatus status
  2: required list<binary> modelInfoList
}

struct TShowTrailReq {
  1: required string modelId
  2: optional string trailId
}

struct TShowTrailResp {
  1: required common.TSStatus status
  2: required list<binary> trailInfoList
}

struct TUpdateModelInfoReq {
  1: required string modelId
  2: required string trailId
  3: required map<string, string> modelInfo
}

struct TUpdateModelStateReq {
  1: required string modelId
  2: required common.TrainingState state
  3: optional string bestTrailId
}

service IConfigNodeRPCService {

  // ======================================================
  // DataNode
  // ======================================================

  /**
   * Register a new DataNode into the cluster
   *
   * @return SUCCESS_STATUS if the new DataNode registered successfully
   *         REJECT_NODE_START if the configuration chek of the DataNode to be registered fails,
   *                           and a detailed error message will be returned.
   */
  TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req)

  /**
   * Restart an existed DataNode
   *
   * @return SUCCESS_STATUS if DataNode restart request is accepted
   *         REJECT_NODE_START if the configuration chek of the DataNode to be restarted fails,
   *                           and a detailed error message will be returned.
   */
  TDataNodeRestartResp restartDataNode(TDataNodeRestartReq req)

  /**
   * Get system configurations. i.e. configurations that is not associated with the DataNodeId
   */
  TSystemConfigurationResp getSystemConfiguration()

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
   * Report that the specified DataNode will be shutdown.
   * The ConfigNode-leader will mark it as Unknown.
   *
   * @return SUCCESS_STATUS if reporting successfully
   */
  common.TSStatus reportDataNodeShutdown(common.TDataNodeLocation dataNodeLocation)

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
  // Database
  // ======================================================

  /**
   * Set a new Databse, all fields in TDatabaseSchema can be customized
   * while the undefined fields will automatically use default values
   *
   * @return SUCCESS_STATUS if the new Database set successfully
   *         ILLEGAL_PATH if the new Database name is illegal
   *         DATABASE_CONFIG_ERROR if some of the DatabaseSchema is illeagal
   *         DATABASE_ALREADY_EXISTS if the Database already exist
   */
  common.TSStatus setDatabase(TDatabaseSchema databaseSchema)

  /**
   * Alter a Database's schema, including
   * TTL, ReplicationFactor, timePartitionInterval and RegionGroupNum
   *
   * @return SUCCESS_STATUS if the specified DatabaseSchema is altered successfully
   *         ILLEGAL_PATH if the new Database name is illegal
   *         DATABASE_CONFIG_ERROR if some of the DatabaseSchema is illeagal
   *         DATABASE_NOT_EXIST if the specified Database doesn't exist
   */
  common.TSStatus alterDatabase(TDatabaseSchema databaseSchema)

  /**
   * Generate a DeleteDatabaseProcedure to delete a specified Database
   *
   * @return SUCCESS_STATUS if the DeleteDatabaseProcedure submitted successfully
   *         TIMESERIES_NOT_EXIST if the specific Database doesn't exist
   *         EXECUTE_STATEMENT_ERROR if failed to submit the DeleteDatabaseProcedure
   */
  common.TSStatus deleteDatabase(TDeleteDatabaseReq req)

  /**
   * Generate a set of DeleteDatabaseProcedure to delete some specific Databases
   *
   * @return SUCCESS_STATUS if the DeleteDatabaseProcedure submitted successfully
   *         TIMESERIES_NOT_EXIST if the specific Database doesn't exist
   *         EXECUTE_STATEMENT_ERROR if failed to submit the DeleteDatabaseProcedure
   */
  common.TSStatus deleteDatabases(TDeleteDatabasesReq req)

  /** Update the specific Database's TTL */
  common.TSStatus setTTL(common.TSetTTLReq req)

  /** Update the specific Database's SchemaReplicationFactor */
  common.TSStatus setSchemaReplicationFactor(TSetSchemaReplicationFactorReq req)

  /** Update the specific Database's DataReplicationFactor */
  common.TSStatus setDataReplicationFactor(TSetDataReplicationFactorReq req)

  /** Update the specific Database's PartitionInterval */
  common.TSStatus setTimePartitionInterval(TSetTimePartitionIntervalReq req)

  /** Count the matched Databases */
  TCountDatabaseResp countMatchedDatabases(list<string> DatabasePathPattern)

  /** Get the matched Databases' TDatabaseSchema */
  TDatabaseSchemaResp getMatchedDatabaseSchemas(list<string> DatabasePathPattern)

  // ======================================================
  // SchemaPartition
  // ======================================================

  /**
   * Get SchemaPartitionTable by specific PathPatternTree,
   * the returned SchemaPartitionTable will not contain the unallocated SeriesPartitionSlots
   * See https://apache-iotdb.feishu.cn/docs/doccnqe3PLPEKwsCX1xadXQ2JOg for detailed matching rules
   */
  TSchemaPartitionTableResp getSchemaPartitionTable(TSchemaPartitionReq req)

  /**
   * Get or create SchemaPartitionTable by specific PathPatternTree,
   * the returned SchemaPartitionTable always contains all the SeriesPartitionSlots
   * since the unallocated SeriesPartitionSlots will be allocated by the way
   *
   * @return SUCCESS_STATUS if the SchemaPartitionTable got or created successfully
   *         NOT_ENOUGH_DATA_NODE if the number of cluster DataNodes is not enough for creating new SchemaRegions
   *         STORAGE_GROUP_NOT_EXIST if some Databases don't exist
   */
  TSchemaPartitionTableResp getOrCreateSchemaPartitionTable(TSchemaPartitionReq req)

  // ======================================================
  // Node Management
  // ======================================================

  /**
   * Get the partition info used for schema node query and get the node info in CluterSchemaInfo.
   */
  TSchemaNodeManagementResp getSchemaNodeManagementPartition(TSchemaNodeManagementReq req)

  // ======================================================
  // DataPartition
  // ======================================================

  /**
   * Get DataPartitionTable by specific PartitionSlotsMap,
   * the returned DataPartitionTable will not contain the unallocated SeriesPartitionSlots and TimePartitionSlots
   */
  TDataPartitionTableResp getDataPartitionTable(TDataPartitionReq req)

  /**
   * Get or create DataPartitionTable by specific PartitionSlotsMap,
   * the returned SchemaPartitionTable always contains all the SeriesPartitionSlots and TimePartitionSlots
   * since the unallocated SeriesPartitionSlots and TimePartitionSlots will be allocated by the way
   *
   * @return SUCCESS_STATUS if the DataPartitionTable got or created successfully
   *         NOT_ENOUGH_DATA_NODE if the number of cluster DataNodes is not enough for creating new DataRegions
   *         STORAGE_GROUP_NOT_EXIST if some Databases don't exist
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
   * @return SUCCESS_STATUS if the AddConfigNodeProcedure submitted successfully.
   *         REJECT_NODE_START if the configuration chek of the ConfigNode to be registered fails,
   *                           and a detailed error message will be returned.
   */
  TConfigNodeRegisterResp registerConfigNode(TConfigNodeRegisterReq req)

  /** The ConfigNode-leader will guide the Non-Seed-ConfigNode to join the ConsensusGroup when first startup */
  common.TSStatus addConsensusGroup(TAddConsensusGroupReq req)

  /** The ConfigNode-leader will notify the Non-Seed-ConfigNode that the registration success */
  common.TSStatus notifyRegisterSuccess()

  /**
   * Restart an existed ConfigNode
   *
   * @return SUCCESS_STATUS if ConfigNode restart request is accepted
   *         REJECT_NODE_START if the configuration chek of the ConfigNode to be restarted fails,
   *                           and a detailed error message will be returned.
   */
  common.TSStatus restartConfigNode(TConfigNodeRestartReq req)

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
   * Let the specific ConfigNode delete the peer
   *
   * @return SUCCESS_STATUS if delete peer  successfully
   *         REMOVE_CONFIGNODE_FAILED if the specific ConfigNode doesn't exist in the current cluster
   *                                  or Ratis internal failure
   */
  common.TSStatus deleteConfigNodePeer(common.TConfigNodeLocation configNodeLocation)

  /**
   * Report that the specified ConfigNode will be shutdown.
   * The ConfigNode-leader will mark it as Unknown.
   *
   * @return SUCCESS_STATUS if reporting successfully
   */
  common.TSStatus reportConfigNodeShutdown(common.TConfigNodeLocation configNodeLocation)

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

  /**
   * Return the UDF table
   */
  TGetUDFTableResp getUDFTable()

  /**
   * Return the UDF jar list of the jar name list
   */
  TGetJarInListResp getUDFJar(TGetJarInListReq req)

  // ======================================================
  // Trigger
  // ======================================================

  /**
   * Create a statless trigger on all online DataNodes or Create a stateful trigger on a specific DataNode
   * and sync Information of it to all ConfigNodes
   *
   * @return SUCCESS_STATUS if the trigger was created successfully
   *         EXECUTE_STATEMENT_ERROR if operations on any node failed
   */
  common.TSStatus createTrigger(TCreateTriggerReq req)

  /**
   * Remove a trigger on all online ConfigNodes and DataNodes
   *
   * @return SUCCESS_STATUS if the function was removed successfully
   *         EXECUTE_STATEMENT_ERROR if operations on any node failed
   */
  common.TSStatus dropTrigger(TDropTriggerReq req)

  /** Get TDataNodeLocation of a stateful trigger */
  TGetLocationForTriggerResp getLocationOfStatefulTrigger(string triggerName)

  /**
     * Return the trigger table
     */
  TGetTriggerTableResp getTriggerTable()

  /**
     * Return the Stateful trigger table
     */
  TGetTriggerTableResp getStatefulTriggerTable()

  /**
     * Return the trigger jar list of the trigger name list
     */
  TGetJarInListResp getTriggerJar(TGetJarInListReq req)

  // ======================================================
  // Pipe Plugin
  // ======================================================

  /**
   * Create a pipe plugin on the specified DataNode
   *
   * @return SUCCESS_STATUS if the pipe plugin was created successfully
   *         EXECUTE_STATEMENT_ERROR if operations on any node failed
   */
  common.TSStatus createPipePlugin(TCreatePipePluginReq req)

  /**
   * Remove a pipe plugin on the DataNodes
   *
   * @return SUCCESS_STATUS if the pipe plugin was removed successfully
   *         EXECUTE_STATEMENT_ERROR if operations on any node failed
   */
  common.TSStatus dropPipePlugin(TDropPipePluginReq req)

  /**
   * Return the pipe plugin table
   */
  TGetPipePluginTableResp getPipePluginTable();

  // ======================================================
  // Maintenance Tools
  // ======================================================

  /** Execute Level Compaction and unsequence Compaction task on all DataNodes */
  common.TSStatus merge()

  /** Persist all the data points in the memory table of the database to the disk, and seal the data file on all DataNodes */
  common.TSStatus flush(common.TFlushReq req)

  /** Clear the cache of chunk, chunk metadata and timeseries metadata to release the memory footprint on all DataNodes */
  common.TSStatus clearCache()

  /** Load configuration on all DataNodes */
  common.TSStatus loadConfiguration()

  /** Set system status on DataNodes */
  common.TSStatus setSystemStatus(string status)

  /** TestOnly. Set the target DataNode to the specified status */
  common.TSStatus setDataNodeStatus(TSetDataNodeStatusReq req)

  /** Migrate a region replica from one dataNode to another */
  common.TSStatus migrateRegion(TMigrateRegionReq req)

  /** Kill query */
  common.TSStatus killQuery(string queryId, i32 dataNodeId)

  /** Get all DataNodeLocations of Running DataNodes */
  TGetDataNodeLocationsResp getRunningDataNodeLocations()

  // ======================================================
  // Cluster Tools
  // ======================================================

  /** Show cluster ConfigNodes' and DataNodes' information */
  TShowClusterResp showCluster()

  /** Show variables who should be consist in the same cluster */
  TShowVariablesResp showVariables()

  /** Show cluster DataNodes' information */
  TShowDataNodesResp showDataNodes()

  /** Show cluster ConfigNodes' information */
  TShowConfigNodesResp showConfigNodes()

  /** Show cluster Databases' information */
  TShowDatabaseResp showDatabase(list<string> databasePathPattern)

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
  // Template
  // ======================================================

  /**
   * Create schema template
   */
  common.TSStatus createSchemaTemplate(TCreateSchemaTemplateReq req)

  /**
   * Get all schema template info and template set info for DataNode registeration
   */
  TGetAllTemplatesResp getAllTemplates()

  /**
   * Get one schema template info
   */
  TGetTemplateResp getTemplate(string req)

  /**
   * Set given schema template to given path
   */
  common.TSStatus setSchemaTemplate(TSetSchemaTemplateReq req)

  /**
   * Get paths setting given schema template
   */
  TGetPathsSetTemplatesResp getPathsSetTemplate(string req)

  /**
   * Deactivate schema template from paths matched by given pattern tree in cluster
   */
  common.TSStatus deactivateSchemaTemplate(TDeactivateSchemaTemplateReq req)

  /**
   * Unset schema template from given path
   */
  common.TSStatus unsetSchemaTemplate(TUnsetSchemaTemplateReq req)

  /**
   * Drop schema template
   */
  common.TSStatus dropSchemaTemplate(string req)

  /**
   * Generate a set of DeleteTimeSeriesProcedure to delete some specific TimeSeries
   *
   * @return SUCCESS_STATUS if the DeleteTimeSeriesProcedure submitted and executed successfully
   *         TIMESERIES_NOT_EXIST if the specific TimeSeries doesn't exist
   *         EXECUTE_STATEMENT_ERROR if failed to submit or execute the DeleteTimeSeriesProcedure
   */
  common.TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req)

  // ======================================================
  // Sync
  // ======================================================

  /** Create PipeSink */
  common.TSStatus createPipeSink(TPipeSinkInfo req)

  /** Drop PipeSink */
  common.TSStatus dropPipeSink(TDropPipeSinkReq req)

  /** Get PipeSink by name, if name is empty, get all PipeSink */
  TGetPipeSinkResp getPipeSink(TGetPipeSinkReq req)

  /** Create Pipe */
  common.TSStatus createPipe(TCreatePipeReq req)

  /** Start Pipe */
  common.TSStatus startPipe(string pipeName)

  /** Stop Pipe */
  common.TSStatus stopPipe(string pipeName)

  /** Drop Pipe */
  common.TSStatus dropPipe(string pipeName)

  /** Show Pipe by name, if name is empty, show all Pipe */
  TShowPipeResp showPipe(TShowPipeReq req)

  /* Get all pipe information. It is used for DataNode registration and restart*/
  TGetAllPipeInfoResp getAllPipeInfo();

  /* Get all pipe information. It is used for DataNode registration and restart*/
  common.TSStatus recordPipeMessage(TRecordPipeMessageReq req);

  // ======================================================
  // TestTools
  // ======================================================

  /** Get a particular DataPartition's corresponding Regions */
  TGetRegionIdResp getRegionId(TGetRegionIdReq req)

  /** Get a specific SeriesSlot's TimeSlots by start time and end time */
  TGetTimeSlotListResp getTimeSlotList(TGetTimeSlotListReq req)

  /** Get the given database's assigned SeriesSlots */
  TGetSeriesSlotListResp getSeriesSlotList(TGetSeriesSlotListReq req)

  // ====================================================
  // CQ
  // ====================================================

  /**
   * Create a CQ
   *
   * @return SUCCESS_STATUS if the cq was created successfully
   */
  common.TSStatus createCQ(TCreateCQReq req)

  /**
   * Drop a CQ
   *
   * @return SUCCESS_STATUS if the CQ was removed successfully
   */
  common.TSStatus dropCQ(TDropCQReq req)

  /**
   * Return the cq table of config leader
   */
  TShowCQResp showCQ()

  // ====================================================
  // ML Model
  // ====================================================

  /**
   * Create a model
   *
   * @return SUCCESS_STATUS if the model was created successfully
   */
  common.TSStatus createModel(TCreateModelReq req)

  /**
   * Drop a model
   *
   * @return SUCCESS_STATUS if the model was removed successfully
   */
  common.TSStatus dropModel(TDropModelReq req)

  /**
   * Return the model table
   */
  TShowModelResp showModel(TShowModelReq req)

  /**
   * Return the trail table
   */
  TShowTrailResp showTrail(TShowTrailReq req)

  /**
   * Update the model info
   *
   * @return SUCCESS_STATUS if the model was removed successfully
   */
  common.TSStatus updateModelInfo(TUpdateModelInfoReq req)

  /**
   * Update the model state
   *
   * @return SUCCESS_STATUS if the model was removed successfully
   */
  common.TSStatus updateModelState(TUpdateModelStateReq req)
}

