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
namespace java org.apache.iotdb.mpp.rpc.thrift
namespace py iotdb.thrift.datanode

struct TCreateSchemaRegionReq {
  1: required common.TRegionReplicaSet regionReplicaSet
  2: required string storageGroup
}

struct TCreateDataRegionReq {
  1: required common.TRegionReplicaSet regionReplicaSet
  2: required string storageGroup
}

struct TInvalidateCacheReq {
  1: required bool storageGroup
  2: required string fullPath
}

struct TRegionLeaderChangeReq {
  1: required common.TConsensusGroupId regionId
  2: required common.TDataNodeLocation newLeaderNode
}

struct TRegionLeaderChangeResp {
  1: required common.TSStatus status
  2: required i64 consensusLogicalTimestamp
}

struct TRegionMigrateResult {
  1: optional common.TConsensusGroupId regionId
  2: optional common.TSStatus migrateResult
  3: optional map<common.TDataNodeLocation, common.TRegionMigrateFailedType> failedNodeAndReason
  4: required common.TRegionMaintainTaskStatus taskStatus
}

struct TCreatePeerReq {
  1: required common.TConsensusGroupId regionId
  2: required list<common.TDataNodeLocation> regionLocations
  3: required string storageGroup
}

struct TMaintainPeerReq {
  1: required common.TConsensusGroupId regionId
  2: required common.TDataNodeLocation destNode
  3: required i64 taskId
}

struct TResetPeerListReq {
  1: required common.TConsensusGroupId regionId
  2: required list<common.TDataNodeLocation> correctLocations
}

struct TFragmentInstanceId {
  1: required string queryId
  2: required i32 fragmentId
  3: required string instanceId
}

struct TGetDataBlockRequest {
  1: required TFragmentInstanceId sourceFragmentInstanceId
  2: required i32 startSequenceId
  3: required i32 endSequenceId
  // Index of upstream SinkChannel
  4: required i32 index
}

struct TGetDataBlockResponse {
  1: required list<binary> tsBlocks
}

struct TAcknowledgeDataBlockEvent {
  1: required TFragmentInstanceId sourceFragmentInstanceId
  2: required i32 startSequenceId
  3: required i32 endSequenceId
  // Index of upstream SinkChannel
  4: required i32 index
}

struct TCloseSinkChannelEvent {
  1: required TFragmentInstanceId sourceFragmentInstanceId
  // Index of upstream SinkChannel
  2: required i32 index
}

struct TNewDataBlockEvent {
  1: required TFragmentInstanceId targetFragmentInstanceId
  2: required string targetPlanNodeId
  3: required TFragmentInstanceId sourceFragmentInstanceId
  4: required i32 startSequenceId
  5: required list<i64> blockSizes
}

struct TEndOfDataBlockEvent {
  1: required TFragmentInstanceId targetFragmentInstanceId
  2: required string targetPlanNodeId
  3: required TFragmentInstanceId sourceFragmentInstanceId
  4: required i32 lastSequenceId
}

struct TFragmentInstance {
  1: required binary body
}

struct TPlanNode {
  1: required binary body
}

struct TSendFragmentInstanceReq {
  1: required TFragmentInstance fragmentInstance
  2: optional common.TConsensusGroupId consensusGroupId
}

struct TSendFragmentInstanceResp {
  1: required bool accepted
  2: optional string message
  3: optional bool needRetry
}

struct TSendSinglePlanNodeReq {
  1: required TPlanNode planNode
  2: required common.TConsensusGroupId consensusGroupId
}

struct TSendSinglePlanNodeResp {
  1: required bool accepted
  2: optional string message
  3: optional common.TSStatus status
}

struct TSendBatchPlanNodeReq {
  1: required list<TSendSinglePlanNodeReq> requests;
}

struct TSendBatchPlanNodeResp {
  1: required list<TSendSinglePlanNodeResp> responses;
}

struct TFetchFragmentInstanceInfoReq {
  1: required TFragmentInstanceId fragmentInstanceId
}

// TODO: Need to supply more fields according to implementation
struct TFragmentInstanceInfoResp {
  1: required string state
  2: optional i64 endTime
  3: optional list<string> failedMessages
  4: optional list<binary> failureInfoList
}

struct TCancelQueryReq {
  1: required string queryId
  2: required list<TFragmentInstanceId> fragmentInstanceIds
  3: required bool hasThrowable
}

struct TCancelPlanFragmentReq {
  1: required string planFragmentId
}

struct TCancelFragmentInstanceReq {
  1: required TFragmentInstanceId fragmentInstanceId
}

struct TCancelResp {
  1: required bool cancelled
  2: optional string message
}

struct TSchemaFetchRequest {
  1: required binary serializedPathPatternTree
  2: required bool isPrefixMatchPath
}

struct TSchemaFetchResponse {
  1: required binary serializedSchemaTree
}

struct TDisableDataNodeReq {
  1: required common.TDataNodeLocation dataNodeLocation
}

struct TCreateFunctionInstanceReq {
  1: required binary udfInformation
  2: optional binary jarFile
}

struct TDropFunctionInstanceReq {
  1: required string functionName
  2: required bool needToDeleteJar
}

struct TCreateTriggerInstanceReq {
  1: required binary triggerInformation
  2: optional binary jarFile
}

struct TActiveTriggerInstanceReq {
  1: required string triggerName
}

struct TInactiveTriggerInstanceReq {
  1: required string triggerName
}

struct TDropTriggerInstanceReq {
  1: required string triggerName
  2: required bool needToDeleteJarFile
}

struct TUpdateTriggerLocationReq {
  1: required string triggerName
  2: required common.TDataNodeLocation newLocation
}

struct TFireTriggerReq {
  1: required string triggerName
  2: required binary tablet
  3: required byte triggerEvent
}

struct TFireTriggerResp {
  1: required bool foundExecutor
  2: required i32 fireResult
}

struct TCreatePipePluginInstanceReq {
  1: required binary pipePluginMeta
  2: required binary jarFile
}

struct TDropPipePluginInstanceReq {
  1: required string pipePluginName
  2: required bool needToDeleteJar
}

struct TInvalidatePermissionCacheReq {
  1: required string username
  2: required string roleName
}

struct TDataNodeHeartbeatReq {
  1: required i64 heartbeatTimestamp
  2: required bool needJudgeLeader
  3: required bool needSamplingLoad
  4: required i64 timeSeriesQuotaRemain
  5: optional list<i32> schemaRegionIds
  6: optional list<i32> dataRegionIds
  7: optional map<string, common.TSpaceQuota> spaceQuotaUsage
  8: optional bool needPipeMetaList
  9: optional i64 deviceQuotaRemain
  10: optional TDataNodeActivation activation
  11: optional set<common.TEndPoint> configNodeEndPoints
}

struct TDataNodeActivation {
  1: required bool activated
  2: required i64 deviceNumRemain
  3: required i64 sensorNumRemain
}

struct TDataNodeHeartbeatResp {
  1: required i64 heartbeatTimestamp
  2: required string status
  3: optional string statusReason
  4: optional map<common.TConsensusGroupId, bool> judgedLeaders
  5: optional TLoadSample loadSample
  6: optional map<i32, i64> regionSeriesUsageMap
  7: optional map<i32, i64> regionDeviceUsageMap
  8: optional map<i32, i64> regionDisk
  // TODO: schemaLimitLevel is not used from 1.3.0, keep it for compatibility
  9: optional TSchemaLimitLevel schemaLimitLevel
  10: optional list<binary> pipeMetaList
  11: optional string activateStatus
  12: optional set<common.TEndPoint> confirmedConfigNodeEndPoints
  13: optional map<common.TConsensusGroupId, i64> consensusLogicalTimeMap
  14: optional list<bool> pipeCompletedList
  15: optional list<i64> pipeRemainingEventCountList
  16: optional list<double> pipeRemainingTimeList
}

struct TPipeHeartbeatReq {
  1: required i64 heartbeatId
}

struct TPipeHeartbeatResp {
  1: required list<binary> pipeMetaList
  2: optional list<bool> pipeCompletedList
  3: optional list<i64> pipeRemainingEventCountList
  4: optional list<double> pipeRemainingTimeList
}

enum TSchemaLimitLevel{
    DEVICE,
    TIMESERIES
}

struct TLoadSample {
  // Percentage of occupied cpu in DataNode
  1: required double cpuUsageRate
  // Percentage of occupied memory space in DataNode
  2: required double memoryUsageRate
  // Percentage of occupied disk space in DataNode
  3: required double diskUsageRate
  // The size of free disk space
  // Unit: Byte
  4: required double freeDiskSpace
}

struct TRegionRouteReq {
  1: required i64 timestamp
  2: required map<common.TConsensusGroupId, common.TRegionReplicaSet> regionRouteMap
}

struct TUpdateTemplateReq {
  1: required byte type
  2: required binary templateInfo
}

struct TTsFilePieceReq {
    1: required binary body
    2: required string uuid
    3: required common.TConsensusGroupId consensusGroupId
}

struct TLoadCommandReq {
    1: required i32 commandType
    2: required string uuid
    3: optional bool isGeneratedByPipe
    4: optional binary progressIndex
}

struct TLoadResp {
  1: required bool accepted
  2: optional string message
  3: optional common.TSStatus status
}

struct TConstructSchemaBlackListReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
}

struct TRollbackSchemaBlackListReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
}

struct TInvalidateMatchedSchemaCacheReq {
  1: required binary pathPatternTree
}

struct TFetchSchemaBlackListReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
}

struct TFetchSchemaBlackListResp {
  1: required common.TSStatus status
  2: required binary pathPatternTree
}

struct TDeleteDataForDeleteSchemaReq {
  1: required list<common.TConsensusGroupId> dataRegionIdList
  2: required binary pathPatternTree
  3: optional bool isGeneratedByPipe
}

struct TDeleteTimeSeriesReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
  3: optional bool isGeneratedByPipe
}

struct TConstructSchemaBlackListWithTemplateReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required map<string, list<i32>> templateSetInfo
}

struct TRollbackSchemaBlackListWithTemplateReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required map<string, list<i32>> templateSetInfo
}

struct TDeactivateTemplateReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required map<string, list<i32>> templateSetInfo
  3: optional bool isGeneratedByPipe
}

struct TCountPathsUsingTemplateReq {
  1: required i32 templateId
  2: required binary patternTree
  3: required list<common.TConsensusGroupId> schemaRegionIdList
}

struct TCountPathsUsingTemplateResp {
  1: required common.TSStatus status
  2: optional i64 count
}

struct TCheckSchemaRegionUsingTemplateReq{
  1: required list<common.TConsensusGroupId> schemaRegionIdList
}

struct TCheckSchemaRegionUsingTemplateResp {
  1: required common.TSStatus status
  2: required bool result
}

struct TCheckTimeSeriesExistenceReq {
  1: required binary patternTree
  2: required list<common.TConsensusGroupId> schemaRegionIdList
}

struct TCheckTimeSeriesExistenceResp {
  1: required common.TSStatus status
  2: optional bool exists
}

struct TPushPipeMetaReq {
  1: required list<binary> pipeMetas
}

struct TPushPipeMetaResp {
  1: required common.TSStatus status
  2: optional list<TPushPipeMetaRespExceptionMessage> exceptionMessages
}

struct TPushPipeMetaRespExceptionMessage {
  1: required string pipeName
  2: required string message
  3: required i64 timeStamp
}

struct TPushSinglePipeMetaReq {
  1: optional binary pipeMeta // Should not set both to null.
  2: optional string pipeNameToDrop // If it is not null, pipe with indicated name on datanode will be dropped.
}

struct TPushMultiPipeMetaReq {
  1: optional list<binary> pipeMetas // Should not set both to null.
  2: optional list<string> pipeNamesToDrop // If it is not null, pipes with indicated names on datanode will be dropped.
}

struct TPushTopicMetaReq {
  1: required list<binary> topicMetas
}

struct TPushSingleTopicMetaReq {
   1: optional binary topicMeta // Should not set both to null.
   2: optional string topicNameToDrop
}

struct TPushMultiTopicMetaReq {
   1: optional list<binary> topicMetas // Should not set both to null.
   2: optional list<string> topicNamesToDrop
}

struct TPushTopicMetaResp {
  1: required common.TSStatus status
  2: optional list<TPushTopicMetaRespExceptionMessage> exceptionMessages
}

struct TPushTopicMetaRespExceptionMessage {
  1: required string topicName
  2: required string message
  3: required i64 timeStamp
}

struct TPushConsumerGroupMetaReq {
  1: required list<binary> consumerGroupMetas
}

struct TPushSingleConsumerGroupMetaReq {
   1: optional binary consumerGroupMeta // Should not set both to null.
   2: optional string consumerGroupNameToDrop
}

struct TPushConsumerGroupMetaResp {
  1: required common.TSStatus status
  2: optional list<TPushConsumerGroupMetaRespExceptionMessage> exceptionMessages
}

struct TPushConsumerGroupMetaRespExceptionMessage {
  1: required string consumerGroupId
  2: required string message
  3: required i64 timeStamp
}

struct TConstructViewSchemaBlackListReq {
    1: required list<common.TConsensusGroupId> schemaRegionIdList
    2: required binary pathPatternTree
}

struct TRollbackViewSchemaBlackListReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
}

struct TDeleteViewSchemaReq {
   1: required list<common.TConsensusGroupId> schemaRegionIdList
   2: required binary pathPatternTree
   3: optional bool isGeneratedByPipe
}

struct TAlterViewReq {
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required list<binary> viewBinaryList
  3: optional bool isGeneratedByPipe
}

// ====================================================
// CQ
// ====================================================
struct TExecuteCQ {
  1: required string queryBody
  2: required i64 startTime
  3: required i64 endTime
  4: required i64 timeout
  5: required string zoneId
  6: required string cqId
  7: required string username
}

/**
* BEGIN: Used for EXPLAIN ANALYZE
**/
struct TOperatorStatistics{
  1: required string planNodeId
  2: required string operatorType
  3: required i64 totalExecutionTimeInNanos
  4: required i64 nextCalledCount
  5: required i64 hasNextCalledCount
  6: required map<string,string> specifiedInfo
  7: required i64 outputRows
  8: required i64 memoryUsage
  9: optional i64 count
}

struct TQueryStatistics {
  1: i64 loadTimeSeriesMetadataDiskSeqCount,
  2: i64 loadTimeSeriesMetadataDiskUnSeqCount,
  3: i64 loadTimeSeriesMetadataMemSeqCount,
  4: i64 loadTimeSeriesMetadataMemUnSeqCount,
  5: i64 loadTimeSeriesMetadataAlignedDiskSeqCount,
  6: i64 loadTimeSeriesMetadataAlignedDiskUnSeqCount,
  7: i64 loadTimeSeriesMetadataAlignedMemSeqCount,
  8: i64 loadTimeSeriesMetadataAlignedMemUnSeqCount,

  9: i64 loadTimeSeriesMetadataDiskSeqTime,
  10: i64 loadTimeSeriesMetadataDiskUnSeqTime,
  11: i64 loadTimeSeriesMetadataMemSeqTime,
  12: i64 loadTimeSeriesMetadataMemUnSeqTime,
  13: i64 loadTimeSeriesMetadataAlignedDiskSeqTime,
  14: i64 loadTimeSeriesMetadataAlignedDiskUnSeqTime,
  15: i64 loadTimeSeriesMetadataAlignedMemSeqTime,
  16: i64 loadTimeSeriesMetadataAlignedMemUnSeqTime,

  17: i64 constructNonAlignedChunkReadersDiskCount,
  18: i64 constructNonAlignedChunkReadersMemCount,
  19: i64 constructAlignedChunkReadersDiskCount,
  20: i64 constructAlignedChunkReadersMemCount,

  21: i64 constructNonAlignedChunkReadersDiskTime,
  22: i64 constructNonAlignedChunkReadersMemTime,
  23: i64 constructAlignedChunkReadersDiskTime,
  24: i64 constructAlignedChunkReadersMemTime,

  25: i64 pageReadersDecodeAlignedDiskCount,
  26: i64 pageReadersDecodeAlignedDiskTime,
  27: i64 pageReadersDecodeAlignedMemCount,
  28: i64 pageReadersDecodeAlignedMemTime,
  29: i64 pageReadersDecodeNonAlignedDiskCount,
  30: i64 pageReadersDecodeNonAlignedDiskTime,
  31: i64 pageReadersDecodeNonAlignedMemCount,
  32: i64 pageReadersDecodeNonAlignedMemTime,
  33: i64 pageReaderMaxUsedMemorySize

  34: i64 alignedTimeSeriesMetadataModificationCount
  35: i64 alignedTimeSeriesMetadataModificationTime
  36: i64 nonAlignedTimeSeriesMetadataModificationCount
  37: i64 nonAlignedTimeSeriesMetadataModificationTime
}


struct TFetchFragmentInstanceStatisticsReq {
  1: required TFragmentInstanceId fragmentInstanceId
}

struct TFetchFragmentInstanceStatisticsResp {
  1: required common.TSStatus status
  2: optional TFragmentInstanceId fragmentInstanceId
  3: optional string dataRegion
  4: optional i64 startTimeInMS
  5: optional i64 endTimeInMS
  6: optional TQueryStatistics queryStatistics
  7: optional map<string, TOperatorStatistics> operatorStatisticsMap
  8: optional i64 initDataQuerySourceCost
  9: optional i64 seqUnclosedNum
  10: optional i64 seqClosednNum
  11: optional i64 unseqClosedNum
  12: optional i64 unseqUnclosedNum
  13: optional i64 readyQueuedTime
  14: optional i64 blockQueuedTime
  15: optional string ip
  16: optional string state
}

/**
* END: Used for EXPLAIN ANALYZE
**/

service IDataNodeRPCService {

  // -----------------------------------For Data Node-----------------------------------------------

  /**
  * dispatch FragmentInstance to remote node for query request
  */
  TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req);

  /**
  * dispatch PlanNodes in batches to remote node for write request in order to save resource
  */
  TSendBatchPlanNodeResp sendBatchPlanNode(TSendBatchPlanNodeReq req);

  TFragmentInstanceInfoResp fetchFragmentInstanceInfo(TFetchFragmentInstanceInfoReq req);

  TCancelResp cancelQuery(TCancelQueryReq req);

  TCancelResp cancelPlanFragment(TCancelPlanFragmentReq req);

  TCancelResp cancelFragmentInstance(TCancelFragmentInstanceReq req);

  TSchemaFetchResponse fetchSchema(TSchemaFetchRequest req);

  TLoadResp sendTsFilePieceNode(TTsFilePieceReq req);

  TLoadResp sendLoadCommand(TLoadCommandReq req);


  // -----------------------------------For Config Node-----------------------------------------------

  /**
   * Config node will create a schema region on a list of data nodes.
   *
   * @param data nodes of the schema region, and schema region id generated by config node
   */
  common.TSStatus createSchemaRegion(TCreateSchemaRegionReq req)

  /**
   * Config node will create a data region on a list of data nodes.
   *
   * @param data nodes of the data region, and data region id generated by config node
   */
  common.TSStatus createDataRegion(TCreateDataRegionReq req)

  /**
   * Config node will invalidate Partition Info cache.
   *
   * @param bool:isStorageGroup, string:fullPath
   */
  common.TSStatus invalidatePartitionCache(TInvalidateCacheReq req)

  /**
   * Config node will invalidate Schema Info cache.
   *
   * @param bool:isStorageGroup, string:fullPath
   */
  common.TSStatus invalidateSchemaCache(TInvalidateCacheReq req)

  /**
   * Config node will delete a data/schema region of a certain storageGroup.
   *
   * @param data nodes of the data region, and data region id generated by config node
   */
  common.TSStatus deleteRegion(common.TConsensusGroupId consensusGroupId)

  /**
   * Change the leader of specified RegionGroup to another DataNode
   *
   * @param The specified RegionGroup and the new leader DataNode
   */
  TRegionLeaderChangeResp changeRegionLeader(TRegionLeaderChangeReq req)

  /**
   * Create a new Region peer in the given DataNode for the specified RegionGroup
   *
   * @param TCreatePeerReq which contains RegionId and its colleagues' locations
   */
  common.TSStatus createNewRegionPeer(TCreatePeerReq req)

  /**
   * Add a Region peer to the specified RegionGroup
   *
   * @param TMaintainPeerReq which contains RegionId and the DataNodeLocation that selected to perform the add peer process
   */
  common.TSStatus addRegionPeer(TMaintainPeerReq req)

  /**
   * Remove a Region peer from the specified RegionGroup
   *
   * @param TMaintainPeerReq which contains RegionId and the DataNodeLocation that selected to perform the remove peer process
   */
  common.TSStatus removeRegionPeer(TMaintainPeerReq req)

  /**
   * Delete a Region peer in the given ConsensusGroup and all of its data on the specified DataNode
   *
   * @param TMaintainPeerReq which contains RegionId and the DataNodeLocation where the specified Region peer located
   */
  common.TSStatus deleteOldRegionPeer(TMaintainPeerReq req)

  /**
   * Reset a consensus group's peer list
   */
  common.TSStatus resetPeerList(TResetPeerListReq req);

  /**
   * Get the result of a region maintainance task
   */
  TRegionMigrateResult getRegionMaintainResult(i64 taskId)

  /**
   * Config node will disable the Data node, the Data node will not accept read/write request when disabled
   * @param data node location
   */
  common.TSStatus disableDataNode(TDisableDataNodeReq req)

  /**
   * Config node will stop the Data node.
   */
  common.TSStatus stopDataNode()

  /**
   * ConfigNode will ask DataNode for heartbeat in every few seconds.
   *
   * @param ConfigNode will send the latest config_node_list and load balancing policies in TDataNodeHeartbeatReq
   **/
  TDataNodeHeartbeatResp getDataNodeHeartBeat(TDataNodeHeartbeatReq req)

  /**
   * ConfigNode will ask DataNode to update region cache
   *
   * @param ConfigNode will send timestamp and new regionRouteMap in TRegionRouteReq
   **/
  common.TSStatus updateRegionCache(TRegionRouteReq req)

  /**
   * Config node will create a function on a list of data nodes.
   *
   * @param function name, function class name, and executable uris
   **/
  common.TSStatus createFunction(TCreateFunctionInstanceReq req)

  /**
   * Config node will drop a function on a list of data nodes.
   *
   * @param function name
   **/
  common.TSStatus dropFunction(TDropFunctionInstanceReq req)

  /**
   * Config node will create a trigger instance on data node.
   *
   * @param TriggerInformation, jar file.
   **/
  common.TSStatus createTriggerInstance(TCreateTriggerInstanceReq req)

  /**
   * Config node will active a trigger instance on data node.
   *
   * @param trigger name.
   **/
  common.TSStatus activeTriggerInstance(TActiveTriggerInstanceReq req)


  /**
   * Config node will inactive a trigger instance on data node.
   *
   * @param trigger name.
   **/
  common.TSStatus inactiveTriggerInstance(TInactiveTriggerInstanceReq req)


  /**
   * Config node will drop a trigger on all online config nodes and data nodes.
   *
   * @param trigger name, whether need to delete jar
   **/
  common.TSStatus dropTriggerInstance(TDropTriggerInstanceReq req)

  /**
   * Config node will renew DataNodeLocation of a stateful trigger.
   *
   * @param trigger name, new DataNodeLocation
   **/
  common.TSStatus updateTriggerLocation (TUpdateTriggerLocationReq req)

  /**
   * Fire a stateful trigger on current data node.
   *
   * @param trigger name, tablet and event
   **/
  TFireTriggerResp fireTrigger(TFireTriggerReq req)

  /**
   * Config node will invalidate permission Info cache.
   *
   * @param string:username, list<string>:roleList
   */
  common.TSStatus invalidatePermissionCache(TInvalidatePermissionCacheReq req)

  /**
   * Config node will create a pipe plugin on a list of data nodes.
   *
   * @param function name, function class name, and executable uris
   **/
  common.TSStatus createPipePlugin(TCreatePipePluginInstanceReq req)

  /**
   * Config node will drop a pipe plugin on a list of data nodes.
   *
   * @param function name
   **/
  common.TSStatus dropPipePlugin(TDropPipePluginInstanceReq req)

  /* Maintenance Tools */

  common.TSStatus merge()

  common.TSStatus flush(common.TFlushReq req)

  common.TSStatus settle(common.TSettleReq req)

  common.TSStatus startRepairData()

  common.TSStatus stopRepairData()

  common.TSStatus clearCache()

  common.TShowConfigurationResp showConfiguration()

  common.TSStatus setConfiguration(common.TSetConfigurationReq req)

  common.TSStatus loadConfiguration()

  common.TSStatus setSystemStatus(string status)

  common.TSStatus killQueryInstance(string queryId)

  /**
     * Config node will Set the TTL for the database on a list of data nodes.
     */
    common.TSStatus setTTL(common.TSetTTLReq req)

  /**
   * Update template cache when template info or template set info is updated
   */
  common.TSStatus updateTemplate(TUpdateTemplateReq req)

  /**
   * Construct schema black list in target schemaRegion to block R/W on matched timeseries
   */
  common.TSStatus constructSchemaBlackList(TConstructSchemaBlackListReq req)

  /**
   * Remove the schema black list to recover R/W on matched timeseries
   */
  common.TSStatus rollbackSchemaBlackList(TRollbackSchemaBlackListReq req)

  /**
   * Config node will invalidate Schema Info cache, which matched by given pathPatternTree.
   *
   * @param binary: pathPatternTree
   */
  common.TSStatus invalidateMatchedSchemaCache(TInvalidateMatchedSchemaCacheReq req)

  /**
   * Config node will fetch the schema info in black list.
   *
   * @param binary: pathPatternTree
   */
  TFetchSchemaBlackListResp fetchSchemaBlackList(TFetchSchemaBlackListReq req)

  /**
   * Config node inform this dataNode to execute a distribution data deleion queryengine task
   */
  common.TSStatus deleteDataForDeleteSchema(TDeleteDataForDeleteSchemaReq req)

  /**
   * Delete matched timeseries and remove according schema black list in target schemRegion
   */
  common.TSStatus deleteTimeSeries(TDeleteTimeSeriesReq req)

  /**
   * Construct schema black list in target schemaRegion to block R/W on matched timeseries represent by template
   */
  common.TSStatus constructSchemaBlackListWithTemplate(TConstructSchemaBlackListWithTemplateReq req)

  /**
   * Remove the schema black list to recover R/W on matched timeseries represent by template
   */
  common.TSStatus rollbackSchemaBlackListWithTemplate(TRollbackSchemaBlackListWithTemplateReq req)

  /**
   * Deactivate template on device matched by input path pattern
   * and remove according template schema black list in target schemRegion
   */
  common.TSStatus deactivateTemplate(TDeactivateTemplateReq req)

  TCountPathsUsingTemplateResp countPathsUsingTemplate(TCountPathsUsingTemplateReq req)

  TCheckSchemaRegionUsingTemplateResp checkSchemaRegionUsingTemplate(TCheckSchemaRegionUsingTemplateReq req)

  TCheckTimeSeriesExistenceResp checkTimeSeriesExistence(TCheckTimeSeriesExistenceReq req)

  common.TSStatus constructViewSchemaBlackList(TConstructViewSchemaBlackListReq req)

  common.TSStatus rollbackViewSchemaBlackList(TRollbackViewSchemaBlackListReq req)

  common.TSStatus deleteViewSchema(TDeleteViewSchemaReq req)

  common.TSStatus alterView(TAlterViewReq req)

 /**
  * Send pipeMetas to DataNodes, for synchronization
  */
  TPushPipeMetaResp pushPipeMeta(TPushPipeMetaReq req)

 /**
  * Send one pipeMeta to DataNodes, for create/start/stop/drop one pipe
  */
  TPushPipeMetaResp pushSinglePipeMeta(TPushSinglePipeMetaReq req)

 /**
  * Send multiple pipeMetas to DataNodes, for create/drop subscriptions
  */
  TPushPipeMetaResp pushMultiPipeMeta(TPushMultiPipeMetaReq req)

 /**
  * Send topicMetas to DataNodes, for synchronization
  */
  TPushTopicMetaResp pushTopicMeta(TPushTopicMetaReq req)

 /**
  * Send one topic meta to DataNodes.
  */
  TPushTopicMetaResp pushSingleTopicMeta(TPushSingleTopicMetaReq req)

 /**
  * Send multiple topic metas to DataNodes, for create/drop subscriptions
  */
  TPushTopicMetaResp pushMultiTopicMeta(TPushMultiTopicMetaReq req)

 /**
  * Send consumerGroupMetas to DataNodes, for synchronization
  */
  TPushConsumerGroupMetaResp pushConsumerGroupMeta(TPushConsumerGroupMetaReq req)

 /**
  * Send one consumer group meta to DataNodes.
  */
  TPushConsumerGroupMetaResp pushSingleConsumerGroupMeta(TPushSingleConsumerGroupMetaReq req)

  /**
  * ConfigNode will ask DataNode for pipe meta in every few seconds
  **/
  TPipeHeartbeatResp pipeHeartbeat(TPipeHeartbeatReq req)

 /**
  * Execute CQ on DataNode
  */
  common.TSStatus executeCQ(TExecuteCQ req)

  /**
   * Set space quota
   **/
  common.TSStatus setSpaceQuota(common.TSetSpaceQuotaReq req)

  /**
   * Set throttle quota
   **/
  common.TSStatus setThrottleQuota(common.TSetThrottleQuotaReq req)

  /**
  * Fetch fragment instance statistics for EXPLAIN ANALYZE
  */
  TFetchFragmentInstanceStatisticsResp fetchFragmentInstanceStatistics(TFetchFragmentInstanceStatisticsReq req)

  common.TTestConnectionResp submitTestConnectionTask(common.TNodeLocations nodeLocations)

  /** Empty rpc, only for connection test */
  common.TSStatus testConnectionEmptyRPC()
}

service MPPDataExchangeService {
  TGetDataBlockResponse getDataBlock(TGetDataBlockRequest req);

  void onAcknowledgeDataBlockEvent(TAcknowledgeDataBlockEvent e);

  void onCloseSinkChannelEvent(TCloseSinkChannelEvent e);

  void onNewDataBlockEvent(TNewDataBlockEvent e);

  void onEndOfDataBlockEvent(TEndOfDataBlockEvent e);

  /** Empty rpc, only for connection test */
  common.TSStatus testConnectionEmptyRPC()
}
