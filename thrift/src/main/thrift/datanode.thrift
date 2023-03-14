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
  3: optional i64 ttl
}

struct TInvalidateCacheReq {
  1: required bool storageGroup
  2: required string fullPath
}

struct TRegionLeaderChangeReq {
  1: required common.TConsensusGroupId regionId
  2: required common.TDataNodeLocation newLeaderNode
}

struct TCreatePeerReq {
  1: required common.TConsensusGroupId regionId
  2: required list<common.TDataNodeLocation> regionLocations
  3: required string storageGroup
  4: optional i64 ttl
}

struct TMaintainPeerReq {
  1: required common.TConsensusGroupId regionId
  2: required common.TDataNodeLocation destNode
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
  // index of upstream SinkHandle
  4: required i32 index
}

struct TGetDataBlockResponse {
  1: required list<binary> tsBlocks
}

struct TAcknowledgeDataBlockEvent {
  1: required TFragmentInstanceId sourceFragmentInstanceId
  2: required i32 startSequenceId
  3: required i32 endSequenceId
  // index of upstream SinkHandle
  4: required i32 index
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
}

struct TSendPlanNodeReq {
  1: required TPlanNode planNode
  2: required common.TConsensusGroupId consensusGroupId
}

struct TSendPlanNodeResp {
  1: required bool accepted
  2: optional string message
  3: optional common.TSStatus status
}

struct TFetchFragmentInstanceInfoReq {
  1: required TFragmentInstanceId fragmentInstanceId
}

// TODO: need to supply more fields according to implementation
struct TFragmentInstanceInfoResp {
  1: required string state
  2: optional i64 endTime
  3: optional list<string> failedMessages
  4: optional list<binary> failureInfoList
}

struct TCancelQueryReq {
  1: required string queryId
  2: required list<TFragmentInstanceId> fragmentInstanceIds
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

struct THeartbeatReq {
  1: required i64 heartbeatTimestamp
  2: required bool needJudgeLeader
  3: required bool needSamplingLoad
}

struct THeartbeatResp {
  1: required i64 heartbeatTimestamp
  2: required string status
  3: optional string statusReason
  4: optional map<common.TConsensusGroupId, bool> judgedLeaders
  5: optional TLoadSample loadSample
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

struct TUpdateConfigNodeGroupReq {
  1: required list<common.TConfigNodeLocation> configNodeLocations
}

struct TUpdateTemplateReq{
  1: required byte type
  2: required binary templateInfo
}

struct TTsFilePieceReq{
    1: required binary body
    2: required string uuid
    3: required common.TConsensusGroupId consensusGroupId
}

struct TLoadCommandReq{
    1: required i32 commandType
    2: required string uuid
}

struct TLoadResp{
  1: required bool accepted
  2: optional string message
  3: optional common.TSStatus status
}

struct TConstructSchemaBlackListReq{
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
}

struct TRollbackSchemaBlackListReq{
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
}

struct TInvalidateMatchedSchemaCacheReq{
  1: required binary pathPatternTree
}

struct TFetchSchemaBlackListReq{
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
}

struct TFetchSchemaBlackListResp{
  1: required common.TSStatus status
  2: required binary pathPatternTree
}

struct TDeleteDataForDeleteSchemaReq{
  1: required list<common.TConsensusGroupId> dataRegionIdList
  2: required binary pathPatternTree
}

struct TDeleteTimeSeriesReq{
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required binary pathPatternTree
}

struct TConstructSchemaBlackListWithTemplateReq{
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required map<string, list<i32>> templateSetInfo
}

struct TRollbackSchemaBlackListWithTemplateReq{
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required map<string, list<i32>> templateSetInfo
}

struct TDeactivateTemplateReq{
  1: required list<common.TConsensusGroupId> schemaRegionIdList
  2: required map<string, list<i32>> templateSetInfo
}

struct TCountPathsUsingTemplateReq{
  1: required i32 templateId
  2: required binary patternTree
  3: required list<common.TConsensusGroupId> schemaRegionIdList
}

struct TCountPathsUsingTemplateResp{
  1: required common.TSStatus status
  2: optional i64 count
}

struct TCreatePipeOnDataNodeReq{
  1: required binary pipeInfo
}

struct TOperatePipeOnDataNodeReq {
    1: required string pipeName
    // ordinal of {@linkplain SyncOperation}
    2: required i8 operation
    3: optional i64 createTime
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

// ====================================================
// ML Node
// ====================================================
struct TDeleteModelMetricsReq {
  1: required string modelId
}

struct TFetchTimeseriesReq {
  1: required i64 sessionId
  2: required i64 statementId
  3: required list<string> queryExpressions
  4: optional string queryFilter
  5: optional i32 fetchSize
  6: optional i64 timeout
}

struct TFetchTimeseriesResp {
  1: required common.TSStatus status
  2: required i64 queryId
  3: required list<string> columnNameList
  4: required list<string> columnTypeList
  5: required map<string, i32> columnNameIndexMap
  6: required list<binary> tsDataset
  7: required bool hasMoreData
}

struct TFetchWindowBatchReq {
  1: required i64 sessionId
  2: required i64 statementId
  3: required list<string> queryExpressions
  4: required TGroupByTimeParameter groupByTimeParameter
  5: optional string queryFilter
  6: optional i32 fetchSize
  7: optional i64 timeout
}

struct TGroupByTimeParameter {
  1: required i64 startTime
  2: required i64 endTime
  3: required i64 interval
  4: required i64 slidingStep
  5: optional list<i32> indexes
}

struct TFetchWindowBatchResp {
  1: required common.TSStatus status
  2: required i64 queryId
  3: required list<string> columnNameList
  4: required list<string> columnTypeList
  5: required map<string, i32> columnNameIndexMap
  6: required list<list<binary>> windowDataset
  7: required bool hasMoreData
}

struct TRecordModelMetricsReq {
  1: required string modelId
  2: required string trialId
  3: required list<common.EvaluateMetric> metrics
  4: required i64 timestamp
  5: required list<double> values
}

service IDataNodeRPCService {

  // -----------------------------------For Data Node-----------------------------------------------

  /**
  * dispatch FragmentInstance to remote node for query request
  */
  TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req);

  /**
  * dispatch PlanNode to remote node for write request in order to save resource
  */
  TSendPlanNodeResp sendPlanNode(TSendPlanNodeReq req);

  TFragmentInstanceInfoResp fetchFragmentInstanceInfo(TFetchFragmentInstanceInfoReq req);

  TCancelResp cancelQuery(TCancelQueryReq req);

  TCancelResp cancelPlanFragment(TCancelPlanFragmentReq req);

  TCancelResp cancelFragmentInstance(TCancelFragmentInstanceReq req);

  TSchemaFetchResponse fetchSchema(TSchemaFetchRequest req)

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
  common.TSStatus changeRegionLeader(TRegionLeaderChangeReq req);

  /**
   * Create a new Region peer in the given DataNode for the specified RegionGroup
   *
   * @param TCreatePeerReq which contains RegionId and its colleagues' locations
   */
  common.TSStatus createNewRegionPeer(TCreatePeerReq req);

  /**
   * Add a Region peer to the specified RegionGroup
   *
   * @param TMaintainPeerReq which contains RegionId and the DataNodeLocation that selected to perform the add peer process
   */
  common.TSStatus addRegionPeer(TMaintainPeerReq req);

  /**
   * Remove a Region peer from the specified RegionGroup
   *
   * @param TMaintainPeerReq which contains RegionId and the DataNodeLocation that selected to perform the remove peer process
   */
  common.TSStatus removeRegionPeer(TMaintainPeerReq req);

  /**
   * Delete a Region peer in the given ConsensusGroup and all of its data on the specified DataNode
   *
   * @param TMaintainPeerReq which contains RegionId and the DataNodeLocation where the specified Region peer located
   */
  common.TSStatus deleteOldRegionPeer(TMaintainPeerReq req);

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
  * @param ConfigNode will send the latest config_node_list and load balancing policies in THeartbeatReq
  **/
  THeartbeatResp getDataNodeHeartBeat(THeartbeatReq req)

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

  common.TSStatus clearCache()

  common.TSStatus loadConfiguration()

  common.TSStatus setSystemStatus(string status)

  common.TSStatus killQueryInstance(string queryId)

  /**
   * Config node will Set the TTL for the database on a list of data nodes.
   */
  common.TSStatus setTTL(common.TSetTTLReq req)
  
  /**
   * configNode will notify all DataNodes when the capacity of the ConfigNodeGroup is expanded or reduced
   *
   * @param list<common.TConfigNodeLocation> configNodeLocations
   */
  common.TSStatus updateConfigNodeGroup(TUpdateConfigNodeGroupReq req)

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
   * Config node inform this dataNode to execute a distribution data deleion mpp task
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

 /**
  * Create PIPE on DataNode
  */
  common.TSStatus createPipeOnDataNode(TCreatePipeOnDataNodeReq req)

 /**
  * Start, stop or drop PIPE on DataNode
  */
  common.TSStatus operatePipeOnDataNode(TOperatePipeOnDataNodeReq req)

 /**
  * Start, stop or drop PIPE on DataNode for rollback
  */
  common.TSStatus operatePipeOnDataNodeForRollback(TOperatePipeOnDataNodeReq req)

 /**
  * Execute CQ on DataNode
  */
  common.TSStatus executeCQ(TExecuteCQ req)

 /**
  * Delete model training metrics on DataNode
  */
  common.TSStatus deleteModelMetrics(TDeleteModelMetricsReq req)

  // ----------------------------------- For ML Node -----------------------------------------------

 /**
  * Fecth the data of the specified time series
  */
  TFetchTimeseriesResp fetchTimeseries(TFetchTimeseriesReq req)

 /**
  * Fecth window batches of the specified time series
  */
  TFetchWindowBatchResp fetchWindowBatch(TFetchWindowBatchReq req)

 /**
  * Record model training metrics on DataNode
  */
  common.TSStatus recordModelMetrics(TRecordModelMetricsReq req)
}

service MPPDataExchangeService {
  TGetDataBlockResponse getDataBlock(TGetDataBlockRequest req);

  void onAcknowledgeDataBlockEvent(TAcknowledgeDataBlockEvent e);

  void onNewDataBlockEvent(TNewDataBlockEvent e);

  void onEndOfDataBlockEvent(TEndOfDataBlockEvent e);
}
