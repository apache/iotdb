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

struct TMigrateRegionReq{
    1: required common.TRegionReplicaSet migrateRegion
}

struct TMigrateRegionResp{
    1: required common.TSStatus migrateResult
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
}

struct TGetDataBlockResponse {
  1: required list<binary> tsBlocks
}

struct TAcknowledgeDataBlockEvent {
  1: required TFragmentInstanceId sourceFragmentInstanceId
  2: required i32 startSequenceId
  3: required i32 endSequenceId
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
  2: required common.TConsensusGroupId consensusGroupId
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

struct TFetchFragmentInstanceStateReq {
  1: required TFragmentInstanceId fragmentInstanceId
}

// TODO: need to supply more fields according to implementation
struct TFragmentInstanceStateResp {
  1: required string state
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

struct TCreateFunctionRequest {
  1: required string udfName
  2: required string className
  3: required list<string> uris
}

struct TDropFunctionRequest {
  1: required string udfName
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
  2: optional map<common.TConsensusGroupId, bool> judgedLeaders
  3: optional i16 cpu
  4: optional i16 memory
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

  TFragmentInstanceStateResp fetchFragmentInstanceState(TFetchFragmentInstanceStateReq req);

  TCancelResp cancelQuery(TCancelQueryReq req);

  TCancelResp cancelPlanFragment(TCancelPlanFragmentReq req);

  TCancelResp cancelFragmentInstance(TCancelFragmentInstanceReq req);

  TSchemaFetchResponse fetchSchema(TSchemaFetchRequest req)


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
     * Config node will migrate a region(maybe data region or schema region)
     * from one data node to another
     *
     * @param new replica set of the region
     */
  TMigrateRegionResp migrateRegion(TMigrateRegionReq req)

  /**
  * ConfigNode will ask DataNode for heartbeat in every few seconds.
  *
  * @param ConfigNode will send the latest config_node_list and load balancing policies in THeartbeatReq
  **/
  THeartbeatResp getDataNodeHeartBeat(THeartbeatReq req)

  /**
   * Config node will create a function on a list of data nodes.
   *
   * @param function name, function class name, and executable uris
   **/
  common.TSStatus createFunction(TCreateFunctionRequest req)

  /**
   * Config node will drop a function on a list of data nodes.
   *
   * @param function name
   **/
  common.TSStatus dropFunction(TDropFunctionRequest req)

  /**
   * Config node will invalidate permission Info cache.
   *
   * @param string:username, list<string>:roleList
   */
  common.TSStatus invalidatePermissionCache(TInvalidatePermissionCacheReq req)

  common.TSStatus flush(common.TFlushReq req)

  common.TSStatus setTTL(common.TSetTTLReq req)
}

service MPPDataExchangeService {
  TGetDataBlockResponse getDataBlock(TGetDataBlockRequest req);

  void onAcknowledgeDataBlockEvent(TAcknowledgeDataBlockEvent e);

  void onNewDataBlockEvent(TNewDataBlockEvent e);

  void onEndOfDataBlockEvent(TEndOfDataBlockEvent e);
}
