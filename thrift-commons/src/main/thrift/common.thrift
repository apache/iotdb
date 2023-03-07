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

namespace java org.apache.iotdb.common.rpc.thrift
namespace py iotdb.thrift.common
namespace go common

// Define a set of ip:port address
struct TEndPoint {
  1: required string ip
  2: required i32 port
}

// The return status code and message in each response.
struct TSStatus {
  1: required i32 code
  2: optional string message
  3: optional list<TSStatus> subStatus
  4: optional TEndPoint redirectNode
}

enum TConsensusGroupType {
  ConfigRegion,
  DataRegion,
  SchemaRegion
}

struct TConsensusGroupId {
  1: required TConsensusGroupType type
  2: required i32 id
}

struct TSeriesPartitionSlot {
  1: required i32 slotId
}

struct TTimePartitionSlot {
  1: required i64 startTime
}

struct TRegionReplicaSet {
  1: required TConsensusGroupId regionId
  2: required list<TDataNodeLocation> dataNodeLocations
}

struct TNodeResource {
  1: required i32 cpuCoreNum
  2: required i64 maxMemory
}

struct TConfigNodeLocation {
  1: required i32 configNodeId
  2: required TEndPoint internalEndPoint
  3: required TEndPoint consensusEndPoint
}

struct TDataNodeLocation {
  1: required i32 dataNodeId
  // TEndPoint for DataNode's client rpc
  2: required TEndPoint clientRpcEndPoint
  // TEndPoint for DataNode's cluster internal rpc
  3: required TEndPoint internalEndPoint
  // TEndPoint for exchange data between DataNodes
  4: required TEndPoint mPPDataExchangeEndPoint
  // TEndPoint for DataNode's dataRegion consensus protocol
  5: required TEndPoint dataRegionConsensusEndPoint
  // TEndPoint for DataNode's schemaRegion consensus protocol
  6: required TEndPoint schemaRegionConsensusEndPoint
}

struct TDataNodeConfiguration {
  1: required TDataNodeLocation location
  2: required TNodeResource resource
}

enum TRegionMigrateFailedType {
  AddPeerFailed,
  RemovePeerFailed,
  RemoveConsensusGroupFailed,
  DeleteRegionFailed,
  CreateRegionFailed
}

struct TFlushReq {
   1: optional string isSeq
   2: optional list<string> storageGroups
}

struct TSettleReq {
   1: required list<string> paths
}

// for node management
struct TSchemaNode {
  1: required string nodeName
  2: required byte nodeType
}

struct TSetTTLReq {
  1: required list<string> storageGroupPathPattern
  2: required i64 TTL
}

// for File
struct TFile {
  1: required string fileName
  2: required binary file
}

struct TFilesResp {
  1: required TSStatus status
  2: required list<TFile> files
}

enum TAggregationType {
  COUNT,
  AVG,
  SUM,
  FIRST_VALUE,
  LAST_VALUE,
  MAX_TIME,
  MIN_TIME,
  MAX_VALUE,
  MIN_VALUE,
  EXTREME,
  COUNT_IF,
  TIME_DURATION
}

// for MLNode
enum TrainingState {
  PENDING,
  RUNNING,
  FINISHED,
  FAILED,
  DROPPING
}

enum ModelTask {
  FORECAST
}

enum EvaluateMetric {
  MSE,
  MAE,
  RMSE
}