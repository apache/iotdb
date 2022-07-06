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
  PartitionRegion,
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

struct TDataNodeInfo {
  1: required TDataNodeLocation location
  2: required i32 cpuCoreNum
  3: required i64 maxMemory
}

// For show regions
struct TRegionInfo {
  1: required TConsensusGroupId consensusGroupId
  2: required string storageGroup
  3: required i32 dataNodeId
  4: required string clientRpcIp
  5: required i32 clientRpcPort
  6: required i64 seriesSlots
  7: required i64 timeSlots
  8: optional string status
}

struct TFlushReq {
   1: optional string isSeq
   2: optional list<string> storageGroups
   3: optional i32 dataNodeId
}

struct TDataNodesInfo {
  1: required i32 dataNodeId
  2: required string status
  3: required string rpcAddresss
  4: required i32 rpcPort
  5: required i32 dataRegionNum
  6: required i32 schemaRegionNum
}

struct TSetTTLReq {
  1: required string storageGroup
  2: required i64 TTL
}
