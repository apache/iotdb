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

include "rpc.thrift"
namespace java org.apache.iotdb.confignode.rpc.thrift
namespace py iotdb.thrift.confignode

// TODO: using thrift-common
struct TRegionReplicaSet {
    1: required i32 regionId
    2: required string groupType
    3: required list<rpc.EndPoint> endpoint
}

struct TSeriesPartitionSlot {
    1: required i32 slotId
}

struct TTimePartitionSlot {
    1: required i64 startTime
}

// DataNode
struct TDataNodeRegisterReq {
    1: required rpc.EndPoint endPoint
}

struct TGlobalConfig {
    1: optional string dataNodeConsensusProtocolClass
    2: optional i32 seriesPartitionSlotNum
    3: optional string seriesPartitionSlotExecutorClass
}

struct TDataNodeRegisterResp {
    1: required rpc.TSStatus status
    2: optional i32 dataNodeID
    3: optional TGlobalConfig globalConfig
}

struct TDataNodeMessageResp {
  1: required rpc.TSStatus status
  // map<DataNodeId, DataNodeMessage>
  2: optional map<i32, TDataNodeMessage> dataNodeMessageMap
}

struct TDataNodeMessage {
  1: required i32 dataNodeId
  2: required rpc.EndPoint endPoint
}

// StorageGroup
struct TSetStorageGroupReq {
    1: required string storageGroup
    2: optional i64 ttl
}

struct TDeleteStorageGroupReq {
    1: required string storageGroup
}

struct TStorageGroupMessageResp {
  1: required rpc.TSStatus status
  // map<string, StorageGroupMessage>
  2: optional map<string, TStorageGroupMessage> storageGroupMessageMap
}

struct TStorageGroupMessage {
    1: required string storageGroup
}

// Schema
struct TSchemaPartitionReq {
    1: required binary pathPatternTree
}

struct TSchemaPartitionResp {
  1: required rpc.TSStatus status
    // map<StorageGroupName, map<TSeriesPartitionSlot, TRegionReplicaSet>>
  2: optional map<string, map<TSeriesPartitionSlot, TRegionReplicaSet>> schemaRegionMap
}

// Data
struct TDataPartitionReq {
    // map<StorageGroupName, map<TSeriesPartitionSlot, list<TTimePartitionSlot>>>
    1: required map<string, map<TSeriesPartitionSlot, list<TTimePartitionSlot>>> partitionSlotsMap
}

struct TDataPartitionResp {
  1: required rpc.TSStatus status
  // map<StorageGroupName, map<TSeriesPartitionSlot, map<TTimePartitionSlot, list<TRegionReplicaSet>>>>
  2: optional map<string, map<TSeriesPartitionSlot, map<TTimePartitionSlot, list<TRegionReplicaSet>>>> dataPartitionMap
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

service ConfigIService {

  /* DataNode */

  TDataNodeRegisterResp registerDataNode(TDataNodeRegisterReq req)

  TDataNodeMessageResp getDataNodesMessage(i32 dataNodeID)

  /* StorageGroup */

  rpc.TSStatus setStorageGroup(TSetStorageGroupReq req)

  rpc.TSStatus deleteStorageGroup(TDeleteStorageGroupReq req)

  TStorageGroupMessageResp getStorageGroupsMessage()

  /* Schema */

  TSchemaPartitionResp getSchemaPartition(TSchemaPartitionReq req)

  TSchemaPartitionResp getOrCreateSchemaPartition(TSchemaPartitionReq req)

  /* Data */

  TDataPartitionResp getDataPartition(TDataPartitionReq req)

  TDataPartitionResp getOrCreateDataPartition(TDataPartitionReq req)

  /* Authorize */
  rpc.TSStatus operatePermission(TAuthorizerReq req)

}