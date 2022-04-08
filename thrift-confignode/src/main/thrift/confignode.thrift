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

// DataNode
struct DataNodeRegisterReq {
    1: required rpc.EndPoint endPoint
}

struct DataNodeRegisterResp {
    1: required rpc.TSStatus status
    2: optional i32 dataNodeID
    3: optional string consensusType
    4: optional i32 seriesPartitionSlotNum
    5: optional string seriesPartitionSlotExecutorClass
}

struct DataNodeMessageResp {
  1: required rpc.TSStatus status
  // map<dataNodeId, DataNodeMessage>
  2: optional map<i32, DataNodeMessage> dataNodeMessageMap
}

struct DataNodeMessage {
  1: required i32 dataNodeId
  2: required rpc.EndPoint endPoint
}

// StorageGroup
struct SetStorageGroupReq {
    1: required string storageGroup
    2: optional i64 ttl
}

struct DeleteStorageGroupReq {
    1: required string storageGroup
}

struct StorageGroupMessageResp {
  1: required rpc.TSStatus status
  // map<string, StorageGroupMessage>
  2: optional map<string, StorageGroupMessage> storageGroupMessageMap
}

struct StorageGroupMessage {
    1: required string storageGroup
}

// Region
struct RegionMessage {
    1: required i32 regionId
    2: required list<rpc.EndPoint> endpoint
}

// Schema
struct FetchSchemaPartitionReq {
    // Full paths of devices
    1: required list<string> devicePaths
}

struct ApplySchemaPartitionReq {
    1: required string storageGroup
    2: required list<i32> seriesPartitionSlots
}

struct SchemaPartitionResp {
  1: required rpc.TSStatus status
    // map<StorageGroupName, map<SeriesPartitionSlot, RegionMessage>>
  2: optional map<string, map<i32, RegionMessage>> schemaRegionMap
}

// Data
struct FetchDataPartitionReq {
    1: required map<i32, list<i64>> deviceGroupIDToStartTimeMap
}

struct ApplyDataPartitionReq {
    1: required string storageGroup
    // map<SeriesPartitionSlot, list<TimePartitionSlot>>
    2: required map<i32, list<i64>> seriesPartitionTimePartitionSlots
}

struct DataPartitionResp {
  1: required rpc.TSStatus status
  // map<StorageGroupName, map<SeriesPartitionSlot, map<TimePartitionSlot, list<RegionMessage>>>>
  2: required map<string, map<i32, map<i64, list<RegionMessage>>>> dataPartitionMap
}

service ConfigIService {

  /* DataNode */

  DataNodeRegisterResp registerDataNode(DataNodeRegisterReq req)

  DataNodeMessageResp getDataNodesMessage(i32 dataNodeID)

  /* StorageGroup */

  rpc.TSStatus setStorageGroup(SetStorageGroupReq req)

  rpc.TSStatus deleteStorageGroup(DeleteStorageGroupReq req)

  StorageGroupMessageResp getStorageGroupsMessage()

  /* Schema */

  SchemaPartitionResp fetchSchemaPartition(FetchSchemaPartitionReq req)

  SchemaPartitionResp applySchemaPartition(ApplySchemaPartitionReq req)

  /* Data */

  DataPartitionResp fetchDataPartition(FetchDataPartitionReq req)

  DataPartitionResp applyDataPartition(ApplyDataPartitionReq req)

}