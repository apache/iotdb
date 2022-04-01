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

struct DataNodeRegisterReq {
    1: required rpc.EndPoint endPoint
}

struct DataNodeRegisterResp {
    1: required rpc.TSStatus registerResult
    2: optional i32 dataNodeID
}

struct DataNodeMessage {
  1: required i32 dataNodeID
  2: required rpc.EndPoint endPoint
}

struct SetStorageGroupReq {
    1: required string storageGroup
}

struct DeleteStorageGroupReq {
    1: required string storageGroup
}

struct StorageGroupMessage {
    1: required string storageGroup
}

struct GetDeviceGroupIDReq {
    1: required string device
}

struct GetSchemaPartitionReq {
    1: required string storageGroup
    2: required list<i32> deviceGroupIDs
}

struct SchemaPartitionInfo {
    1: required map<i32, i32> deviceGroupSchemaRegionGroupMap
    2: required map<i32, list<i32>> SchemaRegionGroupDataNodeMap
}

struct GetDataPartitionReq {
    1: required string storageGroup
    2: required map<i32, list<i64>> deviceGroupStartTimeMap
}

struct DataPartitionInfo {
    1: required map<i32, map<i64, list<i32>>> deviceGroupStartTimeDataRegionGroupMap
    2: required map<i32, list<i32>> dataRegionGroupDataNodeMap
}

struct DeviceGroupHashInfo {
    1: required i32 deviceGroupCount
    2: required string hashClass
}

struct CreateUserReq{
    1: required string username
    2: required string password
}

struct FetchDataPartitionReq {
    1: required map<i32, list<i64>> deviceGroupIDToStartTimeMap
}

struct FetchSchemaPartitionReq {
    1: required list<string> devicePaths
}

struct FetchPartitionReq {
    1: required map<i32, list<i64>> deviceGroupIDToStartTimeMap
}

struct RegionInfo {
    1: required i32 regionId
    2: required list<rpc.EndPoint> endPointList
}

struct DataPartitionInfoResp {
    // Map<StorageGroup, Map<DeviceGroupID, Map<TimePartitionId, List<DataRegionReplicaInfo>>>>
    1: required map<string, map<i32, map<i64, list<RegionInfo>>>> dataPartitionMap
}

struct SchemaPartitionInfoResp {
    // Map<StorageGroup, Map<DeviceGroupID, SchemaRegionPlaceInfo>>
    1: required map<string, map<i32, RegionInfo>> schemaPartitionInfo
}

struct PartitionInfoResp {
    // Map<StorageGroup, Map<DeviceGroupID, Map<TimePartitionId, List<DataRegionReplicaInfo>>>>
    1: required map<string, map<i32, map<i64, list<RegionInfo>>>> dataPartitionMap
    // Map<StorageGroup, Map<DeviceGroupID, SchemaRegionPlaceInfo>>
    2: required map<string, map<i32, RegionInfo>> schemaPartitionInfo
}

service ConfigIService {
  // Return TSStatusCode.SUCCESS_STATUS and the register DataNode id when successful registered.
  // Otherwise, return TSStatusCode.INTERNAL_SERVER_ERROR
  DataNodeRegisterResp registerDataNode(DataNodeRegisterReq req)

  map<i32, DataNodeMessage> getDataNodesMessage(i32 dataNodeID)

  rpc.TSStatus setStorageGroup(SetStorageGroupReq req)

  rpc.TSStatus deleteStorageGroup(DeleteStorageGroupReq req)

  map<string, StorageGroupMessage> getStorageGroupsMessage()

  // Gets SchemaRegions for DeviceGroups in a StorageGroup
  SchemaPartitionInfo getSchemaPartition(GetSchemaPartitionReq req)

  // Gets DataRegions for DeviceGroups in a StorageGroup at different starttime
  DataPartitionInfo getDataPartition(GetDataPartitionReq req)

  DeviceGroupHashInfo getDeviceGroupHashInfo()

  rpc.TSStatus createUser(CreateUserReq req)

  DataPartitionInfoResp fetchDataPartitionInfo(FetchDataPartitionReq req)

  SchemaPartitionInfoResp fetchSchemaPartitionInfo(FetchSchemaPartitionReq req)

  PartitionInfoResp fetchPartitionInfo(FetchPartitionReq req)

}