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

struct SetStorageGroupReq {
    1: required string storageGroup
}

struct DeleteStorageGroupReq {
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
    1: required list<list<i32>> dataNodeIDs
    2: required list<i32> schemaRegionIDs
}

struct GetDataPartitionReq {
    1: required string storageGroup
    2: required map<i32, list<i64>> deviceGroupStartTimeMap
}

struct DataPartitionInfo {
    1: required map<i32, list<list<i32>>> dataNodeIDsMap
    2: required map<i32, list<i32>> dataRegionIDsMap
}

struct DeviceGroupHashInfo {
    1: required i32 deviceGroupCount
    2: required string hashClass
}

struct DataNodeRegisterReq {
    1: required rpc.EndPoint endPoint
}

struct DataNodeRegisterResp {
    1: required rpc.TSStatus registerResult
    2: optional i32 dataNodeID
}

struct DataNodeInfo{
  1: required i32 dataNodeID
  2: required rpc.EndPoint endPoint
}

struct DataNodesInfo {
    1: required map<i32, DataNodeInfo> dataNodesMap
}

service ConfigIService {
  rpc.TSStatus setStorageGroup(SetStorageGroupReq req)

  rpc.TSStatus deleteStorageGroup(DeleteStorageGroupReq req)

  DeviceGroupHashInfo getDeviceGroupHashInfo()

  SchemaPartitionInfo getSchemaPartition(GetSchemaPartitionReq req)

  DataPartitionInfo getDataPartition(GetDataPartitionReq req)

  rpc.TSStatus registerDataNode(DataNodeRegisterReq req)

  DataNodesInfo getDataNodesInfo(i32 dataNodeID)
}