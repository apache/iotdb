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

struct TDataNodeLocation {
  1: required i32 dataNodeId
  // EndPoint for DataNode's external rpc
  2: required EndPoint externalEndPoint
  // EndPoint for DataNode's internal rpc
  3: required EndPoint internalEndPoint
  // EndPoint for transfering data between DataNodes
  4: required EndPoint dataBlockManagerEndPoint
  // EndPoint for DataNode's ConsensusLayer
  5: required EndPoint consensusEndPoint
}