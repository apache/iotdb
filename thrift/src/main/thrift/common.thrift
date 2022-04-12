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

 struct EndPoint {
   1: required string ip
   2: required i32 port
 }

 // The return status code and message in each response.
 struct TSStatus {
   1: required i32 code
   2: optional string message
   3: optional list<TSStatus> subStatus
   4: optional EndPoint redirectNode
 }

 struct TRegionReplicaSet {
     1: required i32 regionId
     2: required string groupType
     3: required list<EndPoint> endpoint
 }

 struct TSeriesPartitionSlot {
     1: required i32 slotId
 }

 struct TTimePartitionSlot {
     1: required i64 startTime
 }