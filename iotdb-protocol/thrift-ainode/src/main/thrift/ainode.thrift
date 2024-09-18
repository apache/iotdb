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
namespace java org.apache.iotdb.ainode.rpc.thrift
namespace py iotdb.thrift.ainode

struct TDeleteModelReq {
  1: required string modelId
}

struct TAIHeartbeatReq{
  1: required i64 heartbeatTimestamp
  2: required bool needSamplingLoad
}

struct TAIHeartbeatResp{
  1: required i64 heartbeatTimestamp
  2: required string status
  3: optional string statusReason
  4: optional common.TLoadSample loadSample
}

struct TRegisterModelReq {
  1: required string uri
  2: required string modelId
}

struct TConfigs {
  1: required list<i32> input_shape
  2: required list<i32> output_shape
  3: required list<byte> input_type
  4: required list<byte> output_type
}

struct TRegisterModelResp {
  1: required common.TSStatus status
  2: optional TConfigs configs
  3: optional string attributes
}

struct TInferenceReq {
  1: required string modelId
  2: required binary dataset
  3: required list<string> typeList
  4: required list<string> columnNameList
  5: required map<string, i32> columnNameIndexMap
  6: optional TWindowParams windowParams
  7: optional map<string, string> inferenceAttributes
}

struct TWindowParams {
  1: required i32 windowInterval
  2: required i32 windowStep
}

struct TInferenceResp {
  1: required common.TSStatus status
  2: required list<binary> inferenceResult
}

service IAINodeRPCService {

  // -------------- For Config Node --------------

  common.TSStatus deleteModel(TDeleteModelReq req)

  TRegisterModelResp registerModel(TRegisterModelReq req)

  TAIHeartbeatResp getAIHeartbeat(TAIHeartbeatReq req)

  // -------------- For Data Node --------------

  TInferenceResp inference(TInferenceReq req)
}