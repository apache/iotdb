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

struct TAIHeartbeatReq {
  1: required i64 heartbeatTimestamp
  2: required bool needSamplingLoad
  3: optional bool activated
}

struct TAIHeartbeatResp {
  1: required i64 heartbeatTimestamp
  2: required string status
  3: optional string statusReason
  4: optional common.TLoadSample loadSample
  5: optional string activateStatus
}

struct TRegisterModelReq {
  1: required string modelId
  2: required string uri
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
  3: optional map<string, string> inferenceAttributes
}

struct TInferenceResp {
  1: required common.TSStatus status
  2: optional list<binary> inferenceResult
}

struct IDataSchema {
  1: required string schemaName
  2: optional list<i64> timeRange
}

struct TTuningReq {
  1: required string dbType
  2: required string modelId
  3: required string existingModelId
  4: optional list<IDataSchema> targetDataSchema;
  5: optional map<string, string> parameters;
}

struct TForecastReq {
  1: required string modelId
  2: required binary inputData
  3: required i32 outputLength
  4: optional string historyCovs
  5: optional string futureCovs
  6: optional map<string, string> options
}

struct TForecastResp {
  1: required common.TSStatus status
  2: optional binary forecastResult
}

struct TShowModelsReq {
  1: optional string modelId
}

struct TShowModelsResp {
  1: required common.TSStatus status
  2: optional list<string> modelIdList
  3: optional map<string, string> modelTypeMap
  4: optional map<string, string> categoryMap
  5: optional map<string, string> stateMap
}

struct TShowLoadedModelsReq {
  1: required list<string> deviceIdList
}

struct TShowLoadedModelsResp {
    1: required common.TSStatus status
    2: required map<string, map<string, i32>> deviceLoadedModelsMap
}

struct TShowAIDevicesResp {
    1: required common.TSStatus status
    2: required map<string, string> deviceIdMap
}

struct TLoadModelReq {
  1: required string existingModelId
  2: required list<string> deviceIdList
}

struct TUnloadModelReq {
  1: required string modelId
  2: required list<string> deviceIdList
}

service IAINodeRPCService {

  common.TSStatus stopAINode()

  TAIHeartbeatResp getAIHeartbeat(TAIHeartbeatReq req)

  TShowAIDevicesResp showAIDevices()

  TShowModelsResp showModels(TShowModelsReq req)

  TShowLoadedModelsResp showLoadedModels(TShowLoadedModelsReq req)

  common.TSStatus deleteModel(TDeleteModelReq req)

  TRegisterModelResp registerModel(TRegisterModelReq req)

  common.TSStatus loadModel(TLoadModelReq req)

  common.TSStatus unloadModel(TUnloadModelReq req)

  TInferenceResp inference(TInferenceReq req)

  TForecastResp forecast(TForecastReq req)

  common.TSStatus createTuningTask(TTuningReq req)
}