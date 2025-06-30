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
  3: optional TWindowParams windowParams
  4: optional map<string, string> inferenceAttributes
}

struct TWindowParams {
  1: required i32 windowInterval
  2: required i32 windowStep
}

struct TInferenceResp {
  1: required common.TSStatus status
  2: required list<binary> inferenceResult
}

struct IDataSchema {
  1: required string schemaName
  2: optional list<i64> timeRange
}

struct TTrainingReq {
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
  4: optional map<string, string> options
}

struct TForecastResp {
  1: required common.TSStatus status
  2: required binary forecastResult
}

struct TShowModelsResp {
  1: required common.TSStatus status
  2: optional list<string> modelIdList
  3: optional map<string, string> modelTypeMap
  4: optional map<string, string> categoryMap
  5: optional map<string, string> stateMap
}

service IAINodeRPCService {

  // -------------- For Config Node --------------
  TShowModelsResp showModels()

  common.TSStatus deleteModel(TDeleteModelReq req)

  TRegisterModelResp registerModel(TRegisterModelReq req)

  TAIHeartbeatResp getAIHeartbeat(TAIHeartbeatReq req)

  common.TSStatus createTrainingTask(TTrainingReq req)

  // -------------- For Data Node --------------

  TInferenceResp inference(TInferenceReq req)

  TForecastResp forecast(TForecastReq req)
}