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
namespace java org.apache.iotdb.mlnode.rpc.thrift
namespace py iotdb.thrift.mlnode

struct TCreateTrainingTaskReq {
  1: required string modelId
  2: required bool isAuto
  3: required map<string, string> modelConfigs
  4: required list<string> queryExpressions
  5: optional string queryFilter
}

struct TDeleteModelReq {
  1: required string modelId
}

struct TForecastReq {
  1: required string modelPath
  2: required binary dataset
}

struct TForecastResp {
  1: required common.TSStatus status
  2: required binary forecastResult
}

service IMLNodeRPCService {

  // -------------- For Config Node --------------

  common.TSStatus createTrainingTask(TCreateTrainingTaskReq req)

  common.TSStatus deleteModel(TDeleteModelReq req)

  // -------------- For Data Node --------------

  TForecastResp forecast(TForecastReq req)
}