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
namespace java org.apache.iotdb.consensus.multileader.thrift

struct TLogBatch {
  1: required binary data
  2: required i64 searchIndex
  3: required bool fromWAL
}

struct TSyncLogReq {
  # source peer where the TSyncLogReq is generated
  1: required string peerId
  2: required common.TConsensusGroupId consensusGroupId
  3: required list<TLogBatch> batches
}

struct TSyncLogRes {
  1: required list<common.TSStatus> status
}

service MultiLeaderConsensusIService {
  TSyncLogRes syncLog(TSyncLogReq req)
}