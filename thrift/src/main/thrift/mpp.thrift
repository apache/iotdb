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

namespace java org.apache.iotdb.mpp.rpc.thrift


struct TFragmentInstanceId {
  1: required string queryId
  2: required i32 fragmentId
  3: required string instanceId
}

struct GetDataBlockRequest {
  1: required TFragmentInstanceId sourceFragmentInstanceId
  2: required i32 startSequenceId
  3: required i32 endSequenceId
}

struct GetDataBlockResponse {
  1: required list<binary> tsBlocks
}

struct AcknowledgeDataBlockEvent {
  1: required TFragmentInstanceId sourceFragmentInstanceId
  2: required i32 startSequenceId
  3: required i32 endSequenceId
}

struct NewDataBlockEvent {
  1: required TFragmentInstanceId targetFragmentInstanceId
  2: required string targetPlanNodeId
  3: required TFragmentInstanceId sourceFragmentInstanceId
  4: required i32 startSequenceId
  5: required list<i64> blockSizes
}

struct EndOfDataBlockEvent {
  1: required TFragmentInstanceId targetFragmentInstanceId
  2: required string targetPlanNodeId
  3: required TFragmentInstanceId sourceFragmentInstanceId
  4: required i32 lastSequenceId
}

struct TFragmentInstance {
  1: required binary body
}

struct TConsensusGroupId {
  1: required i32 id
  2: required string type
}

struct TSendFragmentInstanceReq {
  1: required TFragmentInstance fragmentInstance
  2: required TConsensusGroupId consensusGroupId
  3: required string queryType
}

struct TSendFragmentInstanceResp {
  1: required bool accepted
  2: optional string message
}

struct TFetchFragmentInstanceStateReq {
  1: required TFragmentInstanceId fragmentInstanceId
}

// TODO: need to supply more fields according to implementation
struct TFragmentInstanceStateResp {
  1: required string state
}

struct TCancelQueryReq {
  1: required string queryId
}

struct TCancelPlanFragmentReq {
  1: required string planFragmentId
}

struct TCancelFragmentInstanceReq {
  1: required TFragmentInstanceId fragmentInstanceId
}

struct TCancelResp {
  1: required bool cancelled
  2: optional string messsage
}

struct SchemaFetchRequest {
  1: required binary serializedPathPatternTree
  2: required bool isPrefixMatchPath
}

struct SchemaFetchResponse {
  1: required binary serializedSchemaTree
}

service InternalService {
    TSendFragmentInstanceResp sendFragmentInstance(TSendFragmentInstanceReq req);

    TFragmentInstanceStateResp fetchFragmentInstanceState(TFetchFragmentInstanceStateReq req);

    TCancelResp cancelQuery(TCancelQueryReq req);

    TCancelResp cancelPlanFragment(TCancelPlanFragmentReq req);

    TCancelResp cancelFragmentInstance(TCancelFragmentInstanceReq req);

    SchemaFetchResponse fetchSchema(SchemaFetchRequest req)
}

service DataBlockService {
  GetDataBlockResponse getDataBlock(GetDataBlockRequest req);

  void onAcknowledgeDataBlockEvent(AcknowledgeDataBlockEvent e);

  void onNewDataBlockEvent(NewDataBlockEvent e);

  void onEndOfDataBlockEvent(EndOfDataBlockEvent e);
}
