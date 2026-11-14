/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
namespace java org.apache.iotdb.consensus.traft.paper.thrift
include "common.thrift"

struct TPaperBatch {
  1: required common.TConsensusGroupId groupId
  2: required list<binary> messages
}
struct TPaperWrite {
  1: required common.TConsensusGroupId groupId
  2: required string requestId
  3: required bool barrier
  4: required i64 timestamp
  5: required binary payload
}
struct TPaperControl {
  1: required common.TConsensusGroupId groupId
  2: required string operation
  3: optional i32 target
}
service TRaftPaperService {
  // Replies addressed to the sender (acknowledgments, votes, reports, repair requests)
  // return in the response so that one round trip carries a request and its answer.
  list<binary> deliver(1: TPaperBatch request)
  common.TSStatus write(1: TPaperWrite request)
  common.TSStatus control(1: TPaperControl request)
}
