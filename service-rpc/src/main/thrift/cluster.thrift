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
namespace java org.apache.iotdb.cluster.rpc.thrift


typedef i32 int 
typedef i16 short
typedef i64 long

// leader -> follower
struct HeartBeatRequest {
  1: required long term // leader's
  2: required long commitLogIndex  // leader's
}

// follower -> leader
struct HeartBeatResponse {
  1: required long term // follower's
  2: required long lastLogIndex // follower's
}

// node -> node
struct VoteRequest {
  1: required long term
  2: required long lastLogTerm
  3: required long lastLogIndex
}

// leader -> follower
struct AppendEntryRequest {
  1: required long term // leader's
  2: required binary entry // data
  3: required long previousLogTerm // leader's previous log term
  4: required long previousLogIndex // leader's
  5: required long leaderID
}

// follower -> leader
struct AppendEntryResponse {
  1: required long  // -1 means you are leader, otherwise return the voter's term
  2: required bool success
}

// leader -> follower
struct AppendEntriesRequest {
  1: required long term // leader's
  2: required list<binary> entries // data
  3: required long previousLogTerm // leader's previous log term
  4: required long previousLogIndex // leader's
  5: required long leaderID
}

service TSIService {

	HeartBeatResponse sendHeartBeat(1:HeartBeatRequest request);

	// -1 means agree, otherwise return the voter's term
	long startVote(1:VoteRequest voteRequest);

  // -1: agree, -2: log index mismatch , otherwise return the follower's term
  long appendEntry(1:AppendEntryRequest request)

  // -1: agree, -2: log index mismatch , otherwise return the follower's term
  long appendEntries(1:AppendEntriesRequest request)

}
