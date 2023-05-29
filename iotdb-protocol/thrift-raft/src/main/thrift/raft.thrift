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
namespace java org.apache.iotdb.consensus.raft.thrift

struct AppendEntriesRequest {
  1: required i64 term // leader's
  2: required common.TEndPoint leader
  3: required list<binary> entries // data
  4: required i64 leaderCommit
  5: required common.TConsensusGroupId groupId
  6: required i32 leaderId
}

struct AppendCompressedEntriesRequest {
  1: required i64 term // leader's
  2: required common.TEndPoint leader
  3: required binary entryBytes // data
  4: required i64 leaderCommit
  5: required common.TConsensusGroupId groupId
  6: required i32 leaderId
  7: required i8 compressionType
  8: required i32 uncompressedSize
}

struct AppendEntryResult {
  1: required i64 status;
  2: optional i64 lastLogIndex;
  3: optional common.TConsensusGroupId groupId;
  4: optional common.TEndPoint receiver;
  5: optional i32 receiverId;
}

// leader -> follower
struct AppendEntryRequest {
  1: required i64 term // leader's
  2: required common.TEndPoint leader
  3: required i64 prevLogIndex
  4: required i64 prevLogTerm
  5: required i64 leaderCommit
  6: required binary entry // data

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  7: optional common.TConsensusGroupId groupId
  8: optional i32 leaderId
}

// leader -> follower
struct HeartBeatRequest {
  1: required i64 term // leader's meta log
  2: required i64 commitLogIndex  // leader's meta log
  3: required i64 commitLogTerm
  4: required common.TEndPoint leader
  // if the leader does not know the follower's id, and require it reports to the leader, then true

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  5: required common.TConsensusGroupId groupId
  6: required i32 leaderId
}

// node -> node
struct ElectionRequest {
  1: required i64 term
  2: required i64 lastLogTerm
  3: required i64 lastLogIndex
  4: required common.TEndPoint elector

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  5: optional common.TConsensusGroupId groupId
  6: optional i32 electorId
}
// follower -> leader
struct HeartBeatResponse {
  1: required i64 term
  2: optional i64 lastLogIndex // follower's meta log
  3: optional i64 lastLogTerm // follower's meta log
  // used to perform a catch up when necessary
  4: required common.TEndPoint follower
  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  5: required common.TConsensusGroupId groupId
  6: required bool installingSnapshot // whether the follower is installing snapshot now
  7: required i64 commitIndex
  8: required i32 followerId
}

struct SendSnapshotRequest {
  1: required binary snapshotBytes
  // for data group
  2: required common.TConsensusGroupId groupId
  3: required common.TEndPoint source
}

struct RequestCommitIndexResponse {
  1: required i64 term // leader's meta log
  2: required i64 commitLogIndex  // leader's meta log
  3: required i64 commitLogTerm
}

struct ExecuteReq {
  1: required binary requestBytes
  2: optional common.TConsensusGroupId groupId
}

exception NoMemberException {
  1: required string message
}

service RaftService {
  /**
  * Leader will call this method to all followers to ensure its authority.
  * <br>For the receiver,
  * The method will check the authority of the leader.
  *
  * @param request information of the leader
  * @return if the leader is valid, HeartBeatResponse.term will set -1, and the follower will tell
  * leader its lastLogIndex; otherwise, the follower will tell the fake leader its term.
  **/
  HeartBeatResponse sendHeartbeat(1:HeartBeatRequest request);

	/**
  * If a node wants to be a leader, it'll call the method to other nodes to get a vote.
  * <br>For the receiver,
  * The method will check whether the node can be a leader.
  *
  * @param voteRequest a candidate that wants to be a leader.
  * @return -1 means agree, otherwise return the voter's term
  **/
  i64 startElection(1:ElectionRequest request);

  /**
  * Leader will call this method to send a batch of entries to all followers.
  * <br>For the receiver,
  * The method will check the authority of the leader and if the local log is complete.
  * If the leader is valid and local log is complete, the follower will append these entries to local log.
  *
  * @param request entries that need to be appended and the information of the leader.
  * @return -1: agree, -2: log index mismatch , otherwise return the follower's term
  **/
  AppendEntryResult appendEntries(1:AppendEntriesRequest request)

  AppendEntryResult appendCompressedEntries(1:AppendCompressedEntriesRequest request)

  common.TSStatus sendSnapshot(1:SendSnapshotRequest request)

  /**
  * Test if a log of "index" and "term" exists.
  **/
  bool matchTerm(1:i64 index, 2:i64 term, 3:common.TConsensusGroupId groupId)

  /**
  * Execute a binarized non-query PhysicalPlan
  **/
  common.TSStatus executeRequest(1:ExecuteReq request)

  void ping()

  /**
   * Ask the leader for its commit index, used to check whether the node has caught up with the
   * leader.
   **/
   RequestCommitIndexResponse requestCommitIndex(1:common.TConsensusGroupId groupId)

   /**
   * Read a chunk of a file from the client. If the remaining of the file does not have enough
   * bytes, only the remaining will be returned.
   **/
   binary readFile(1:string filePath, 2:i64 offset, 3:i32 length)

   common.TSStatus forceElection(1:common.TConsensusGroupId groupId)
}