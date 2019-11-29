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

include "rpc.thrift"
namespace java org.apache.iotdb.cluster.rpc.thrift

typedef i32 int 
typedef i16 short
typedef i64 long

// leader -> follower
struct HeartBeatRequest {
  1: required long term // leader's meta log
  2: required long commitLogIndex  // leader's meta log
  3: required Node leader
  4: required bool requireIdentifier
  5: required bool regenerateIdentifier
  // serialized partitionTable
  6: optional binary partitionTableBytes

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  7: optional Node header
}

// follower -> leader
struct HeartBeatResponse {
  1: required long term
  2: optional long lastLogIndex // follower's meta log
  3: optional long lastLogTerm // follower's meta log
  // used to perform a catch up when necessary
  4: optional Node follower
  5: optional int followeIdentifier
  6: required bool requirePartitionTable

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  7: optional Node header
}

// node -> node
struct ElectionRequest {
  1: required long term
  2: required long lastLogTerm
  3: required long lastLogIndex

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  4: optional Node header
  5: optional long dataLogLastIndex
  6: optional long dataLogLastTerm
}

// leader -> follower
struct AppendEntryRequest {
  1: required long term // leader's
  2: required binary entry // data

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  3: optional Node header
}

// leader -> follower
struct AppendEntriesRequest {
  1: required long term // leader's
  2: required list<binary> entries // data

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  3: optional Node header
}

struct AddNodeResponse {
  // -1: accept to add new node or the node is already in this cluster, otherwise: fail to
  // add new node
  1: required int respNum
  2: optional binary partitionTableBytes
}

struct Node {
  1: required string ip
  2: required int port
  3: required int nodeIdentifier
  4: required int dataPort
}

struct SendSnapshotRequest {
  1: required binary snapshotBytes
  // for data group
  2: optional Node header
}

struct PullSnapshotRequest {
  1: required int requiredSocket
  // for data group
  2: optional Node header
}

struct PullSnapshotResp {
  1: optional binary snapshotBytes
}

struct ExecutNonQueryReq {
  1: required binary planBytes
  2: optional Node header
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
	HeartBeatResponse sendHeartBeat(1:HeartBeatRequest request);

	/**
  * If a node wants to be a leader, it'll call the method to other nodes to get a vote.
  * <br>For the receiver,
  * The method will check whether the node can be a leader.
  *
  * @param voteRequest a candidate that wants to be a leader.
  * @return -1 means agree, otherwise return the voter's term
  **/
  long startElection(1:ElectionRequest electionRequest);

  /**
  * Leader will call this method to send a batch of entries to all followers.
  * <br>For the receiver,
  * The method will check the authority of the leader and if the local log is complete.
  * If the leader is valid and local log is complete, the follower will append these entries to local log.
  *
  * @param request entries that need to be appended and the information of the leader.
  * @return -1: agree, -2: log index mismatch , otherwise return the follower's term
  **/
  long appendEntries(1:AppendEntriesRequest request)

  /**
  * Leader will call this method to send a entry to all followers.
  * <br>For the receiver,
  * The method will check the authority of the leader and if the local log is complete.
  * If the leader is valid and local log is complete, the follower will append the entry to local log.
  *
  * @param request entry that needs to be appended and the information of the leader.
  * @return -1: agree, -2: log index mismatch , otherwise return the follower's term
  **/
  long appendEntry(1:AppendEntryRequest request)

  void sendSnapshot(1:SendSnapshotRequest request)

  PullSnapshotResp pullSnapshot(1:PullSnapshotRequest request)

  /**
  * execute a binarized non-query PhysicalPlan
  **/
  rpc.TSStatus executeNonQueryPlan(1:ExecutNonQueryReq request)
}



service TSDataService extends RaftService {


}

service TSMetaService extends RaftService {
  /**
  * Node which is not leader will call this method to try to add itself into the cluster as a new node.
  * <br>For the receiver,
  * If the local node is leader, it'll check whether the cluster can add this new node;
  * otherwise, the local node will transfer the request to the leader.
  *
  * @param node a new node that needs to be added
  **/
  AddNodeResponse addNode(1: Node node)
}
