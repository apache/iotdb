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
  3: required Node leader
}

// follower -> leader
struct HeartBeatResponse {
  1: required long term // follower's
  2: optional long lastLogIndex // follower's
  // used to perform a catch up when necessary
  3: optional Node follower
}

// node -> node
struct ElectionRequest {
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

// leader -> follower
struct AppendEntriesRequest {
  1: required long term // leader's
  2: required list<binary> entries // data
  3: required long previousLogTerm // leader's previous log term
  4: required long previousLogIndex // leader's
  5: required long leaderID
}

struct Node{
  1: required string ip
  2: required int port
}

service RaftService {
/**
  * Leader will call this method to all followers to ensure its authority.
  * <br>For the receiver,
  * The method will check the authority of the leader.
  *
  * @param request information of the leader
  * @return if the leader is valid, HeartBeatResponse.term will set -1, and the follower will tell
  * leader its lastLogIndex;
  * otherwise, the follower will tell the fake leader its term.
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
}

service TSDataService extends RaftService{

	/**
  * Leader will call this method to send a batch of entries to all followers.
  * <br>For the receiver,
  * The method will check the authority of the leader and if the local log is complete.
  * If the leader is valid and local log is complete, the follower will append these entries to local log.
  *
  * @param request entries that need to be appended and the information of the leader.
  * @return -1: agree, -2: log index mismatch , otherwise return the follower's term
  **/
  long appendDataEntries(1:AppendEntriesRequest request)

  /**
  * Leader will call this method to send a entry to all followers.
  * <br>For the receiver,
  * The method will check the authority of the leader and if the local log is complete.
  * If the leader is valid and local log is complete, the follower will append the entry to local log.
  *
  * @param request entry that needs to be appended and the information of the leader.
  * @return -1: agree, -2: log index mismatch , otherwise return the follower's term
  **/
    long appendDataEntry(1:AppendEntryRequest request)
}

service TSMetaService extends RaftService {

/**
* Leader will call this method to send a entry to all followers.
* <br>For the receiver,
* The method will check the authority of the leader and if the local log is complete.
* If the leader is valid and local log is complete, the follower will append the entry to local log.
*
* @param request entry that needs to be appended and the information of the leader.
* @return -1: agree, -2: log index mismatch , otherwise return the follower's term
**/
  long appendMetadataEntry(1:AppendEntryRequest request)

/**
* Leader will call this method to send a batch of entries to all followers.
* <br>For the receiver,
* The method will check the authority of the leader and if the local log is complete.
* If the leader is valid and local log is complete, the follower will append these entries to local log.
*
* @param request entries that need to be appended and the information of the leader.
* @return -1: agree, -2: log index mismatch , otherwise return the follower's term
**/
  long appendMetadataEntries(1:AppendEntriesRequest request)

/**
* Node which is not leader will call this method to try to add itself into the cluster as a new node.
* <br>For the receiver,
* If the local node is leader, it'll check whether the cluster can add this new node;
* otherwise, the local node will transfer the request to the leader.
*
* @param node a new node that needs to be added
* @return 1: accept to add new node, 0: the node is already in this cluster, -1: fail to add new node
**/
  int addNode(1: Node node)
}
