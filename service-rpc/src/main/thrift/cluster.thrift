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
  // if the leader does not know the follower's id, and require it reports to the leader, then true
  4: required bool requireIdentifier
  5: required bool regenerateIdentifier //if the leader finds the follower's id is conflicted, then true
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
  5: optional int followerIdentifier
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
  4: required Node elector

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  5: optional Node header
  6: optional long dataLogLastIndex
  7: optional long dataLogLastTerm
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
  2: required int metaPort
  3: required int nodeIdentifier
  4: required int dataPort
}

struct SendSnapshotRequest {
  1: required binary snapshotBytes
  // for data group
  2: optional Node header
}

struct PullSnapshotRequest {
  1: required list<int> requiredSlots
  // for data group
  2: optional Node header
}

struct PullSnapshotResp {
  1: optional map<int, binary> snapshotBytes
}

struct ExecutNonQueryReq {
  1: required binary planBytes
  2: optional Node header
}

struct PullSchemaRequest {
  1: required string prefixPath
  2: optional Node header
}

struct PullSchemaResp {
  1: required binary schemaBytes
}

struct SingleSeriesQueryRequest {
  1: required string path
  2: optional binary filterBytes
  3: required long queryId
  4: required Node requester
  5: optional bool pushdownUnseq
  6: required Node header
  7: optional bool withValueFilter
  8: required int dataTypeOrdinal
}

// the spec and load of a node, for query coordinating
struct TNodeStatus {

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
  long startElection(1:ElectionRequest request);

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

  /**
  * Execute a binarized non-query PhysicalPlan
  **/
  rpc.TSStatus executeNonQueryPlan(1:ExecutNonQueryReq request)

  /**
  * Ask the leader for its commit index, used to check whether the node has caught up with the
  * leader.
  **/
  long requestCommitIndex(1:Node header)

  /**
  * Pull all timeseries schemas prefixed by a given path.
  **/
  PullSchemaResp pullTimeSeriesSchema(1: PullSchemaRequest request)

  binary readFile(1:string filePath, 2:i64 offset, 3:i32 length, 4:Node header)
}



service TSDataService extends RaftService {

  /**
  * Query a time series without value filter.
  * @return a readerId >= 0 if the query succeeds, otherwise the query fails
  * TODO-Cluster: support query multiple series in a request
  **/
  long querySingleSeries(1:SingleSeriesQueryRequest request)

  /**
  * Fetch at max fetchSize time-value pairs using the resultSetId generated by querySingleSeries.
  * @return a ByteBuffer containing the serialized time-value pairs or an empty buffer if there
  * are not more results.
  **/
  binary fetchSingleSeries(1:Node header, 2:long readerId)

   /**
   * Query a time series and generate an IReaderByTimestamp.
   * @return a readerId >= 0 if the query succeeds, otherwise the query fails
   **/
  long querySingleSeriesByTimestamp(1:SingleSeriesQueryRequest request)

   /**
   * Fetch one value at given timestamp using the resultSetId generated by
   * querySingleSeriesByTimestamp.
   * @return a ByteBuffer containing the serialized value or an empty buffer if there
   * are not more results.
   * TODO-Cluster: make this a batched interface
   **/
   binary fetchSingleSeriesByTimestamp(1:Node header, 2:long readerId, 3:long timestamp)

  /**
  * Find the local query established for the remote query and release all its resource.
  **/
  void endQuery(1:Node header, 2:Node thisNode, 3:long queryId)

  /**
  * Given a path pattern (path with wildcard), return all paths it matches.
  **/
  list<string> getAllPaths(1:Node header, 2:string path)

  PullSnapshotResp pullSnapshot(1:PullSnapshotRequest request)
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

  /**
  * Remove a node from the cluster. If the node is not in the cluster or the cluster size will
  * less than replication number, the request will be rejected.
  * return -1(RESPONSE_AGREE) or -3(RESPONSE_REJECT) or -9(RESPONSE_CLUSTER_TOO_SMALL)
  **/
  long removeNode(1: Node node)

  TNodeStatus queryNodeStatus()

  Node checkAlive()
}
