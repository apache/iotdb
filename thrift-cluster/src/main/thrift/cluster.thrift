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
namespace py iotdb.thrift.cluster

typedef i32 int
typedef i64 long

// TODO-Cluster: update rpc change list when ready to merge
// leader -> follower
struct HeartBeatRequest {
  1: required long term // leader's meta log
  2: required long commitLogIndex  // leader's meta log
  3: required long commitLogTerm
  4: required Node leader
  // if the leader does not know the follower's id, and require it reports to the leader, then true
  5: required bool requireIdentifier
  6: required bool regenerateIdentifier //if the leader finds the follower's id is conflicted,
  // then true
  // serialized partitionTable
  7: optional binary partitionTableBytes

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  8: optional Node header
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

struct RequestCommitIndexResponse {
  1: required long term // leader's meta log
  2: required long commitLogIndex  // leader's meta log
  3: required long commitLogTerm
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
  2: required Node leader
  3: required long prevLogIndex
  4: required long prevLogTerm
  5: required long leaderCommit
  6: required binary entry // data

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  7: optional Node header
}

// leader -> follower
struct AppendEntriesRequest {
  1: required long term // leader's
  2: required Node leader
  3: required list<binary> entries // data
  4: required long prevLogIndex
  5: required long prevLogTerm
  6: required long leaderCommit

  // because a data server may play many data groups members, this is used to identify which
  // member should process the request or response. Only used in data group communication.
  7: optional Node header
}

struct AddNodeResponse {
  // -1: accept to add new node or the node is already in this cluster, otherwise: fail to
  // add new node
  1: required int respNum
  2: optional binary partitionTableBytes
  3: optional CheckStatusResponse checkStatusResponse
}

struct Node {
  // Used for communication between cluster nodes, eg heartbeat、raft logs and snapshots etc.
  1: required string internalIp
  2: required int metaPort
  3: required int nodeIdentifier
  4: required int dataPort
  5: required int clientPort
  // Used for communication between client and server, when the cluster is set up for the first time,
  // the clientIp of other nodes is unknown
  6: required string clientIp
}

// leader -> follower
struct StartUpStatus {
  1: required long partitionInterval
  2: required int hashSalt
  3: required int replicationNumber
  4: required list<Node> seedNodeList
  5: required string clusterName
}

// follower -> leader
struct CheckStatusResponse {
  1: required bool partitionalIntervalEquals
  2: required bool hashSaltEquals
  3: required bool replicationNumEquals
  4: required bool seedNodeEquals
  5: required bool clusterNameEquals
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
  // set to true if the previous holder has been removed from the cluster.
  // This will make the previous holder read-only so that different new
  // replicas can pull the same snapshot.
  3: required bool requireReadOnly
}

struct PullSnapshotResp {
  1: optional map<int, binary> snapshotBytes
}

struct ExecutNonQueryReq {
  1: required binary planBytes
  2: optional Node header
}

struct PullSchemaRequest {
  1: required list<string> prefixPaths
  2: optional Node header
}

struct PullSchemaResp {
  1: required binary schemaBytes
}

struct SingleSeriesQueryRequest {
  1: required string path
  2: optional binary timeFilterBytes
  3: optional binary valueFilterBytes
  4: required long queryId
  5: required Node requester
  6: required Node header
  7: required int dataTypeOrdinal
  8: required set<string> deviceMeasurements
  9: required bool ascending
  10: required int fetchSize
  11: required int deduplicatedPathNum
}

struct MultSeriesQueryRequest {
  1: required list<string> path
  2: optional binary timeFilterBytes
  3: optional binary valueFilterBytes
  4: required long queryId
  5: required Node requester
  6: required Node header
  7: required list<int> dataTypeOrdinal
  8: required map<string,set<string>> deviceMeasurements
  9: required bool ascending
  10: required int fetchSize
  11: required int deduplicatedPathNum
}

struct PreviousFillRequest {
  1: required string path
  2: required long queryTime
  3: required long beforeRange
  4: required long queryId
  5: required Node requester
  6: required Node header
  7: required int dataTypeOrdinal
  8: required set<string> deviceMeasurements
}

// the spec and load of a node, for query coordinating
struct TNodeStatus {

}

struct GetAggrResultRequest {
  1: required string path
  2: required list<string> aggregations
  3: required int dataTypeOrdinal
  4: optional binary timeFilterBytes
  5: required Node header
  6: required long queryId
  7: required Node requestor
  8: required set<string> deviceMeasurements
  9: required bool ascending
}

struct GroupByRequest {
  1: required string path
  2: required int dataTypeOrdinal
  3: optional binary timeFilterBytes
  4: required long queryId
  5: required list<int> aggregationTypeOrdinals
  6: required Node header
  7: required Node requestor
  8: required set<string> deviceMeasurements
  9: required bool ascending
}

struct LastQueryRequest {
  1: required list<string> paths
  2: required list<int> dataTypeOrdinals
  3: required long queryId
  4: required map<string, set<string>> deviceMeasurements
  5: optional binary filterBytes
  6: required Node header
  7: required Node requestor
}

struct GetAllPathsResult {
  1: required list<string> paths
  2: optional list<string> aliasList
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
  RequestCommitIndexResponse requestCommitIndex(1:Node header)


  /**
  * Read a chunk of a file from the client. If the remaining of the file does not have enough
  * bytes, only the remaining will be returned.
  * Notice that when the last chunk of the file is read, the file will be deleted immediately.
  **/
  binary readFile(1:string filePath, 2:long offset, 3:int length)

  /**
  * Test if a log of "index" and "term" exists.
  **/
  bool matchTerm(1:long index, 2:long term, 3:Node header)

  /**
  * When a follower finds that it already has a file in a snapshot locally, it calls this
  * interface to notify the leader to remove the associated hardlink.
  **/
  void removeHardLink(1: string hardLinkPath)
}



service TSDataService extends RaftService {

  /**
  * Query a time series without value filter.
  * @return a readerId >= 0 if the query succeeds, otherwise the query fails
  * TODO-Cluster: support query multiple series in a request
  **/
  long querySingleSeries(1:SingleSeriesQueryRequest request)

  /**
  * Query mult time series without value filter.
  * @return a readerId >= 0 if the query succeeds, otherwise the query fails
  **/
  long queryMultSeries(1:MultSeriesQueryRequest request)

  /**
  * Fetch at max fetchSize time-value pairs using the resultSetId generated by querySingleSeries.
  * @return a ByteBuffer containing the serialized time-value pairs or an empty buffer if there
  * are not more results.
  **/
  binary fetchSingleSeries(1:Node header, 2:long readerId)

    /**
    * Fetch mult series at max fetchSize time-value pairs using the resultSetId generated by querySingleSeries.
    * @return a map containing key-value,the serialized time-value pairs or an empty buffer if there
    * are not more results.
    **/
    map<string,binary> fetchMultSeries(1:Node header, 2:long readerId, 3:list<string> paths)

   /**
   * Query a time series and generate an IReaderByTimestamp.
   * @return a readerId >= 0 if the query succeeds, otherwise the query fails
   **/
  long querySingleSeriesByTimestamp(1:SingleSeriesQueryRequest request)

   /**
   * Fetch values at given timestamps using the resultSetId generated by
   * querySingleSeriesByTimestamp.
   * @return a ByteBuffer containing the serialized value or an empty buffer if there
   * are not more results.
   **/
   binary fetchSingleSeriesByTimestamps(1:Node header, 2:long readerId, 3:list<long> timestamps)

  /**
  * Find the local query established for the remote query and release all its resource.
  **/
  void endQuery(1:Node header, 2:Node thisNode, 3:long queryId)

  /**
  * Given path patterns (paths with wildcard), return all paths they match.
  **/
  GetAllPathsResult getAllPaths(1:Node header, 2:list<string> path, 3:bool withAlias)

  /**
   * Given path patterns (paths with wildcard), return all devices they match.
   **/
  set<string> getAllDevices(1:Node header, 2:list<string> path)

  /**
   * Get the devices from the header according to the showDevicesPlan
   **/
  binary getDevices(1:Node header, 2: binary planBinary)

  list<string> getNodeList(1:Node header, 2:string path, 3:int nodeLevel)

  /**
   * Given path patterns(paths with wildcard), return all children nodes they match
   **/
  set<string> getChildNodeInNextLevel(1: Node header, 2: string path)

  set<string> getChildNodePathInNextLevel(1: Node header, 2: string path)

  binary getAllMeasurementSchema(1: Node header, 2: binary planBinary)

  list<binary> getAggrResult(1:GetAggrResultRequest request)

  list<string> getUnregisteredTimeseries(1: Node header, 2: list<string> timeseriesList)

  PullSnapshotResp pullSnapshot(1:PullSnapshotRequest request)

  /**
  * Create a GroupByExecutor for a path, executing the given aggregations.
  * @return the executorId
  **/
  long getGroupByExecutor(1:GroupByRequest request)

  /**
  * Fetch the group by result in the interval [startTime, endTime) from the given executor.
  * @return the serialized AggregationResults, each is the result of one of the previously
  * required aggregations, and their orders are the same.
  **/
  list<binary> getGroupByResult(1:Node header, 2:long executorId, 3:long startTime, 4:long endTime)


  /**
  * Pull all timeseries schemas prefixed by a given path.
  **/
  PullSchemaResp pullTimeSeriesSchema(1: PullSchemaRequest request)

  /**
  * Pull all measurement schemas prefixed by a given path.
  **/
  PullSchemaResp pullMeasurementSchema(1: PullSchemaRequest request)

  /**
  * Perform a previous fill and return the timevalue pair in binary.
  * @return a binary TimeValuePair
  **/
  binary previousFill(1: PreviousFillRequest request)

  /**
  * Query the last point of a series.
  * @return a binary TimeValuePair
  **/
  binary last(1: LastQueryRequest request)

  int getPathCount(1: Node header 2: list<string> pathsToQuery 3: int level)

  /**
  * During slot transfer, when a member has pulled snapshot from a group, the member will use this
  * method to inform the group that one replica of such slots has been pulled.
  **/
  bool onSnapshotApplied(1: Node header 2: list<int> slots)

  binary peekNextNotNullValue(1: Node header, 2: long executorId, 3: long startTime, 4: long
  endTime)

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
  AddNodeResponse addNode(1: Node node, 2: StartUpStatus startUpStatus)


  CheckStatusResponse  checkStatus(1: StartUpStatus startUpStatus)

  /**
  * Remove a node from the cluster. If the node is not in the cluster or the cluster size will
  * less than replication number, the request will be rejected.
  * return -1(RESPONSE_AGREE) or -3(RESPONSE_REJECT) or -9(RESPONSE_CLUSTER_TOO_SMALL)
  **/
  long removeNode(1: Node node)

  /**
  * When a node is removed from the cluster, if it is not the meta leader, it cannot receive
  * the commit command by heartbeat since it has been removed, so the leader should tell it
  * directly that it is no longer in the cluster.
  **/
  void exile()

  TNodeStatus queryNodeStatus()

  Node checkAlive()

  /**
  * When a node starts, it send handshakes to all other nodes so they know the node is alive
  * again. Notice that heartbeats exists only between leaders and followers, so coordinators
  * cannot know when another node resumes, and handshakes are mainly used to update node status
  * on coordinator side.
  **/
  void handshake(1:Node sender);
}


struct DataPartitionEntry{
  1: required long startTime,
  2: required long endTime,
  3: required list<Node> nodes
}

/**
* for cluster maintainer.
* The interface will replace the JMX based NodeTool APIs.
**/
service ClusterInfoService {

    /**
     * Get physical hash ring
     */
    list<Node> getRing();

    /**
     * Get data partition information of input path and time range.
     * @param path input path
     * @return data partition information
     */
    list<DataPartitionEntry> getDataPartition(1:string path, 2:long startTime, 3:long endTime);

    /**
     * Get metadata partition information of input path
     *
     * @param path input path
     * @return metadata partition information
     */
    list<Node> getMetaPartition(1:string path);

    /**
     * Get status of all nodes
     *
     * @return key: node, value: live or not
     */
    map<Node, bool> getAllNodeStatus();

    /**
     * @return A multi-line string with each line representing the total time consumption, invocation
     *     number, and average time consumption.
     */
    string getInstrumentingInfo();

}