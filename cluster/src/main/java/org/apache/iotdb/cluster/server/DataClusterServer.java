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

package org.apache.iotdb.cluster.server;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.exception.NoHeaderNodeException;
import org.apache.iotdb.cluster.exception.NotInSameGroupException;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService.AsyncProcessor;
import org.apache.iotdb.cluster.server.NodeReport.DataMemberReport;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.utils.nodetool.function.Partition;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataClusterServer extends RaftServer implements TSDataService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(DataClusterServer.class);

  // key: the header of a data group, the member representing this node in this group
  private Map<Node, DataGroupMember> headerGroupMap = new ConcurrentHashMap<>();
  private PartitionTable partitionTable;
  private DataGroupMember.Factory dataMemberFactory;

  public DataClusterServer(Node thisNode, DataGroupMember.Factory dataMemberFactory) {
    super(thisNode);
    this.dataMemberFactory = dataMemberFactory;
  }


  /**
   * Add a DataGroupMember into this server, if a member with the same header exists, the old
   * member will be stopped and replaced by the new one.
   * @param dataGroupMember
   */
  public void addDataGroupMember(DataGroupMember dataGroupMember) {
    DataGroupMember removedMember = headerGroupMap.remove(dataGroupMember.getHeader());
    if (removedMember != null) {
      removedMember.stop();
    }
    headerGroupMap.put(dataGroupMember.getHeader(), dataGroupMember);
  }

  /**
   *
   * @param header the header of the group which the local node is in
   * @param resultHandler can be set to null if the request is an internal request
   * @param request the toString() of this parameter should explain what the request is and it is
   *                only used in logs for tracing
   * @return
   */
  public DataGroupMember getDataMember(Node header, AsyncMethodCallback resultHandler,
      Object request) {
    // if the resultHandler is not null, then the request is a external one and must be with a
    // header
    if (header == null) {
      if (resultHandler != null) {
        resultHandler.onError(new NoHeaderNodeException());
      }
      return null;
    }
    // avoid creating two members for a header
    Exception ex = null;
    synchronized (headerGroupMap) {
      DataGroupMember member = headerGroupMap.get(header);
      if (member == null) {
        logger.info("Received a request \"{}\" from unregistered header {}", request, header);
        if (partitionTable != null) {
          try {
            member = createNewMember(header);
          } catch (NotInSameGroupException | TTransportException e) {
            ex = e;
          }
        } else {
          logger.info("Partition is not ready, cannot create member");
          ex = new PartitionTableUnavailableException(thisNode);
        }
      }
      if (ex != null && resultHandler != null) {
        resultHandler.onError(ex);
      }
      return member;
    }
  }

  /**
   *
   * @param header
   * @return A DataGroupMember representing this node in the data group of the header.
   * @throws NotInSameGroupException If this node is not in the group of the header.
   * @throws TTransportException
   */
  private DataGroupMember createNewMember(Node header)
      throws NotInSameGroupException, TTransportException {
    DataGroupMember member;
    synchronized (partitionTable) {
      PartitionGroup partitionGroup = partitionTable.getHeaderGroup(header);
      if (partitionGroup.contains(thisNode)) {
        // the two nodes are in the same group, create a new data member
        member = dataMemberFactory.create(partitionGroup, thisNode);
        DataGroupMember prevMember = headerGroupMap.put(header, member);
        if (prevMember != null) {
          prevMember.stop();
        }
        logger.info("Created a member for header {}", header);
        member.start();
      } else {
        logger.info("This node {} does not belong to the group {}", thisNode, partitionGroup);
        throw new NotInSameGroupException(partitionTable.getHeaderGroup(header),
            thisNode);
      }
    }
    return member;
  }

  // Forward requests. Find the DataGroupMember that is in the group of the header of the
  // request, and forward the request to it. See methods in DataGroupMember for details.

  @Override
  public void sendHeartBeat(HeartBeatRequest request, AsyncMethodCallback resultHandler) {
    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header, resultHandler, request);
    if (member != null) {
      member.sendHeartBeat(request, resultHandler);
    }
  }

  @Override
  public void startElection(ElectionRequest request, AsyncMethodCallback resultHandler) {
    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header, resultHandler, request);
    if (member != null) {
      member.startElection(request, resultHandler);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback resultHandler) {
    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header, resultHandler, request);
    if (member != null) {
      member.appendEntries(request, resultHandler);
    }
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header, resultHandler, request);
    if (member != null) {
      member.appendEntry(request, resultHandler);
    }
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header, resultHandler, request);
    if (member != null) {
      member.sendSnapshot(request, resultHandler);
    }
  }

  @Override
  public void pullSnapshot(PullSnapshotRequest request, AsyncMethodCallback resultHandler) {
    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header, resultHandler, request);
    if (member != null) {
      member.pullSnapshot(request, resultHandler);
    }
  }

  @Override
  public void executeNonQueryPlan(ExecutNonQueryReq request,
      AsyncMethodCallback<TSStatus> resultHandler) {
    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header, resultHandler, request);
    if (member != null) {
      member.executeNonQueryPlan(request, resultHandler);
    }
  }

  @Override
  public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
    DataGroupMember member = getDataMember(header, resultHandler, "Request commit index");
    if (member != null) {
      member.requestCommitIndex(header, resultHandler);
    }
  }

  @Override
  public void readFile(String filePath, long offset, int length, Node header,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataGroupMember member = getDataMember(header, resultHandler, "Read file:" + filePath);
    if (member != null) {
      member.readFile(filePath, offset, length, header, resultHandler);
    }
  }

  @Override
  public void querySingleSeries(SingleSeriesQueryRequest request,
      AsyncMethodCallback<Long> resultHandler) {
    DataGroupMember member = getDataMember(request.getHeader(), resultHandler,
        "Query series:" + request.getPath());
    if (member != null) {
      member.querySingleSeries(request, resultHandler);
    }
  }

  @Override
  public void fetchSingleSeries(Node header, long readerId,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataGroupMember member = getDataMember(header, resultHandler, "Fetch reader:" + readerId);
    if (member != null) {
      member.fetchSingleSeries(header, readerId, resultHandler);
    }
  }

  @Override
  public void getAllPaths(Node header, String path, AsyncMethodCallback<List<String>> resultHandler) {
    DataGroupMember member = getDataMember(header, resultHandler, "Find path:" + path);
    if (member != null) {
      member.getAllPaths(header, path, resultHandler);
    }
  }

  @Override
  public void endQuery(Node header, Node thisNode, long queryId,
      AsyncMethodCallback<Void> resultHandler) {
    DataGroupMember member = getDataMember(header, resultHandler,
        "End query:" + thisNode + "#" + queryId);
    if (member != null) {
      member.endQuery(header, thisNode, queryId, resultHandler);
    }
  }

  @Override
  public void querySingleSeriesByTimestamp(SingleSeriesQueryRequest request,
      AsyncMethodCallback<Long> resultHandler) {
    DataGroupMember member = getDataMember(request.getHeader(), resultHandler,
        "Query by timestamp:" + request.getQueryId() + "#" + request.getPath() + " of " + request.getRequester());
    if (member != null) {
      member.querySingleSeriesByTimestamp(request, resultHandler);
    }
  }

  @Override
  public void fetchSingleSeriesByTimestamp(Node header, long readerId, ByteBuffer timeBuffer,
      AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataGroupMember member = getDataMember(header, resultHandler,
        "Fetch by timestamp:" + readerId);
    if (member != null) {
      member.fetchSingleSeriesByTimestamp(header, readerId, timeBuffer, resultHandler);
    }
  }

  @Override
  public void pullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    Node header = request.getHeader();
    DataGroupMember member = getDataMember(header, resultHandler, request);
    if (member != null) {
      member.pullTimeSeriesSchema(request, resultHandler);
    }
  }

  @Override
  public void getAllDevices(Node header, String path,
      AsyncMethodCallback<Set<String>> resultHandler) {
    DataGroupMember dataMember = getDataMember(header, resultHandler, "Get all devices");
    dataMember.getAllDevices(header, path, resultHandler);
  }

  @Override
  public void getNodeList(Node header, String path, int nodeLevel,
      AsyncMethodCallback<List<String>> resultHandler) {
    DataGroupMember dataMember = getDataMember(header, resultHandler, "Get node list");
    dataMember.getNodeList(header, path, nodeLevel, resultHandler);
  }

  @Override
  public void getAggrResult(GetAggrResultRequest request,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    DataGroupMember dataMember = getDataMember(request.getHeader(), resultHandler, request);
    dataMember.getAggrResult(request, resultHandler);
  }

  @Override
  public void getGroupByExecutor(GroupByRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataGroupMember dataMember = getDataMember(request.getHeader(), resultHandler, request);
    dataMember.getGroupByExecutor(request, resultHandler);
  }

  @Override
  public void getGroupByResult(Node header, long executorId, long startTime, long endTime,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    DataGroupMember dataMember = getDataMember(header, resultHandler, "Fetch group by");
    dataMember.getGroupByResult(header, executorId, startTime, endTime, resultHandler);
  }

  @Override
  AsyncProcessor getProcessor() {
    return new AsyncProcessor(this);
  }

  @Override
  TNonblockingServerSocket getServerSocket() throws TTransportException {
    return new TNonblockingServerSocket(new InetSocketAddress(config.getLocalIP(),
        thisNode.getDataPort()), connectionTimeoutInMS);
  }

  @Override
  String getClientThreadPrefix() {
    return "DataClientThread-";
  }

  @Override
  String getServerClientName() {
    return "DataServerThread-";
  }

  /**
   * Try adding the node into the group of each DataGroupMember, and if the DataGroupMember no
   * longer stays in that group, also remove and stop it.
   * If the new group contains this node, also create and add a new DataGroupMember for it.
   * @param node
   * @param newGroup
   */
  public void addNode(Node node, PartitionGroup newGroup) {
    Iterator<Entry<Node, DataGroupMember>> entryIterator = headerGroupMap.entrySet().iterator();
    synchronized (headerGroupMap) {
      while (entryIterator.hasNext()) {
        Entry<Node, DataGroupMember> entry = entryIterator.next();
        DataGroupMember dataGroupMember = entry.getValue();
        // the member may be extruded from the group, remove and stop it if so
        boolean shouldLeave = dataGroupMember.addNode(node);
        if (shouldLeave) {
          logger.info("This node does not belong to {} any more", dataGroupMember.getAllNodes());
          entryIterator.remove();
          dataGroupMember.stop();
        }
      }

      if (newGroup.contains(thisNode)) {
        try {
          logger.info("Adding this node into a new group {}", newGroup);
          DataGroupMember dataGroupMember = dataMemberFactory.create(newGroup, thisNode);
          addDataGroupMember(dataGroupMember);
          dataGroupMember.start();
          dataGroupMember
              .pullNodeAdditionSnapshots(partitionTable.getNodeSlots(node), node);
        } catch (TTransportException e) {
          logger.error("Fail to create data newMember for new header {}", node, e);
        }
      }
    }
  }

  public void bulidDataGroupMembers(PartitionTable partitionTable)
      throws TTransportException {
    setPartitionTable(partitionTable);
    List<PartitionGroup> partitionGroups = partitionTable.getLocalGroups();
    for (PartitionGroup partitionGroup : partitionGroups) {
      logger.debug("Building member of data group: {}", partitionGroup);
      DataGroupMember dataGroupMember = dataMemberFactory.create(partitionGroup, thisNode);
      dataGroupMember.start();
      addDataGroupMember(dataGroupMember);
    }
    logger.info("Data group members are ready");
  }

  /**
   * Try removing a node from the groups of each DataGroupMember.
   * If the node is the header of some group, set the member to read only so that it can still
   * provide data for other nodes that has not yet pulled its data.
   * If the node is the local node, remove all members whose group is not headed by this node.
   * Otherwise, just change the node list of the member and pull new data.
   * And create a new DataGroupMember if this node should join a new group because of this removal.
   * @param node
   * @param removalResult cluster changes due to the node removal
   */
  public void removeNode(Node node, NodeRemovalResult removalResult) {
    Iterator<Entry<Node, DataGroupMember>> entryIterator = headerGroupMap.entrySet().iterator();
    synchronized (headerGroupMap) {
      while (entryIterator.hasNext()) {
        Entry<Node, DataGroupMember> entry = entryIterator.next();
        DataGroupMember dataGroupMember = entry.getValue();
        if (dataGroupMember.getHeader().equals(node)) {
          // the group is removed as the node is removed, so new writes should be rejected as
          // they belong to the new holder, but the member is kept alive for other nodes to pull
          // snapshots
          dataGroupMember.setReadOnly();
          //TODO-Cluster: when to call removeLocalData?
        } else {
          if (node.equals(thisNode)) {
            // this node is removed, it is no more replica of other groups
            List<Integer> nodeSlots = partitionTable.getNodeSlots(dataGroupMember.getHeader());
            dataGroupMember.removeLocalData(nodeSlots);
            entryIterator.remove();
            dataGroupMember.stop();
          } else {
            // the group should be updated and pull new slots from the removed node
            dataGroupMember.removeNode(node, removalResult);
          }
        }
      }
      PartitionGroup newGroup = removalResult.getNewGroup();
      if (newGroup != null) {
        logger.info("{} should join a new group {}", thisNode, newGroup);
        try {
          createNewMember(newGroup.getHeader());
        } catch (NotInSameGroupException | TTransportException e) {
          // ignored
        }
      }
    }
  }

  public void setPartitionTable(PartitionTable partitionTable) {
    this.partitionTable = partitionTable;
  }

  /**
   * When the node joins a cluster, it also creates a new data group and a corresponding member
   * which has no data. This is to make that member pull data from other nodes.
   */
  public void pullSnapshots() {
    List<Integer> slots = partitionTable.getNodeSlots(thisNode);
    DataGroupMember dataGroupMember = headerGroupMap.get(thisNode);
    dataGroupMember.pullNodeAdditionSnapshots(slots, thisNode);
  }

  /**
   *
   * @return The reports of every DataGroupMember in this node.
   */
  public List<DataMemberReport> genMemberReports() {
    List<DataMemberReport> dataMemberReports = new ArrayList<>();
    for (DataGroupMember value : headerGroupMap.values()) {
      dataMemberReports.add(value.genReport());
    }
    return dataMemberReports;
  }
}
