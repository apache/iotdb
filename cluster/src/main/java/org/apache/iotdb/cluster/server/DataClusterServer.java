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

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.CheckConsistencyException;
import org.apache.iotdb.cluster.exception.NoHeaderNodeException;
import org.apache.iotdb.cluster.exception.NotInSameGroupException;
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.partition.NodeAdditionResult;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntriesRequest;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.ExecutNonQueryReq;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GetAllPathsResult;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.LastQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PreviousFillRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService.AsyncProcessor;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService.Processor;
import org.apache.iotdb.cluster.server.member.DataGroupMember;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.monitor.NodeReport.DataMemberReport;
import org.apache.iotdb.cluster.server.service.DataAsyncService;
import org.apache.iotdb.cluster.server.service.DataSyncService;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DataClusterServer extends RaftServer
    implements TSDataService.AsyncIface, TSDataService.Iface {

  private static final Logger logger = LoggerFactory.getLogger(DataClusterServer.class);

  // key: the header of a data group, value: the member representing this node in this group and
  // it is currently at service
  private Map<Node, DataGroupMember> headerGroupMap = new ConcurrentHashMap<>();
  private Map<Node, DataAsyncService> asyncServiceMap = new ConcurrentHashMap<>();
  private Map<Node, DataSyncService> syncServiceMap = new ConcurrentHashMap<>();
  // key: the header of a data group, value: the member representing this node in this group but
  // it is out of service because another node has joined the group and expelled this node, or
  // the node itself is removed, but it is still stored to provide snapshot for other nodes
  private StoppedMemberManager stoppedMemberManager;
  private PartitionTable partitionTable;
  private DataGroupMember.Factory dataMemberFactory;
  private MetaGroupMember metaGroupMember;

  public DataClusterServer(
      Node thisNode, DataGroupMember.Factory dataMemberFactory, MetaGroupMember metaGroupMember) {
    super(thisNode);
    this.dataMemberFactory = dataMemberFactory;
    this.metaGroupMember = metaGroupMember;
    this.stoppedMemberManager = new StoppedMemberManager(dataMemberFactory, thisNode);
  }

  @Override
  public void stop() {
    closeLogManagers();
    for (DataGroupMember member : headerGroupMap.values()) {
      member.stop();
    }
    super.stop();
  }

  /**
   * Add a DataGroupMember into this server, if a member with the same header exists, the old member
   * will be stopped and replaced by the new one.
   *
   * @param dataGroupMember
   */
  public void addDataGroupMember(DataGroupMember dataGroupMember) {
    DataGroupMember removedMember = headerGroupMap.remove(dataGroupMember.getHeader());
    if (removedMember != null) {
      removedMember.stop();
      asyncServiceMap.remove(dataGroupMember.getHeader());
      syncServiceMap.remove(dataGroupMember.getHeader());
    }
    stoppedMemberManager.remove(dataGroupMember.getHeader());

    headerGroupMap.put(dataGroupMember.getHeader(), dataGroupMember);
  }

  private <T> DataAsyncService getDataAsyncService(
      Node header, AsyncMethodCallback<T> resultHandler, Object request) {
    return asyncServiceMap.computeIfAbsent(
        header,
        h -> {
          DataGroupMember dataMember = getDataMember(h, resultHandler, request);
          return dataMember != null ? new DataAsyncService(dataMember) : null;
        });
  }

  private DataSyncService getDataSyncService(Node header) {
    return syncServiceMap.computeIfAbsent(
        header,
        h -> {
          DataGroupMember dataMember = getDataMember(h, null, null);
          return dataMember != null ? new DataSyncService(dataMember) : null;
        });
  }

  /**
   * @param header the header of the group which the local node is in
   * @param resultHandler can be set to null if the request is an internal request
   * @param request the toString() of this parameter should explain what the request is and it is
   *     only used in logs for tracing
   * @return
   */
  public <T> DataGroupMember getDataMember(
      Node header, AsyncMethodCallback<T> resultHandler, Object request) {
    // if the resultHandler is not null, then the request is a external one and must be with a
    // header
    if (header == null) {
      if (resultHandler != null) {
        resultHandler.onError(new NoHeaderNodeException());
      }
      return null;
    }
    DataGroupMember member = stoppedMemberManager.get(header);
    if (member != null) {
      return member;
    }

    // avoid creating two members for a header
    Exception ex = null;
    synchronized (headerGroupMap) {
      member = headerGroupMap.get(header);
      if (member != null) {
        return member;
      }
      logger.info("Received a request \"{}\" from unregistered header {}", request, header);
      if (partitionTable != null) {
        try {
          member = createNewMember(header);
        } catch (NotInSameGroupException | CheckConsistencyException e) {
          ex = e;
        }
      } else {
        logger.info("Partition is not ready, cannot create member");
        ex = new PartitionTableUnavailableException(thisNode);
      }
      if (ex != null && resultHandler != null) {
        resultHandler.onError(ex);
      }
      return member;
    }
  }

  /**
   * @param header
   * @return A DataGroupMember representing this node in the data group of the header.
   * @throws NotInSameGroupException If this node is not in the group of the header.
   */
  private DataGroupMember createNewMember(Node header)
      throws NotInSameGroupException, CheckConsistencyException {
    DataGroupMember member;
    PartitionGroup partitionGroup;
    partitionGroup = partitionTable.getHeaderGroup(header);
    if (partitionGroup == null || !partitionGroup.contains(thisNode)) {
      // if the partition table is old, this node may have not been moved to the new group
      metaGroupMember.syncLeaderWithConsistencyCheck(true);
      partitionGroup = partitionTable.getHeaderGroup(header);
    }
    if (partitionGroup != null && partitionGroup.contains(thisNode)) {
      // the two nodes are in the same group, create a new data member
      member = dataMemberFactory.create(partitionGroup, thisNode);
      DataGroupMember prevMember = headerGroupMap.put(header, member);
      if (prevMember != null) {
        prevMember.stop();
      }
      logger.info("Created a member for header {}", header);
      member.start();
    } else {
      // the member may have been stopped after syncLeader
      member = stoppedMemberManager.get(header);
      if (member != null) {
        return member;
      }
      logger.info(
          "This node {} does not belong to the group {}, header {}",
          thisNode,
          partitionGroup,
          header);
      throw new NotInSameGroupException(partitionGroup, thisNode);
    }
    return member;
  }

  // Forward requests. Find the DataGroupMember that is in the group of the header of the
  // request, and forward the request to it. See methods in DataGroupMember for details.

  @Override
  public void sendHeartbeat(
      HeartBeatRequest request, AsyncMethodCallback<HeartBeatResponse> resultHandler) {
    Node header = request.getHeader();
    DataAsyncService service = getDataAsyncService(header, resultHandler, request);
    if (service != null) {
      service.sendHeartbeat(request, resultHandler);
    }
  }

  @Override
  public void startElection(ElectionRequest request, AsyncMethodCallback<Long> resultHandler) {
    Node header = request.getHeader();
    DataAsyncService service = getDataAsyncService(header, resultHandler, request);
    if (service != null) {
      service.startElection(request, resultHandler);
    }
  }

  @Override
  public void appendEntries(AppendEntriesRequest request, AsyncMethodCallback<Long> resultHandler) {
    Node header = request.getHeader();
    DataAsyncService service = getDataAsyncService(header, resultHandler, request);
    if (service != null) {
      service.appendEntries(request, resultHandler);
    }
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback<Long> resultHandler) {
    Node header = request.getHeader();
    DataAsyncService service = getDataAsyncService(header, resultHandler, request);
    if (service != null) {
      service.appendEntry(request, resultHandler);
    }
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback<Void> resultHandler) {
    Node header = request.getHeader();
    DataAsyncService service = getDataAsyncService(header, resultHandler, request);
    if (service != null) {
      service.sendSnapshot(request, resultHandler);
    }
  }

  @Override
  public void pullSnapshot(
      PullSnapshotRequest request, AsyncMethodCallback<PullSnapshotResp> resultHandler) {
    Node header = request.getHeader();
    DataAsyncService service = getDataAsyncService(header, resultHandler, request);
    if (service != null) {
      service.pullSnapshot(request, resultHandler);
    }
  }

  @Override
  public void executeNonQueryPlan(
      ExecutNonQueryReq request, AsyncMethodCallback<TSStatus> resultHandler) {
    Node header = request.getHeader();
    DataAsyncService service = getDataAsyncService(header, resultHandler, request);
    if (service != null) {
      service.executeNonQueryPlan(request, resultHandler);
    }
  }

  @Override
  public void requestCommitIndex(Node header, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "Request commit index");
    if (service != null) {
      service.requestCommitIndex(header, resultHandler);
    }
  }

  @Override
  public void readFile(
      String filePath, long offset, int length, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(thisNode, resultHandler, "Read file:" + filePath);
    if (service != null) {
      service.readFile(filePath, offset, length, resultHandler);
    }
  }

  @Override
  public void querySingleSeries(
      SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(
            request.getHeader(), resultHandler, "Query series:" + request.getPath());
    if (service != null) {
      service.querySingleSeries(request, resultHandler);
    }
  }

  @Override
  public void fetchSingleSeries(
      Node header, long readerId, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(header, resultHandler, "Fetch reader:" + readerId);
    if (service != null) {
      service.fetchSingleSeries(header, readerId, resultHandler);
    }
  }

  @Override
  public void getAllPaths(
      Node header,
      List<String> paths,
      boolean withAlias,
      AsyncMethodCallback<GetAllPathsResult> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "Find path:" + paths);
    if (service != null) {
      service.getAllPaths(header, paths, withAlias, resultHandler);
    }
  }

  @Override
  public void endQuery(
      Node header, Node thisNode, long queryId, AsyncMethodCallback<Void> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "End query");
    if (service != null) {
      service.endQuery(header, thisNode, queryId, resultHandler);
    }
  }

  @Override
  public void querySingleSeriesByTimestamp(
      SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(
            request.getHeader(),
            resultHandler,
            "Query by timestamp:"
                + request.getQueryId()
                + "#"
                + request.getPath()
                + " of "
                + request.getRequester());
    if (service != null) {
      service.querySingleSeriesByTimestamp(request, resultHandler);
    }
  }

  @Override
  public void fetchSingleSeriesByTimestamp(
      Node header, long readerId, long time, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(header, resultHandler, "Fetch by timestamp:" + readerId);
    if (service != null) {
      service.fetchSingleSeriesByTimestamp(header, readerId, time, resultHandler);
    }
  }

  @Override
  public void pullTimeSeriesSchema(
      PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
    Node header = request.getHeader();
    DataAsyncService service = getDataAsyncService(header, resultHandler, request);
    if (service != null) {
      service.pullTimeSeriesSchema(request, resultHandler);
    }
  }

  @Override
  public void pullMeasurementSchema(
      PullSchemaRequest request, AsyncMethodCallback<PullSchemaResp> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(request.getHeader(), resultHandler, "Pull measurement schema");
    service.pullMeasurementSchema(request, resultHandler);
  }

  @Override
  public void getAllDevices(
      Node header, List<String> paths, AsyncMethodCallback<Set<String>> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "Get all devices");
    service.getAllDevices(header, paths, resultHandler);
  }

  @Override
  public void getDevices(
      Node header, ByteBuffer planBytes, AsyncMethodCallback<ByteBuffer> resultHandler)
      throws TException {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "Get devices");
    service.getDevices(header, planBytes, resultHandler);
  }

  @Override
  public void getNodeList(
      Node header, String path, int nodeLevel, AsyncMethodCallback<List<String>> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "Get node list");
    service.getNodeList(header, path, nodeLevel, resultHandler);
  }

  @Override
  public void getChildNodePathInNextLevel(
      Node header, String path, AsyncMethodCallback<Set<String>> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(header, resultHandler, "Get child node path in next level");
    service.getChildNodePathInNextLevel(header, path, resultHandler);
  }

  @Override
  public void getAllMeasurementSchema(
      Node header, ByteBuffer planBytes, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(header, resultHandler, "Get all measurement schema");
    service.getAllMeasurementSchema(header, planBytes, resultHandler);
  }

  @Override
  public void getAggrResult(
      GetAggrResultRequest request, AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    DataAsyncService service = getDataAsyncService(request.getHeader(), resultHandler, request);
    service.getAggrResult(request, resultHandler);
  }

  @Override
  public void getUnregisteredTimeseries(
      Node header, List<String> timeseriesList, AsyncMethodCallback<List<String>> resultHandler) {
    DataAsyncService service =
        getDataAsyncService(header, resultHandler, "Check if measurements are registered");
    service.getUnregisteredTimeseries(header, timeseriesList, resultHandler);
  }

  @Override
  public void getGroupByExecutor(GroupByRequest request, AsyncMethodCallback<Long> resultHandler) {
    DataAsyncService service = getDataAsyncService(request.getHeader(), resultHandler, request);
    service.getGroupByExecutor(request, resultHandler);
  }

  @Override
  public void getGroupByResult(
      Node header,
      long executorId,
      long startTime,
      long endTime,
      AsyncMethodCallback<List<ByteBuffer>> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "Fetch group by");
    service.getGroupByResult(header, executorId, startTime, endTime, resultHandler);
  }

  @Override
  TProcessor getProcessor() {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      return new AsyncProcessor<>(this);
    } else {
      return new Processor<>(this);
    }
  }

  @Override
  TServerTransport getServerSocket() throws TTransportException {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      return new TNonblockingServerSocket(
          new InetSocketAddress(config.getClusterRpcIp(), thisNode.getDataPort()),
          getConnectionTimeoutInMS());
    } else {
      return new TServerSocket(
          new InetSocketAddress(config.getClusterRpcIp(), thisNode.getDataPort()));
    }
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
   * longer stays in that group, also remove and stop it. If the new group contains this node, also
   * create and add a new DataGroupMember for it.
   *
   * @param node
   * @param result
   */
  public void addNode(Node node, NodeAdditionResult result) {
    Iterator<Entry<Node, DataGroupMember>> entryIterator = headerGroupMap.entrySet().iterator();
    synchronized (headerGroupMap) {
      while (entryIterator.hasNext()) {
        Entry<Node, DataGroupMember> entry = entryIterator.next();
        DataGroupMember dataGroupMember = entry.getValue();
        // the member may be extruded from the group, remove and stop it if so
        boolean shouldLeave = dataGroupMember.addNode(node, result);
        if (shouldLeave) {
          logger.info("This node does not belong to {} any more", dataGroupMember.getAllNodes());
          entryIterator.remove();
          removeMember(entry.getKey(), entry.getValue());
        }
      }

      if (result.getNewGroup().contains(thisNode)) {
        logger.info("Adding this node into a new group {}", result.getNewGroup());
        DataGroupMember dataGroupMember = dataMemberFactory.create(result.getNewGroup(), thisNode);
        addDataGroupMember(dataGroupMember);
        dataGroupMember.start();
        dataGroupMember.pullNodeAdditionSnapshots(
            ((SlotPartitionTable) partitionTable).getNodeSlots(node), node);
      }
    }
  }

  private void removeMember(Node header, DataGroupMember dataGroupMember) {
    dataGroupMember.syncLeader();
    dataGroupMember.setReadOnly();
    dataGroupMember.stop();
    stoppedMemberManager.put(header, dataGroupMember);
  }

  /**
   * Set the partition table as the in-use one and build a DataGroupMember for each local group (the
   * group which the local node is in) and start them.
   *
   * @param partitionTable
   * @throws TTransportException
   */
  @SuppressWarnings("java:S1135")
  public void buildDataGroupMembers(PartitionTable partitionTable) {
    setPartitionTable(partitionTable);
    // TODO-Cluster: if there are unchanged members, do not stop and restart them
    // clear previous members if the partition table is reloaded
    for (DataGroupMember value : headerGroupMap.values()) {
      value.stop();
    }

    for (DataGroupMember value : headerGroupMap.values()) {
      value.setUnchanged(false);
    }

    List<PartitionGroup> partitionGroups = partitionTable.getLocalGroups();
    for (PartitionGroup partitionGroup : partitionGroups) {
      DataGroupMember prevMember = headerGroupMap.get(partitionGroup.getHeader());
      if (prevMember == null || !prevMember.getAllNodes().equals(partitionGroup)) {
        logger.info("Building member of data group: {}", partitionGroup);
        // no previous member or member changed
        DataGroupMember dataGroupMember = dataMemberFactory.create(partitionGroup, thisNode);
        dataGroupMember.start();
        // the previous member will be replaced here
        addDataGroupMember(dataGroupMember);
        dataGroupMember.setUnchanged(true);
      } else {
        prevMember.setUnchanged(true);
      }
    }

    // remove out-dated members of this node
    headerGroupMap.entrySet().removeIf(e -> !e.getValue().isUnchanged());

    logger.info("Data group members are ready");
  }

  /**
   * Try removing a node from the groups of each DataGroupMember. If the node is the header of some
   * group, set the member to read only so that it can still provide data for other nodes that has
   * not yet pulled its data. If the node is the local node, remove all members whose group is not
   * headed by this node. Otherwise, just change the node list of the member and pull new data. And
   * create a new DataGroupMember if this node should join a new group because of this removal.
   *
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
          entryIterator.remove();
          removeMember(entry.getKey(), entry.getValue());
        } else {
          if (node.equals(thisNode)) {
            // this node is removed, it is no more replica of other groups
            List<Integer> nodeSlots =
                ((SlotPartitionTable) partitionTable).getNodeSlots(dataGroupMember.getHeader());
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
        } catch (NotInSameGroupException e) {
          // ignored
        } catch (CheckConsistencyException ce) {
          logger.error("remove node failed, error={}", ce.getMessage());
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
    List<Integer> slots = ((SlotPartitionTable) partitionTable).getNodeSlots(thisNode);
    DataGroupMember dataGroupMember = headerGroupMap.get(thisNode);
    dataGroupMember.pullNodeAdditionSnapshots(slots, thisNode);
  }

  /** @return The reports of every DataGroupMember in this node. */
  public List<DataMemberReport> genMemberReports() {
    List<DataMemberReport> dataMemberReports = new ArrayList<>();
    for (DataGroupMember value : headerGroupMap.values()) {

      dataMemberReports.add(value.genReport());
    }
    return dataMemberReports;
  }

  @Override
  public void previousFill(
      PreviousFillRequest request, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service = getDataAsyncService(request.getHeader(), resultHandler, request);
    service.previousFill(request, resultHandler);
  }

  public void closeLogManagers() {
    for (DataGroupMember member : headerGroupMap.values()) {
      member.closeLogManager();
    }
  }

  @Override
  public void matchTerm(
      long index, long term, Node header, AsyncMethodCallback<Boolean> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "Match term");
    service.matchTerm(index, term, header, resultHandler);
  }

  @Override
  public void last(LastQueryRequest request, AsyncMethodCallback<ByteBuffer> resultHandler) {
    DataAsyncService service = getDataAsyncService(request.getHeader(), resultHandler, "last");
    service.last(request, resultHandler);
  }

  @Override
  public void getPathCount(
      Node header,
      List<String> pathsToQuery,
      int level,
      AsyncMethodCallback<Integer> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "count path");
    service.getPathCount(header, pathsToQuery, level, resultHandler);
  }

  @Override
  public void onSnapshotApplied(
      Node header, List<Integer> slots, AsyncMethodCallback<Boolean> resultHandler) {
    DataAsyncService service = getDataAsyncService(header, resultHandler, "Snapshot applied");
    service.onSnapshotApplied(header, slots, resultHandler);
  }

  @Override
  public long querySingleSeries(SingleSeriesQueryRequest request) throws TException {
    return getDataSyncService(request.getHeader()).querySingleSeries(request);
  }

  @Override
  public ByteBuffer fetchSingleSeries(Node header, long readerId) throws TException {
    return getDataSyncService(header).fetchSingleSeries(header, readerId);
  }

  @Override
  public long querySingleSeriesByTimestamp(SingleSeriesQueryRequest request) throws TException {
    return getDataSyncService(request.getHeader()).querySingleSeriesByTimestamp(request);
  }

  @Override
  public ByteBuffer fetchSingleSeriesByTimestamp(Node header, long readerId, long timestamp)
      throws TException {
    return getDataSyncService(header).fetchSingleSeriesByTimestamp(header, readerId, timestamp);
  }

  @Override
  public void endQuery(Node header, Node thisNode, long queryId) throws TException {
    getDataSyncService(header).endQuery(header, thisNode, queryId);
  }

  @Override
  public GetAllPathsResult getAllPaths(Node header, List<String> path, boolean withAlias)
      throws TException {
    return getDataSyncService(header).getAllPaths(header, path, withAlias);
  }

  @Override
  public Set<String> getAllDevices(Node header, List<String> path) throws TException {
    return getDataSyncService(header).getAllDevices(header, path);
  }

  @Override
  public List<String> getNodeList(Node header, String path, int nodeLevel) throws TException {
    return getDataSyncService(header).getNodeList(header, path, nodeLevel);
  }

  @Override
  public Set<String> getChildNodePathInNextLevel(Node header, String path) throws TException {
    return getDataSyncService(header).getChildNodePathInNextLevel(header, path);
  }

  @Override
  public ByteBuffer getAllMeasurementSchema(Node header, ByteBuffer planBinary) throws TException {
    return getDataSyncService(header).getAllMeasurementSchema(header, planBinary);
  }

  @Override
  public ByteBuffer getDevices(Node header, ByteBuffer planBinary) throws TException {
    return getDataSyncService(header).getDevices(header, planBinary);
  }

  @Override
  public List<ByteBuffer> getAggrResult(GetAggrResultRequest request) throws TException {
    return getDataSyncService(request.getHeader()).getAggrResult(request);
  }

  @Override
  public List<String> getUnregisteredTimeseries(Node header, List<String> timeseriesList)
      throws TException {
    return getDataSyncService(header).getUnregisteredTimeseries(header, timeseriesList);
  }

  @Override
  public PullSnapshotResp pullSnapshot(PullSnapshotRequest request) throws TException {
    return getDataSyncService(request.getHeader()).pullSnapshot(request);
  }

  @Override
  public long getGroupByExecutor(GroupByRequest request) throws TException {
    return getDataSyncService(request.header).getGroupByExecutor(request);
  }

  @Override
  public List<ByteBuffer> getGroupByResult(
      Node header, long executorId, long startTime, long endTime) throws TException {
    return getDataSyncService(header).getGroupByResult(header, executorId, startTime, endTime);
  }

  @Override
  public PullSchemaResp pullTimeSeriesSchema(PullSchemaRequest request) throws TException {
    return getDataSyncService(request.getHeader()).pullTimeSeriesSchema(request);
  }

  @Override
  public PullSchemaResp pullMeasurementSchema(PullSchemaRequest request) throws TException {
    return getDataSyncService(request.getHeader()).pullMeasurementSchema(request);
  }

  @Override
  public ByteBuffer previousFill(PreviousFillRequest request) throws TException {
    return getDataSyncService(request.getHeader()).previousFill(request);
  }

  @Override
  public ByteBuffer last(LastQueryRequest request) throws TException {
    return getDataSyncService(request.getHeader()).last(request);
  }

  @Override
  public int getPathCount(Node header, List<String> pathsToQuery, int level) throws TException {
    return getDataSyncService(header).getPathCount(header, pathsToQuery, level);
  }

  @Override
  public boolean onSnapshotApplied(Node header, List<Integer> slots) {
    return getDataSyncService(header).onSnapshotApplied(header, slots);
  }

  @Override
  public HeartBeatResponse sendHeartbeat(HeartBeatRequest request) {
    return getDataSyncService(request.getHeader()).sendHeartbeat(request);
  }

  @Override
  public long startElection(ElectionRequest request) {
    return getDataSyncService(request.getHeader()).startElection(request);
  }

  @Override
  public long appendEntries(AppendEntriesRequest request) throws TException {
    return getDataSyncService(request.getHeader()).appendEntries(request);
  }

  @Override
  public long appendEntry(AppendEntryRequest request) throws TException {
    return getDataSyncService(request.getHeader()).appendEntry(request);
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request) throws TException {
    getDataSyncService(request.getHeader()).sendSnapshot(request);
  }

  @Override
  public TSStatus executeNonQueryPlan(ExecutNonQueryReq request) throws TException {
    return getDataSyncService(request.getHeader()).executeNonQueryPlan(request);
  }

  @Override
  public long requestCommitIndex(Node header) throws TException {
    return getDataSyncService(header).requestCommitIndex(header);
  }

  @Override
  public ByteBuffer readFile(String filePath, long offset, int length) throws TException {
    return getDataSyncService(thisNode).readFile(filePath, offset, length);
  }

  @Override
  public boolean matchTerm(long index, long term, Node header) {
    return getDataSyncService(header).matchTerm(index, term, header);
  }

  @Override
  public ByteBuffer peekNextNotNullValue(Node header, long executorId, long startTime, long endTime)
      throws TException {
    return getDataSyncService(header).peekNextNotNullValue(header, executorId, startTime, endTime);
  }

  @Override
  public void peekNextNotNullValue(
      Node header,
      long executorId,
      long startTime,
      long endTime,
      AsyncMethodCallback<ByteBuffer> resultHandler)
      throws TException {
    resultHandler.onComplete(
        getDataSyncService(header).peekNextNotNullValue(header, executorId, startTime, endTime));
  }

  @Override
  public void removeHardLink(String hardLinkPath) throws TException {
    getDataSyncService(thisNode).removeHardLink(hardLinkPath);
  }

  @Override
  public void removeHardLink(String hardLinkPath, AsyncMethodCallback<Void> resultHandler) {
    getDataAsyncService(thisNode, resultHandler, hardLinkPath)
        .removeHardLink(hardLinkPath, resultHandler);
  }
}
