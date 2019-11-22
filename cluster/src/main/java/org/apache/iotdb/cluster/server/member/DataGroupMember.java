/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.LogManager;
import org.apache.iotdb.cluster.log.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.log.manage.SingleSnapshotLogManager.SimpleSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.heartbeat.HeartBeatThread;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataGroupMember extends RaftMember implements TSDataService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(DataGroupMember.class);

  private TSDataService.AsyncClient.Factory clientFactory;

  private Node thisNode;
  // the data port of each node in this group
  private Map<Node, Integer> dataPortMap = new ConcurrentHashMap<>();
  private MetaGroupMember metaGroupMember;

  private DataGroupMember(TProtocolFactory factory, PartitionGroup nodes, Node thisNode,
      LogManager logManager, MetaGroupMember metaGroupMember) throws IOException {
    this.thisNode = thisNode;
    this.logManager = logManager;
    this.metaGroupMember = metaGroupMember;
    allNodes = nodes;
    clientFactory = new TSDataService.AsyncClient.Factory(new TAsyncClientManager(), factory);
    queryProcessor = new QueryProcessor(new QueryProcessExecutor());
  }

  @Override
  public void start() throws TTransportException {
    super.start();
    heartBeatService.submit(new HeartBeatThread(this));
  }

  @Override
  AsyncClient getAsyncClient(TNonblockingTransport transport) {
    return clientFactory.getAsyncClient(transport);
  }

  @Override
  public AsyncClient connectNode(Node node) {
    if (node.equals(thisNode)) {
      return null;
    }

    AsyncClient client = null;
    try {
      client = getAsyncClient(new TNonblockingSocket(node.getIp(), node.getDataPort(),
          CONNECTION_TIME_OUT_MS));
    } catch (IOException e) {
      logger.warn("Cannot connect to node {}", node, e);
    }
    return client;
  }

  /**
   * The first node (on the hash ring) in this data group is the header. It determines the duty
   * (what range on the ring do the group take responsibility for) of the group and although other
   * nodes in this may change, this node is unchangeable unless the data group is dismissed. It
   * is also the identifier of this data group.
   */
  public Node getHeader() {
    return allNodes.get(0);
  }

  public static class Factory {
    private TProtocolFactory protocolFactory;
    private MetaGroupMember metaGroupMember;
    private LogApplier applier;

    Factory(TProtocolFactory protocolFactory, MetaGroupMember metaGroupMember, LogApplier applier) {
      this.protocolFactory = protocolFactory;
      this.metaGroupMember = metaGroupMember;
      this.applier = applier;
    }

    public DataGroupMember create(PartitionGroup partitionGroup, Node thisNode)
        throws IOException {
      return new DataGroupMember(protocolFactory, partitionGroup, thisNode,
          new PartitionedSnapshotLogManager(applier),
          metaGroupMember);
    }
  }

  /**
   * Try to add a Node into this group
   * @param node
   * @return true if this node should leave the group
   */
  public synchronized boolean addNode(Node node) {
    synchronized (allNodes) {

      int insertIndex = -1;
      for (int i = 0; i < allNodes.size() - 1; i++) {
        Node prev = allNodes.get(i);
        Node next = allNodes.get(i + 1);
        if (prev.nodeIdentifier < node.nodeIdentifier && node.nodeIdentifier < next.nodeIdentifier
            || prev.nodeIdentifier < node.nodeIdentifier && next.nodeIdentifier < prev.nodeIdentifier
            || node.nodeIdentifier < next.nodeIdentifier && next.nodeIdentifier < prev.nodeIdentifier
            ) {
          insertIndex = i + 1;
          break;
        }
      }
      if (insertIndex > 0) {
        allNodes.add(insertIndex, node);
        // if the local node is the last node and the insertion succeeds, this node should leave
        // the group
        return allNodes.indexOf(thisNode) == allNodes.size() - 1;
      }
      return false;
    }
  }

  @Override
  long processElectionRequest(ElectionRequest electionRequest) {
    // to be a data group leader, a node should also be qualified to be the meta group leader
    // which guarantees the data group leader has the newest partition table.
    long metaResponse = metaGroupMember.processElectionRequest(electionRequest);
    if (metaResponse != Response.RESPONSE_AGREE) {
      return Response.RESPONSE_META_LOG_STALE;
    }

    // check data logs
    long thatTerm = electionRequest.getTerm();
    long thatLastLogId = electionRequest.getDataLogLastIndex();
    long thatLastLogTerm = electionRequest.getDataLogLastTerm();
    logger.info("Received an election request, term:{}, lastLogId:{}, lastLogTerm:{}", thatTerm,
        thatLastLogId, thatLastLogTerm);

    long lastLogIndex = logManager.getLastLogIndex();
    long lastLogTerm = logManager.getLastLogTerm();
    long thisTerm = term.get();
    return verifyElector(thisTerm, lastLogIndex, lastLogTerm, thatTerm, thatLastLogId, thatLastLogTerm);
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
    PartitionedSnapshot snapshot = new PartitionedSnapshot();
    try {
      snapshot.deserialize(ByteBuffer.wrap(request.getSnapshotBytes()));
      applySnapshot(snapshot);
      resultHandler.onComplete(null);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  private void applySnapshot(PartitionedSnapshot snapshot) {
    List<Integer> sockets = metaGroupMember.getPartitionTable().getNodeSockets(getHeader());
    for (Integer socket : sockets) {
      SimpleSnapshot subSnapshot = (SimpleSnapshot) snapshot.getSnapshot(socket);
      if (subSnapshot != null) {
        applySnapshot(subSnapshot);
      }
    }
  }
}
