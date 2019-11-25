/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.PartitionedSnapshot;
import org.apache.iotdb.cluster.log.RemoteSimpleSnapshot;
import org.apache.iotdb.cluster.log.SimpleSnapshot;
import org.apache.iotdb.cluster.log.Snapshot;
import org.apache.iotdb.cluster.log.manage.PartitionedSnapshotLogManager;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.ElectionRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotResp;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSDataService;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.PullSnapshotHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.ForwardPullSnapshotHandler;
import org.apache.iotdb.cluster.server.heartbeat.HeartBeatThread;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.thrift.TException;
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

  private ExecutorService pullSnapshotService;
  private PartitionedSnapshotLogManager logManager;

  private DataGroupMember(TProtocolFactory factory, PartitionGroup nodes, Node thisNode,
      PartitionedSnapshotLogManager logManager, MetaGroupMember metaGroupMember) throws IOException {
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
    pullSnapshotService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @Override
  public void stop() {
    super.stop();
    pullSnapshotService.shutdownNow();
    pullSnapshotService = null;
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
        logger.debug("Node {} is inserted into the data group {}", node, this);
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
      logger.debug("Received a snapshot {}", snapshot);
      applySnapshot(snapshot);
      resultHandler.onComplete(null);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  private void applySnapshot(PartitionedSnapshot snapshot) {
    synchronized (logManager) {
      List<Integer> sockets = metaGroupMember.getPartitionTable().getNodeSockets(getHeader());
      for (Integer socket : sockets) {
        SimpleSnapshot subSnapshot = (SimpleSnapshot) snapshot.getSnapshot(socket);
        if (subSnapshot != null) {
          applySnapshot(subSnapshot, socket);
        }
      }
      logManager.setLastLogId(snapshot.getLastLogId());
      logManager.setLastLogTerm(snapshot.getLastLogTerm());
    }
  }

  private void applySnapshot(SimpleSnapshot snapshot, int socket) {
    synchronized (logManager) {
      for (Log log : snapshot.getSnapshot()) {
        logManager.getApplier().apply(log);
      }
      logManager.setSnapshot(snapshot, socket);
    }
  }

  @Override
  public void pullSnapshot(PullSnapshotRequest request, AsyncMethodCallback resultHandler) {
    if (character != NodeCharacter.LEADER) {
      logger.debug("Forwarding a pull snapshot request to the leader {}", leader);
      // forward the request to the leader
      if (leader != null) {
        AsyncClient client = connectNode(leader);
        try {
          client.pullSnapshot(request, new ForwardPullSnapshotHandler(resultHandler));
        } catch (TException e) {
          resultHandler.onError(e);
        }
      } else {
        resultHandler.onError(new LeaderUnknownException());
      }
      return;
    }

    // this synchronized should work with the one in AppendEntry when a log is going to commit,
    // which may prevent the newly arrived data from being invisible to the new header.
    synchronized (logManager) {
      logManager.takeSnapshot();
      int requiredSocket = request.getRequiredSocket();
      PartitionedSnapshot allSnapshot = (PartitionedSnapshot) logManager.getSnapshot();
      Snapshot snapshot = allSnapshot.getSnapshot(requiredSocket);
      PullSnapshotResp resp = new PullSnapshotResp();
      resp.setSnapshotBytes(snapshot.serialize());
      logger.debug("Sending snapshot {} to the requester", snapshot);
      resultHandler.onComplete(resp);
    }
  }

  public void pullSnapshots(List<Integer> sockets) {
    synchronized (logManager) {
      logger.info("Pulling sockets {} from remote", sockets);
      PartitionedSnapshot snapshot = (PartitionedSnapshot) logManager.getSnapshot();
      Map<Integer, Node> prevHolders = metaGroupMember.getPartitionTable().getPreviousNodeMap(thisNode);
      logger.debug("Holders of each socket: {}", prevHolders);

      for (int socket : sockets) {
        Node node = prevHolders.get(socket);
        if (snapshot.getSnapshot(socket) == null) {
          Future<SimpleSnapshot> snapshotFuture =
              pullSnapshotService.submit(new PullSnapshotTask(node, socket, this,
                  metaGroupMember.getPartitionTable().getHeaderGroup(node)));
          logManager.setSnapshot(new RemoteSimpleSnapshot(snapshotFuture), socket);
        }
      }
    }
  }

  class PullSnapshotTask implements Callable<SimpleSnapshot> {

    int socket;
    // the new member created by a node addition
    DataGroupMember newMember;
    // the nodes the may hold the target socket
    List<Node> oldMembers;
    // the header of the old members
    Node header;

    PullSnapshotTask(Node header, int socket,
        DataGroupMember member, List<Node> oldMembers) {
      this.header = header;
      this.socket = socket;
      this.newMember = member;
      this.oldMembers = oldMembers;
    }

    @Override
    public SimpleSnapshot call() {
      PullSnapshotRequest request = new PullSnapshotRequest();
      request.setHeader(header);
      request.setRequiredSocket(socket);
      AtomicReference<SimpleSnapshot> snapshotRef = new AtomicReference<>();
      boolean finished = false;
      int nodeIndex = -1;
      while (!finished) {
        try {
          // sequentially pick up a node that may have this socket
          nodeIndex = (nodeIndex + 1) % oldMembers.size();
          TSDataService.AsyncClient client =
              (TSDataService.AsyncClient) newMember.connectNode(oldMembers.get(nodeIndex));
          if (client == null) {
            // network is bad, wait and retry
            Thread.sleep(CONNECTION_TIME_OUT_MS);
          } else {
            synchronized (snapshotRef) {
              client.pullSnapshot(request, new PullSnapshotHandler(snapshotRef));
              snapshotRef.wait(CONNECTION_TIME_OUT_MS);
            }
            SimpleSnapshot result = snapshotRef.get();
            if (result != null) {
              if (logger.isInfoEnabled()) {
                logger.info("Received a snapshot {} of socket {} from {}", snapshotRef.get(),
                    socket, oldMembers.get(nodeIndex));
              }
              newMember.applySnapshot(result, socket);
              finished = true;
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("Unexpected interruption when pulling socket {}", socket, e);
          finished = true;
        } catch (TException e) {
          logger.debug("Cannot pull socket {} from {}, retry", socket, header, e);
        }
      }
      return snapshotRef.get();
    }
  }
}
