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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.meta.AddNodeLog;
import org.apache.iotdb.cluster.log.meta.PhysicalPlanLog;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncProcessor;
import org.apache.iotdb.cluster.server.handlers.caller.JoinClusterHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.ForwardAddNodeHandler;
import org.apache.iotdb.cluster.server.heartbeat.HeartBeatThread;
import org.apache.iotdb.cluster.server.heartbeat.MetaHeartBeatThread;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MetaCluster manages cluster metadata, such as what nodes are in the cluster and data partition.
 */
public class MetaClusterServer extends RaftServer implements TSMetaService.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(MetaClusterServer.class);
  private static final int DEFAULT_JOIN_RETRY = 10;
  private static final String NODES_FILE_NAME = "nodes";

  // blind nodes are nodes that does not know the nodes in the cluster
  private Set<Node> blindNodes = new HashSet<>();
  private Map<Node, Integer> idConflictNodes = new HashMap<>();
  private Map<Integer, Node> idNodeMap = null;

  private AsyncClient.Factory clientFactory;
  private IoTDB ioTDB;
  private QueryProcessor queryProcessor;
  private PartitionTable partitionTable;

  public MetaClusterServer() throws IOException {
    super();
    clientFactory = new AsyncClient.Factory(new TAsyncClientManager(), protocolFactory);
    loadNodes();
  }

  /**
   * Use the initial nodes to build a partition table. As the logs catch up, the partitionTable
   * will eventually be consistent with the leader's.
   */
  private void buildPartitionTable() {
    //TODO-Cluster: implement
  }

  @Override
  public void start() throws TTransportException {
    super.start();
    ioTDB = new IoTDB();
    ioTDB.active();
    queryProcessor = new QueryProcessor(new QueryProcessExecutor());
  }

  @Override
  void stop() {
    super.stop();
    ioTDB.stop();
    ioTDB = null;
  }

  @Override
  RaftService.AsyncClient getAsyncClient(TNonblockingTransport transport) {
    return clientFactory.getAsyncClient(transport);
  }

  @Override
  AsyncProcessor getProcessor() {
    return new AsyncProcessor(this);
  }

  /**
   * load the nodes from a local file
   */
  private void loadNodes() {
    File nodeFile = new File(NODES_FILE_NAME);
    if (!nodeFile.exists()) {
      logger.info("No node file found");
      return;
    }
    initIdNodeMap();
    try (BufferedReader reader = new BufferedReader(new FileReader(NODES_FILE_NAME))) {
      String line;
      while ((line = reader.readLine()) != null) {
        loadNode(line);
      }
      logger.info("Load {} nodes: {}", idNodeMap.size(), idNodeMap);
    } catch (IOException e) {
      logger.error("Cannot load nodes", e);
    }
  }

  private void loadNode(String url) {
    String[] split = url.split(":");
    if (split.length != 3) {
      logger.warn("Incorrect node url: {}", url);
      return;
    }
    // TODO: check url format
    String ip = split[1];
    try {
      int identifier = Integer.parseInt(split[0]);
      int port = Integer.parseInt(split[2]);
      Node node = new Node();
      node.setIp(ip);
      node.setPort(port);
      node.setNodeIdentifier(identifier);
      allNodes.add(node);
      idNodeMap.put(identifier, node);
    } catch (NumberFormatException e) {
      logger.warn("Incorrect node url: {}", url);
    }
  }

  private synchronized void saveNodes() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODES_FILE_NAME))){
      for (Node node : idNodeMap.values()) {
        writer.write(node.getNodeIdentifier() + ":" + node.ip + ":" + node.port);
        writer.newLine();
      }
    } catch (IOException e) {
      logger.error("Cannot save the nodes", e);
    }
  }

  @Override
  public void apply(Log log) {
    if (log instanceof AddNodeLog) {
      AddNodeLog addNodeLog = (AddNodeLog) log;
      Node newNode = new Node();
      newNode.setIp(addNodeLog.getIp());
      newNode.setPort(addNodeLog.getPort());
      newNode.setNodeIdentifier(addNodeLog.getNodeIdentifier());
      if (!allNodes.contains(newNode)) {
        synchronized (idNodeMap) {
          registerNodeIdentifier(newNode, newNode.getNodeIdentifier());
          allNodes.add(newNode);
          saveNodes();
          // update the partition table
          partitionTable.addNode(newNode);
        }
      }
    } else if (log instanceof PhysicalPlanLog) {
      PhysicalPlanLog physicalPlanLog = (PhysicalPlanLog) log;
      try {
        queryProcessor.getExecutor().processNonQuery(physicalPlanLog.getPlan());
      } catch (ProcessorException e) {
        logger.error("Log {} cannot be applied:", e);
      }
    } else {
      // TODO-Cluster support more types of logs
      logger.error("Unsupported log: {}", log);
    }
  }

  /**
   * Compare the old partition table and the new one, then issue data transfer if necessary.
   * When data transfer is issued, the data flow is unidirectional, which means only the senders
   * will actively send the data or the receivers will actively pull the data (this is to be
   * discussed).
   * @param oldTable
   * @param newTable
   */
  private void repartition(PartitionTable oldTable, PartitionTable newTable) {
    // TODO-Cluster: implement
  }

  /**
   * This node itself is a seed node, and it is going to build the initial cluster with other seed
   * nodes
   */
  public void buildCluster() {
    // just establish the heart beat thread and it will do the remaining
    heartBeatService.submit(new MetaHeartBeatThread(this));
  }

  /**
   * This node is a node seed node and wants to join an established cluster
   */
  public void joinCluster() {
    int retry = DEFAULT_JOIN_RETRY;
    Node[] nodes = allNodes.toArray(new Node[0]);
    JoinClusterHandler handler = new JoinClusterHandler();

    AtomicLong response = new AtomicLong(RESPONSE_UNSET);
    handler.setResponse(response);

    while (retry > 0) {
      // randomly pick up a node to try
      Node node = nodes[random.nextInt(nodes.length)];
      try {
        if (joinCluster(node, response, handler)) {
          logger.info("Joined a cluster, starting the heartbeat thread");
          setCharacter(NodeCharacter.FOLLOWER);
          setLastHeartBeatReceivedTime(System.currentTimeMillis());
          heartBeatService.submit(new HeartBeatThread(this));
          return;
        }
      } catch (TException e) {
        logger.warn("Cannot join the cluster from {}, because:", node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Cannot join the cluster from {}, because time out after {}ms",
            node, CONNECTION_TIME_OUT_MS);
      }
      // start next try
      retry--;
    }
    // all tries failed
    logger.error("Cannot join the cluster after {} retries", DEFAULT_JOIN_RETRY);
    stop();
  }

  private boolean joinCluster(Node node, AtomicLong response, JoinClusterHandler handler)
      throws TException, InterruptedException {
    AsyncClient client = (AsyncClient) connectNode(node);
    if (client != null) {
      response.set(RESPONSE_UNSET);
      handler.setContact(node);

      client.addNode(thisNode, handler);

      synchronized (response) {
        if (response.get() == RESPONSE_UNSET) {
          response.wait(CONNECTION_TIME_OUT_MS);
        }
      }
      long resp = response.get();
      if (resp == RESPONSE_AGREE) {
        logger.info("Node {} admitted this node into the cluster", node);
        return true;
      } else if (resp == RESPONSE_IDENTIFIER_CONFLICT) {
        logger.info("The identifier {} conflicts the existing ones, regenerate a new one", node.getNodeIdentifier());
        setNodeIdentifier(genNodeIdentifier());
      }
      logger.warn("Joining the cluster is rejected by {} for response {}", node, resp);
      return false;
    }
    return false;
  }

  @Override
  void processLegalHeartbeat(HeartBeatRequest request, HeartBeatResponse response,
      long leaderTerm) {
    if (request.isRequireIdentifier()) {
      // the leader wants to know who the node is
      if (request.isRegenerateIdentifier()) {
        // the previously sent id conflicted, generate a new one
        setNodeIdentifier(genNodeIdentifier());
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Send identifier {} to the leader", thisNode.getNodeIdentifier());
      }
      response.setFolloweIdentifier(thisNode.getNodeIdentifier());
    }

    if (!allNodesIdKnown()) {
      // this node is blind to the cluster
      if (request.isSetNodeSet()) {
        // if the leader has sent the node set then accept it
        allNodes = request.getNodeSet();
        logger.info("Received cluster nodes from the leader: {}", allNodes);
        initIdNodeMap();
        for (Node node : allNodes) {
          idNodeMap.put(node.getNodeIdentifier(), node);
        }
      } else {
        // require the node list
        logger.debug("Request cluster nodes from the leader");
        response.setRequireNodeList(true);
      }
    }
    super.processLegalHeartbeat(request, response, leaderTerm);
  }

  @Override
  public void processValidHeartbeatResp(HeartBeatResponse response, Node receiver) {
    // register the id of the node
    if (response.isSetFolloweIdentifier()) {
      registerNodeIdentifier(receiver, response.getFolloweIdentifier());
    }
    // record the requirement of node list of the follower
    if (response.isRequireNodeList()) {
      addBlindNode(receiver);
    }
    super.processValidHeartbeatResp(response, receiver);
  }

  /**
   * When a node requires node list in its heartbeat response, add it into blindNodes so in the
   * heartbeat the node list will be sent to the node.
   * @param node
   */
  private void addBlindNode(Node node) {
    logger.debug("Node {} requires the node list", node);
    blindNodes.add(node);
  }

  /**
   *
   * @param node
   * @return whether a node wants the node list.
   */
  public boolean isNodeBlind(Node node) {
    return blindNodes.contains(node);
  }

  /**
   * Remove the node from the blindNodes when the node list is sent.
   * @param node
   */
  public void removeBlindNode(Node node) {
    blindNodes.remove(node);
  }

  /**
   * Register the identifier for the node if it does not conflict with other nodes.
   * @param node
   * @param identifier
   */
  private void registerNodeIdentifier(Node node, int identifier) {
    synchronized (idNodeMap) {
      if (idNodeMap.containsKey(identifier)) {
        return;
      }
      node.setNodeIdentifier(identifier);
      logger.info("Node {} registered with id {}", node, identifier);
      idNodeMap.put(identifier, node);
      idConflictNodes.remove(node);
    }
  }

  /**
   * idNodeMap is initialized when the first leader wins or the follower receives the node list
   * from the leader or a node recovers
   */
  private void initIdNodeMap() {
    idNodeMap = new HashMap<>();
    idNodeMap.put(thisNode.getNodeIdentifier(), thisNode);
  }

  @Override
  public void appendEntry(AppendEntryRequest request, AsyncMethodCallback resultHandler) {
    if (idNodeMap == null) {
      // this node lacks information of the cluster and refuse to work
      logger.debug("This node is blind to the cluster and cannot accept logs");
      resultHandler.onComplete(RESPONSE_CLUSTER_UNKNOWN);
      return;
    }
    super.appendEntry(request, resultHandler);
  }

  public Map<Integer, Node> getIdNodeMap() {
    return idNodeMap;
  }

  public void setIdNodeMap(
      Map<Integer, Node> idNodeMap) {
    this.idNodeMap = idNodeMap;
  }

  /**
   *
   * @return Whether all nodes' identifier is known.
   */
  public boolean allNodesIdKnown() {
    return idNodeMap != null && idNodeMap.size() == allNodes.size();
  }

  @Override
  public void addNode(Node node, AsyncMethodCallback resultHandler) {
    if (!allNodesIdKnown()) {
      logger.info("Cannot add node now because not all nodes' id are known");
      logger.debug("Known nodes: {}, all nodes: {}", idNodeMap, allNodes);
      resultHandler.onComplete((int) RESPONSE_CLUSTER_UNKNOWN);
      return;
    }

    logger.info("A node {} wants to join this cluster", node);
    if (node == thisNode) {
      resultHandler.onError(new AddSelfException());
      return;
    }

    if (character == NodeCharacter.LEADER) {
      if (idNodeMap.containsKey(node.getNodeIdentifier())) {
        logger.debug("Node {} is already in the cluster", node);
        resultHandler.onComplete((int) RESPONSE_AGREE);
        return;
      }

      // node adding must be serialized
      synchronized (logManager) {
        AddNodeLog addNodeLog = new AddNodeLog();
        addNodeLog.setPreviousLogIndex(logManager.getLastLogIndex());
        addNodeLog.setPreviousLogTerm(logManager.getLastLogTerm());
        addNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);
        addNodeLog.setCurrLogTerm(getTerm().get());

        addNodeLog.setIp(node.getIp());
        addNodeLog.setPort(node.getPort());
        addNodeLog.setNodeIdentifier(node.getNodeIdentifier());

        logManager.appendLog(addNodeLog);

        logger.info("Send the join request of {} to other nodes", node);
        // adding a node requires strong consistency, -2 for this node and the new node
        AppendLogResult result = sendLogToFollowers(addNodeLog, allNodes.size() - 2);

        switch (result) {
          case OK:
            logger.info("Join request of {} is accepted", node);
            resultHandler.onComplete((int) RESPONSE_AGREE);
            logManager.commitLog(logManager.getLastLogIndex());
            return;
          case TIME_OUT:
            logger.info("Join request of {} timed out", node);
            resultHandler.onError(new RequestTimeOutException(addNodeLog));
            logManager.removeLastLog();
            return;
          case LEADERSHIP_STALE:
          default:
            logManager.removeLastLog();
            // if the leader is found, forward to it
        }
      }
    }
    if (character == NodeCharacter.FOLLOWER && leader != null) {
      logger.info("Forward the join request of {} to leader {}", node, leader);
      if (forwardAddNode(node, resultHandler)) {
        return;
      }
    }
    resultHandler.onError(new LeaderUnknownException());
  }

  /**
   * Forward the join cluster request to the leader.
   * @param node
   * @param resultHandler
   * @return true if the forwarding succeeds, false otherwise.
   */
  private boolean forwardAddNode(Node node, AsyncMethodCallback resultHandler) {
    TSMetaService.AsyncClient client = (TSMetaService.AsyncClient) connectNode(leader);
    if (client != null) {
      try {
        client.addNode(node, new ForwardAddNodeHandler(resultHandler));
        return true;
      } catch (TException e) {
        logger.warn("Cannot connect to node {}", node, e);
      }
    }
    return false;
  }

  public Map<Node, Integer> getIdConflictNodes() {
    return idConflictNodes;
  }

  @Override
  public void onElectionWins() {
    if (idNodeMap == null) {
      initIdNodeMap();
    }
  }
}
