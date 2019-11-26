/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.server.member;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.SimpleSnapshot;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.applier.MetaLogApplier;
import org.apache.iotdb.cluster.log.manage.SingleSnapshotLogManager;
import org.apache.iotdb.cluster.log.meta.AddNodeLog;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.SocketPartitionTable;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.server.DataClusterServer;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendGroupEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.JoinClusterHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.ForwardAddNodeHandler;
import org.apache.iotdb.cluster.server.heartbeat.MetaHeartBeatThread;
import org.apache.iotdb.cluster.server.member.DataGroupMember.Factory;
import org.apache.iotdb.db.qp.QueryProcessor;
import org.apache.iotdb.db.qp.executor.QueryProcessExecutor;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaGroupMember extends RaftMember implements TSMetaService.AsyncIface {

  private static final String NODE_IDENTIFIER_NAME = "node_identifier";
  private static final Logger logger = LoggerFactory.getLogger(MetaGroupMember.class);
  private static final int DEFAULT_JOIN_RETRY = 10;
  private static final String NODES_FILE_NAME = "nodes";
  public static final int REPLICATION_NUM =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();

  private TProtocolFactory protocolFactory;
  private AsyncClient.Factory clientFactory;

  // blind nodes are nodes that does not know the nodes in the cluster
  private Set<Node> blindNodes = new HashSet<>();
  private Map<Node, Integer> idConflictNodes = new HashMap<>();
  private Map<Integer, Node> idNodeMap = null;

  private PartitionTable partitionTable;
  private DataClusterServer dataClusterServer;

  // all data servers in this node shares the same partitioned data log manager
  private LogApplier metaLogApplier = new MetaLogApplier(this);
  private LogApplier dataLogApplier = new DataLogApplier();
  private DataGroupMember.Factory dataMemberFactory;

  private SingleSnapshotLogManager logManager;

  public MetaGroupMember(TProtocolFactory factory, Node thisNode)
      throws IOException {
    super("Meta");
    allNodes = new ArrayList<>();
    this.protocolFactory = factory;
    clientFactory = new AsyncClient.Factory(new TAsyncClientManager(), factory);
    dataMemberFactory = new Factory(protocolFactory, this, dataLogApplier);

    setThisNode(thisNode);
    loadIdentifier();
  }

  @Override
  void initLogManager() {
    logManager = new SingleSnapshotLogManager(metaLogApplier);
    super.logManager = logManager;
  }

  @Override
  public void start() throws TTransportException {
    super.start();
    initLogManager();
    initDataServers();

    if (!loadNodes()) {
      addSeedNodes();
    }
    queryProcessor = new QueryProcessor(new QueryProcessExecutor());
  }

  @Override
  public void stop() {
    super.stop();
    dataClusterServer.stop();
  }

  private void initDataServers() throws TTransportException {
    this.dataClusterServer = new DataClusterServer(config.getLocalDataPort(), dataMemberFactory);
    dataClusterServer.start();
  }

  private void addSeedNodes() {
    List<String> seedUrls = config.getSeedNodeUrls();
    for (String seedUrl : seedUrls) {
      String[] split = seedUrl.split(":");
      if (split.length != 3) {
        logger.warn("Bad seed url: {}", seedUrl);
        continue;
      }
      String ip = split[0];
      // TODO-Cluster: check ip format
      try {
        int metaPort = Integer.parseInt(split[1]);
        int dataPort = Integer.parseInt(split[2]);
        if (!ip.equals(thisNode.ip) || metaPort != thisNode.port) {
          Node seedNode = new Node();
          seedNode.setIp(ip);
          seedNode.setPort(metaPort);
          seedNode.setDataPort(dataPort);
          if (!allNodes.contains(seedNode)) {
            allNodes.add(seedNode);
          }
        }
      } catch (NumberFormatException e) {
        logger.warn("Bad seed url: {}", seedUrl);
      }
    }
  }

  @Override
  RaftService.AsyncClient getAsyncClient(TNonblockingTransport transport) {
    return clientFactory.getAsyncClient(transport);
  }

  private void loadNode(String url) {
    String[] split = url.split(":");
    if (split.length != 4) {
      logger.warn("Incorrect node url: {}", url);
      return;
    }
    // TODO: check url format
    String ip = split[1];
    try {
      int identifier = Integer.parseInt(split[0]);
      int metaPort = Integer.parseInt(split[2]);
      int dataPort = Integer.parseInt(split[3]);
      Node node = new Node();
      node.setIp(ip);
      node.setPort(metaPort);
      node.setNodeIdentifier(identifier);
      node.setDataPort(dataPort);
      allNodes.add(node);
      idNodeMap.put(identifier, node);
    } catch (NumberFormatException e) {
      logger.warn("Incorrect node url: {}", url);
    }
  }

  public void applyAddNode(Node newNode) {
    synchronized (allNodes) {
      if (!allNodes.contains(newNode)) {
        logger.debug("Adding a new node {} into {}", newNode, allNodes);
        registerNodeIdentifier(newNode, newNode.getNodeIdentifier());
        allNodes.add(newNode);
        saveNodes();
        // update the partition table
        PartitionGroup newGroup;
        newGroup = partitionTable.addNode(newNode);

        // TODO-Cluster serialize the table to the persist store
        dataClusterServer.addNode(newNode);
        if (newGroup.contains(thisNode)) {
          try {
            DataGroupMember dataGroupMember = dataMemberFactory.create(newGroup, thisNode);
            dataClusterServer.addDataGroupMember(dataGroupMember);
            dataGroupMember.start();
            dataGroupMember.pullSnapshots(partitionTable.getNodeSockets(newNode), newNode);
          } catch (IOException | TTransportException e) {
            logger.error("Fail to create data newMember for new header {}", newNode, e);
          }
        }
      }
    }
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

    AtomicReference<AddNodeResponse> response = new AtomicReference(null);
    handler.setResponse(response);

    while (retry > 0) {
      // randomly pick up a node to try
      Node node = nodes[random.nextInt(nodes.length)];
      try {
        if (joinCluster(node, response, handler)) {
          logger.info("Joined a cluster, starting the heartbeat thread");
          setCharacter(NodeCharacter.FOLLOWER);
          setLastHeartBeatReceivedTime(System.currentTimeMillis());
          heartBeatService.submit(new MetaHeartBeatThread(this));
          // TODO-Cluster: pull data from the previous socket holders
          return;
        }
      } catch (TException | IOException e) {
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

  private boolean joinCluster(Node node, AtomicReference<AddNodeResponse> response,
      JoinClusterHandler handler)
      throws TException, InterruptedException, IOException {
    AsyncClient client = (AsyncClient) connectNode(node);
    if (client != null) {
      response.set(null);
      handler.setContact(node);

      synchronized (response) {
        client.addNode(thisNode, handler);
        response.wait(CONNECTION_TIME_OUT_MS);
      }
      AddNodeResponse resp = response.get();
      if (resp == null) {
        logger.warn("Join cluster request timed out");
      } else if (resp.getRespNum() == Response.RESPONSE_AGREE) {
        logger.info("Node {} admitted this node into the cluster", node);
        ByteBuffer partitionTableBuffer = ByteBuffer.wrap(resp.getPartitionTableBytes());
        partitionTable = new SocketPartitionTable(thisNode);
        partitionTable.deserialize(partitionTableBuffer);
        partitionTable.setPreviousNodeMap(thisNode, resp.getPreviousHolders());

        allNodes = partitionTable.getAllNodes();
        logger.info("Received cluster nodes from the leader: {}", allNodes);
        initIdNodeMap();
        for (Node n : allNodes) {
          idNodeMap.put(n.getNodeIdentifier(), n);
        }
        buildDataGroups();
        dataClusterServer.pullSnapshots();
        return true;
      } else if (resp.getRespNum() == Response.RESPONSE_IDENTIFIER_CONFLICT) {
        logger.info("The identifier {} conflicts the existing ones, regenerate a new one", node.getNodeIdentifier());
        setNodeIdentifier(genNodeIdentifier());
      } else {
        logger.warn("Joining the cluster is rejected by {} for response {}", node, resp.getRespNum());
      }
      return false;
    }
    return false;
  }

  @Override
  void processValidHeartbeatReq(HeartBeatRequest request, HeartBeatResponse response,
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

    if (partitionTable == null) {
      // this node is blind to the cluster
      if (request.isSetPartitionTableBytes()) {
        synchronized (this) {
          // if the leader has sent the node set then accept it
          ByteBuffer byteBuffer = ByteBuffer.wrap(request.getPartitionTableBytes());
          partitionTable = new SocketPartitionTable(thisNode);
          partitionTable.deserialize(byteBuffer);
          allNodes = partitionTable.getAllNodes();
          dataClusterServer.setPartitionTable(partitionTable);
          try {
            buildDataGroups();
          } catch (IOException | TTransportException e) {
            logger.error("Cannot build data groups", e);
          }

          logger.info("Received partition table from the leader: {}", allNodes);
          initIdNodeMap();
          for (Node node : allNodes) {
            idNodeMap.put(node.getNodeIdentifier(), node);
          }
        }
      } else {
        // require the node list
        logger.debug("Request cluster nodes from the leader");
        response.setRequirePartitionTable(true);
      }
    }
  }

  @Override
  public void processValidHeartbeatResp(HeartBeatResponse response, Node receiver) {
    // register the id of the node
    if (response.isSetFolloweIdentifier()) {
      registerNodeIdentifier(receiver, response.getFolloweIdentifier());
      // if all nodes' ids are known, we can build the partition table
      if (allNodesIdKnown()) {
        buildPartitionTable();
      }
    }
    // record the requirement of node list of the follower
    if (response.isRequirePartitionTable()) {
      addBlindNode(receiver);
    }
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
    if (partitionTable == null) {
      // this node lacks information of the cluster and refuse to work
      logger.debug("This node is blind to the cluster and cannot accept logs");
      resultHandler.onComplete(Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE);
      return;
    }

    super.appendEntry(request, resultHandler);
  }

  @Override
  long appendEntry(Log log) {
    long resp = super.appendEntry(log);

    if (resp == Response.RESPONSE_AGREE && log instanceof AddNodeLog) {
      metaLogApplier.apply(log);
    }
    return resp;
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
  private boolean allNodesIdKnown() {
    return idNodeMap != null && idNodeMap.size() == allNodes.size();
  }

  /**
   * Use the initial nodes to build a partition table. As the logs catch up, the partitionTable
   * will eventually be consistent with the leader's.
   */
  private synchronized void buildPartitionTable() {
    if (partitionTable != null) {
      return;
    }
    partitionTable = new SocketPartitionTable(allNodes, thisNode);
    logger.info("Partition table is set up");
    synchronized (partitionTable) {
      try {
        buildDataGroups();
      } catch (IOException | TTransportException e) {
        logger.error("Build partition table failed: ", e);
        stop();
      }
    }
  }

  @Override
  public void addNode(Node node, AsyncMethodCallback resultHandler) {
    AddNodeResponse response = new AddNodeResponse();
    if (partitionTable == null) {
      logger.info("Cannot add node now because the partition table is not set");
      logger.debug("Known nodes: {}, all nodes: {}", idNodeMap, allNodes);
      response.setRespNum((int) Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE);
      resultHandler.onComplete(response);
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
        response.setRespNum((int) Response.RESPONSE_AGREE);
        synchronized (partitionTable) {
          response.setPreviousHolders(partitionTable.getPreviousNodeMap(node));
          response.setPartitionTableBytes(partitionTable.serialize());
        }
        resultHandler.onComplete(response);
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
        addNodeLog.setDataPort(node.getDataPort());

        logManager.appendLog(addNodeLog);
        // add node is instantly applied
        metaLogApplier.apply(addNodeLog);

        logger.info("Send the join request of {} to other nodes", node);
        AppendLogResult result = sendLogToAllGroups(addNodeLog);

        switch (result) {
          case OK:
            logger.info("Join request of {} is accepted", node);
            synchronized (partitionTable) {
              response.setPartitionTableBytes(partitionTable.serialize());
            }
            response.setRespNum((int) Response.RESPONSE_AGREE);
            response.setPreviousHolders(partitionTable.getPreviousNodeMap(node));
            resultHandler.onComplete(response);
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
   * Send the log the all data groups and return a success only when each group's quorum has
   * accepted this log.
   * @param log
   * @return
   */
  private AppendLogResult sendLogToAllGroups(Log log) {
    List<Node> nodeRing = partitionTable.getAllNodes();

    // each group is considered success if such members receive the log
    int groupQuorum = REPLICATION_NUM / 2 + 1;
    // each node will form a group
    int nodeSize = nodeRing.size();
    int[] groupRemainings = new int[nodeSize];
    for (int i = 0; i < groupRemainings.length; i++) {
      groupRemainings[i] = groupQuorum;
    }

    AtomicLong newLeaderTerm = new AtomicLong(term.get());
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(term.get());
    request.setEntry(log.serialize());

    synchronized (groupRemainings) {
      // ask a vote from every node
      for (int i = 0; i < nodeSize; i++) {
        Node node = nodeRing.get(i);
        AsyncClient client = (AsyncClient) connectNode(node);
        if (client != null) {
          try {
            client.appendEntry(request, new AppendGroupEntryHandler(groupRemainings, i, node,
                leaderShipStale, log, newLeaderTerm));
          } catch (TException e) {
            logger.error("Cannot send log to node {}", node, e);
          }
        }
      }

      try {
        groupRemainings.wait(CONNECTION_TIME_OUT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (!leaderShipStale.get()) {
      boolean succeed = true;
      // if all quorums of all groups have received this log, it is considered succeeded.
      for (int remaining : groupRemainings) {
        if (remaining > 0) {
          return AppendLogResult.TIME_OUT;
        }
      }
    } else {
      return AppendLogResult.LEADERSHIP_STALE;
    }

    return AppendLogResult.OK;
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

  /**
   * load the nodes from a local file
   * @return true if the local file is found, false otherwise
   */
  private boolean loadNodes() {
    File nodeFile = new File(NODES_FILE_NAME);
    if (!nodeFile.exists()) {
      logger.info("No node file found");
      return false;
    }
    initIdNodeMap();
    try (BufferedReader reader = new BufferedReader(new FileReader(NODES_FILE_NAME))) {
      String line;
      while ((line = reader.readLine()) != null) {
        loadNode(line);
      }
      logger.info("Load {} nodes: {}", allNodes.size(), allNodes);
    } catch (IOException e) {
      logger.error("Cannot load nodes", e);
      return false;
    }
    buildPartitionTable();
    return true;
  }

  private synchronized void saveNodes() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODES_FILE_NAME))){
      for (Node node : allNodes) {
        writer.write(node.getNodeIdentifier() + ":" + node.ip + ":" + node.port + ":" + node.dataPort);
        writer.newLine();
      }
    } catch (IOException e) {
      logger.error("Cannot save the nodes", e);
    }
  }

  // if the identifier file does not exist, a new identifier will be generated
  private void loadIdentifier() {
    File file = new File(NODE_IDENTIFIER_NAME);
    Integer nodeId = null;
    if (file.exists()) {
      try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
        nodeId = Integer.parseInt(reader.readLine());
      } catch (Exception e) {
        logger.warn("Cannot read the identifier from file, generating a new one");
      }
    }
    if (nodeId != null) {
      setNodeIdentifier(nodeId);
      return;
    }

    setNodeIdentifier(genNodeIdentifier());
  }

  private int genNodeIdentifier() {
    return Objects.hash(thisNode.getIp(), thisNode.getPort(),
        System.currentTimeMillis());
  }

  private void setNodeIdentifier(int identifier) {
    logger.info("The identifier of this node has been set to {}", identifier);
    thisNode.setNodeIdentifier(identifier);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODE_IDENTIFIER_NAME))) {
      writer.write(String.valueOf(identifier));
    } catch (IOException e) {
      logger.error("Cannot save the node identifier");
    }
  }

  private void buildDataGroups() throws IOException, TTransportException {
    List<PartitionGroup> partitionGroups = partitionTable.getLocalGroups();

    dataClusterServer.setPartitionTable(partitionTable);
    for (PartitionGroup partitionGroup : partitionGroups) {
      DataGroupMember dataGroupMember = dataMemberFactory.create(partitionGroup, thisNode);
      dataGroupMember.start();
      dataClusterServer.addDataGroupMember(dataGroupMember);
    }
    logger.info("Data group members are ready");
  }

  public PartitionTable getPartitionTable() {
    return partitionTable;
  }

  @Override
  public void sendSnapshot(SendSnapshotRequest request, AsyncMethodCallback resultHandler) {
    SimpleSnapshot snapshot = new SimpleSnapshot();
    try {
      snapshot.deserialize(ByteBuffer.wrap(request.getSnapshotBytes()));
      applySnapshot(snapshot);
      resultHandler.onComplete(null);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  private void applySnapshot(SimpleSnapshot snapshot) {
    synchronized (logManager) {
      for (Log log : snapshot.getSnapshot()) {
        logManager.getApplier().apply(log);
      }
      logManager.setSnapshot(snapshot);
    }
    logManager.setLastLogTerm(snapshot.getLastLogTerm());
    logManager.setLastLogId(snapshot.getLastLogId());
  }


  @Override
  public void pullSnapshot(PullSnapshotRequest request, AsyncMethodCallback resultHandler) {
    resultHandler.onError(new UnsupportedOperationException("Cannot pull snapshot from a meta "
        + "group newMember"));
  }
}
