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

package org.apache.iotdb.cluster.server.member;

import static org.apache.iotdb.cluster.server.RaftServer.connectionTimeoutInMS;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.ClusterFileFlushPolicy;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.client.MetaClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.AddSelfException;
import org.apache.iotdb.cluster.exception.LeaderUnknownException;
import org.apache.iotdb.cluster.exception.NotInSameGroupException;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.applier.MetaLogApplier;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.manage.MetaSingleSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.SlotPartitionTable;
import org.apache.iotdb.cluster.query.ClusterQueryParser;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.query.reader.RemoteSeriesReaderByTimestamp;
import org.apache.iotdb.cluster.query.reader.RemoteSimpleSeriesReader;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartBeatResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaRequest;
import org.apache.iotdb.cluster.rpc.thrift.PullSchemaResp;
import org.apache.iotdb.cluster.rpc.thrift.SendSnapshotRequest;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.TNodeStatus;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.server.ClientServer;
import org.apache.iotdb.cluster.server.DataClusterServer;
import org.apache.iotdb.cluster.server.NodeCharacter;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendGroupEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.caller.JoinClusterHandler;
import org.apache.iotdb.cluster.server.handlers.caller.NodeStatusHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullTimeseriesSchemaHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.GenericForwardHandler;
import org.apache.iotdb.cluster.server.heartbeat.MetaHeartBeatThread;
import org.apache.iotdb.cluster.server.member.DataGroupMember.Factory;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupAlreadySetException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaGroupMember extends RaftMember implements TSMetaService.AsyncIface {
  private static final String NODE_IDENTIFIER_FILE_NAME = "node_identifier";
  private static final String PARTITION_FILE_NAME = "partitions";
  private static final String TEMP_SUFFIX = ".tmp";

  private static final Logger logger = LoggerFactory.getLogger(MetaGroupMember.class);
  private static final int DEFAULT_JOIN_RETRY = 10;
  public static final int REPLICATION_NUM =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();

  private TProtocolFactory protocolFactory;

  // blind nodes are nodes that does not know the nodes in the cluster
  private Set<Node> blindNodes = new HashSet<>();
  private Set<Node> idConflictNodes = new HashSet<>();
  private Map<Integer, Node> idNodeMap = null;

  private PartitionTable partitionTable;
  private DataClusterServer dataClusterServer;
  private ClientServer clientServer;

  private LogApplier metaLogApplier = new MetaLogApplier(this);
  private LogApplier dataLogApplier = new DataLogApplier(this);
  private DataGroupMember.Factory dataMemberFactory;

  private MetaSingleSnapshotLogManager logManager;

  private ClientPool dataClientPool;

  @TestOnly
  public MetaGroupMember() {
  }

  public MetaGroupMember(TProtocolFactory factory, Node thisNode)
      throws IOException {
    super("Meta", new ClientPool(new MetaClient.Factory(new TAsyncClientManager(), factory)));
    allNodes = new ArrayList<>();
    this.protocolFactory = factory;
    dataMemberFactory = new Factory(protocolFactory, this, dataLogApplier,
        new TAsyncClientManager());
    dataClientPool =
        new ClientPool(new DataClient.Factory(new TAsyncClientManager(), factory));
    initLogManager();
    setThisNode(thisNode);
    loadIdentifier();

    dataClusterServer = new DataClusterServer(thisNode, dataMemberFactory);
    clientServer = new ClientServer(this);
  }

  public void closePartition(String storageGroupName, boolean isSeq) {
    synchronized (logManager) {
      CloseFileLog log = new CloseFileLog(storageGroupName, isSeq);
      log.setCurrLogTerm(getTerm().get());
      log.setPreviousLogIndex(logManager.getLastLogIndex());
      log.setPreviousLogTerm(logManager.getLastLogTerm());
      log.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      logManager.appendLog(log);

      logger.info("Send the close file request of {} to other nodes", log);
      AppendLogResult result = sendLogToAllGroups(log);

      switch (result) {
        case OK:
          logger.info("Close file request of {} is accepted", log);
          logManager.commitLog(logManager.getLastLogIndex());
        case TIME_OUT:
          logger.info("Close file request of {} timed out", log);
          logManager.removeLastLog();
        case LEADERSHIP_STALE:
        default:
          logManager.removeLastLog();
      }
    }
  }

  @Override
  void initLogManager() {
    logManager = new MetaSingleSnapshotLogManager(metaLogApplier);
    super.logManager = logManager;
  }

  @Override
  public void start() throws TTransportException {
    addSeedNodes();
    super.start();

    queryProcessor = new ClusterQueryParser(this);
    QueryCoordinator.getINSTANCE().setMetaGroupMember(this);
    StorageEngine.getInstance().setFileFlushPolicy(new ClusterFileFlushPolicy(this));
  }

  @Override
  public void stop() {
    super.stop();
    if (dataClusterServer != null) {
      dataClusterServer.stop();
      clientServer.stop();
    }
  }

  private void initSubServers() throws TTransportException, StartupException {
    dataClusterServer.start();
    clientServer.start();
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
      try {
        int metaPort = Integer.parseInt(split[1]);
        int dataPort = Integer.parseInt(split[2]);
        if (!ip.equals(thisNode.ip) || metaPort != thisNode.metaPort) {
          Node seedNode = new Node();
          seedNode.setIp(ip);
          seedNode.setMetaPort(metaPort);
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

  public void applyAddNode(Node newNode) {
    synchronized (allNodes) {
      if (!allNodes.contains(newNode)) {
        logger.debug("Adding a new node {} into {}", newNode, allNodes);
        registerNodeIdentifier(newNode, newNode.getNodeIdentifier());
        allNodes.add(newNode);
        idNodeMap.put(newNode.getNodeIdentifier(), newNode);

        // update the partition table
        PartitionGroup newGroup = partitionTable.addNode(newNode);
        savePartitionTable();

        dataClusterServer.addNode(newNode);
        if (newGroup.contains(thisNode)) {
          try {
            logger.info("Adding this node into a new group {}", newGroup);
            DataGroupMember dataGroupMember = dataMemberFactory.create(newGroup, thisNode);
            dataClusterServer.addDataGroupMember(dataGroupMember);
            dataGroupMember.start();
            dataGroupMember.pullSnapshots(partitionTable.getNodeSlots(newNode), newNode);
          } catch (TTransportException e) {
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
  public boolean joinCluster() {
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
          return true;
        }
        // wait a heartbeat to start the next try
        Thread.sleep(ClusterConstant.HEART_BEAT_INTERVAL_MS);
      } catch (TException | StartupException e) {
        logger.warn("Cannot join the cluster from {}, because:", node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Cannot join the cluster from {}, because time out after {}ms",
            node, ClusterConstant.CONNECTION_TIME_OUT_MS);
      }
      // start next try
      retry--;
    }
    // all tries failed
    logger.error("Cannot join the cluster after {} retries", DEFAULT_JOIN_RETRY);
    stop();
    return false;
  }

  private boolean joinCluster(Node node, AtomicReference<AddNodeResponse> response,
      JoinClusterHandler handler)
      throws TException, InterruptedException, StartupException {
    AsyncClient client = (AsyncClient) connectNode(node);
    if (client != null) {
      response.set(null);
      handler.setContact(node);

      synchronized (response) {
        client.addNode(thisNode, handler);
        response.wait(ClusterConstant.CONNECTION_TIME_OUT_MS);
      }
      AddNodeResponse resp = response.get();
      if (resp == null) {
        logger.warn("Join cluster request timed out");
      } else if (resp.getRespNum() == Response.RESPONSE_AGREE) {
        logger.info("Node {} admitted this node into the cluster", node);
        ByteBuffer partitionTableBuffer = ByteBuffer.wrap(resp.getPartitionTableBytes());
        partitionTable = new SlotPartitionTable(thisNode);
        partitionTable.deserialize(partitionTableBuffer);
        savePartitionTable();

        allNodes = new ArrayList<>(partitionTable.getAllNodes());
        logger.info("Received cluster nodes from the leader: {}", allNodes);
        initIdNodeMap();
        for (Node n : allNodes) {
          idNodeMap.put(n.getNodeIdentifier(), n);
        }

        initSubServers();
        buildDataGroups();
        dataClusterServer.pullSnapshots();
        return true;
      } else if (resp.getRespNum() == Response.RESPONSE_IDENTIFIER_CONFLICT) {
        logger.info("The identifier {} conflicts the existing ones, regenerate a new one",
            thisNode.getNodeIdentifier());
        setNodeIdentifier(genNodeIdentifier());
      } else {
        logger
            .warn("Joining the cluster is rejected by {} for response {}", node, resp.getRespNum());
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
      response.setFollowerIdentifier(thisNode.getNodeIdentifier());
    }

    if (partitionTable == null) {
      // this node is blind to the cluster
      if (request.isSetPartitionTableBytes()) {
        synchronized (this) {
          // if the leader has sent the node set then accept it
          ByteBuffer byteBuffer = ByteBuffer.wrap(request.getPartitionTableBytes());
          partitionTable = new SlotPartitionTable(thisNode);
          partitionTable.deserialize(byteBuffer);
          allNodes = new ArrayList<>(partitionTable.getAllNodes());
          savePartitionTable();

          startSubServers();

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
    if (response.isSetFollowerIdentifier()) {
      registerNodeIdentifier(receiver, response.getFollowerIdentifier());
      // if all nodes' ids are known, we can build the partition table
      if (allNodesIdKnown()) {
        if (partitionTable == null && !loadPartitionTable()) {
          partitionTable = new SlotPartitionTable(allNodes, thisNode);
          logger.info("Partition table is set up");
        }
        startSubServers();
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
   */
  private void addBlindNode(Node node) {
    logger.debug("Node {} requires the node list", node);
    blindNodes.add(node);
  }

  /**
   * @return whether a node wants the partition table.
   */
  public boolean isNodeBlind(Node node) {
    return blindNodes.contains(node);
  }

  /**
   * Remove the node from the blindNodes when the node list is sent.
   */
  public void removeBlindNode(Node node) {
    blindNodes.remove(node);
  }

  /**
   * Register the identifier for the node if it does not conflict with other nodes.
   */
  private void registerNodeIdentifier(Node node, int identifier) {
    synchronized (idNodeMap) {
      if (idNodeMap.containsKey(identifier)) {
        idConflictNodes.add(node);
        return;
      }
      node.setNodeIdentifier(identifier);
      logger.info("Node {} registered with id {}", node, identifier);
      idNodeMap.put(identifier, node);
      idConflictNodes.remove(node);
    }
  }

  /**
   * idNodeMap is initialized when the first leader wins or the follower receives the node list from
   * the leader or a node recovers
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

  public Map<Integer, Node> getIdNodeMap() {
    return idNodeMap;
  }

  /**
   * @return Whether all nodes' identifier is known.
   */
  private boolean allNodesIdKnown() {
    return idNodeMap != null && idNodeMap.size() == allNodes.size();
  }

  /**
   * Use the initial nodes to build a partition table. As the logs catch up, the partitionTable will
   * eventually be consistent with the leader's.
   */
  private synchronized void startSubServers() {
    synchronized (partitionTable) {
      try {
        initSubServers();
        buildDataGroups();
      } catch (TTransportException | StartupException e) {
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

    // try to process the request locally, if it cannot be processed locally, forward it
    if (processAddNodeLocally(node, response, resultHandler)) {
      return;
    }

    if (character == NodeCharacter.FOLLOWER && leader != null) {
      logger.info("Forward the join request of {} to leader {}", node, leader);
      if (forwardAddNode(node, resultHandler)) {
        return;
      }
    }
    resultHandler.onError(new LeaderUnknownException(getAllNodes()));
  }

  private boolean processAddNodeLocally(Node node, AddNodeResponse response,
      AsyncMethodCallback resultHandler) {
    if (character == NodeCharacter.LEADER) {
      if (allNodes.contains(node)) {
        logger.debug("Node {} is already in the cluster", node);
        response.setRespNum((int) Response.RESPONSE_AGREE);
        synchronized (partitionTable) {
          response.setPartitionTableBytes(partitionTable.serialize());
        }
        resultHandler.onComplete(response);
        return true;
      }

      Node idConflictNode = idNodeMap.get(node.getNodeIdentifier());
      if (idConflictNode != null) {
        logger.debug("{}'s id conflicts with {}", node, idConflictNode);
        response.setRespNum((int) Response.RESPONSE_IDENTIFIER_CONFLICT);
        resultHandler.onComplete(response);
        return true;
      }

      // node adding must be serialized
      synchronized (logManager) {
        AddNodeLog addNodeLog = new AddNodeLog();
        addNodeLog.setCurrLogTerm(getTerm().get());
        addNodeLog.setPreviousLogIndex(logManager.getLastLogIndex());
        addNodeLog.setPreviousLogTerm(logManager.getLastLogTerm());
        addNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);

        addNodeLog.setNewNode(node);

        logManager.appendLog(addNodeLog);

        logger.info("Send the join request of {} to other nodes", node);
        AppendLogResult result = sendLogToAllGroups(addNodeLog);

        switch (result) {
          case OK:
            logger.info("Join request of {} is accepted", node);
            // add node is instantly applied to update the partition table
            try {
              logManager.getApplier().apply(addNodeLog);
            } catch (QueryProcessException e) {
              logManager.removeLastLog();
              resultHandler.onError(e);
              return true;
            }
            synchronized (partitionTable) {
              response.setPartitionTableBytes(partitionTable.serialize());
            }
            response.setRespNum((int) Response.RESPONSE_AGREE);
            resultHandler.onComplete(response);
            logManager.commitLog(logManager.getLastLogIndex());
            return true;
          case TIME_OUT:
            logger.info("Join request of {} timed out", node);
            resultHandler.onError(new RequestTimeOutException(addNodeLog));
            logManager.removeLastLog();
            return true;
          case LEADERSHIP_STALE:
          default:
            logManager.removeLastLog();
            // if the leader is found, forward to it
        }
      }
    }
    return false;
  }

  /**
   * Send the log the all data groups and return a success only when each group's quorum has
   * accepted this log.
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

    askGroupVotes(groupRemainings, nodeRing, request, leaderShipStale, log, newLeaderTerm);

    if (!leaderShipStale.get()) {
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

  private void askGroupVotes(int[] groupRemainings, List<Node> nodeRing,
      AppendEntryRequest request, AtomicBoolean leaderShipStale, Log log,
      AtomicLong newLeaderTerm) {
    int nodeSize = nodeRing.size();
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
        } else {
          // node == this node
          for (int j = 0; j < REPLICATION_NUM; j++) {
            int nodeIndex = i - j;
            if (nodeIndex < 0) {
              nodeIndex += groupRemainings.length;
            }
            groupRemainings[nodeIndex]--;
          }
        }
      }

      try {
        groupRemainings.wait(ClusterConstant.CONNECTION_TIME_OUT_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Forward the join cluster request to the leader.
   *
   * @return true if the forwarding succeeds, false otherwise.
   */
  private boolean forwardAddNode(Node node, AsyncMethodCallback resultHandler) {
    TSMetaService.AsyncClient client = (TSMetaService.AsyncClient) connectNode(leader);
    if (client != null) {
      try {
        client.addNode(node, new GenericForwardHandler(resultHandler));
        return true;
      } catch (TException e) {
        logger.warn("Cannot connect to node {}", node, e);
      }
    }
    return false;
  }

  public Set<Node> getIdConflictNodes() {
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
   *
   * @return true if the local file is found, false otherwise
   */
  private boolean loadPartitionTable() {
    File partitionFile = new File(PARTITION_FILE_NAME);
    if (!partitionFile.exists()) {
      logger.info("No node file found");
      return false;
    }
    initIdNodeMap();
    try (DataInputStream inputStream =
        new DataInputStream(new BufferedInputStream(new FileInputStream(PARTITION_FILE_NAME)))) {
      int size = inputStream.readInt();
      byte[] tableBuffer = new byte[size];
      inputStream.read(tableBuffer);

      partitionTable = new SlotPartitionTable(thisNode);
      partitionTable.deserialize(ByteBuffer.wrap(tableBuffer));
      allNodes = new ArrayList<>(partitionTable.getAllNodes());
      for (Node node : allNodes) {
        idNodeMap.put(node.getNodeIdentifier(), node);
      }

      logger.info("Load {} nodes: {}", allNodes.size(), allNodes);
    } catch (IOException e) {
      logger.error("Cannot load nodes", e);
      return false;
    }
    return true;
  }

  private synchronized void savePartitionTable() {
    File tempFile = new File(PARTITION_FILE_NAME + TEMP_SUFFIX);
    File oldFile = new File(PARTITION_FILE_NAME);
    try (DataOutputStream outputStream =
        new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempFile)))) {
      synchronized (partitionTable) {
        byte[] tableBuffer = partitionTable.serialize().array();
        outputStream.writeInt(tableBuffer.length);
        outputStream.write(tableBuffer);
        outputStream.flush();
      }
    } catch (IOException e) {
      logger.error("Cannot save the nodes", e);
    }
    tempFile.renameTo(oldFile);
  }

  // if the identifier file does not exist, a new identifier will be generated
  private void loadIdentifier() {
    File file = new File(NODE_IDENTIFIER_FILE_NAME);
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
    return Objects.hash(thisNode.getIp(), thisNode.getMetaPort(),
        System.currentTimeMillis());
  }

  private void setNodeIdentifier(int identifier) {
    logger.info("The identifier of this node has been set to {}", identifier);
    thisNode.setNodeIdentifier(identifier);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODE_IDENTIFIER_FILE_NAME))) {
      writer.write(String.valueOf(identifier));
    } catch (IOException e) {
      logger.error("Cannot save the node identifier");
    }
  }

  private void buildDataGroups() throws TTransportException {
    List<PartitionGroup> partitionGroups = partitionTable.getLocalGroups();

    dataClusterServer.setPartitionTable(partitionTable);
    for (PartitionGroup partitionGroup : partitionGroups) {
      logger.debug("Building member of data group: {}", partitionGroup);
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
    MetaSimpleSnapshot snapshot = new MetaSimpleSnapshot();
    try {
      snapshot.deserialize(request.snapshotBytes);
      applySnapshot(snapshot);
      resultHandler.onComplete(null);
    } catch (Exception e) {
      resultHandler.onError(e);
    }
  }

  private void applySnapshot(MetaSimpleSnapshot snapshot) {
    synchronized (logManager) {
      for (Log log : snapshot.getSnapshot()) {
        try {
          logManager.getApplier().apply(log);
        } catch (QueryProcessException e) {
          logger.error("{}: Cannot apply a log {} in snapshot, ignored", name, log, e);
        }
      }
      for (String storageGroup : snapshot.getStorageGroups()) {
        try {
          MManager.getInstance().setStorageGroupToMTree(storageGroup);
        } catch (StorageGroupAlreadySetException ignored) {
          // ignore duplicated storage group
        } catch (MetadataException e) {
          logger.error("{}: Cannot add storage group {} in snapshot", name, storageGroup);
        }
      }
      logManager.setSnapshot(snapshot);
    }
    logManager.setLastLogTerm(snapshot.getLastLogTerm());
    logManager.setLastLogId(snapshot.getLastLogId());
  }

  @Override
  public TSStatus executeNonQuery(PhysicalPlan plan) {
    if (!PartitionUtils.isPlanPartitioned(plan)) {
      return processNonPartitionedPlan(plan);
    } else {
      try {
        return processPartitionedPlan(plan);
      } catch (UnsupportedPlanException e) {
        TSStatus status = StatusUtils.UNSUPPORTED_OPERATION.deepCopy();
        status.getStatusType().setMessage(e.getMessage());
        return status;
      }
    }
  }

  private TSStatus processNonPartitionedPlan(PhysicalPlan plan) {
    if (character == NodeCharacter.LEADER) {
      TSStatus status = processPlanLocally(plan);
      if (status != null) {
        return status;
      }
    }
    return forwardPlan(plan, leader);
  }

  private TSStatus processPartitionedPlan(PhysicalPlan plan) throws UnsupportedPlanException {
    logger.debug("{}: Received a partitioned plan {}", name, plan);
    if (partitionTable == null) {
      logger.debug("{}: Partition table is not ready", name);
      return StatusUtils.PARTITION_TABLE_NOT_READY;
    }

    PartitionGroup partitionGroup = PartitionUtils.partitionPlan(plan, partitionTable);
    // the storage group is not found locally, forward it to the leader
    if (partitionGroup == null) {
      if (character != NodeCharacter.LEADER) {
        logger
            .debug("{}: Cannot found partition group for {}, forwarding to {}", name, plan, leader);
        return forwardPlan(plan, leader);
      } else {
        logger.debug("{}: Cannot found storage group for {}", name, plan);
        return StatusUtils.NO_STORAGE_GROUP;
      }
    }
    logger.debug("{}: The data group of {} is {}", name, plan, partitionGroup);

    if (partitionGroup.contains(thisNode)) {
      // the query should be handled by a group the local node is in, handle it with in the group
      return dataClusterServer.getDataMember(partitionGroup.getHeader(), null, plan)
          .executeNonQuery(plan);
    } else {
      // forward the query to the group that should handle it
      return forwardPlan(plan, partitionGroup);
    }
  }

  /**
   * Pull the all timeseries schemas of a given prefixPath from a remote node.
   */
  public List<MeasurementSchema> pullTimeSeriesSchemas(String prefixPath)
      throws StorageGroupNotSetException {
    logger.debug("{}: Pulling timeseries schemas of {}", name, prefixPath);
    PartitionGroup partitionGroup;
    try {
      partitionGroup = PartitionUtils.partitionByPathTime(prefixPath, 0, partitionTable);
    } catch (StorageGroupNotSetException e) {
      // the storage group is not found locally, but may be found in the leader, retry after
      // synchronizing with the leader
      if (syncLeader()) {
        partitionGroup = PartitionUtils.partitionByPathTime(prefixPath, 0, partitionTable);
      } else {
        throw e;
      }
    }
    if (partitionGroup.contains(thisNode)) {
      // the node is in the target group, synchronize with leader should be enough
      dataClusterServer.getDataMember(partitionGroup.getHeader(), null,
          "Pull timeseries of " + prefixPath).syncLeader();
      List<MeasurementSchema> timeseriesSchemas = new ArrayList<>();
      MManager.getInstance().collectSeries(prefixPath, timeseriesSchemas);
      return timeseriesSchemas;
    }

    PullSchemaRequest pullSchemaRequest = new PullSchemaRequest();
    pullSchemaRequest.setHeader(partitionGroup.getHeader());
    pullSchemaRequest.setPrefixPath(prefixPath);
    AtomicReference<List<MeasurementSchema>> timeseriesSchemas = new AtomicReference<>();
    for (Node node : partitionGroup) {
      logger.debug("{}: Pulling timeseries schemas of {} from {}", name, prefixPath, node);
      AsyncClient client = (AsyncClient) connectNode(node);
      synchronized (timeseriesSchemas) {
        try {
          client.pullTimeSeriesSchema(pullSchemaRequest, new PullTimeseriesSchemaHandler(node,
              prefixPath, timeseriesSchemas));
          timeseriesSchemas.wait(connectionTimeoutInMS);
        } catch (TException | InterruptedException e) {
          logger
              .error("{}: Cannot pull timeseries schemas of {} from {}", name, prefixPath, node, e);
          continue;
        }
      }
      List<MeasurementSchema> schemas = timeseriesSchemas.get();
      if (schemas != null) {
        return schemas;
      }
    }
    return Collections.emptyList();
  }

  @Override
  public void pullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    Node header = request.getHeader();
    DataGroupMember dataGroupMember = dataClusterServer.getDataMember(header, resultHandler,
        "Pull timeseries");
    if (dataGroupMember == null) {
      resultHandler
          .onError(new NotInSameGroupException(partitionTable.getHeaderGroup(header), thisNode));
      return;
    }

    dataGroupMember.pullTimeSeriesSchema(request, resultHandler);
  }

  public TSDataType getSeriesType(String pathStr) throws MetadataException {
    try {
      return SchemaUtils.getSeriesType(pathStr);
    } catch (PathNotExistException e) {
      List<MeasurementSchema> schemas = pullTimeSeriesSchemas(pathStr);
      // TODO-Cluster: should we register the schemas locally?
      if (schemas.isEmpty()) {
        throw e;
      }
      SchemaUtils.registerTimeseries(schemas.get(0));
      return schemas.get(0).getType();
    }
  }

  public IReaderByTimestamp getReaderByTimestamp(Path path, QueryContext context)
      throws IOException, StorageEngineException {
    // make sure the partition table is new
    syncLeader();
    PartitionGroup partitionGroup;
    try {
      // TODO-Cluster#350: use time in partitioning
      partitionGroup = PartitionUtils.partitionByPathTime(path.getFullPath(), 0,
          partitionTable);
    } catch (StorageGroupNotSetException e) {
      throw new StorageEngineException(e);
    }

    if (partitionGroup.contains(thisNode)) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember = dataClusterServer.getDataMember(partitionGroup.getHeader(),
          null, String.format("Query: %s by time, queryId: %d", path, context.getQueryId()));
      return dataGroupMember.getReaderByTimestamp(path, context);
    } else {
      return getRemoteReaderByTimestamp(path, partitionGroup, context);
    }
  }

  private IReaderByTimestamp getRemoteReaderByTimestamp(
      Path path, PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException, IOException {
    // query a remote node
    AtomicReference<Long> result = new AtomicReference<>();
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(path.getFullPath());
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setRequester(thisNode);

    for (Node node : partitionGroup) {
      logger.debug("{}: querying {} from {}", name, path, node);
      GenericHandler<Long> handler = new GenericHandler<>(node, result);
      DataClient client = (DataClient) dataClientPool.getClient(node);
      try {
        synchronized (result) {
          result.set(null);
          client.querySingleSeriesByTimestamp(request, handler);
          result.wait(connectionTimeoutInMS);
        }
        Long readerId = result.get();
        if (readerId != null) {
          ((RemoteQueryContext) context).registerRemoteNode(partitionGroup.getHeader(), node);
          logger.debug("{}: get a readerId {} for {} from {}", name, readerId, path, node);
          return new RemoteSeriesReaderByTimestamp(readerId, node, partitionGroup.getHeader(), this);
        }
      } catch (TException | InterruptedException e) {
        logger.error("{}: Cannot query {} from {}", name, path, node, e);
      }
    }
    throw new StorageEngineException(new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  public ManagedSeriesReader getSeriesReader(Path path, TSDataType dataType, Filter filter,
      QueryContext context, boolean pushDownUnseq, boolean withValueFilter)
      throws IOException, StorageEngineException {
    // make sure the partition table is new
    syncLeader();
    PartitionGroup partitionGroup;
    try {
      // TODO-Cluster#350: use time in partitioning
      partitionGroup = PartitionUtils.partitionByPathTime(path.getFullPath(), 0,
          partitionTable);
    } catch (StorageGroupNotSetException e) {
      throw new StorageEngineException(e);
    }

    if (partitionGroup.contains(thisNode)) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember = dataClusterServer.getDataMember(partitionGroup.getHeader(),
          null, String.format("Query: %s, time filter: %s, queryId: %d", path, filter,
              context.getQueryId()));
      if (withValueFilter) {
        return dataGroupMember.getSeriesReaderWithValueFilter(path, dataType, filter, context);
      } else {
        return dataGroupMember.getSeriesReaderWithoutValueFilter(path, dataType, filter, context,
            true);
      }
    } else {
      return getRemoteSeriesReader(filter, dataType, path, partitionGroup, context,
          pushDownUnseq, withValueFilter);
    }
  }

  private ManagedSeriesReader getRemoteSeriesReader(Filter filter, TSDataType dataType, Path path,
      PartitionGroup partitionGroup,
      QueryContext context, boolean pushDownUnseq, boolean withValueFilter)
      throws IOException, StorageEngineException {
    // query a remote node
    AtomicReference<Long> result = new AtomicReference<>();
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    if (filter != null) {
      request.setFilterBytes(SerializeUtils.serializeFilter(filter));
    }
    request.setPath(path.getFullPath());
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setRequester(thisNode);
    request.setPushdownUnseq(pushDownUnseq);
    request.setWithValueFilter(withValueFilter);
    request.setDataTypeOrdinal(dataType.ordinal());

    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      logger.debug("{}: querying {} from {}", name, path, node);
      GenericHandler<Long> handler = new GenericHandler<>(node, result);
      DataClient client = (DataClient) dataClientPool.getClient(node);
      try {
        synchronized (result) {
          result.set(null);
          client.querySingleSeries(request, handler);
          result.wait(connectionTimeoutInMS);
        }
        Long readerId = result.get();
        if (readerId != null) {
          ((RemoteQueryContext) context).registerRemoteNode(partitionGroup.getHeader(), node);
          logger.debug("{}: get a readerId {} for {} from {}", name, readerId, path, node);
          return new RemoteSimpleSeriesReader(readerId, node, partitionGroup.getHeader(), this);
        }
      } catch (TException | InterruptedException e) {
        logger.error("{}: Cannot query {} from {}", name, path, node, e);
      }
    }
    throw new StorageEngineException(new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  public ClientPool getDataClientPool() {
    return dataClientPool;
  }

  /**
   * Get all paths after removing wildcards in the path
   *
   * @param storageGroupName the storage group of this path
   * @param path a path with wildcard
   * @return all paths after removing wildcards in the path
   */
  public List<String> getMatchedPaths(String storageGroupName, String path) throws MetadataException {
    // find the data group that should hold the timeseries schemas of the storage group
    PartitionGroup partitionGroup = partitionTable.route(storageGroupName, 0);
    if (partitionGroup.contains(thisNode)) {
      // this node is a member of the group, perform a local query after synchronizing with the
      // leader
      dataClusterServer.getDataMember(partitionGroup.getHeader(), null, "Get paths of " + path)
          .syncLeader();
      return MManager.getInstance().getPaths(path);
    } else {
      AtomicReference<List<String>> result = new AtomicReference<>();

      List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
      for (Node node : coordinatedNodes) {
        try {
          DataClient client = (DataClient) dataClientPool.getClient(node);
          GenericHandler<List<String>> handler = new GenericHandler<>(node, result);
          result.set(null);
          synchronized (result) {
            client.getAllPaths(partitionGroup.getHeader(), path, handler);
            result.wait(connectionTimeoutInMS);
          }
          List<String> paths = result.get();
          if (paths != null) {
            return paths;
          }
        } catch (IOException | TException | InterruptedException e) {
          throw new MetadataException(e);
        }
      }
    }
    return Collections.emptyList();
  }

  public Map<Node, Boolean> getAllNodeStatus() {
    if(getPartitionTable() == null){
      // the cluster is being built.
      return null;
    }
    Map<Node, Boolean> nodeStatus = new HashMap<>();
    for (Node node : allNodes) {
      nodeStatus.put(node, thisNode == node);
    }
    NodeStatusHandler nodeStatusHandler = new NodeStatusHandler(nodeStatus);
    try {
      synchronized (nodeStatus) {
        for (Node node : allNodes) {
          TSMetaService.AsyncClient client = (AsyncClient) connectNode(node);
          if (node != thisNode && client != null) {
            client.checkAlive(nodeStatusHandler);
          }
        }
        nodeStatus.wait(ClusterConstant.CHECK_ALIVE_TIME_OUT_MS);
      }
    } catch (InterruptedException | TException e) {
      logger.warn("Cannot get the status of all nodes");
    }
    return nodeStatus;
  }

  ClientServer getClientServer() {
    return clientServer;
  }

  @Override
  public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
    resultHandler.onComplete(new TNodeStatus());
  }

  @Override
  public void checkAlive(AsyncMethodCallback<Node> resultHandler) {
    resultHandler.onComplete(thisNode);
  }
}
