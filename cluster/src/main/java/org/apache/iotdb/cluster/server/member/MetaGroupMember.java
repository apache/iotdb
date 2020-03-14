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
import static org.apache.iotdb.db.conf.IoTDBConstant.PATH_WILDCARD;
import static org.apache.iotdb.db.utils.SchemaUtils.getAggregationType;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.iotdb.cluster.exception.PartitionTableUnavailableException;
import org.apache.iotdb.cluster.exception.RequestTimeOutException;
import org.apache.iotdb.cluster.exception.UnsupportedPlanException;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.applier.DataLogApplier;
import org.apache.iotdb.cluster.log.applier.MetaLogApplier;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.CloseFileLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.log.manage.MetaSingleSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.partition.NodeRemovalResult;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.partition.PartitionTable;
import org.apache.iotdb.cluster.partition.SlotPartitionTable;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.groupby.RemoteGroupByExecutor;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.query.reader.EmptyReader;
import org.apache.iotdb.cluster.query.reader.ManagedMergeReader;
import org.apache.iotdb.cluster.query.reader.MergedReaderByTime;
import org.apache.iotdb.cluster.query.reader.RemoteSeriesReaderByTimestamp;
import org.apache.iotdb.cluster.query.reader.RemoteSimpleSeriesReader;
import org.apache.iotdb.cluster.rpc.thrift.AddNodeResponse;
import org.apache.iotdb.cluster.rpc.thrift.AppendEntryRequest;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusRequest;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.GetAggrResultRequest;
import org.apache.iotdb.cluster.rpc.thrift.GroupByRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatRequest;
import org.apache.iotdb.cluster.rpc.thrift.HeartbeatResponse;
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
import org.apache.iotdb.cluster.server.NodeReport;
import org.apache.iotdb.cluster.server.NodeReport.MetaMemberReport;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.handlers.caller.AppendGroupEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.CheckStatusHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.caller.JoinClusterHandler;
import org.apache.iotdb.cluster.server.handlers.caller.NodeStatusHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PullTimeseriesSchemaHandler;
import org.apache.iotdb.cluster.server.handlers.forwarder.GenericForwardHandler;
import org.apache.iotdb.cluster.server.heartbeat.MetaHeartbeatThread;
import org.apache.iotdb.cluster.server.member.DataGroupMember.Factory;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.cluster.utils.PartitionUtils.Intervals;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaGroupMember extends RaftMember implements TSMetaService.AsyncIface {

  static final String NODE_IDENTIFIER_FILE_NAME = "node_identifier";
  static final String PARTITION_FILE_NAME = "partitions";
  private static final String TEMP_SUFFIX = ".tmp";

  private static final Logger logger = LoggerFactory.getLogger(MetaGroupMember.class);
  private static final int DEFAULT_JOIN_RETRY = 10;
  private static final int REPORT_INTERVAL_SEC = 10;
  public final int REPLICATION_NUM =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();

  // blind nodes are nodes that does not know the nodes in the cluster
  private Set<Node> blindNodes = new HashSet<>();
  private Set<Node> idConflictNodes = new HashSet<>();
  private Map<Integer, Node> idNodeMap = null;

  private PartitionTable partitionTable;
  private DataClusterServer dataClusterServer;
  private ClientServer clientServer;

  private LogApplier metaLogApplier = new MetaLogApplier(this);
  private DataGroupMember.Factory dataMemberFactory;

  private MetaSingleSnapshotLogManager logManager;

  private ClientPool dataClientPool;

  private ScheduledExecutorService reportThread;

  @TestOnly
  public MetaGroupMember() {
  }

  public MetaGroupMember(TProtocolFactory factory, Node thisNode) throws QueryProcessException {
    super("Meta", new ClientPool(new MetaClient.Factory(factory)));
    allNodes = new ArrayList<>();
    LogApplier dataLogApplier = new DataLogApplier(this);
    dataMemberFactory = new Factory(factory, this, dataLogApplier);
    dataClientPool =
        new ClientPool(new DataClient.Factory(factory));
    initLogManager();
    setThisNode(thisNode);
    loadIdentifier();

    dataClusterServer = new DataClusterServer(thisNode, dataMemberFactory);
    clientServer = new ClientServer(this);
  }

  /**
   * Tell all nodes in the group to close current TsFile.
   *
   * @param storageGroupName
   * @param isSeq
   */
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
          break;
        case TIME_OUT:
          logger.info("Close file request of {} timed out", log);
          logManager.removeLastLog();
          break;
        case LEADERSHIP_STALE:
        default:
          logManager.removeLastLog();
      }
    }
  }

  public DataClusterServer getDataClusterServer() {
    return dataClusterServer;
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

    QueryCoordinator.getINSTANCE().setMetaGroupMember(this);
    StorageEngine.getInstance().setFileFlushPolicy(new ClusterFileFlushPolicy(this));
    reportThread = Executors.newSingleThreadScheduledExecutor(n -> new Thread(n,
        "NodeReportThread"));
    reportThread.scheduleAtFixedRate(() -> logger.info(genNodeReport().toString()),
        REPORT_INTERVAL_SEC, REPORT_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    super.stop();
    if (getDataClusterServer() != null) {
      getDataClusterServer().stop();
      clientServer.stop();
    }
    if (reportThread != null) {
      reportThread.shutdownNow();
    }
  }

  private void initSubServers() throws TTransportException, StartupException {
    getDataClusterServer().start();
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

        getDataClusterServer().addNode(newNode);
        if (newGroup.contains(thisNode)) {
          try {
            logger.info("Adding this node into a new group {}", newGroup);
            DataGroupMember dataGroupMember = dataMemberFactory.create(newGroup, thisNode);
            getDataClusterServer().addDataGroupMember(dataGroupMember);
            dataGroupMember.start();
            dataGroupMember
                .pullNodeAdditionSnapshots(partitionTable.getNodeSlots(newNode), newNode);
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
    heartBeatService.submit(new MetaHeartbeatThread(this));
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
          setLastHeartbeatReceivedTime(System.currentTimeMillis());
          heartBeatService.submit(new MetaHeartbeatThread(this));
          return true;
        }
        // wait a heartbeat to start the next try
        Thread.sleep(RaftServer.heartBeatIntervalMs);
      } catch (TException | StartupException e) {
        logger.warn("Cannot join the cluster from {}, because:", node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Cannot join the cluster from {}, because time out after {}ms",
            node, connectionTimeoutInMS);
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
        response.wait(connectionTimeoutInMS);
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
        syncLeader();

        initSubServers();
        buildDataGroups();
        getDataClusterServer().pullSnapshots();
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
  void processValidHeartbeatReq(HeartbeatRequest request, HeartbeatResponse response) {
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
  public void processValidHeartbeatResp(HeartbeatResponse response, Node receiver) {
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

      // check status of the new node
//      AsyncClient client = (AsyncClient) connectNode(node);
//      try {
//        AsyncMethodCallback<String> result = new AsyncMethodCallback<String>() {
//          String words;
//          @Override
//          public void onComplete(String s) {
//            this.words = s;
//          }
//          @Override
//          public void onError(Exception e) {
//            System.out.println("some error happens");
//          }
//          public String getWords() {
//            return words;
//          }
//        };
//        System.out.println(result);
//        client.echo("hello world", result);
//      } catch (TException e) {
//        e.printStackTrace();
//      }
      CheckStatusRequest checkStatusRequest = new CheckStatusRequest();
      checkStatusRequest.setHashSalt(ClusterConstant.HASH_SALT);
      checkStatusRequest
          .setPartitionInterval(IoTDBDescriptor.getInstance().getConfig().getPartitionInterval());
      checkStatusRequest.setReplicationNumber(config.getReplicationNum());
      CheckStatusHandler checkStatusHandler = new CheckStatusHandler();
      try {
        sendStatusToNewNode(node, checkStatusRequest, checkStatusHandler);
      } catch (TException exception) {
        logger.error("Failed to send current state to the new node {}", node, exception);
      }
      if (!checkStatusHandler.getCheckStatusResponse().isPartitionalIntervalEquals()) {
        logger.info("The partition interval of the new node {} conflicts.", node);
        return true;
      } else if (!checkStatusHandler.getCheckStatusResponse().isHashSaltIntervalEquals()) {
        logger.info("The hash salt of the new node {} conflicts.", node);
        return true;
      } else if (!checkStatusHandler.getCheckStatusResponse().isReplicationNumEquals()) {
        logger.info("The replication number of the new node {} conflicts.", node);
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
            logManager.commitLog(addNodeLog.getCurrLogIndex());
            synchronized (partitionTable) {
              response.setPartitionTableBytes(partitionTable.serialize());
            }
            response.setRespNum((int) Response.RESPONSE_AGREE);
            resultHandler.onComplete(response);
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


  private void sendStatusToNewNode(Node node, CheckStatusRequest checkStatusRequest,
      AsyncMethodCallback<CheckStatusResponse> response)
      throws TException {
    AsyncClient client = (AsyncClient) connectNode(node);
    client.checkStatus(checkStatusRequest, response);
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
    Arrays.fill(groupRemainings, groupQuorum);

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
        groupRemainings.wait(connectionTimeoutInMS);
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
    if (thisNode.isSetNodeIdentifier()) {
      return;
    }
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

    getDataClusterServer().setPartitionTable(partitionTable);
    for (PartitionGroup partitionGroup : partitionGroups) {
      logger.debug("Building member of data group: {}", partitionGroup);
      DataGroupMember dataGroupMember = dataMemberFactory.create(partitionGroup, thisNode);
      dataGroupMember.start();
      getDataClusterServer().addDataGroupMember(dataGroupMember);
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
      for (String storageGroup : snapshot.getStorageGroups()) {
        try {
          MManager.getInstance().setStorageGroup(storageGroup);
        } catch (StorageGroupAlreadySetException ignored) {
          // ignore duplicated storage group
        } catch (MetadataException e) {
          logger.error("{}: Cannot add storage group {} in snapshot", name, storageGroup);
        }
      }
      for (Log log : snapshot.getSnapshot()) {
        try {
          logManager.getApplier().apply(log);
        } catch (QueryProcessException e) {
          logger.error("{}: Cannot apply a log {} in snapshot, ignored", name, log, e);
        }
      }
      logManager.setSnapshot(snapshot);
    }
    logManager.setLastLogTerm(snapshot.getLastLogTerm());
    logManager.setLastLogId(snapshot.getLastLogId());
  }

  @Override
  public TSStatus executeNonQuery(PhysicalPlan plan) {
    if (PartitionUtils.isLocalPlan(plan)) {// run locally
      //TODO run locally.
      return null;
    } else if (PartitionUtils.isGlobalPlan(plan)) {// forward the plan to all nodes
      return processNonPartitionedPlan(plan);
    } else { //split the plan and forward them to some PartitionGroups
      try {
        return processPartitionedPlan(plan);
      } catch (UnsupportedPlanException e) {
        TSStatus status = StatusUtils.UNSUPPORTED_OPERATION.deepCopy();
        status.setMessage(e.getMessage());
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
    return forwardPlan(plan, leader, null);
  }

  private TSStatus processPartitionedPlan(PhysicalPlan plan) throws UnsupportedPlanException {
    logger.debug("{}: Received a partitioned plan {}", name, plan);
    if (partitionTable == null) {
      logger.debug("{}: Partition table is not ready", name);
      return StatusUtils.PARTITION_TABLE_NOT_READY;
    }

    Map<PhysicalPlan, PartitionGroup> planGroupMap = null;
    try {
      planGroupMap = partitionTable.splitAndRoutePlan(plan);
    } catch (MetadataException e) {
      logger.debug("Cannot route plan {}", plan, e);
    }
    // the storage group is not found locally, forward it to the leader
    if (planGroupMap == null || planGroupMap.isEmpty()) {
      if (character != NodeCharacter.LEADER) {
        logger
            .debug("{}: Cannot found partition groups for {}, forwarding to {}", name, plan,
                leader);
        return forwardPlan(plan, leader, null);
      } else {
        logger.debug("{}: Cannot found storage groups for {}", name, plan);
        return StatusUtils.NO_STORAGE_GROUP;
      }
    }
    logger.debug("{}: The data group of {} is {}", name, plan, planGroupMap);

    TSStatus status;
    List<Entry<PhysicalPlan, PartitionGroup>> succeededEntries = new ArrayList<>();
    List<String> errorCodePartitionGroups = new ArrayList<>();
    for (Map.Entry<PhysicalPlan, PartitionGroup> entry : planGroupMap.entrySet()) {
      TSStatus subStatus;
      if (entry.getValue().contains(thisNode)) {
        // the query should be handled by a group the local node is in, handle it with in the group
        subStatus = getLocalDataMember(entry.getValue().getHeader(), null, plan)
            .executeNonQuery(entry.getKey());
      } else {
        // forward the query to the group that should handle it
        subStatus = forwardPlan(entry.getKey(), entry.getValue());
      }
      if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        errorCodePartitionGroups.add(String.format("[%s@%s:%s]",
            subStatus.getCode(), entry.getValue().getHeader(),
            subStatus.getMessage()));
      } else {
        succeededEntries.add(entry);
      }
    }
    if (errorCodePartitionGroups.isEmpty()) {
      status = StatusUtils.OK;
    } else {
      status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
      status.setMessage("The following errors occurred when executing the query, "
          + "please retry or contact the DBA: " + errorCodePartitionGroups.toString());
      //TODO-Cluster: abort the succeeded ones if necessary.
    }
    return status;
  }

  TSStatus forwardPlan(PhysicalPlan plan, PartitionGroup group) {
    // if a plan is partitioned to any group, it must be processed by its data server instead of
    // meta server
    for (Node node : group) {
      TSStatus status;
      try {
        status = forwardPlan(plan, getDataClient(node), node, group.getHeader());
      } catch (IOException e) {
        status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
        status.setMessage(e.getMessage());
      }
      if (status != StatusUtils.TIME_OUT) {
        return status;
      }
    }
    return StatusUtils.TIME_OUT;
  }


  /**
   * Pull the all timeseries schemas of a given prefixPath from a remote node.
   */
  public List<MeasurementSchema> pullTimeSeriesSchemas(List<String> prefixPaths)
      throws MetadataException {
    logger.debug("{}: Pulling timeseries schemas of {}", name, prefixPaths);
    Map<PartitionGroup, List<String>> partitionGroupPathMap = new HashMap<>();
    for (String prefixPath : prefixPaths) {
      PartitionGroup partitionGroup;
      try {
        partitionGroup = partitionTable.partitionByPathTime(prefixPath, 0);
      } catch (StorageGroupNotSetException e) {
        // the storage group is not found locally, but may be found in the leader, retry after
        // synchronizing with the leader
        if (syncLeader()) {
          partitionGroup = partitionTable.partitionByPathTime(prefixPath, 0);
        } else {
          throw e;
        }
      }
      partitionGroupPathMap.computeIfAbsent(partitionGroup, g -> new ArrayList<>()).add(prefixPath);
    }

    List<MeasurementSchema> schemas = new ArrayList<>();
    for (Entry<PartitionGroup, List<String>> partitionGroupListEntry : partitionGroupPathMap
        .entrySet()) {
      PartitionGroup partitionGroup = partitionGroupListEntry.getKey();
      List<String> paths = partitionGroupListEntry.getValue();
      pullTimeSeriesSchemas(partitionGroup, paths, schemas);
    }
    return schemas;
  }

  public void pullTimeSeriesSchemas(PartitionGroup partitionGroup,
      List<String> prefixPaths, List<MeasurementSchema> results) {
    if (partitionGroup.contains(thisNode)) {
      // the node is in the target group, synchronize with leader should be enough
      getLocalDataMember(partitionGroup.getHeader(), null,
          "Pull timeseries of " + prefixPaths).syncLeader();
      for (String prefixPath : prefixPaths) {
        MManager.getInstance().collectSeries(prefixPath, results);
      }
      return;
    }

    PullSchemaRequest pullSchemaRequest = new PullSchemaRequest();
    pullSchemaRequest.setHeader(partitionGroup.getHeader());
    pullSchemaRequest.setPrefixPaths(prefixPaths);
    AtomicReference<List<MeasurementSchema>> timeseriesSchemas = new AtomicReference<>();
    for (Node node : partitionGroup) {
      logger.debug("{}: Pulling timeseries schemas of {} from {}", name, prefixPaths, node);
      AsyncClient client = (AsyncClient) connectNode(node);
      synchronized (timeseriesSchemas) {
        try {
          client.pullTimeSeriesSchema(pullSchemaRequest, new PullTimeseriesSchemaHandler(node,
              prefixPaths, timeseriesSchemas));
          timeseriesSchemas.wait(connectionTimeoutInMS);
        } catch (TException | InterruptedException e) {
          logger
              .error("{}: Cannot pull timeseries schemas of {} from {}", name, prefixPaths, node,
                  e);
          continue;
        }
      }
      List<MeasurementSchema> schemas = timeseriesSchemas.get();
      if (schemas != null) {
        results.addAll(schemas);
        break;
      }
    }
  }

  @Override
  public void pullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) {
    Node header = request.getHeader();
    DataGroupMember dataGroupMember = getLocalDataMember(header, resultHandler,
        "Pull timeseries");
    if (dataGroupMember == null) {
      resultHandler
          .onError(new NotInSameGroupException(partitionTable.getHeaderGroup(header), thisNode));
      return;
    }

    dataGroupMember.pullTimeSeriesSchema(request, resultHandler);
  }

  public List<TSDataType> getSeriesTypesByPath(List<Path> paths, List<String> aggregations)
      throws MetadataException {
    try {
      return SchemaUtils.getSeriesTypesByPath(paths, aggregations);
    } catch (PathNotExistException e) {
      List<String> pathStr = new ArrayList<>();
      for (Path path : paths) {
        pathStr.add(path.getFullPath());
      }
      List<MeasurementSchema> schemas = pullTimeSeriesSchemas(pathStr);
      // TODO-Cluster: should we register the schemas locally?
      if (schemas.isEmpty()) {
        throw e;
      }

      List<TSDataType> result = new ArrayList<>();
      for (int i = 0; i < schemas.size(); i++) {
        String aggregation = aggregations.get(i);
        TSDataType dataType = getAggregationType(aggregation);
        if (dataType == null) {
          MeasurementSchema schema = schemas.get(i);
          result.add(schema.getType());
          SchemaUtils.registerTimeseries(schema);
        } else {
          result.add(dataType);
        }
      }
      return result;
    }
  }

  public List<TSDataType> getSeriesTypesByString(List<String> pathStrs, String aggregation)
      throws MetadataException {
    try {
      return SchemaUtils.getSeriesTypesByString(pathStrs, aggregation);
    } catch (PathNotExistException e) {
      List<MeasurementSchema> schemas = pullTimeSeriesSchemas(pathStrs);
      // TODO-Cluster: should we register the schemas locally?
      if (schemas.isEmpty()) {
        throw e;
      }

      List<TSDataType> result = new ArrayList<>();
      for (MeasurementSchema schema : schemas) {
        result.add(schema.getType());
        SchemaUtils.registerTimeseries(schema);
      }
      return result;
    }
  }

  public IReaderByTimestamp getReaderByTimestamp(Path path, TSDataType dataType,
      QueryContext context)
      throws StorageEngineException {
    // make sure the partition table is new
    syncLeader();
    List<PartitionGroup> partitionGroups = routeFilter(null, path);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending query of {} to {} groups", name, path, partitionGroups.size());
    }
    List<IReaderByTimestamp> readers = new ArrayList<>();
    for (PartitionGroup partitionGroup : partitionGroups) {
      readers.add(getSeriesReaderByTime(partitionGroup, path, context, dataType));
    }
    return new MergedReaderByTime(readers);
  }

  // get SeriesReader from a PartitionGroup
  private IReaderByTimestamp getSeriesReaderByTime(PartitionGroup partitionGroup, Path path,
      QueryContext context, TSDataType dataType) throws StorageEngineException {
    if (partitionGroup.contains(thisNode)) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember = getLocalDataMember(partitionGroup.getHeader(),
          null, String.format("Query: %s, queryId: %d", path,
              context.getQueryId()));
      logger.debug("{}: creating a local reader for {}#{}", name, path.getFullPath(),
          context.getQueryId());
      return dataGroupMember.getReaderByTimestamp(path, dataType, context);
    } else {
      return getRemoteReaderByTimestamp(path, dataType, partitionGroup, context);
    }
  }

  private IReaderByTimestamp getRemoteReaderByTimestamp(
      Path path, TSDataType dataType, PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException {
    // query a remote node
    AtomicReference<Long> result = new AtomicReference<>();
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(path.getFullPath());
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setRequester(thisNode);
    request.setDataTypeOrdinal(dataType.ordinal());

    for (Node node : partitionGroup) {
      logger.debug("{}: querying {} from {}", name, path, node);
      GenericHandler<Long> handler = new GenericHandler<>(node, result);
      try {
        DataClient client = getDataClient(node);
        synchronized (result) {
          result.set(null);
          client.querySingleSeriesByTimestamp(request, handler);
          result.wait(connectionTimeoutInMS);
        }
        Long readerId = result.get();
        if (readerId != null) {
          ((RemoteQueryContext) context).registerRemoteNode(partitionGroup.getHeader(), node);
          logger.debug("{}: get a readerId {} for {} from {}", name, readerId, path, node);
          return new RemoteSeriesReaderByTimestamp(readerId, node, partitionGroup.getHeader(),
              this);
        }
      } catch (TException | InterruptedException | IOException e) {
        logger.error("{}: Cannot query {} from {}", name, path, node, e);
      }
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  public ManagedSeriesReader getSeriesReader(Path path, TSDataType dataType, Filter timeFilter,
      Filter valueFilter, QueryContext context)
      throws StorageEngineException {
    // make sure the partition table is new
    syncLeader();
    List<PartitionGroup> partitionGroups = routeFilter(timeFilter, path);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending data query of {} to {} groups", name, path, partitionGroups.size());
    }
    ManagedMergeReader mergeReader = new ManagedMergeReader(dataType);
    try {
      for (PartitionGroup partitionGroup : partitionGroups) {
        IPointReader seriesReader = getSeriesReader(partitionGroup, path, timeFilter, valueFilter,
            context, dataType);
        if (seriesReader.hasNextTimeValuePair()) {
          mergeReader.addReader(seriesReader, 0);
        }
      }
    } catch (IOException e) {
      throw new StorageEngineException(e);
    }
    return mergeReader;
  }

  public List<AggregateResult> getAggregateResult(Path path, List<String> aggregations,
      TSDataType dataType, Filter timeFilter,
      QueryContext context) throws StorageEngineException {
    // make sure the partition table is new
    syncLeader();
    List<PartitionGroup> partitionGroups = routeFilter(timeFilter, path);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending aggregation query of {} to {} groups", name, path,
          partitionGroups.size());
    }
    List<AggregateResult> results = null;
    // get the aggregation result of each group and merge them
    for (PartitionGroup partitionGroup : partitionGroups) {
      List<AggregateResult> groupResult = getAggregateResult(path, aggregations, dataType,
          timeFilter, partitionGroup, context);
      if (results == null) {
        results = groupResult;
      } else {
        for (int i = 0; i < results.size(); i++) {
          results.get(i).merge(groupResult.get(i));
        }
      }
    }
    return results;
  }

  private List<AggregateResult> getAggregateResult(Path path, List<String> aggregations,
      TSDataType dataType, Filter timeFilter, PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException {
    if (!partitionGroup.contains(thisNode)) {
      return getRemoteAggregateResult(path, aggregations, dataType, timeFilter, partitionGroup,
          context);
    } else {
      DataGroupMember dataMember = getLocalDataMember(partitionGroup.getHeader());
      try {
        logger.debug("{}: querying aggregation {} of {} in {} locally", name, aggregations, path,
            partitionGroup.getHeader());
        List<AggregateResult> aggrResult = dataMember
            .getAggrResult(aggregations, dataType, path.getFullPath(), timeFilter, context);
        logger.debug("{}: queried aggregation {} of {} in {} locally are {}", name, aggregations,
            path, partitionGroup.getHeader(), aggrResult);
        return aggrResult;
      } catch (IOException | QueryProcessException | LeaderUnknownException e) {
        throw new StorageEngineException(e);
      }
    }
  }

  private List<AggregateResult> getRemoteAggregateResult(Path path, List<String> aggregations,
      TSDataType dataType, Filter timeFilter, PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException {
    AtomicReference<List<ByteBuffer>> resultReference = new AtomicReference<>();
    GetAggrResultRequest request = new GetAggrResultRequest();
    request.setPath(path.getFullPath());
    request.setAggregations(aggregations);
    request.setDataTypeOrdinal(dataType.ordinal());
    request.setQueryId(context.getQueryId());
    request.setRequestor(thisNode);
    request.setHeader(partitionGroup.getHeader());
    if (timeFilter != null) {
      request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    }

    for (Node node : partitionGroup) {
      logger.debug("{}: querying aggregation {} of {} from {} of {}", name, aggregations, path,
          node, partitionGroup.getHeader());
      GenericHandler<List<ByteBuffer>> handler = new GenericHandler<>(node, resultReference);
      try {
        DataClient client = getDataClient(node);
        synchronized (resultReference) {
          resultReference.set(null);
          client.getAggrResult(request, handler);
          resultReference.wait(connectionTimeoutInMS);
        }
        List<ByteBuffer> resultBuffers = resultReference.get();
        if (resultBuffers != null) {
          List<AggregateResult> results = new ArrayList<>(resultBuffers.size());
          for (ByteBuffer resultBuffer : resultBuffers) {
            AggregateResult result = AggregateResult.deserializeFrom(resultBuffer);
            results.add(result);
          }
          ((RemoteQueryContext) context).registerRemoteNode(node, partitionGroup.getHeader());
          logger.debug("{}: queried aggregation {} of {} from {} of {} are {}", name, aggregations,
              path, node, partitionGroup.getHeader(), results);
          return results;
        }
      } catch (TException | InterruptedException | IOException e) {
        logger.error("{}: Cannot query {} from {}", name, path, node, e);
      }
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  private List<PartitionGroup> routeFilter(Filter filter, Path path) throws StorageEngineException {
    List<PartitionGroup> partitionGroups = new ArrayList<>();
    Intervals intervals = PartitionUtils.extractTimeInterval(filter);
    long firstLB = intervals.getLowerBound(0);
    long lastUB = intervals.getUpperBound(intervals.getIntervalSize() - 1);

    if (firstLB == Long.MIN_VALUE || lastUB == Long.MAX_VALUE) {
      // as there is no TimeLowerBound or TimeUpperBound, the query should be broadcast to every
      // group
      // TODO-Cluster: cache the AllGroups in PartitionTable?
      for (Node node : partitionTable.getAllNodes()) {
        partitionGroups.add(partitionTable.getHeaderGroup(node));
      }
    } else {
      // compute the related data groups of all intervals
      // TODO-Cluster: change to a broadcast when the computation is too expensive
      try {
        String storageGroupName = MManager.getInstance()
            .getStorageGroupName(path.getFullPath());
        Set<Node> groupHeaders = new HashSet<>();
        for (int i = 0; i < intervals.getIntervalSize(); i++) {
          PartitionUtils.getIntervalHeaders(storageGroupName, intervals.getLowerBound(i),
              intervals.getUpperBound(i), partitionTable, groupHeaders);
        }
        for (Node groupHeader : groupHeaders) {
          partitionGroups.add(partitionTable.getHeaderGroup(groupHeader));
        }
      } catch (MetadataException e) {
        throw new StorageEngineException(e);
      }
    }
    return partitionGroups;
  }

  // get SeriesReader from a PartitionGroup
  private IPointReader getSeriesReader(PartitionGroup partitionGroup, Path path,
      Filter timeFilter, Filter valueFilter, QueryContext context, TSDataType dataType)
      throws IOException,
      StorageEngineException {
    if (partitionGroup.contains(thisNode)) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember = getLocalDataMember(partitionGroup.getHeader(),
          null, String.format("Query: %s, time filter: %s, queryId: %d", path, timeFilter,
              context.getQueryId()));
      logger.debug("{}: creating a local reader for {}#{}", name, path.getFullPath(),
          context.getQueryId());
      return dataGroupMember.getSeriesPointReader(path, dataType, timeFilter, valueFilter, context);
    } else {
      return getRemoteSeriesPointReader(timeFilter, valueFilter, dataType, path, partitionGroup,
          context);
    }
  }

  private IPointReader getRemoteSeriesPointReader(Filter timeFilter,
      Filter valueFilter, TSDataType dataType, Path path,
      PartitionGroup partitionGroup,
      QueryContext context)
      throws IOException, StorageEngineException {
    // query a remote node
    AtomicReference<Long> result = new AtomicReference<>();
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    if (timeFilter != null) {
      request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    }
    if (valueFilter != null) {
      request.setValueFilterBytes(SerializeUtils.serializeFilter(valueFilter));
    }
    request.setPath(path.getFullPath());
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setRequester(thisNode);
    request.setDataTypeOrdinal(dataType.ordinal());

    List<Node> orderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : orderedNodes) {
      logger.debug("{}: querying {} from {}", name, path, node);
      GenericHandler<Long> handler = new GenericHandler<>(node, result);
      DataClient client = getDataClient(node);
      try {
        synchronized (result) {
          result.set(null);
          client.querySingleSeries(request, handler);
          result.wait(connectionTimeoutInMS);
        }
        Long readerId = result.get();
        if (readerId != null) {
          if (readerId != -1) {
            ((RemoteQueryContext) context).registerRemoteNode(partitionGroup.getHeader(), node);
            logger.debug("{}: get a readerId {} for {} from {}", name, readerId, path, node);
            return new RemoteSimpleSeriesReader(readerId, node, partitionGroup.getHeader(), this,
                dataType);
          } else {
            // there is no satisfying data on the remote node, create an empty reader to reduce
            // further communication
            logger.debug("{}: no data for {} from {}", name, path, node);
            return new EmptyReader();
          }
        }
      } catch (TException | InterruptedException e) {
        logger.error("{}: Cannot query {} from {}", name, path, node, e);
      }
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  private ClientPool getDataClientPool() {
    return dataClientPool;
  }


  /**
   * Get all paths after removing wildcards in the path
   *
   * @param originPath a path potentially with wildcard
   * @return all paths after removing wildcards in the path
   */
  public List<String> getMatchedPaths(String originPath) throws MetadataException {
    if (!originPath.contains(PATH_WILDCARD)) {
      // path without wildcards does not need to be processed
      return Collections.singletonList(originPath);
    }
    // make sure this node knows all storage groups
    syncLeader();
    // get all storage groups this path may belong to
    Map<String, String> sgPathMap = MManager.getInstance().determineStorageGroup(originPath);
    logger.debug("The storage groups of path {} are {}", originPath, sgPathMap.keySet());
    List<String> ret = new ArrayList<>();
    for (Entry<String, String> entry : sgPathMap.entrySet()) {
      String storageGroupName = entry.getKey();
      String fullPath = entry.getValue();
      ret.addAll(getMatchedPaths(storageGroupName, fullPath));
    }
    logger.debug("The paths of path {} are {}", originPath, ret);

    return ret;
  }

  /**
   * Get all devices after removing wildcards in the path
   *
   * @param originPath a path potentially with wildcard
   * @return all paths after removing wildcards in the path
   */
  public Set<String> getMatchedDevices(String originPath) throws MetadataException {
    // make sure this node knows all storage groups
    syncLeader();
    // get all storage groups this path may belong to
    Map<String, String> sgPathMap = MManager.getInstance().determineStorageGroup(originPath);
    Set<String> ret = new HashSet<>();
    logger.debug("The storage groups of path {} are {}", originPath, sgPathMap.keySet());
    for (Entry<String, String> entry : sgPathMap.entrySet()) {
      String storageGroupName = entry.getKey();
      String fullPath = entry.getValue();
      ret.addAll(getMatchedDevices(storageGroupName, fullPath));
    }
    logger.debug("The devices of path {} are {}", originPath, ret);

    return ret;
  }

  private List<String> getMatchedPaths(String storageGroupName, String path)
      throws MetadataException {
    // find the data group that should hold the timeseries schemas of the storage group
    PartitionGroup partitionGroup = partitionTable.route(storageGroupName, 0);
    if (partitionGroup.contains(thisNode)) {
      // this node is a member of the group, perform a local query after synchronizing with the
      // leader
      getLocalDataMember(partitionGroup.getHeader(), null, "Get paths of " + path)
          .syncLeader();
      List<String> allTimeseriesName = MManager.getInstance().getAllTimeseriesName(path);
      logger.debug("{}: get matched paths of {} locally, result {}", name, partitionGroup,
          allTimeseriesName);
      return allTimeseriesName;
    } else {
      AtomicReference<List<String>> result = new AtomicReference<>();

      List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
      for (Node node : coordinatedNodes) {
        try {
          DataClient client = getDataClient(node);
          GenericHandler<List<String>> handler = new GenericHandler<>(node, result);
          result.set(null);
          synchronized (result) {
            client.getAllPaths(partitionGroup.getHeader(), path, handler);
            result.wait(connectionTimeoutInMS);
          }
          List<String> paths = result.get();
          logger.debug("{}: get matched paths of {} from {}, result {}", name, partitionGroup,
              node, paths);
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

  private Set<String> getMatchedDevices(String storageGroupName, String path)
      throws MetadataException {
    // find the data group that should hold the timeseries schemas of the storage group
    PartitionGroup partitionGroup = partitionTable.route(storageGroupName, 0);
    if (partitionGroup.contains(thisNode)) {
      // this node is a member of the group, perform a local query after synchronizing with the
      // leader
      getLocalDataMember(partitionGroup.getHeader(), null, "Get devices of " + path)
          .syncLeader();
      Set<String> devices = MManager.getInstance().getDevices(path);
      logger.debug("{}: get matched devices of {} locally, result {}", name, path,
          devices);
      return devices;
    } else {
      AtomicReference<Set<String>> result = new AtomicReference<>();

      List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
      for (Node node : coordinatedNodes) {
        try {
          DataClient client = getDataClient(node);
          GenericHandler<Set<String>> handler = new GenericHandler<>(node, result);
          result.set(null);
          synchronized (result) {
            client.getAllDevices(partitionGroup.getHeader(), path, handler);
            result.wait(connectionTimeoutInMS);
          }
          Set<String> paths = result.get();
          logger.debug("{}: get matched devices of {} from {}, result {}", name, partitionGroup,
              node, paths);
          if (paths != null) {
            return paths;
          }
        } catch (IOException | TException | InterruptedException e) {
          throw new MetadataException(e);
        }
      }
    }
    return Collections.emptySet();
  }

  public Map<Node, Boolean> getAllNodeStatus() {
    if (getPartitionTable() == null) {
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

  @Override
  public void queryNodeStatus(AsyncMethodCallback<TNodeStatus> resultHandler) {
    resultHandler.onComplete(new TNodeStatus());
  }

  @Override
  public void checkAlive(AsyncMethodCallback<Node> resultHandler) {
    resultHandler.onComplete(thisNode);
  }

  @Override
  public void checkStatus(CheckStatusRequest status,
      AsyncMethodCallback<CheckStatusResponse> resultHandler) {
    long partitionInterval = status.getPartitionInterval();
    int hashSalt = status.getHashSalt();
    int replicationNum = status.getReplicationNumber();
    boolean partitionIntervalEquals = true;
    boolean hashSaltEquals = true;
    boolean replicationNumEquals = true;
    if (IoTDBDescriptor.getInstance().getConfig().getPartitionInterval() != partitionInterval) {
      partitionIntervalEquals = false;
    }
    if (ClusterConstant.HASH_SALT != hashSalt) {
      hashSaltEquals = false;
    }
    if (ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum() != replicationNum) {
      replicationNumEquals = false;
    }
    CheckStatusResponse response = new CheckStatusResponse();
    response.setPartitionalIntervalEquals(partitionIntervalEquals);
    response.setHashSaltIntervalEquals(hashSaltEquals);
    response.setReplicationNumEquals(replicationNumEquals);
    resultHandler.onComplete(response);
  }

  @TestOnly
  public void setPartitionTable(PartitionTable partitionTable) {
    this.partitionTable = partitionTable;
    DataClusterServer dataClusterServer = getDataClusterServer();
    if (dataClusterServer != null) {
      dataClusterServer.setPartitionTable(partitionTable);
    }
  }

  @Override
  public void removeNode(Node node, AsyncMethodCallback<Long> resultHandler) {
    if (partitionTable == null) {
      logger.info("Cannot add node now because the partition table is not set");
      resultHandler.onError(new PartitionTableUnavailableException(thisNode));
      return;
    }

    // try to process the request locally, if it cannot be processed locally, forward it
    if (processRemoveNodeLocally(node, resultHandler)) {
      return;
    }

    if (character == NodeCharacter.FOLLOWER && leader != null) {
      logger.info("Forward the node removal request of {} to leader {}", node, leader);
      if (forwardRemoveNode(node, resultHandler)) {
        return;
      }
    }
    resultHandler.onError(new LeaderUnknownException(getAllNodes()));
  }

  private boolean forwardRemoveNode(Node node, AsyncMethodCallback resultHandler) {
    TSMetaService.AsyncClient client = (TSMetaService.AsyncClient) connectNode(leader);
    if (client != null) {
      try {
        client.removeNode(node, new GenericForwardHandler(resultHandler));
        return true;
      } catch (TException e) {
        logger.warn("Cannot connect to node {}", node, e);
      }
    }
    return false;
  }

  private boolean processRemoveNodeLocally(Node node, AsyncMethodCallback resultHandler) {
    if (character == NodeCharacter.LEADER) {
      if (allNodes.size() <= ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum()) {
        resultHandler.onComplete(Response.RESPONSE_CLUSTER_TOO_SMALL);
        return true;
      }

      Node target = null;
      synchronized (allNodes) {
        for (Node n : allNodes) {
          if (n.ip.equals(node.ip) && n.metaPort == node.metaPort) {
            target = n;
            break;
          }
        }
      }

      if (target == null) {
        logger.debug("Node {} is not in the cluster", node);
        resultHandler.onComplete(Response.RESPONSE_REJECT);
        return true;
      }

      // node removal must be serialized
      synchronized (logManager) {
        RemoveNodeLog removeNodeLog = new RemoveNodeLog();
        removeNodeLog.setCurrLogTerm(getTerm().get());
        removeNodeLog.setPreviousLogIndex(logManager.getLastLogIndex());
        removeNodeLog.setPreviousLogTerm(logManager.getLastLogTerm());
        removeNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);

        removeNodeLog.setRemovedNode(target);

        logManager.appendLog(removeNodeLog);

        logger.info("Send the node removal request of {} to other nodes", target);
        AppendLogResult result = sendLogToAllGroups(removeNodeLog);

        switch (result) {
          case OK:
            logger.info("Removal request of {} is accepted", target);
            logManager.commitLog(removeNodeLog.getCurrLogIndex());
            resultHandler.onComplete(Response.RESPONSE_AGREE);
            return true;
          case TIME_OUT:
            logger.info("Removal request of {} timed out", target);
            resultHandler.onError(new RequestTimeOutException(removeNodeLog));
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

  public void applyRemoveNode(Node oldNode) {
    synchronized (allNodes) {
      if (allNodes.contains(oldNode)) {
        logger.debug("Removing a node {} from {}", oldNode, allNodes);
        allNodes.remove(oldNode);
        idNodeMap.remove(oldNode.nodeIdentifier);

        // update the partition table
        NodeRemovalResult result = partitionTable.removeNode(oldNode);

        getDataClusterServer().removeNode(oldNode, result);
        if (oldNode.equals(leader)) {
          setCharacter(NodeCharacter.ELECTOR);
          lastHeartbeatReceivedTime = Long.MIN_VALUE;
        }

        if (oldNode.equals(thisNode)) {
          // use super.stop() so that the data server will not be closed
          super.stop();
          if (clientServer != null) {
            clientServer.stop();
          }
        } else if (thisNode.equals(leader)) {
          // as the old node is removed, it cannot know this by heartbeat, so it should be
          // directly kicked out of the cluster
          MetaClient metaClient = (MetaClient) connectNode(oldNode);
          try {
            metaClient.exile(new GenericHandler<>(oldNode, null));
          } catch (TException e) {
            logger.warn("Cannot inform {} its removal", oldNode, e);
          }
        }

        savePartitionTable();
      }
    }
  }

  @Override
  public void exile(AsyncMethodCallback<Void> resultHandler) {
    applyRemoveNode(thisNode);
    resultHandler.onComplete(null);
  }

  private MetaMemberReport genMemberReport() {
    return new MetaMemberReport(character, leader, term.get(),
        logManager.getLastLogTerm(), logManager.getLastLogIndex(), readOnly);
  }

  private NodeReport genNodeReport() {
    NodeReport report = new NodeReport(thisNode);
    report.setMetaMemberReport(genMemberReport());
    report.setDataMemberReportList(dataClusterServer.genMemberReports());
    return report;
  }

  @Override
  public void setAllNodes(List<Node> allNodes) {
    super.setAllNodes(allNodes);
    idNodeMap = new HashMap<>();
    for (Node node : allNodes) {
      idNodeMap.put(node.getNodeIdentifier(), node);
    }
  }

  /**
   * @param header        the header of the group which the local node is in
   * @param resultHandler can be set to null if the request is an internal request
   * @param request       the toString() of this parameter should explain what the request is and it
   *                      is only used in logs for tracing
   * @return
   */
  protected DataGroupMember getLocalDataMember(Node header,
      AsyncMethodCallback resultHandler, Object request) {
    return dataClusterServer.getDataMember(header, resultHandler, request);
  }

  protected DataGroupMember getLocalDataMember(Node header) {
    return dataClusterServer.getDataMember(header, null, "Internal call");
  }

  public DataClient getDataClient(Node node) throws IOException {
    return (DataClient) getDataClientPool().getClient(node);
  }

  public List<GroupByExecutor> getGroupByExecutors(Path path, TSDataType dataType,
      QueryContext context, Filter timeFilter, List<Integer> aggregationTypes)
      throws StorageEngineException {
    // make sure the partition table is new
    syncLeader();
    List<PartitionGroup> partitionGroups = routeFilter(timeFilter, path);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending group by query of {} to {} groups", name, path,
          partitionGroups.size());
    }
    List<GroupByExecutor> executors = new ArrayList<>();
    for (PartitionGroup partitionGroup : partitionGroups) {
      GroupByExecutor groupByExecutor = getGroupByExecutor(path, partitionGroup,
          timeFilter, context, dataType, aggregationTypes);
      executors.add(groupByExecutor);
    }
    return executors;
  }

  private GroupByExecutor getGroupByExecutor(Path path,
      PartitionGroup partitionGroup, Filter timeFilter, QueryContext context, TSDataType dataType,
      List<Integer> aggregationTypes) throws StorageEngineException {
    if (partitionGroup.contains(thisNode)) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember = getLocalDataMember(partitionGroup.getHeader());
      logger.debug("{}: creating a local group by executor for {}#{}", name,
          path.getFullPath(), context.getQueryId());
      return dataGroupMember
          .getGroupByExecutor(path, dataType, timeFilter, aggregationTypes, context);
    } else {
      return getRemoteGroupByExecutor(timeFilter, aggregationTypes, dataType, path, partitionGroup,
          context);
    }
  }

  private GroupByExecutor getRemoteGroupByExecutor(Filter timeFilter,
      List<Integer> aggregationTypes, TSDataType dataType, Path path, PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException {
    // query a remote node
    AtomicReference<Long> result = new AtomicReference<>();
    GroupByRequest request = new GroupByRequest();
    if (timeFilter != null) {
      request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    }
    request.setPath(path.getFullPath());
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setAggregationTypeOrdinals(aggregationTypes);
    request.setDataTypeOrdinal(dataType.ordinal());
    request.setRequestor(thisNode);

    List<Node> orderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : orderedNodes) {
      logger.debug("{}: querying group by {} from {}", name, path, node);
      GenericHandler<Long> handler = new GenericHandler<>(node, result);
      try {
        DataClient client = getDataClient(node);
        synchronized (result) {
          result.set(null);
          client.getGroupByExecutor(request, handler);
          result.wait(connectionTimeoutInMS);
        }
        Long executorId = result.get();
        if (executorId != null) {
          if (executorId != -1) {
            ((RemoteQueryContext) context).registerRemoteNode(partitionGroup.getHeader(), node);
            logger.debug("{}: get an executorId {} for {}@{} from {}", name, executorId,
                aggregationTypes, path, node);
            RemoteGroupByExecutor remoteGroupByExecutor = new RemoteGroupByExecutor(executorId,
                this, node, partitionGroup.getHeader());
            for (Integer aggregationType : aggregationTypes) {
              remoteGroupByExecutor.addAggregateResult(AggregateResultFactory.getAggrResultByType(
                  AggregationType.values()[aggregationType], dataType));
            }
            return remoteGroupByExecutor;
          } else {
            // there is no satisfying data on the remote node, create an empty reader to reduce
            // further communication
            logger.debug("{}: no data for {} from {}", name, path, node);
            return new EmptyReader();
          }
        }
      } catch (TException | InterruptedException | IOException e) {
        logger.error("{}: Cannot query {} from {}", name, path, node, e);
      }
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

}
