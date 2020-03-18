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
import org.apache.iotdb.cluster.rpc.thrift.StartUpStatus;
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

  // the file contains the identifier of the local node
  static final String NODE_IDENTIFIER_FILE_NAME = "node_identifier";
  // the file contains the serialized partition table
  static final String PARTITION_FILE_NAME = "partitions";
  // in case of data loss, some file changes would be made to a temporary file first
  private static final String TEMP_SUFFIX = ".tmp";

  private static final Logger logger = LoggerFactory.getLogger(MetaGroupMember.class);
  // when joining a cluster this node will retry at most "DEFAULT_JOIN_RETRY" times if the
  // network is bad
  private static final int DEFAULT_JOIN_RETRY = 10;
  // every "REPORT_INTERVAL_SEC" seconds, a reporter thread will print the status of all raft
  // members in this node
  private static final int REPORT_INTERVAL_SEC = 10;
  // how many times is a data record replicated, also the number of nodes in a data group
  public final int REPLICATION_NUM =
      ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();

  // blind nodes are nodes that do not have the partition table, and if the node is the leader,
  // the partition table should be sent to them at the next heartbeat
  private Set<Node> blindNodes = new HashSet<>();
  // as a leader, when a follower sent the node its identifier, the identifier may conflict with
  // other nodes, such conflicting nodes will be recorded and at the next heartbeat, they will be
  // required to regenerate an identifier.
  private Set<Node> idConflictNodes = new HashSet<>();
  // the identifier and its belonging node, for conflict detection, may be used in more places in
  // the future
  private Map<Integer, Node> idNodeMap = null;

  // nodes in the cluster and data partitioning
  private PartitionTable partitionTable;
  // each node contains multiple DataGroupMembers and they are managed by a DataClusterServer
  // acting as a broker
  private DataClusterServer dataClusterServer;
  // an override of TSServiceImpl, which redirect JDBC and session requests to the
  // MetaGroupMember so they can be processed cluster-wide
  private ClientServer clientServer;

  // this logManger manages the logs of the meta group
  private MetaSingleSnapshotLogManager logManager;

  // dataClientPool provides reusable thrift clients to connect to the DataGroupMembers of other
  // nodes
  private ClientPool dataClientPool;

  // every "REPORT_INTERVAL_SEC" seconds, "reportThread" will print the status of all raft
  // members in this node
  private ScheduledExecutorService reportThread;

  private StartUpStatus startUpStatus;

  @TestOnly
  public MetaGroupMember() {
  }

  public MetaGroupMember(TProtocolFactory factory, Node thisNode) throws QueryProcessException {
    super("Meta", new ClientPool(new MetaClient.Factory(factory)));
    allNodes = new ArrayList<>();

    dataClientPool = new ClientPool(new DataClient.Factory(factory));
    // committed logs are applied to the state machine (the IoTDB instance) through the applier
    LogApplier metaLogApplier = new MetaLogApplier(this);
    logManager = new MetaSingleSnapshotLogManager(metaLogApplier);
    super.logManager = logManager;

    setThisNode(thisNode);
    // load the identifier from the disk or generate a new one
    loadIdentifier();

    LogApplier dataLogApplier = new DataLogApplier(this);
    Factory dataMemberFactory = new Factory(factory, this, dataLogApplier);
    dataClusterServer = new DataClusterServer(thisNode, dataMemberFactory);
    clientServer = new ClientServer(this);
    startUpStatus = getStartUpStatus();
  }

  /**
   * Find the DataGroupMember that manages the partition of "storageGroupName"@"partitionId", and
   * close the partition through that member.
   *
   * @param storageGroupName
   * @param partitionId
   * @param isSeq
   * @return true if the member is a leader and the partition is closed, false otherwise
   */
  public boolean closePartition(String storageGroupName, long partitionId, boolean isSeq) {
    Node header = partitionTable.routeToHeaderByTime(storageGroupName, partitionId);
    return getLocalDataMember(header).closePartition(storageGroupName, partitionId, isSeq);
  }

  public DataClusterServer getDataClusterServer() {
    return dataClusterServer;
  }

  /**
   * Add seed nodes from the config, start the heartbeat and catch-up thread pool, initialize
   * QueryCoordinator and FileFlushPolicy, then start the reportThread. Calling the method twice
   * does not induce side effect.
   *
   * @throws TTransportException
   */
  @Override
  public void start() throws TTransportException {
    if (heartBeatService != null) {
      return;
    }
    addSeedNodes();
    super.start();

    QueryCoordinator.getINSTANCE().setMetaGroupMember(this);
    StorageEngine.getInstance().setFileFlushPolicy(new ClusterFileFlushPolicy(this));
    reportThread = Executors.newSingleThreadScheduledExecutor(n -> new Thread(n,
        "NodeReportThread"));
    reportThread.scheduleAtFixedRate(() -> logger.info(genNodeReport().toString()),
        REPORT_INTERVAL_SEC, REPORT_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   * Stop the heartbeat and catch-up thread pool, DataClusterServer, ClientServer and reportThread.
   * Calling the method twice does not induce side effects.
   */
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

  /**
   * Start DataClusterServer and ClientServer so this node will be able to respond to other nodes
   * and clients.
   *
   * @throws TTransportException
   * @throws StartupException
   */
  private void initSubServers() throws TTransportException, StartupException {
    getDataClusterServer().start();
    clientServer.start();
  }

  /**
   * Parse the seed nodes from the cluster configuration and add them into the node list. Each
   * seedUrl should be like "{hostName}:{metaPort}:{dataPort}" Ignore bad-formatted seedUrls.
   */
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

  /**
   * Apply the addition of a new node. Register its identifier, add it to the node list and
   * partition table, serialize the partition table and update the DataGroupMembers.
   *
   * @param newNode
   */
  public void applyAddNode(Node newNode) {
    synchronized (allNodes) {
      if (!allNodes.contains(newNode)) {
        logger.debug("Adding a new node {} into {}", newNode, allNodes);
        registerNodeIdentifier(newNode, newNode.getNodeIdentifier());
        allNodes.add(newNode);

        // update the partition table
        PartitionGroup newGroup = partitionTable.addNode(newNode);
        savePartitionTable();

        getDataClusterServer().addNode(newNode, newGroup);
      }
    }
  }

  /**
   * This node itself is a seed node, and it is going to build the initial cluster with other seed
   * nodes. This method is to skip the one-by-one addition to establish a large cluster quickly.
   */
  public void buildCluster() {
    // just establish the heartbeat thread and it will do the remaining
    heartBeatService.submit(new MetaHeartbeatThread(this));
  }

  /**
   * This node is not a seed node and wants to join an established cluster. Pick up a node randomly
   * from the seed nodes and send a join request to it.
   *
   * @return true if the node has successfully joined the cluster, false otherwise.
   */
  public boolean joinCluster() {
    JoinClusterHandler handler = new JoinClusterHandler();
    AtomicReference<AddNodeResponse> response = new AtomicReference(null);
    handler.setResponse(response);

    int retry = DEFAULT_JOIN_RETRY;
    while (retry > 0) {
      // randomly pick up a node to try
      Node node = allNodes.get(random.nextInt(allNodes.size()));
      try {
        if (joinCluster(node, startUpStatus, response, handler)) {
          logger.info("Joined a cluster, starting the heartbeat thread");
          setCharacter(NodeCharacter.FOLLOWER);
          setLastHeartbeatReceivedTime(System.currentTimeMillis());
          heartBeatService.submit(new MetaHeartbeatThread(this));
          return true;
        }
        // wait a heartbeat to start the next try
        Thread.sleep(RaftServer.heartBeatIntervalMs);
      } catch (TException e) {
        logger.warn("Cannot join the cluster from {}, because:", node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to join a cluster", e);
      }
      // start next try
      retry--;
    }
    // all tries failed
    logger.error("Cannot join the cluster after {} retries", DEFAULT_JOIN_RETRY);
    return false;
  }


  private StartUpStatus getStartUpStatus() {
    StartUpStatus startUpStatus = new StartUpStatus();
    startUpStatus
        .setPartitionInterval(IoTDBDescriptor.getInstance().getConfig().getPartitionInterval());
    startUpStatus.setHashSalt(ClusterConstant.HASH_SALT);
    startUpStatus
        .setReplicationNumber(ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum());
    return startUpStatus;
  }

  /**
   * Send a join cluster request to "node". If the joining is accepted, set the partition table,
   * start DataClusterServer and ClientServer and initialize DataGroupMembers.
   *
   * @return rue if the node has successfully joined the cluster, false otherwise.
   * @throws TException
   * @throws InterruptedException
   */
  private boolean joinCluster(Node node, StartUpStatus startUpStatus,
      AtomicReference<AddNodeResponse> response, JoinClusterHandler handler)
      throws TException, InterruptedException {
    AsyncClient client = (AsyncClient) connectNode(node);
    if (client != null) {
      response.set(null);
      handler.setContact(node);

      synchronized (response) {
        client.addNode(thisNode, startUpStatus, handler);
        response.wait(connectionTimeoutInMS);
      }
      AddNodeResponse resp = response.get();
      if (resp == null) {
        logger.warn("Join cluster request timed out");
      } else if (resp.getRespNum() == Response.RESPONSE_AGREE) {
        logger.info("Node {} admitted this node into the cluster", node);
        ByteBuffer partitionTableBuffer = resp.partitionTableBytes;
        acceptPartitionTable(partitionTableBuffer);
        getDataClusterServer().pullSnapshots();
        return true;
      } else if (resp.getRespNum() == Response.RESPONSE_IDENTIFIER_CONFLICT) {
        logger.info("The identifier {} conflicts the existing ones, regenerate a new one",
            thisNode.getNodeIdentifier());
        setNodeIdentifier(genNodeIdentifier());
      } else if (resp.getRespNum() == Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT) {
        CheckStatusResponse checkStatusResponse = resp.getCheckStatusResponse();
        String parameters = "";
        parameters +=
            checkStatusResponse.isPartitionalIntervalEquals() ? "" : ", partition interval";
        parameters += checkStatusResponse.isHashSaltEquals() ? "" : ", hash salt";
        parameters += checkStatusResponse.isReplicationNumEquals() ? "" : ", replication number";
        logger.info(
            "The start up configuration {} conflicts the cluster. Please reset the configurations. ",
            parameters.substring(1));
      } else {
        logger
            .warn("Joining the cluster is rejected by {} for response {}", node, resp.getRespNum());
      }
      return false;
    }
    return false;
  }

  /**
   * Process the heartbeat request from a valid leader. Generate and tell the leader the identifier
   * of the node if necessary. If the partition table is missing, use the one from the request or
   * require it in the response.
   *
   * @param request
   * @param response
   */
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
      // this node does not have a partition table yet
      if (request.isSetPartitionTableBytes()) {
        synchronized (this) {
          // if the leader has sent the node set then accept it
          ByteBuffer byteBuffer = request.partitionTableBytes;
          acceptPartitionTable(byteBuffer);
        }
      } else {
        // require the partition table
        logger.debug("Request cluster nodes from the leader");
        response.setRequirePartitionTable(true);
      }
    }
  }

  /**
   * Deserialize a partition table from the buffer, save it locally, add nodes from the partition
   * table and start DataClusterServer and ClientServer.
   *
   * @param partitionTableBuffer
   */
  private void acceptPartitionTable(ByteBuffer partitionTableBuffer) {
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

    startSubServers();
  }

  /**
   * Process a HeartBeatResponse from a follower. If the follower has provided its identifier, try
   * registering for it and if all nodes have registered and there is no available partition table,
   * initialize a new one and start the ClientServer and DataClusterServer. If the follower requires
   * a partition table, add it to the blind node list so that at the next heartbeat this node will
   * send it a partition table
   *
   * @param response
   * @param receiver
   */
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
    // record the requirement of partition table of the follower
    if (response.isRequirePartitionTable()) {
      addBlindNode(receiver);
    }
  }

  /**
   * When a node requires a partition table in its heartbeat response, add it into blindNodes so in
   * the next heartbeat the partition table will be sent to the node.
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
   * Remove the node from the blindNodes when the partition table is sent, so partition table will
   * not be sent in each heartbeat.
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
   * idNodeMap is initialized when the first leader wins or the follower receives the partition
   * table from the leader or a node recovers
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
   * Start the DataClusterServer and ClientServer so this node can serve other nodes and clients.
   * Also build DataGroupMembers using the partition table.
   */
  private synchronized void startSubServers() {
    synchronized (partitionTable) {
      try {
        initSubServers();
        getDataClusterServer().bulidDataGroupMembers(partitionTable);
      } catch (TTransportException | StartupException e) {
        logger.error("Build partition table failed: ", e);
        stop();
      }
    }
  }

  /**
   * Process the join cluster request of "node". Only proceed when the partition table is ready.
   *
   * @param node          cannot be the local node
   * @param resultHandler
   */
  @Override
  public void addNode(Node node, StartUpStatus startUpStatus, AsyncMethodCallback resultHandler) {
    AddNodeResponse response = new AddNodeResponse();
    if (partitionTable == null) {
      logger.info("Cannot add node now because the partition table is not set");
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
    if (processAddNodeLocally(node, startUpStatus, response, resultHandler)) {
      return;
    }

    if (character == NodeCharacter.FOLLOWER && leader != null) {
      logger.info("Forward the join request of {} to leader {}", node, leader);
      if (forwardAddNode(node, startUpStatus, resultHandler)) {
        return;
      }
    }
    resultHandler.onError(new LeaderUnknownException(getAllNodes()));
  }

  /**
   * Process the join cluster request of "node" as a MetaLeader. A node already joined is accepted
   * immediately. If the identifier of "node" conflicts with an existing node, the request will be
   * turned down.
   *
   * @param node          cannot be the local node
   * @param startUpStatus the start up status of the new node
   * @param response      the response that will be sent to "node"
   * @param resultHandler
   * @return true if the process is over, false if the request should be forwarded
   */
  private boolean processAddNodeLocally(Node node, StartUpStatus startUpStatus,
      AddNodeResponse response, AsyncMethodCallback resultHandler) {
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
      long remotePartitionInterval = startUpStatus.getPartitionInterval();
      int remoteHashSalt = startUpStatus.getHashSalt();
      int remoteReplicationNum = startUpStatus.getReplicationNumber();
      long localPartitionInterval = IoTDBDescriptor.getInstance().getConfig()
          .getPartitionInterval();
      int localHashSalt = ClusterConstant.HASH_SALT;
      int localReplicationNum = ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum();
      boolean partitionIntervalEquals = true;
      boolean hashSaltEquals = true;
      boolean replicationNumEquals = true;

      if (localPartitionInterval != remotePartitionInterval) {
        partitionIntervalEquals = false;
        logger.info("Remote partition interval conflicts with the leader's. Leader: {}, remote: {}",
            localPartitionInterval, remotePartitionInterval);
      }
      if (localHashSalt != remoteHashSalt) {
        hashSaltEquals = false;
        logger.info("Remote hash salt conflicts with the leader's. Leader: {}, remote: {}",
            localHashSalt,
            remoteHashSalt);
      }
      if (localReplicationNum != remoteReplicationNum) {
        replicationNumEquals = false;
        logger.info("Remote replication number conflicts with the leader's. Leader: {}, remote: {}",
            localReplicationNum, remoteReplicationNum);
      }
      if (!(partitionIntervalEquals && hashSaltEquals && replicationNumEquals)) {
        response.setRespNum((int) Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT);
        response.setCheckStatusResponse(
            new CheckStatusResponse(partitionIntervalEquals, hashSaltEquals, replicationNumEquals));
        resultHandler.onComplete(response);
        return true;
      }

      // node adding must be serialized to reduce potential concurrency problem
      synchronized (logManager) {
        AddNodeLog addNodeLog = new AddNodeLog();
        addNodeLog.setCurrLogTerm(getTerm().get());
        addNodeLog.setPreviousLogIndex(logManager.getLastLogIndex());
        addNodeLog.setPreviousLogTerm(logManager.getLastLogTerm());
        addNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);

        addNodeLog.setNewNode(node);

        logManager.appendLog(addNodeLog);

        int retryTime = 1;
        while (true) {
          logger
              .info("Send the join request of {} to other nodes, retry time: {}", node, retryTime);
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
              retryTime++;
              continue;
            case LEADERSHIP_STALE:
            default:
              return false;
          }
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
    // each node will be the header of a group, we use the node to represent the group
    int nodeSize = nodeRing.size();
    // the decreasing counters of how many nodes in a group has received the log, each time a
    // node receive the log, the counters of all groups it is in will decrease by 1
    int[] groupRemainings = new int[nodeSize];
    Arrays.fill(groupRemainings, groupQuorum);

    AtomicLong newLeaderTerm = new AtomicLong(term.get());
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(term.get());
    request.setEntry(log.serialize());

    // ask for votes from each node
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

  /**
   * Send "request" to each node in "nodeRing" and when a node returns a success, decrease all
   * conuters of the groups it is in of "groupRemainings"
   *
   * @param groupRemainings
   * @param nodeRing
   * @param request
   * @param leaderShipStale
   * @param log
   * @param newLeaderTerm
   */
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
          // node == this node, decrease counters of all groups the local node is in
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
        logger.error("Unexpected interruption when waiting for the group votes", e);
      }
    }
  }

  /**
   * Forward the join cluster request to the leader.
   *
   * @return true if the forwarding succeeds, false otherwise.
   */
  private boolean forwardAddNode(Node node, StartUpStatus startUpStatus,
      AsyncMethodCallback resultHandler) {
    TSMetaService.AsyncClient client = (TSMetaService.AsyncClient) connectNode(leader);
    if (client != null) {
      try {
        client.addNode(node, startUpStatus, resultHandler);
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

  /**
   * When this node becomes the MetaLeader (for the first time), it should init the idNodeMap, so
   * that if can require identifiers from all nodes and check if there are conflicts.
   */
  @Override
  public void onElectionWins() {
    if (idNodeMap == null) {
      initIdNodeMap();
    }
  }

  /**
   * Load the partition table from a local file
   *
   * @return true if the local file is found, false otherwise
   */
  private boolean loadPartitionTable() {
    File partitionFile = new File(PARTITION_FILE_NAME);
    if (!partitionFile.exists()) {
      logger.info("No partition table file found");
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
      logger.error("Cannot load the partition table", e);
      return false;
    }
    return true;
  }

  /**
   * Serialize the partition table to a fixed position on the disk. Will first serialize to a
   * temporary file and than replace the old file.
   */
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
      logger.error("Cannot save the partition table", e);
    }
    oldFile.delete();
    tempFile.renameTo(oldFile);
  }

  /**
   * Load the identifier from the disk, if the identifier file does not exist, a new identifier will
   * be generated. Do nothing if the identifier is already set.
   */
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
        logger.warn("Cannot read the identifier from file, generating a new one", e);
      }
    }
    if (nodeId != null) {
      setNodeIdentifier(nodeId);
      return;
    }

    setNodeIdentifier(genNodeIdentifier());
  }

  /**
   * Generate a new identifier using the hash of IP, metaPort and sysTime.
   *
   * @return a new identifier
   */
  private int genNodeIdentifier() {
    return Objects.hash(thisNode.getIp(), thisNode.getMetaPort(),
        System.currentTimeMillis());
  }

  /**
   * Set the node's identifier to "identifier", also save it to a local file in text format.
   *
   * @param identifier
   */
  private void setNodeIdentifier(int identifier) {
    logger.info("The identifier of this node has been set to {}", identifier);
    thisNode.setNodeIdentifier(identifier);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(NODE_IDENTIFIER_FILE_NAME))) {
      writer.write(String.valueOf(identifier));
    } catch (IOException e) {
      logger.error("Cannot save the node identifier", e);
    }
  }

  public PartitionTable getPartitionTable() {
    return partitionTable;
  }

  /**
   * Process a snapshot sent by the MetaLeader. Deserialize the snapshot and apply it. The type of
   * the snapshot should be MetaSimpleSnapshot.
   *
   * @param request
   * @param resultHandler
   */
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

  /**
   * Apply a meta snapshot to IoTDB. The snapshot contains: all storage groups, logs of node
   * addition and removal, and last log term/index in the snapshot.
   *
   * @param snapshot
   */
  private void applySnapshot(MetaSimpleSnapshot snapshot) {
    synchronized (logManager) {
      // register all storage groups in the snapshot
      for (String storageGroup : snapshot.getStorageGroups()) {
        try {
          MManager.getInstance().setStorageGroup(storageGroup);
        } catch (StorageGroupAlreadySetException ignored) {
          // ignore duplicated storage group
        } catch (MetadataException e) {
          logger.error("{}: Cannot add storage group {} in snapshot", name, storageGroup);
        }
      }
      // apply other logs like node removal and addition
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

  /**
   * Execute a non-query plan. According to the type of the plan, the plan will be executed on all
   * nodes (like timeseries deletion) or the nodes that belong to certain groups (like data
   * ingestion).
   *
   * @param plan a non-query plan.
   * @return
   */
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

  /**
   * A non-partitioned plan (like storage group creation) should be executed on all nodes, so the
   * MetaLeader should take the responsible to make sure that every node receives the plan. Thus the
   * plan will be processed locally only by the MetaLeader and forwarded by non-leader nodes.
   *
   * @param plan
   * @return
   */
  private TSStatus processNonPartitionedPlan(PhysicalPlan plan) {
    if (character == NodeCharacter.LEADER) {
      TSStatus status = processPlanLocally(plan);
      if (status != null) {
        return status;
      }
    }
    return forwardPlan(plan, leader, null);
  }

  /**
   * A partitioned plan (like batch insertion) will be split into several sub-plans, each belongs to
   * data group. And these sub-plans will be sent to and executed on the corresponding groups
   * separately.
   *
   * @param plan
   * @return
   * @throws UnsupportedPlanException
   */
  private TSStatus processPartitionedPlan(PhysicalPlan plan) throws UnsupportedPlanException {
    logger.debug("{}: Received a partitioned plan {}", name, plan);
    if (partitionTable == null) {
      logger.debug("{}: Partition table is not ready", name);
      return StatusUtils.PARTITION_TABLE_NOT_READY;
    }

    // split the plan into sub-plans that each only involve one data group
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
    logger.debug("{}: The data groups of {} are {}", name, plan, planGroupMap);

    TSStatus status;
    // those data groups that successfully executed the plan
    List<Entry<PhysicalPlan, PartitionGroup>> succeededEntries = new ArrayList<>();
    // the error codes from the groups that cannot execute the plan
    List<String> errorCodePartitionGroups = new ArrayList<>();
    for (Map.Entry<PhysicalPlan, PartitionGroup> entry : planGroupMap.entrySet()) {
      TSStatus subStatus;
      if (entry.getValue().contains(thisNode)) {
        // the query should be handled by a group the local node is in, handle it with in the group
        subStatus = getLocalDataMember(entry.getValue().getHeader())
            .executeNonQuery(entry.getKey());
      } else {
        // forward the query to the group that should handle it
        subStatus = forwardPlan(entry.getKey(), entry.getValue());
      }
      if (subStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // execution failed, record the error message
        errorCodePartitionGroups.add(String.format("[%s@%s:%s]",
            subStatus.getCode(), entry.getValue().getHeader(),
            subStatus.getMessage()));
      } else {
        succeededEntries.add(entry);
      }
    }
    if (errorCodePartitionGroups.isEmpty()) {
      // no error occurs, the plan is successfully executed
      status = StatusUtils.OK;
    } else {
      status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
      status.setMessage("The following errors occurred when executing the query, "
          + "please retry or contact the DBA: " + errorCodePartitionGroups.toString());
      //TODO-Cluster: abort the succeeded ones if necessary.
    }
    return status;
  }

  /**
   * For ward a plan to the DataGroupMember of one node in the group. Only when all nodes time out,
   * will a TIME_OUT be returned.
   *
   * @param plan
   * @param group
   * @return
   */
  TSStatus forwardPlan(PhysicalPlan plan, PartitionGroup group) {
    for (Node node : group) {
      TSStatus status;
      try {
        // if a plan is partitioned to any group, it must be processed by its data server instead of
        // meta server
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
   * Pull the all timeseries schemas of given prefixPaths from remote nodes. All prefixPaths must
   * contain the storage group.
   */
  public List<MeasurementSchema> pullTimeSeriesSchemas(List<String> prefixPaths)
      throws MetadataException {
    logger.debug("{}: Pulling timeseries schemas of {}", name, prefixPaths);
    // split the paths by the data groups that will hold them
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
    // pull timeseries schema from every group involved
    for (Entry<PartitionGroup, List<String>> partitionGroupListEntry : partitionGroupPathMap
        .entrySet()) {
      PartitionGroup partitionGroup = partitionGroupListEntry.getKey();
      List<String> paths = partitionGroupListEntry.getValue();
      pullTimeSeriesSchemas(partitionGroup, paths, schemas);
    }
    return schemas;
  }

  /**
   * Pull timeseries schemas of "prefixPaths" from "partitionGroup" and store them in "results". If
   * this node is a member of "partitionGroup", synchronize with the group leader and collect local
   * schemas. Otherwise pull schemas from one node in the group.
   *
   * @param partitionGroup
   * @param prefixPaths
   * @param results
   */
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

    // pull schemas from a remote node
    PullSchemaRequest pullSchemaRequest = new PullSchemaRequest();
    pullSchemaRequest.setHeader(partitionGroup.getHeader());
    pullSchemaRequest.setPrefixPaths(prefixPaths);
    AtomicReference<List<MeasurementSchema>> timeseriesSchemas = new AtomicReference<>();
    for (Node node : partitionGroup) {
      logger.debug("{}: Pulling timeseries schemas of {} from {}", name, prefixPaths, node);
      DataClient client;
      try {
        client = getDataClient(node);
      } catch (IOException e) {
        continue;
      }
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

    /**
     * Get the data types of "paths". If "aggregations" is not null, each one of it correspond to
     * one in "paths".
     * First get types locally and if some paths does not exists, pull them from other nodes.
     * @param paths
     * @param aggregations nullable, when not null, correspond to "paths" one-to-one.
     * @return
     * @throws MetadataException
     */
    public List<TSDataType> getSeriesTypesByPath (List < Path > paths, List < String > aggregations) throws
    MetadataException {
      try {
        // try locally first
        return SchemaUtils.getSeriesTypesByPath(paths, aggregations);
      } catch (PathNotExistException e) {
        List<String> pathStr = new ArrayList<>();
        for (Path path : paths) {
          pathStr.add(path.getFullPath());
        }
        // pull schemas remotely
        List<MeasurementSchema> schemas = pullTimeSeriesSchemas(pathStr);
        // TODO-Cluster: should we register the schemas locally?
        if (schemas.isEmpty()) {
          // if one timeseries cannot be found remotely, too, it does not exist
          throw e;
        }

        // consider the aggregations to get the real data type
        List<TSDataType> result = new ArrayList<>();
        for (int i = 0; i < schemas.size(); i++) {
          TSDataType dataType = null;
          if (aggregations != null) {
            String aggregation = aggregations.get(i);
            // aggregations like first/last value does not have fixed data types and will return a
            // null
            dataType = getAggregationType(aggregation);
          }
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

      /**
       * Get the data types of "paths". If "aggregation" is not null, every path will use the
       * aggregation.
       * First get types locally and if some paths does not exists, pull them from other nodes.
       * @param pathStrs
       * @param aggregation
       * @return
       * @throws MetadataException
       */
      public List<TSDataType> getSeriesTypesByString (List < String > pathStrs, String aggregation) throws
      MetadataException {
        try {
          // try locally first
          return SchemaUtils.getSeriesTypesByString(pathStrs, aggregation);
        } catch (PathNotExistException e) {
          // pull schemas remotely
          List<MeasurementSchema> schemas = pullTimeSeriesSchemas(pathStrs);
          // TODO-Cluster: should we register the schemas locally?
          if (schemas.isEmpty()) {
            // if one timeseries cannot be found remotely, too, it does not exist
            throw e;
          }

          // consider the aggregations to get the real data type
          List<TSDataType> result = new ArrayList<>();
          // aggregations like first/last value does not have fixed data types and will return a null
          TSDataType aggregationType = getAggregationType(aggregation);
          for (MeasurementSchema schema : schemas) {
            if (aggregationType == null) {
              result.add(schema.getType());
            } else {
              result.add(aggregationType);
            }
            SchemaUtils.registerTimeseries(schema);
          }
          return result;
        }
      }

      /**
       * Create an IReaderByTimestamp that can read the data of "path" by timestamp in the whole
       * cluster. This will query every group and merge the result from them.
       * @param path
       * @param dataType
       * @param context
       * @return
       * @throws StorageEngineException
       */
      public IReaderByTimestamp getReaderByTimestamp (Path path, TSDataType dataType,
          QueryContext context)
      throws StorageEngineException {
        // make sure the partition table is new
        syncLeader();
        // get all data groups
        List<PartitionGroup> partitionGroups = routeFilter(null, path);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sending query of {} to {} groups", name, path, partitionGroups.size());
        }
        List<IReaderByTimestamp> readers = new ArrayList<>();
        for (PartitionGroup partitionGroup : partitionGroups) {
          // query each group to get a reader in that group
          readers.add(getSeriesReaderByTime(partitionGroup, path, context, dataType));
        }
        // merge the readers
        return new MergedReaderByTime(readers);
      }

      /**
       * Create a IReaderByTimestamp that read data of "path" by timestamp in the given group. If the
       * local node is a member of that group, query locally. Otherwise create a remote reader
       * pointing to one node in that group.
       * @param partitionGroup
       * @param path
       * @param context
       * @param dataType
       * @return
       * @throws StorageEngineException
       */
      private IReaderByTimestamp getSeriesReaderByTime (PartitionGroup partitionGroup, Path path,
          QueryContext context, TSDataType dataType) throws StorageEngineException {
        if (partitionGroup.contains(thisNode)) {
          // the target storage group contains this node, perform a local query
          DataGroupMember dataGroupMember = getLocalDataMember(partitionGroup.getHeader());
          logger.debug("{}: creating a local reader for {}#{}", name, path.getFullPath(),
              context.getQueryId());
          return dataGroupMember.getReaderByTimestamp(path, dataType, context);
        } else {
          return getRemoteReaderByTimestamp(path, dataType, partitionGroup, context);
        }
      }

      /**
       * Create a IReaderByTimestamp that read data of "path" by timestamp in the given group that
       * does not contain the local node. Send a request to one node in that group to build a reader
       * and use that reader's id to build a remote reader.
       * @param path
       * @param dataType
       * @param partitionGroup
       * @param context
       * @return
       * @throws StorageEngineException
       */
      private IReaderByTimestamp getRemoteReaderByTimestamp (
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
              // register the node so the remote resources can be released
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

      /**
       * Create a ManagedSeriesReader that can read the data of "path" with filters in the whole
       * cluster. The data groups that should be queried will be determined by the timeFilter, then
       * for each group a series reader will be created, and finally all such readers will be merged
       * into one.
       * @param path
       * @param dataType
       * @param timeFilter nullable, when null, all data groups will be queried
       * @param valueFilter nullable
       * @param context
       * @return
       * @throws StorageEngineException
       */
      public ManagedSeriesReader getSeriesReader (Path path, TSDataType dataType, Filter timeFilter,
          Filter valueFilter, QueryContext context)
      throws StorageEngineException {
        // make sure the partition table is new
        syncLeader();
        // find the groups that should be queried using the timeFilter
        List<PartitionGroup> partitionGroups = routeFilter(timeFilter, path);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sending data query of {} to {} groups", name, path,
              partitionGroups.size());
        }
        ManagedMergeReader mergeReader = new ManagedMergeReader(dataType);
        try {
          // build a reader for each group and merge them
          for (PartitionGroup partitionGroup : partitionGroups) {
            IPointReader seriesReader = getSeriesReader(partitionGroup, path, timeFilter,
                valueFilter,
                context, dataType);
            if (seriesReader.hasNextTimeValuePair()) {
              // only add readers that have data, and they should basically not overlap with each
              // other (from different time partitions) so the priority does not matter
              mergeReader.addReader(seriesReader, 0);
            }
          }
        } catch (IOException e) {
          throw new StorageEngineException(e);
        }
        return mergeReader;
      }

      /**
       * Perform "aggregations" over "path" in some data groups and merge the results. The groups to
       * be queried is determined by "timeFilter".
       * @param path
       * @param aggregations
       * @param dataType
       * @param timeFilter nullable, when null, all groups will be queried
       * @param context
       * @return
       * @throws StorageEngineException
       */
      public List<AggregateResult> getAggregateResult (Path path, List < String > aggregations,
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

      /**
       * Perform "aggregations" over "path" in "partitionGroup". If the local node is the member of
       * the group, do it locally, otherwise pull the results from a remote node.
       * @param path
       * @param aggregations
       * @param dataType
       * @param timeFilter nullable
       * @param partitionGroup
       * @param context
       * @return
       * @throws StorageEngineException
       */
      private List<AggregateResult> getAggregateResult (Path path, List < String > aggregations,
          TSDataType dataType, Filter timeFilter, PartitionGroup partitionGroup,
          QueryContext context) throws StorageEngineException {
        if (!partitionGroup.contains(thisNode)) {
          return getRemoteAggregateResult(path, aggregations, dataType, timeFilter, partitionGroup,
              context);
        } else {
          // perform the aggregations locally
          DataGroupMember dataMember = getLocalDataMember(partitionGroup.getHeader());
          try {
            logger
                .debug("{}: querying aggregation {} of {} in {} locally", name, aggregations, path,
                    partitionGroup.getHeader());
            List<AggregateResult> aggrResult = dataMember
                .getAggrResult(aggregations, dataType, path.getFullPath(), timeFilter, context);
            logger
                .debug("{}: queried aggregation {} of {} in {} locally are {}", name, aggregations,
                    path, partitionGroup.getHeader(), aggrResult);
            return aggrResult;
          } catch (IOException | QueryProcessException | LeaderUnknownException e) {
            throw new StorageEngineException(e);
          }
        }
      }

      /**
       * Perform "aggregations" over "path" in a remote data group "partitionGroup". Query one node
       * in the group to get the results.
       * @param path
       * @param aggregations
       * @param dataType
       * @param timeFilter nullable
       * @param partitionGroup
       * @param context
       * @return
       * @throws StorageEngineException
       */
      private List<AggregateResult> getRemoteAggregateResult (Path
      path, List < String > aggregations,
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
            // each buffer is an AggregationResult
            List<ByteBuffer> resultBuffers = resultReference.get();
            if (resultBuffers != null) {
              List<AggregateResult> results = new ArrayList<>(resultBuffers.size());
              for (ByteBuffer resultBuffer : resultBuffers) {
                AggregateResult result = AggregateResult.deserializeFrom(resultBuffer);
                results.add(result);
              }
              // register the queried node to release resources
              ((RemoteQueryContext) context).registerRemoteNode(node, partitionGroup.getHeader());
              logger.debug("{}: queried aggregation {} of {} from {} of {} are {}", name,
                  aggregations,
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

      /**
       * Get the data groups that should be queried when querying "path" with "filter".
       * First, the time interval qualified by the filter will be extracted. If any side of the
       * interval is open, query all groups. Otherwise compute all involved groups w.r.t. the time
       * partitioning.
       * @param filter
       * @param path
       * @return
       * @throws StorageEngineException
       */
      private List<PartitionGroup> routeFilter (Filter filter, Path path) throws
      StorageEngineException {
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
              // compute the headers of groups involved in every interval
              PartitionUtils.getIntervalHeaders(storageGroupName, intervals.getLowerBound(i),
                  intervals.getUpperBound(i), partitionTable, groupHeaders);
            }
            // translate the headers to groups
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
      private IPointReader getSeriesReader (PartitionGroup partitionGroup, Path path,
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
          return dataGroupMember
              .getSeriesPointReader(path, dataType, timeFilter, valueFilter, context);
        } else {
          return getRemoteSeriesPointReader(timeFilter, valueFilter, dataType, path, partitionGroup,
              context);
        }
      }

      private IPointReader getRemoteSeriesPointReader (Filter timeFilter,
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
                return new RemoteSimpleSeriesReader(readerId, node, partitionGroup.getHeader(),
                    this,
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

      private ClientPool getDataClientPool () {
        return dataClientPool;
      }

      /**
       * Get all paths after removing wildcards in the path
       *
       * @param originPath a path potentially with wildcard
       * @return all paths after removing wildcards in the path
       */
      public List<String> getMatchedPaths (String originPath) throws MetadataException {
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
      public Set<String> getMatchedDevices (String originPath) throws MetadataException {
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

      private List<String> getMatchedPaths (String storageGroupName, String path)
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

      private Set<String> getMatchedDevices (String storageGroupName, String path)
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

      public Map<Node, Boolean> getAllNodeStatus () {
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
      public void queryNodeStatus (AsyncMethodCallback < TNodeStatus > resultHandler) {
        resultHandler.onComplete(new TNodeStatus());
      }

      @Override
      public void checkAlive (AsyncMethodCallback < Node > resultHandler) {
        resultHandler.onComplete(thisNode);
      }

      @TestOnly
      public void setPartitionTable (PartitionTable partitionTable){
        this.partitionTable = partitionTable;
        DataClusterServer dataClusterServer = getDataClusterServer();
        if (dataClusterServer != null) {
          dataClusterServer.setPartitionTable(partitionTable);
        }
      }

      @Override
      public void removeNode (Node node, AsyncMethodCallback < Long > resultHandler){
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

      private boolean forwardRemoveNode (Node node, AsyncMethodCallback resultHandler){
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

      private boolean processRemoveNodeLocally (Node node, AsyncMethodCallback resultHandler){
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

      public void applyRemoveNode (Node oldNode){
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
      public void exile (AsyncMethodCallback < Void > resultHandler) {
        applyRemoveNode(thisNode);
        resultHandler.onComplete(null);
      }

      private MetaMemberReport genMemberReport () {
        return new MetaMemberReport(character, leader, term.get(),
            logManager.getLastLogTerm(), logManager.getLastLogIndex(), readOnly);
      }

      private NodeReport genNodeReport () {
        NodeReport report = new NodeReport(thisNode);
        report.setMetaMemberReport(genMemberReport());
        report.setDataMemberReportList(dataClusterServer.genMemberReports());
        return report;
      }

      @Override
      public void setAllNodes (List < Node > allNodes) {
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
      protected DataGroupMember getLocalDataMember (Node header,
          AsyncMethodCallback resultHandler, Object request){
        return dataClusterServer.getDataMember(header, resultHandler, request);
      }

      protected DataGroupMember getLocalDataMember (Node header){
        return dataClusterServer.getDataMember(header, null, "Internal call");
      }

      public DataClient getDataClient (Node node) throws IOException {
        return (DataClient) getDataClientPool().getClient(node);
      }

      public List<GroupByExecutor> getGroupByExecutors (Path path, TSDataType dataType,
          QueryContext context, Filter timeFilter, List < Integer > aggregationTypes)
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

      private GroupByExecutor getGroupByExecutor (Path path,
          PartitionGroup partitionGroup, Filter timeFilter, QueryContext context, TSDataType
      dataType,
          List < Integer > aggregationTypes) throws StorageEngineException {
        if (partitionGroup.contains(thisNode)) {
          // the target storage group contains this node, perform a local query
          DataGroupMember dataGroupMember = getLocalDataMember(partitionGroup.getHeader());
          logger.debug("{}: creating a local group by executor for {}#{}", name,
              path.getFullPath(), context.getQueryId());
          return dataGroupMember
              .getGroupByExecutor(path, dataType, timeFilter, aggregationTypes, context);
        } else {
          return getRemoteGroupByExecutor(timeFilter, aggregationTypes, dataType, path,
              partitionGroup,
              context);
        }
      }

      private GroupByExecutor getRemoteGroupByExecutor (Filter timeFilter,
          List < Integer > aggregationTypes, TSDataType dataType, Path path, PartitionGroup
      partitionGroup,
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
                  remoteGroupByExecutor
                      .addAggregateResult(AggregateResultFactory.getAggrResultByType(
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

  @Override
  public void pullTimeSeriesSchema(PullSchemaRequest request,
      AsyncMethodCallback<PullSchemaResp> resultHandler) throws TException {
    // TODO
  }
}
