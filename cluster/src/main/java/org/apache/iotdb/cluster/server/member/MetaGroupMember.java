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

import java.lang.reflect.Array;
import java.net.SocketTimeoutException;
import org.apache.iotdb.cluster.ClusterFileFlushPolicy;
import org.apache.iotdb.cluster.client.async.AsyncClientPool;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncDataClient.FactoryAsync;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncClientPool;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient.FactorySync;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.exception.*;
import org.apache.iotdb.cluster.log.Log;
import org.apache.iotdb.cluster.log.LogApplier;
import org.apache.iotdb.cluster.log.applier.MetaLogApplier;
import org.apache.iotdb.cluster.log.logtypes.AddNodeLog;
import org.apache.iotdb.cluster.log.logtypes.RemoveNodeLog;
import org.apache.iotdb.cluster.log.manage.MetaSingleSnapshotLogManager;
import org.apache.iotdb.cluster.log.snapshot.MetaSimpleSnapshot;
import org.apache.iotdb.cluster.partition.*;
import org.apache.iotdb.cluster.query.ClusterPlanRouter;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.fill.PreviousFillArguments;
import org.apache.iotdb.cluster.query.groupby.RemoteGroupByExecutor;
import org.apache.iotdb.cluster.query.manage.QueryCoordinator;
import org.apache.iotdb.cluster.query.reader.*;
import org.apache.iotdb.cluster.rpc.thrift.*;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;
import org.apache.iotdb.cluster.rpc.thrift.TSMetaService.AsyncClient;
import org.apache.iotdb.cluster.server.*;
import org.apache.iotdb.cluster.server.NodeReport.MetaMemberReport;
import org.apache.iotdb.cluster.server.handlers.caller.AppendGroupEntryHandler;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.handlers.caller.NodeStatusHandler;
import org.apache.iotdb.cluster.server.handlers.caller.PreviousFillHandler;
import org.apache.iotdb.cluster.server.heartbeat.MetaHeartbeatThread;
import org.apache.iotdb.cluster.server.member.DataGroupMember.Factory;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.cluster.utils.PartitionUtils;
import org.apache.iotdb.cluster.utils.PartitionUtils.Intervals;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MetaUtils;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DeleteTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.StringContainer;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.cluster.utils.ClusterUtils.*;
import static org.apache.iotdb.db.utils.EncodingInferenceUtils.getDefaultEncoding;
import static org.apache.iotdb.db.utils.SchemaUtils.getAggregationType;

public class MetaGroupMember extends RaftMember {

  // the file contains the identifier of the local node
  static final String NODE_IDENTIFIER_FILE_NAME =
      IoTDBDescriptor.getInstance().getConfig().getBaseDir() + File.separator + "node_identifier";
  // the file contains the serialized partition table
  static final String PARTITION_FILE_NAME =
      IoTDBDescriptor.getInstance().getConfig().getBaseDir() + File.separator + "partitions";
  // in case of data loss, some file changes would be made to a temporary file first
  private static final String TEMP_SUFFIX = ".tmp";
  private static final String MSG_MULTIPLE_ERROR = "The following errors occurred when executing "
      + "the query, please retry or contact the DBA: ";

  private static final Logger logger = LoggerFactory.getLogger(MetaGroupMember.class);
  // when joining a cluster this node will retry at most "DEFAULT_JOIN_RETRY" times if the
  // network is bad
  private static final int DEFAULT_JOIN_RETRY = 10;
  // every "REPORT_INTERVAL_SEC" seconds, a reporter thread will print the status of all raft
  // members in this node
  private static final int REPORT_INTERVAL_SEC = 10;
  // how many times is a data record replicated, also the number of nodes in a data group
  public static final int REPLICATION_NUM =
      ClusterDescriptor.getInstance().getConfig().getReplicationNum();

  // hardlinks will be checked every hour
  private static final long CLEAN_HARDLINK_INTERVAL_SEC = 3600;

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
  // router calculates the partition groups that a partitioned plan should be sent to
  private ClusterPlanRouter router;
  // each node contains multiple DataGroupMembers and they are managed by a DataClusterServer
  // acting as a broker
  private DataClusterServer dataClusterServer;
  // an override of TSServiceImpl, which redirect JDBC and session requests to the
  // MetaGroupMember so they can be processed cluster-wide
  private ClientServer clientServer;

  // dataClientPool provides reusable thrift clients to connect to the DataGroupMembers of other
  // nodes
  private AsyncClientPool dataAsyncClientPool;
  private SyncClientPool dataSyncClientPool;

  // every "REPORT_INTERVAL_SEC" seconds, "reportThread" will print the status of all raft
  // members in this node
  private ScheduledExecutorService reportThread;

  private StartUpStatus startUpStatus;

  // localExecutor is used to directly execute plans like load configuration (locally)
  private PlanExecutor localExecutor;

  private ScheduledExecutorService hardLinkCleanerThread;

  @TestOnly
  public MetaGroupMember() {
  }

  public MetaGroupMember(TProtocolFactory factory, Node thisNode) throws QueryProcessException {
    super("Meta", new AsyncClientPool(new AsyncMetaClient.FactoryAsync(factory)),
        new SyncClientPool(new FactorySync(factory)));
    allNodes = new ArrayList<>();
    initPeerMap();

    dataAsyncClientPool = new AsyncClientPool(new FactoryAsync(factory));
    dataSyncClientPool = new SyncClientPool(new SyncDataClient.FactorySync(factory));
    // committed logs are applied to the state machine (the IoTDB instance) through the applier
    LogApplier metaLogApplier = new MetaLogApplier(this);
    logManager = new MetaSingleSnapshotLogManager(metaLogApplier, this);
    term.set(logManager.getHardState().getCurrentTerm());
    voteFor = logManager.getHardState().getVoteFor();

    setThisNode(thisNode);
    // load the identifier from the disk or generate a new one
    loadIdentifier();

    Factory dataMemberFactory = new Factory(factory, this);
    dataClusterServer = new DataClusterServer(thisNode, dataMemberFactory, this);
    clientServer = new ClientServer(this);
    startUpStatus = getNewStartUpStatus();
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
    Node header = partitionTable.routeToHeaderByTime(storageGroupName,
        partitionId * StorageEngine.getTimePartitionInterval());
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
  public void start() {
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
    hardLinkCleanerThread = Executors.newSingleThreadScheduledExecutor(n -> new Thread(n,
        "HardLinkCleaner"));
    hardLinkCleanerThread.scheduleAtFixedRate(new HardLinkCleaner(),
        CLEAN_HARDLINK_INTERVAL_SEC, CLEAN_HARDLINK_INTERVAL_SEC, TimeUnit.SECONDS);
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
    }
    if (clientServer != null) {
      clientServer.stop();
    }
    if (reportThread != null) {
      reportThread.shutdownNow();
      try {
        reportThread.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for reportThread to end", e);
      }
    }
    if (hardLinkCleanerThread != null) {
      hardLinkCleanerThread.shutdownNow();
      try {
        hardLinkCleanerThread.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for hardlinkCleaner to end", e);
      }
    }

    logger.info("{}: stopped", name);
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
  protected void addSeedNodes() {
    List<String> seedUrls = config.getSeedNodeUrls();
    // initialize allNodes
    for (String seedUrl : seedUrls) {
      Node node = generateNode(seedUrl);
      if ((!node.getIp().equals(thisNode.ip) || node.getMetaPort() != thisNode.getMetaPort())
          && !allNodes.contains(node)) {
        // do not add the local node since it is added in `setThisNode()`
        allNodes.add(node);
      }
    }
  }

  protected Node generateNode(String nodeUrl) {
    Node result = new Node();
    String[] split = nodeUrl.split(":");
    if (split.length != 3) {
      logger.warn("Bad seed url: {}", nodeUrl);
      return null;
    }
    String ip = split[0];
    try {
      int metaPort = Integer.parseInt(split[1]);
      int dataPort = Integer.parseInt(split[2]);
      result.setIp(ip);
      result.setMetaPort(metaPort);
      result.setDataPort(dataPort);
    } catch (NumberFormatException e) {
      logger.warn("Bad seed url: {}", nodeUrl);
    }
    return result;
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
        NodeAdditionResult result = partitionTable.addNode(newNode);
        savePartitionTable();

        getDataClusterServer().addNode(newNode, result);
      }
    }
  }

  /**
   * This node itself is a seed node, and it is going to build the initial cluster with other seed
   * nodes. This method is to skip the one-by-one addition to establish a large cluster quickly.
   */
  public void buildCluster() {
    checkSeedNodes();
    // just establish the heartbeat thread and it will do the remaining
    loadPartitionTable();
    heartBeatService.submit(new MetaHeartbeatThread(this));
  }

  /**
   * This node is not a seed node and wants to join an established cluster. Pick up a node randomly
   * from the seed nodes and send a join request to it.
   *
   * @return true if the node has successfully joined the cluster, false otherwise.
   */
  public boolean joinCluster() {
    if (allNodes.size() == 1) {
      logger.error("Seed nodes not provided, cannot join cluster");
      return false;
    }

    int retry = DEFAULT_JOIN_RETRY;
    while (retry > 0) {
      // randomly pick up a node to try
      Node node = allNodes.get(random.nextInt(allNodes.size()));
      if (node.equals(thisNode)) {
        continue;
      }
      logger.info("start joining the cluster with the help of {}", node);
      try {
        if (joinCluster(node, startUpStatus)) {
          logger.info("Joined a cluster, starting the heartbeat thread");
          setCharacter(NodeCharacter.FOLLOWER);
          setLastHeartbeatReceivedTime(System.currentTimeMillis());
          heartBeatService.submit(new MetaHeartbeatThread(this));
          return true;
        }
        // wait 5s to start the next try
        Thread.sleep(5000);
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


  private StartUpStatus getNewStartUpStatus() {
    StartUpStatus newStartUpStatus = new StartUpStatus();
    newStartUpStatus
        .setPartitionInterval(IoTDBDescriptor.getInstance().getConfig().getPartitionInterval());
    newStartUpStatus.setHashSalt(ClusterConstant.HASH_SALT);
    newStartUpStatus
        .setReplicationNumber(ClusterDescriptor.getInstance().getConfig().getReplicationNum());
    List<String> seedUrls = ClusterDescriptor.getInstance().getConfig().getSeedNodeUrls();
    List<Node> seedNodeList = new ArrayList<>();
    for (String seedUrl : seedUrls) {
      seedNodeList.add(generateNode(seedUrl));
    }
    newStartUpStatus.setSeedNodeList(seedNodeList);
    return newStartUpStatus;
  }

  /**
   * Send a join cluster request to "node". If the joining is accepted, set the partition table,
   * start DataClusterServer and ClientServer and initialize DataGroupMembers.
   *
   * @return rue if the node has successfully joined the cluster, false otherwise.
   * @throws TException
   * @throws InterruptedException
   */
  private boolean joinCluster(Node node, StartUpStatus startUpStatus)
      throws TException, InterruptedException {

    AddNodeResponse resp;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncMetaClient client = (AsyncMetaClient) getAsyncClient(node);
      resp = SyncClientAdaptor.addNode(client, thisNode, startUpStatus);
    } else {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(node);
      try {
        resp = client.addNode(thisNode, startUpStatus);
      } finally {
        putBackSyncClient(client);
      }
    }

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
      if (logger.isInfoEnabled()) {
        logger.info(
            "The start up configuration {} conflicts the cluster. Please reset the configurations. ",
            parameters.substring(1));
      }
    } else {
      logger
          .warn("Joining the cluster is rejected by {} for response {}", node, resp.getRespNum());
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
  void processValidHeartbeatReq(HeartBeatRequest request, HeartBeatResponse response) {
    if (request.isRequireIdentifier()) {
      // the leader wants to know who the node is
      if (request.isRegenerateIdentifier()) {
        // the previously sent id conflicted, generate a new one
        setNodeIdentifier(genNodeIdentifier());
      }
      logger.debug("Send identifier {} to the leader", thisNode.getNodeIdentifier());
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
    router = new ClusterPlanRouter(partitionTable);

    allNodes = new ArrayList<>(partitionTable.getAllNodes());
    initPeerMap();
    logger.info("Received cluster nodes from the leader: {}", allNodes);
    initIdNodeMap();
    for (Node n : allNodes) {
      idNodeMap.put(n.getNodeIdentifier(), n);
    }
    try {
      syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      logger.error("check consistency failed when accept partition table: {}", e.getMessage());
    }

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
  public void processValidHeartbeatResp(HeartBeatResponse response, Node receiver) {
    // register the id of the node
    if (response.isSetFollowerIdentifier()) {
      registerNodeIdentifier(receiver, response.getFollowerIdentifier());
      // if all nodes' ids are known, we can build the partition table
      if (allNodesIdKnown()) {
        if (partitionTable == null) {
          partitionTable = new SlotPartitionTable(allNodes, thisNode);
          logger.info("Partition table is set up");
        }
        router = new ClusterPlanRouter(partitionTable);
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
      Node conflictNode = idNodeMap.get(identifier);
      if (conflictNode != null && !conflictNode.equals(node)) {
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
    logger.info("Starting sub-servers...");
    synchronized (partitionTable) {
      try {
        initSubServers();
        getDataClusterServer().buildDataGroupMembers(partitionTable);
      } catch (TTransportException | StartupException e) {
        logger.error("Build partition table failed: ", e);
        stop();
      }
    }
    logger.info("Sub-servers started.");
  }

  /**
   * Process the join cluster request of "node". Only proceed when the partition table is ready.
   *
   * @param node cannot be the local node
   */
  public AddNodeResponse addNode(Node node, StartUpStatus startUpStatus)
      throws AddSelfException, LogExecutionException {
    AddNodeResponse response = new AddNodeResponse();
    if (partitionTable == null) {
      logger.info("Cannot add node now because the partition table is not set");
      response.setRespNum((int) Response.RESPONSE_PARTITION_TABLE_UNAVAILABLE);
      return response;
    }

    logger.info("A node {} wants to join this cluster", node);
    if (node.equals(thisNode)) {
      throw new AddSelfException();
    }

    waitLeader();
    // try to process the request locally, if it cannot be processed locally, forward it
    if (processAddNodeLocally(node, startUpStatus, response)) {
      return response;
    }
    return null;
  }

  /**
   * Process the join cluster request of "node" as a MetaLeader. A node already joined is accepted
   * immediately. If the identifier of "node" conflicts with an existing node, the request will be
   * turned down.
   *
   * @param node          cannot be the local node
   * @param startUpStatus the start up status of the new node
   * @param response      the response that will be sent to "node"
   * @return true if the process is over, false if the request should be forwarded
   */
  private boolean processAddNodeLocally(Node node, StartUpStatus startUpStatus,
      AddNodeResponse response) throws LogExecutionException {
    if (character != NodeCharacter.LEADER) {
      return false;
    }
    if (allNodes.contains(node)) {
      logger.debug("Node {} is already in the cluster", node);
      response.setRespNum((int) Response.RESPONSE_AGREE);
      synchronized (partitionTable) {
        response.setPartitionTableBytes(partitionTable.serialize());
      }
      return true;
    }

    Node idConflictNode = idNodeMap.get(node.getNodeIdentifier());
    if (idConflictNode != null) {
      logger.debug("{}'s id conflicts with {}", node, idConflictNode);
      response.setRespNum((int) Response.RESPONSE_IDENTIFIER_CONFLICT);
      return true;
    }

    // check status of the new node
    if (!checkNodeConfig(startUpStatus, response)) {
      return true;
    }

    // node adding must be serialized to reduce potential concurrency problem
    synchronized (logManager) {
      AddNodeLog addNodeLog = new AddNodeLog();
      addNodeLog.setCurrLogTerm(getTerm().get());
      addNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      addNodeLog.setNewNode(node);

      logManager.append(addNodeLog);

      int retryTime = 1;
      while (true) {
        logger
            .info("Send the join request of {} to other nodes, retry time: {}", node, retryTime);
        AppendLogResult result = sendLogToAllGroups(addNodeLog);
        switch (result) {
          case OK:
            logger.info("Join request of {} is accepted", node);
            logManager.commitTo(addNodeLog.getCurrLogIndex(), false);

            synchronized (partitionTable) {
              response.setPartitionTableBytes(partitionTable.serialize());
            }
            response.setRespNum((int) Response.RESPONSE_AGREE);
            logger.info("Sending join response of {}", node);
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

  private boolean checkNodeConfig(StartUpStatus remoteStartUpStatus, AddNodeResponse response) {
    long remotePartitionInterval = remoteStartUpStatus.getPartitionInterval();
    int remoteHashSalt = remoteStartUpStatus.getHashSalt();
    int remoteReplicationNum = remoteStartUpStatus.getReplicationNumber();
    List<Node> remoteSeedNodeList = remoteStartUpStatus.getSeedNodeList();
    long localPartitionInterval = IoTDBDescriptor.getInstance().getConfig()
        .getPartitionInterval();
    int localHashSalt = ClusterConstant.HASH_SALT;
    int localReplicationNum = ClusterDescriptor.getInstance().getConfig().getReplicationNum();
    boolean partitionIntervalEquals = true;
    boolean hashSaltEquals = true;
    boolean replicationNumEquals = true;
    boolean seedNodeEquals = true;

    if (localPartitionInterval != remotePartitionInterval) {
      partitionIntervalEquals = false;
      logger.info("Remote partition interval conflicts with the leader's. Leader: {}, remote: {}",
          localPartitionInterval, remotePartitionInterval);
    }
    if (localHashSalt != remoteHashSalt) {
      hashSaltEquals = false;
      logger.info("Remote hash salt conflicts with the leader's. Leader: {}, remote: {}",
          localHashSalt, remoteHashSalt);
    }
    if (localReplicationNum != remoteReplicationNum) {
      replicationNumEquals = false;
      logger.info("Remote replication number conflicts with the leader's. Leader: {}, remote: {}",
          localReplicationNum, remoteReplicationNum);
    }
    if (!ClusterUtils.checkSeedNodes(true, allNodes, remoteSeedNodeList)) {
      seedNodeEquals = false;
      if (logger.isInfoEnabled()) {
        logger.info("Remote seed node list conflicts with the leader's. Leader: {}, remote: {}",
            Arrays.toString(allNodes.toArray(new Node[0])), remoteSeedNodeList);
      }
    }
    if (!(partitionIntervalEquals && hashSaltEquals && replicationNumEquals && seedNodeEquals)) {
      response.setRespNum((int) Response.RESPONSE_NEW_NODE_PARAMETER_CONFLICT);
      response.setCheckStatusResponse(
          new CheckStatusResponse(partitionIntervalEquals, hashSaltEquals,
              replicationNumEquals, seedNodeEquals));
      return false;
    }
    return true;
  }

  /**
   * Check if the seed nodes are consistent with other nodes. Only used when establishing the
   * initial cluster.
   */
  private void checkSeedNodes() {
    boolean canEstablishCluster = false;
    long startTime = System.currentTimeMillis();
    AtomicInteger consistentNum = new AtomicInteger(1);
    AtomicInteger inconsistentNum = new AtomicInteger(0);
    while (!canEstablishCluster) {
      consistentNum.set(1);
      inconsistentNum.set(0);
      checkSeedNodesOnce(consistentNum, inconsistentNum);

      canEstablishCluster = analyseStartUpCheckResult(consistentNum.get(), inconsistentNum.get(),
          getAllNodes().size(), System.currentTimeMillis() - startTime);
    }
  }

  private void checkSeedNodesOnce(AtomicInteger consistentNum, AtomicInteger inconsistentNum) {
    ExecutorService pool = new ScheduledThreadPoolExecutor(STARTUP_CHECK_THREAD_POOL_SIZE);
    for (Node seedNode : getAllNodes()) {
      Node thisNode = getThisNode();
      if (seedNode.getIp().equals(thisNode.ip)
          && seedNode.getMetaPort() != thisNode.getMetaPort()) {
        continue;
      }

      pool.submit(() -> {
            CheckStatusResponse response = checkStatus(seedNode);
            if (response != null) {
              // check the response
              ClusterUtils.examineCheckStatusResponse(response, consistentNum, inconsistentNum);
            } else {
              logger.warn(
                  "Start up exception. Cannot connect to node {}. Try again in next turn.",
                  seedNode);
            }
          }
      );
    }
    pool.shutdown();
    try {
      pool.awaitTermination(WAIT_START_UP_CHECK_TIME, WAIT_START_UP_CHECK_TIME_UNIT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for start up checks", e);
    }
  }

  private CheckStatusResponse checkStatus(Node seedNode) {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncMetaClient client = (AsyncMetaClient) getAsyncClient(seedNode);
      try {
        return SyncClientAdaptor.checkStatus(client, getStartUpStatus());
      } catch (TException e) {
        logger.warn("Error occurs when check status on node : {}", seedNode);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Current thread is interrupted.");
      }
    } else {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(seedNode);
      try {
        return client.checkStatus(getStartUpStatus());
      } catch (TException e) {
        logger.warn("Error occurs when check status on node : {}", seedNode);
      } finally {
        putBackSyncClient(client);
      }
    }
    return null;
  }

  /**
   * Send the log the all data groups and return a success only when each group's quorum has
   * accepted this log.
   */
  private AppendLogResult sendLogToAllGroups(Log log) {
    List<Node> nodeRing = partitionTable.getAllNodes();

    AtomicLong newLeaderTerm = new AtomicLong(term.get());
    AtomicBoolean leaderShipStale = new AtomicBoolean(false);
    AppendEntryRequest request = new AppendEntryRequest();
    request.setTerm(term.get());
    request.setEntry(log.serialize());
    request.setLeader(getThisNode());
    request.setLeaderCommit(logManager.getCommitLogIndex());
    request.setPrevLogIndex(log.getCurrLogIndex() - 1);
    try {
      request.setPrevLogTerm(logManager.getTerm(log.getCurrLogIndex() - 1));
    } catch (Exception e) {
      logger.error("getTerm failed for newly append entries", e);
    }

    // ask for votes from each node
    int[] groupRemainings = askGroupVotes(nodeRing, request, leaderShipStale, log, newLeaderTerm);

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
   * counters of the groups it is in of "groupRemainings"
   *
   * @param nodeRing
   * @param request
   * @param leaderShipStale
   * @param log
   * @param newLeaderTerm
   * @return a int array indicating how many votes are left in each group to make an agreement
   */
  @SuppressWarnings({"java:S2445", "java:S2274"})
  // groupRemaining is shared with the handlers,
  // and we do not wait infinitely to enable timeouts
  private int[] askGroupVotes(List<Node> nodeRing,
      AppendEntryRequest request, AtomicBoolean leaderShipStale, Log log,
      AtomicLong newLeaderTerm) {
    // each node will be the header of a group, we use the node to represent the group
    int nodeSize = nodeRing.size();
    // the decreasing counters of how many nodes in a group has received the log, each time a
    // node receive the log, the counters of all groups it is in will decrease by 1
    int[] groupRemainings = new int[nodeSize];
    // each group is considered success if such members receive the log
    int groupQuorum = REPLICATION_NUM / 2 + 1;
    Arrays.fill(groupRemainings, groupQuorum);

    synchronized (groupRemainings) {
      // ask a vote from every node
      for (int i = 0; i < nodeSize; i++) {
        Node node = nodeRing.get(i);
        if (node.equals(thisNode)) {
          // node equals this node, decrease counters of all groups the local node is in
          for (int j = 0; j < REPLICATION_NUM; j++) {
            int nodeIndex = i - j;
            if (nodeIndex < 0) {
              nodeIndex += groupRemainings.length;
            }
            groupRemainings[nodeIndex]--;
          }
        } else {
          askRemoteGroupVote(node, groupRemainings, i, leaderShipStale, log, newLeaderTerm,
              request);
        }
      }

      try {
        groupRemainings.wait(RaftServer.getConnectionTimeoutInMS());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for the group votes", e);
      }
    }
    return groupRemainings;
  }

  private void askRemoteGroupVote(Node node, int[] groupRemainings, int nodeIndex,
      AtomicBoolean leaderShipStale, Log log,
      AtomicLong newLeaderTerm, AppendEntryRequest request) {
    AppendGroupEntryHandler handler = new AppendGroupEntryHandler(groupRemainings,
        nodeIndex, node, leaderShipStale, log, newLeaderTerm, this);
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncMetaClient client = (AsyncMetaClient) getAsyncClient(node);
      try {
        client.appendEntry(request, handler);
      } catch (TException e) {
        logger.error("Cannot send log to node {}", node, e);
      }
    } else {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(node);
      getAsyncThreadPool().submit(() -> {
        try {
          handler.onComplete(client.appendEntry(request));
        } catch (TException e) {
          handler.onError(e);
        } finally {
          putBackSyncClient(client);
        }
      });
    }

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
   * Load the partition table from a local file if it can be found.
   */
  private void loadPartitionTable() {
    File partitionFile = new File(PARTITION_FILE_NAME);
    if (!partitionFile.exists()) {
      logger.info("No partition table file found");
      return;
    }
    initIdNodeMap();
    try (DataInputStream inputStream =
        new DataInputStream(new BufferedInputStream(new FileInputStream(partitionFile)))) {
      int size = inputStream.readInt();
      byte[] tableBuffer = new byte[size];
      int readCnt = inputStream.read(tableBuffer);
      if (readCnt < size) {
        throw new IOException(String.format("Expected partition table size: %s, actual read: %s",
            size, readCnt));
      }

      partitionTable = new SlotPartitionTable(thisNode);
      partitionTable.deserialize(ByteBuffer.wrap(tableBuffer));
      allNodes = new ArrayList<>(partitionTable.getAllNodes());
      initPeerMap();
      for (Node node : allNodes) {
        idNodeMap.put(node.getNodeIdentifier(), node);
      }
      router = new ClusterPlanRouter(partitionTable);
      startSubServers();

      logger.info("Load {} nodes: {}", allNodes.size(), allNodes);
    } catch (IOException e) {
      logger.error("Cannot load the partition table", e);
    }
  }

  /**
   * Serialize the partition table to a fixed position on the disk. Will first serialize to a
   * temporary file and than replace the old file.
   */
  private synchronized void savePartitionTable() {
    File tempFile = new File(PARTITION_FILE_NAME + TEMP_SUFFIX);
    tempFile.getParentFile().mkdirs();
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
    if (oldFile.exists()) {
      try {
        Files.delete(Paths.get(oldFile.getAbsolutePath()));
      } catch (IOException e) {
        logger.warn("Old partition table file is not successfully deleted", e);
      }
    }

    if (!tempFile.renameTo(oldFile)) {
      logger.warn("New partition table file is not successfully renamed");
    }
    logger.info("Partition table is saved");
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
        logger.info("Recovered node identifier {}", nodeId);
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
    File idFile = new File(NODE_IDENTIFIER_FILE_NAME);
    idFile.getParentFile().mkdirs();
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(idFile))) {
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
   */
  public void sendSnapshot(SendSnapshotRequest request) {
    MetaSimpleSnapshot snapshot = new MetaSimpleSnapshot();
    snapshot.deserialize(request.snapshotBytes);
    applySnapshot(snapshot);
  }

  /**
   * Apply a meta snapshot to IoTDB. The snapshot contains: all storage groups, logs of node
   * addition and removal, and last log term/index in the snapshot.
   *
   * @param snapshot
   */
  private void applySnapshot(MetaSimpleSnapshot snapshot) {
    synchronized (logManager) {
      // 0. first delete all storage groups
      try {
        IoTDB.metaManager
            .deleteStorageGroups(IoTDB.metaManager.getAllStorageGroupNames());
      } catch (MetadataException e) {
        logger.error("{}: first delete all local storage groups failed, errMessage:{}",
            name,
            e.getMessage());
      }

      // 2.  register all storage groups
      for (Map.Entry<String, Long> entry : snapshot.getStorageGroupTTLMap().entrySet()) {
        try {
          IoTDB.metaManager.setStorageGroup(entry.getKey());
        } catch (MetadataException e) {
          logger.error("{}: Cannot add storage group {} in snapshot, errMessage:{}", name,
              entry.getKey(),
              e.getMessage());
        }

        // 3. register ttl in the snapshot
        try {
          IoTDB.metaManager.setTTL(entry.getKey(), entry.getValue());
          StorageEngine.getInstance().setTTL(entry.getKey(), entry.getValue());
        } catch (MetadataException | StorageEngineException | IOException e) {
          logger
              .error("{}: Cannot set ttl in storage group {} , errMessage: {}", name,
                  entry.getKey(),
                  e.getMessage());
        }
      }

      // 4. replace all users and roles
      try {
        IAuthorizer authorizer = BasicAuthorizer.getInstance();
        applySnapshotUsers(authorizer, snapshot);
        applySnapshotRoles(authorizer, snapshot);
      } catch (AuthException e) {
        logger.error("{}: Cannot get authorizer instance, error is: ", name, e);
      }

      // 5. accept partition table
      acceptPartitionTable(snapshot.getPartitionTableBuffer());

      logManager.applyingSnapshot(snapshot);
    }
  }

  private void applySnapshotUsers(IAuthorizer authorizer, MetaSimpleSnapshot snapshot) {
    try {
      authorizer.replaceAllUsers(snapshot.getUserMap());
    } catch (AuthException e) {
      logger.error("{}:replace users failed", name, e);
    }
  }

  private void applySnapshotRoles(IAuthorizer authorizer, MetaSimpleSnapshot snapshot) {
    try {
      authorizer.replaceAllRoles(snapshot.getRoleMap());
    } catch (AuthException e) {
      logger.error("{}:replace roles failed", name, e);
    }
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
    if (PartitionUtils.isLocalNonQueryPlan(plan)) { // run locally
      return executeNonQueryLocally(plan);
    } else if (PartitionUtils.isGlobalMetaPlan(plan)) { //forward the plan to all meta group nodes
      return processNonPartitionedMetaPlan(plan);
    } else if (PartitionUtils.isGlobalDataPlan(plan)) { //forward the plan to all data group nodes
      return processNonPartitionedDataPlan(plan);
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

  protected TSStatus executeNonQueryLocally(PhysicalPlan plan) {
    boolean execRet;
    try {
      execRet = getLocalExecutor().processNonQuery(plan);
    } catch (QueryProcessException e) {
      logger.debug("meet error while processing non-query. ", e);
      return RpcUtils.getStatus(e.getErrorCode(), e.getMessage());
    } catch (Exception e) {
      logger.error("{}: server Internal Error: ", IoTDBConstant.GLOBAL_DB_NAME, e);
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    return execRet
        ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
        : RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
  }


  /**
   * A non-partitioned plan (like storage group creation) should be executed on all metagroup nodes,
   * so the MetaLeader should take the responsible to make sure that every node receives the plan.
   * Thus the plan will be processed locally only by the MetaLeader and forwarded by non-leader
   * nodes.
   *
   * @param plan
   * @return
   */
  private TSStatus processNonPartitionedMetaPlan(PhysicalPlan plan) {
    if (character == NodeCharacter.LEADER) {
      TSStatus status = processPlanLocally(plan);
      if (status != null) {
        return status;
      }
    } else if (leader != null) {
      return forwardPlan(plan, leader, null);
    }

    waitLeader();
    // the leader can be itself after waiting
    if (character == NodeCharacter.LEADER) {
      TSStatus status = processPlanLocally(plan);
      if (status != null) {
        return status;
      }
    }
    return forwardPlan(plan, leader, null);
  }

  /**
   * A non-partitioned plan (like DeleteData) should be executed on all data group nodes, so the
   * DataGroupLeader should take the responsible to make sure that every node receives the plan.
   * Thus the plan will be processed locally only by the DataGroupLeader and forwarded by non-leader
   * nodes.
   *
   * @param plan
   * @return
   */
  private TSStatus processNonPartitionedDataPlan(PhysicalPlan plan) {
    if (plan instanceof DeleteTimeSeriesPlan) {
      try {
        plan = getDeleteTimeseriesPlanWithFullPaths((DeleteTimeSeriesPlan) plan);
      } catch (PathNotExistException e) {
        TSStatus tsStatus = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
        tsStatus.setMessage(e.getMessage());
        return tsStatus;
      }
    }
    try {
      syncLeaderWithConsistencyCheck();
      List<PartitionGroup> globalGroups = partitionTable.getGlobalGroups();
      logger.debug("Forwarding global data plan {} to {} groups", plan, globalGroups.size());
      return forwardPlan(globalGroups, plan);
    } catch (CheckConsistencyException e) {
      logger.debug("Forwarding global data plan {} to meta leader {}", plan, leader);
      waitLeader();
      return forwardPlan(plan, leader, null);
    }
  }

  DeleteTimeSeriesPlan getDeleteTimeseriesPlanWithFullPaths(DeleteTimeSeriesPlan plan)
      throws PathNotExistException {
    Pair<List<String>, List<String>> getMatchedPathsRet = getMatchedPaths(plan.getPathsStrings());
    List<String> fullPathsStrings = getMatchedPathsRet.left;
    List<String> nonExistPathsStrings = getMatchedPathsRet.right;
    if (!nonExistPathsStrings.isEmpty()) {
      throw new PathNotExistException(new ArrayList<>(nonExistPathsStrings));
    }
    List<Path> fullPaths = new ArrayList<>();
    for (String pathStr : fullPathsStrings) {
      fullPaths.add(new Path(pathStr));
    }
    plan.setPaths(fullPaths);
    return plan;
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
    Map<PhysicalPlan, PartitionGroup> planGroupMap;
    try {
      planGroupMap = splitPlan(plan);
    } catch (CheckConsistencyException checkConsistencyException) {
      TSStatus status = StatusUtils.CONSISTENCY_FAILURE.deepCopy();
      status.setMessage(checkConsistencyException.getMessage());
      return status;
    }

    // the storage group is not found locally
    if (planGroupMap == null || planGroupMap.isEmpty()) {
      if ((plan instanceof InsertPlan || plan instanceof CreateTimeSeriesPlan)
          && ClusterDescriptor.getInstance().getConfig().isEnableAutoCreateSchema()) {
        try {
          autoCreateSchema(plan);
          return executeNonQuery(plan);
        } catch (MetadataException e) {
          logger.error(
              String.format("Failed to set storage group or create timeseries, because %s", e));
        }
      }
      logger.error("{}: Cannot found storage groups for {}", name, plan);
      return StatusUtils.NO_STORAGE_GROUP;
    }
    logger.debug("{}: The data groups of {} are {}", name, plan, planGroupMap);
    return forwardPlan(planGroupMap, plan);
  }

  private Map<PhysicalPlan, PartitionGroup> splitPlan(PhysicalPlan plan)
      throws UnsupportedPlanException, CheckConsistencyException {
    Map<PhysicalPlan, PartitionGroup> planGroupMap = null;
    try {
      planGroupMap = router.splitAndRoutePlan(plan);
    } catch (StorageGroupNotSetException e) {
      syncLeaderWithConsistencyCheck();
      try {
        planGroupMap = router.splitAndRoutePlan(plan);
      } catch (MetadataException ex) {
        // ignore
      }
    } catch (MetadataException e) {
      logger.error("Cannot route plan {}", plan, e);
    }
    return planGroupMap;
  }

  private void autoCreateSchema(PhysicalPlan plan) throws MetadataException {
    // try to set storage group
    String deviceId;
    if (plan instanceof InsertPlan) {
      deviceId = ((InsertPlan) plan).getDeviceId();
    } else {
      deviceId = ((CreateTimeSeriesPlan) plan).getPath().toString();
    }

    String storageGroupName = MetaUtils
        .getStorageGroupNameByLevel(deviceId, IoTDBDescriptor.getInstance()
            .getConfig().getDefaultStorageGroupLevel());
    SetStorageGroupPlan setStorageGroupPlan = new SetStorageGroupPlan(
        new Path(storageGroupName));
    TSStatus setStorageGroupResult = processNonPartitionedMetaPlan(setStorageGroupPlan);
    if (setStorageGroupResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode() &&
        setStorageGroupResult.getCode() != TSStatusCode.PATH_ALREADY_EXIST_ERROR
            .getStatusCode()) {
      throw new MetadataException(
          String.format("Status Code: %d, failed to set storage group %s",
              setStorageGroupResult.getCode(), storageGroupName)
      );
    }
    if (plan instanceof InsertPlan) {
      // try to create timeseries
      boolean isAutoCreateTimeseriesSuccess = autoCreateTimeseries((InsertPlan) plan);
      if (!isAutoCreateTimeseriesSuccess) {
        throw new MetadataException(
            "Failed to create timeseries from InsertPlan automatically."
        );
      }
    }
  }

  /**
   * Forward plans to the DataGroupMember of one node in the corresponding group. Only when all
   * nodes time out, will a TIME_OUT be returned.
   *
   * @param planGroupMap
   * @return
   */
  TSStatus forwardPlan(Map<PhysicalPlan, PartitionGroup> planGroupMap, PhysicalPlan plan) {
//    InsertRowPlan backup = null;
//    if (plan instanceof InsertRowPlan) {
//      backup = (InsertRowPlan) ((InsertRowPlan) plan).clone();
//    }
    // the error codes from the groups that cannot execute the plan
    TSStatus status;
    if (planGroupMap.size() == 1) {
      status = forwardToSingleGroup(planGroupMap.entrySet().iterator().next());
    } else {
      if (plan instanceof InsertTabletPlan) {
        status = forwardInsertTabletPlan(planGroupMap, (InsertTabletPlan) plan);
      } else {
        status = forwardToMultipleGroup(planGroupMap);
      }
    }
    if (plan instanceof InsertPlan
        && status.getCode() == TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode()
        && ClusterDescriptor.getInstance().getConfig().isEnableAutoCreateSchema()) {
      // try to create timeseries
      if (((InsertPlan) plan).getFailedMeasurements() != null) {
        ((InsertPlan) plan).transform();
      }
      boolean hasCreate = autoCreateTimeseries((InsertPlan) plan);
      if (hasCreate) {
        status = forwardPlan(planGroupMap, plan);
      } else {
        logger.error("{}, Cannot auto create timeseries.", thisNode);
      }
    }
    logger.debug("{}: executed {} with answer {}", name, plan, status);
    return status;
  }

  private TSStatus forwardInsertTabletPlan(Map<PhysicalPlan, PartitionGroup> planGroupMap,
      InsertTabletPlan plan) {
    List<String> errorCodePartitionGroups = new ArrayList<>();
    TSStatus tmpStatus;
    TSStatus[] subStatus = null;
    boolean noFailure = true;
    boolean isBatchFailure = false;
    for (Map.Entry<PhysicalPlan, PartitionGroup> entry : planGroupMap.entrySet()) {
      tmpStatus = forwardToSingleGroup(entry);
      logger.debug("{}: from {},{},{}", name, entry.getKey(), entry.getValue(), tmpStatus);
      noFailure =
          (tmpStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) && noFailure;
      isBatchFailure = (tmpStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode())
          || isBatchFailure;
      if (tmpStatus.getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        if (subStatus == null) {
          subStatus = new TSStatus[plan.getRowCount()];
          Arrays.fill(subStatus, RpcUtils.SUCCESS_STATUS);
        }
        PartitionUtils.reordering((InsertTabletPlan) entry.getKey(), subStatus,
            tmpStatus.subStatus.toArray(new TSStatus[]{}));
      }
      if (tmpStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // execution failed, record the error message
        errorCodePartitionGroups.add(String.format("[%s@%s:%s:%s]",
            tmpStatus.getCode(), entry.getValue().getHeader(),
            tmpStatus.getMessage(), tmpStatus.subStatus));
      }
    }
    TSStatus status;
    if (noFailure) {
      status = StatusUtils.OK;
    } else if (isBatchFailure) {
      //noinspection ConstantConditions, subStatus is never null in this case
      status = RpcUtils.getStatus(Arrays.asList(subStatus));
    } else {
      status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
      status.setMessage(MSG_MULTIPLE_ERROR + errorCodePartitionGroups.toString());
    }
    return status;
  }

  private TSStatus forwardToSingleGroup(Map.Entry<PhysicalPlan, PartitionGroup> entry) {
    if (entry.getValue().contains(thisNode)) {
      // the query should be handled by a group the local node is in, handle it with in the group
      logger.debug("Execute {} in a local group of {}", entry.getKey(),
          entry.getValue().getHeader());
      return getLocalDataMember(entry.getValue().getHeader())
          .executeNonQuery(entry.getKey());
    } else {
      // forward the query to the group that should handle it
      logger.debug("Forward {} to a remote group of {}", entry.getKey(),
          entry.getValue().getHeader());
      return forwardPlan(entry.getKey(), entry.getValue());
    }
  }

  private TSStatus forwardToMultipleGroup(Map<PhysicalPlan, PartitionGroup> planGroupMap) {
    List<String> errorCodePartitionGroups = new ArrayList<>();
    TSStatus tmpStatus;
    for (Map.Entry<PhysicalPlan, PartitionGroup> entry : planGroupMap.entrySet()) {
      tmpStatus = forwardToSingleGroup(entry);
      if (tmpStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // execution failed, record the error message
        errorCodePartitionGroups.add(String.format("[%s@%s:%s]",
            tmpStatus.getCode(), entry.getValue().getHeader(),
            tmpStatus.getMessage()));
      }
    }
    TSStatus status;
    if (errorCodePartitionGroups.isEmpty()) {
      status = StatusUtils.OK;
    } else {
      status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
      status.setMessage(MSG_MULTIPLE_ERROR + errorCodePartitionGroups.toString());
    }
    return status;
  }

  /**
   * Forward plans to all DataGroupMember groups. Only when all nodes time out, will a TIME_OUT be
   * returned.
   *
   * @param partitionGroups
   * @return
   * @para plan
   */
  TSStatus forwardPlan(List<PartitionGroup> partitionGroups, PhysicalPlan plan) {
    // the error codes from the groups that cannot execute the plan
    TSStatus status;
    List<String> errorCodePartitionGroups = new ArrayList<>();
    for (PartitionGroup partitionGroup : partitionGroups) {
      if (partitionGroup.contains(thisNode)) {
        // the query should be handled by a group the local node is in, handle it with in the group
        logger.debug("Execute {} in a local group of {}", plan, partitionGroup.getHeader());
        status = getLocalDataMember(partitionGroup.getHeader())
            .executeNonQuery(plan);
      } else {
        // forward the query to the group that should handle it
        logger.debug("Forward {} to a remote group of {}", plan,
            partitionGroup.getHeader());
        status = forwardPlan(plan, partitionGroup);
      }
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        // execution failed, record the error message
        errorCodePartitionGroups.add(String.format("[%s@%s:%s]",
            status.getCode(), partitionGroup.getHeader(),
            status.getMessage()));
      }
    }
    if (errorCodePartitionGroups.isEmpty()) {
      status = StatusUtils.OK;
    } else {
      status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
      status.setMessage(MSG_MULTIPLE_ERROR + errorCodePartitionGroups.toString());
    }
    logger.debug("{}: executed {} with answer {}", name, plan, status);
    return status;
  }

  /**
   * Create timeseries automatically
   *
   * @param insertPlan, some of the timeseries in it are not created yet
   * @return true of all uncreated timeseries are created
   */
  boolean autoCreateTimeseries(InsertPlan insertPlan) {
    List<String> seriesList = new ArrayList<>();
    String deviceId = insertPlan.getDeviceId();
    String storageGroupName;
    try {
      storageGroupName = MetaUtils
          .getStorageGroupNameByLevel(deviceId, IoTDBDescriptor.getInstance()
              .getConfig().getDefaultStorageGroupLevel());
    } catch (MetadataException e) {
      logger.error("Failed to infer storage group from deviceId {}", deviceId);
      return false;
    }
    for (String measurementId : insertPlan.getMeasurements()) {
      seriesList.add(
          new StringContainer(new String[]{deviceId, measurementId}, TsFileConstant.PATH_SEPARATOR)
              .toString());
    }
    PartitionGroup partitionGroup = partitionTable.route(storageGroupName, 0);
    List<String> unregisteredSeriesList = getUnregisteredSeriesList(seriesList, partitionGroup);
    for (String seriesPath : unregisteredSeriesList) {
      int index = seriesList.indexOf(seriesPath);
      TSDataType dataType = insertPlan.getDataTypes() != null
          ? insertPlan.getDataTypes()[index]
          : TypeInferenceUtils.getPredictedDataType(insertPlan instanceof InsertTabletPlan
              ? Array.get(((InsertTabletPlan) insertPlan).getColumns()[index], 0)
              : ((InsertRowPlan) insertPlan).getValues()[index], true);
      TSEncoding encoding = getDefaultEncoding(dataType);
      CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan(new Path(seriesPath),
          dataType, encoding, compressionType, null, null, null, null);
      // TODO-Cluster: add executeNonQueryBatch()
      TSStatus result;
      try {
        result = processPartitionedPlan(createTimeSeriesPlan);
      } catch (UnsupportedPlanException e) {
        logger.error("Failed to create timeseries {} automatically. Unsupported plan exception {} ",
            seriesPath, e.getMessage());
        return false;
      }
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        logger.error("{} failed to execute create timeseries {}", thisNode, seriesPath);
        return false;
      }
    }
    return true;
  }


  /**
   * To check which timeseries in the input list is unregistered
   *
   * @param seriesList
   * @param partitionGroup
   * @return
   */
  List<String> getUnregisteredSeriesList(List<String> seriesList, PartitionGroup partitionGroup) {
    List<String> unregistered = new ArrayList<>();
    for (Node node : partitionGroup) {
      try {
        List<String> result;
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          AsyncDataClient client = getAsyncDataClient(node);
          result = SyncClientAdaptor
              .getUnregisteredMeasurements(client, partitionGroup.getHeader(), seriesList);
        } else {
          SyncDataClient syncDataClient = getSyncDataClient(node);
          result = syncDataClient.getUnregisteredTimeseries(partitionGroup.getHeader(), seriesList);
          putBackSyncClient(syncDataClient);
        }

        if (result != null) {
          unregistered.addAll(result);
          break;
        }
      } catch (TException | IOException e) {
        logger.error("{}: cannot getting unregistered {} and other {} paths from {}", name,
            seriesList.get(0), seriesList.get(seriesList.size() - 1), node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("{}: getting unregistered series list {} ... {} is interrupted from {}", name,
            seriesList.get(0), seriesList.get(seriesList.size() - 1), node, e);
      }
    }
    return new ArrayList<>(unregistered);
  }

  /**
   * Forward a plan to the DataGroupMember of one node in the group. Only when all nodes time out,
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
        if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
          status = forwardDataPlanAsync(plan, node, group.getHeader());
        } else {
          status = forwardDataPlanSync(plan, node, group.getHeader());
        }
      } catch (IOException e) {
        status = StatusUtils.EXECUTE_STATEMENT_ERROR.deepCopy();
        status.setMessage(e.getMessage());
      }
      if (!StatusUtils.TIME_OUT.equals(status)) {
        return status;
      } else {
        logger.warn("Forward {} to {} timed out", plan, node);
      }
    }
    logger.warn("Forward {} to {} timed out", plan, group);
    return StatusUtils.TIME_OUT;
  }

  /**
   * Forward a non-query plan to "receiver" using "client".
   *
   * @param plan     a non-query plan
   * @param receiver
   * @param header   to determine which DataGroupMember of "receiver" will process the request.
   * @return a TSStatus indicating if the forwarding is successful.
   */
  TSStatus forwardDataPlanAsync(PhysicalPlan plan, Node receiver, Node header) throws IOException {
    RaftService.AsyncClient client = getAsyncDataClient(receiver);
    try {
      TSStatus tsStatus = SyncClientAdaptor.executeNonQuery(client, plan, header, receiver);
      if (tsStatus == null) {
        tsStatus = StatusUtils.TIME_OUT;
        logger.warn("{}: Forward {} to {} time out", name, plan, receiver);
      }
      return tsStatus;
    } catch (IOException | TException e) {
      TSStatus status = StatusUtils.INTERNAL_ERROR.deepCopy();
      status.setMessage(e.getMessage());
      logger
          .error("{}: encountered an error when forwarding {} to {}", name, plan, receiver, e);
      return status;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("{}: forward {} to {} interrupted", name, plan, receiver);
      return StatusUtils.TIME_OUT;
    }
  }

  TSStatus forwardDataPlanSync(PhysicalPlan plan, Node receiver, Node header) throws IOException {
    Client client = getSyncDataClient(receiver);
    try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      plan.serialize(dataOutputStream);

      ExecutNonQueryReq req = new ExecutNonQueryReq();
      req.setPlanBytes(byteArrayOutputStream.toByteArray());
      if (header != null) {
        req.setHeader(header);
      }

      TSStatus tsStatus = client.executeNonQueryPlan(req);
      if (tsStatus == null) {
        tsStatus = StatusUtils.TIME_OUT;
        logger.warn("{}: Forward {} to {} time out", name, plan, receiver);
      }
      return tsStatus;
    } catch (IOException e) {
      TSStatus status = StatusUtils.INTERNAL_ERROR.deepCopy();
      status.setMessage(e.getMessage());
      logger
          .error("{}: encountered an error when forwarding {} to {}", name, plan, receiver, e);
      return status;
    } catch (TException e) {
      TSStatus status;
      if (e.getCause() instanceof SocketTimeoutException) {
        status = StatusUtils.TIME_OUT;
        logger.warn("{}: Forward {} to {} time out", name, plan, receiver);
      } else {
        status = StatusUtils.INTERNAL_ERROR.deepCopy();
        status.setMessage(e.getMessage());
        logger
            .error("{}: encountered an error when forwarding {} to {}", name, plan, receiver, e);
      }
      client.getInputProtocol().getTransport().close();
      return status;
    } finally {
      putBackSyncClient(client);
    }
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

        try {
          syncLeaderWithConsistencyCheck();
        } catch (CheckConsistencyException checkConsistencyException) {
          throw new MetadataException(checkConsistencyException.getMessage());
        }
        partitionGroup = partitionTable.partitionByPathTime(prefixPath, 0);

      }
      partitionGroupPathMap.computeIfAbsent(partitionGroup, g -> new ArrayList<>()).add(prefixPath);
    }

    List<MeasurementSchema> schemas = new ArrayList<>();
    // pull timeseries schema from every group involved
    if (logger.isDebugEnabled()) {
      logger.debug("{}: pulling schemas of {} and other {} paths from {} groups", name,
          prefixPaths.get(0), prefixPaths.size() - 1,
          partitionGroupPathMap.size());
    }
    for (Entry<PartitionGroup, List<String>> partitionGroupListEntry : partitionGroupPathMap
        .entrySet()) {
      PartitionGroup partitionGroup = partitionGroupListEntry.getKey();
      List<String> paths = partitionGroupListEntry.getValue();
      pullTimeSeriesSchemas(partitionGroup, paths, schemas);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: pulled {} schemas for {} and other {} paths", name, schemas.size(),
          prefixPaths.get(0), prefixPaths.size() - 1);
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
      int preSize = results.size();
      for (String prefixPath : prefixPaths) {
        IoTDB.metaManager.collectSeries(prefixPath, results);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Pulled {} timeseries schemas of {} and other {} paths from local", name,
            results.size() - preSize, prefixPaths.get(0), prefixPaths.size() - 1);
      }
      return;
    }

    // pull schemas from a remote node
    PullSchemaRequest pullSchemaRequest = new PullSchemaRequest();
    pullSchemaRequest.setHeader(partitionGroup.getHeader());
    pullSchemaRequest.setPrefixPaths(prefixPaths);

    for (Node node : partitionGroup) {
      if (pullTimeSeriesSchemas(node, pullSchemaRequest, results)) {
        break;
      }
    }
  }

  private boolean pullTimeSeriesSchemas(Node node,
      PullSchemaRequest request, List<MeasurementSchema> results) {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Pulling timeseries schemas of {} and other {} paths from {}", name,
          request.getPrefixPaths().get(0), request.getPrefixPaths().size() - 1, node);
    }

    List<MeasurementSchema> schemas = null;
    try {
      schemas = pullTimeSeriesSchemas(node, request);
    } catch (IOException | TException e) {
      logger
          .error("{}: Cannot pull timeseries schemas of {} and other {} paths from {}", name,
              request.getPrefixPaths().get(0), request.getPrefixPaths().size() - 1, node, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger
          .error("{}: Cannot pull timeseries schemas of {} and other {} paths from {}", name,
              request.getPrefixPaths().get(0), request.getPrefixPaths().size() - 1, node, e);
    }

    if (schemas != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("{}: Pulled {} timeseries schemas of {} and other {} paths from {} of {}",
            name,
            schemas.size(), request.getPrefixPaths().get(0), request.getPrefixPaths().size() - 1,
            node,
            request.getHeader());
      }
      results.addAll(schemas);
      return true;
    }
    return false;
  }

  private List<MeasurementSchema> pullTimeSeriesSchemas(Node node,
      PullSchemaRequest request) throws TException, InterruptedException, IOException {
    List<MeasurementSchema> schemas;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = getAsyncDataClient(node);
      schemas = SyncClientAdaptor.pullTimeSeriesSchema(client, request);
    } else {
      SyncDataClient syncDataClient = getSyncDataClient(node);
      PullSchemaResp pullSchemaResp = syncDataClient.pullTimeSeriesSchema(request);
      ByteBuffer buffer = pullSchemaResp.schemaBytes;
      int size = buffer.getInt();
      schemas = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        schemas.add(MeasurementSchema.deserializeFrom(buffer));
      }
    }

    return schemas;
  }

  /**
   * Get the data types of "paths". If "aggregations" is not null, each one of it correspond to one
   * in "paths". First get types locally and if some paths does not exists, pull them from other
   * nodes.
   *
   * @param paths
   * @param aggregations nullable, when not null, correspond to "paths" one-to-one.
   * @return
   * @throws MetadataException
   */
  public Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByPath(List<Path> paths,
      List<String> aggregations) throws
      MetadataException {
    try {
      return getSeriesTypesByPathLocally(paths, aggregations);
    } catch (PathNotExistException e) {
      Pair<List<TSDataType>, List<TSDataType>> types = getSeriesTypesByPathRemotely(
          paths, aggregations);
      if (types == null) {
        throw e;
      }
      return types;
    }
  }

  private Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByPathLocally(List<Path> paths,
      List<String> aggregations) throws MetadataException {
    // try locally first
    List<TSDataType> measurementDataTypes = SchemaUtils.getSeriesTypesByPath(paths,
        (List<String>) null);
    // if the aggregation function is null, the type of column in result set
    // is equal to the real type of the measurement
    if (aggregations == null) {
      return new Pair<>(measurementDataTypes, measurementDataTypes);
    } else {
      // if the aggregation function is not null,
      // we should recalculate the type of column in result set
      List<TSDataType> columnDataTypes = SchemaUtils.getSeriesTypesByPath(paths, aggregations);
      return new Pair<>(columnDataTypes, measurementDataTypes);
    }
  }

  private Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByPathRemotely(List<Path> paths,
      List<String> aggregations) throws MetadataException {
    List<String> pathStr = new ArrayList<>();
    for (Path path : paths) {
      pathStr.add(path.getFullPath());
    }
    // pull schemas remotely
    List<MeasurementSchema> schemas = pullTimeSeriesSchemas(pathStr);
    if (schemas.isEmpty()) {
      // if timeseries cannot be found remotely, too, it does not exist
      return null;
    }

    // consider the aggregations to get the real data type
    List<TSDataType> columnType = new ArrayList<>();
    List<TSDataType> measurementType = new ArrayList<>();
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
        columnType.add(schema.getType());
      } else {
        columnType.add(dataType);
      }
      measurementType.add(schemas.get(i).getType());
    }
    return new Pair<>(columnType, measurementType);
  }

  /**
   * Get the data types of "paths". If "aggregation" is not null, every path will use the
   * aggregation. First get types locally and if some paths does not exists, pull them from other
   * nodes.
   *
   * @param pathStrs
   * @param aggregation
   * @return
   * @throws MetadataException
   */
  public Pair<List<TSDataType>, List<TSDataType>> getSeriesTypesByString(List<String> pathStrs,
      String aggregation) throws
      MetadataException {
    try {
      // try locally first
      List<TSDataType> measurementDataTypes = SchemaUtils.getSeriesTypesByString(pathStrs, null);
      // if the aggregation function is null, the type of column in result set
      // is equal to the real type of the measurement
      if (aggregation == null) {
        return new Pair<>(measurementDataTypes, measurementDataTypes);
      } else {
        // if the aggregation function is not null,
        // we should recalculate the type of column in result set
        List<TSDataType> columnDataTypes = SchemaUtils
            .getSeriesTypesByString(pathStrs, aggregation);
        return new Pair<>(columnDataTypes, measurementDataTypes);
      }
    } catch (PathNotExistException e) {
      // pull schemas remotely
      List<MeasurementSchema> schemas = pullTimeSeriesSchemas(pathStrs);
      if (schemas.isEmpty()) {
        // if one timeseries cannot be found remotely, too, it does not exist
        throw e;
      }

      // consider the aggregations to get the real data type
      List<TSDataType> columnType = new ArrayList<>();
      List<TSDataType> measurementType = new ArrayList<>();
      // aggregations like first/last value does not have fixed data types and will return a null
      TSDataType aggregationType = getAggregationType(aggregation);
      for (MeasurementSchema schema : schemas) {
        if (aggregationType == null) {
          columnType.add(schema.getType());
        } else {
          columnType.add(aggregationType);
        }
      }
      return new Pair<>(columnType, measurementType);
    }
  }

  /**
   * Create an IReaderByTimestamp that can read the data of "path" by timestamp in the whole
   * cluster. This will query every group and merge the result from them.
   *
   * @param path
   * @param dataType
   * @param context
   * @return
   * @throws StorageEngineException
   */
  public IReaderByTimestamp getReaderByTimestamp(Path path,
      Set<String> deviceMeasurements, TSDataType dataType,
      QueryContext context)
      throws StorageEngineException, QueryProcessException {
    // make sure the partition table is new
    try {
      syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new QueryProcessException(e.getMessage());
    }
    // get all data groups
    List<PartitionGroup> partitionGroups = routeFilter(null, path);
    logger.debug("{}: Sending query of {} to {} groups", name, path, partitionGroups.size());
    List<IReaderByTimestamp> readers = new ArrayList<>();
    for (PartitionGroup partitionGroup : partitionGroups) {
      // query each group to get a reader in that group
      readers.add(getSeriesReaderByTime(partitionGroup, path, deviceMeasurements, context,
          dataType));
    }
    // merge the readers
    return new MergedReaderByTime(readers);
  }

  /**
   * Create a IReaderByTimestamp that read data of "path" by timestamp in the given group. If the
   * local node is a member of that group, query locally. Otherwise create a remote reader pointing
   * to one node in that group.
   *
   * @param partitionGroup
   * @param path
   * @param deviceMeasurements
   * @param context
   * @param dataType
   * @return
   * @throws StorageEngineException
   */
  private IReaderByTimestamp getSeriesReaderByTime(PartitionGroup partitionGroup, Path path,
      Set<String> deviceMeasurements, QueryContext context, TSDataType dataType)
      throws StorageEngineException, QueryProcessException {
    if (partitionGroup.contains(thisNode)) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember = getLocalDataMember(partitionGroup.getHeader());
      if (logger.isDebugEnabled()) {
        logger.debug("{}: creating a local reader for {}#{}", name, path.getFullPath(),
            context.getQueryId());
      }
      return dataGroupMember.getReaderByTimestamp(path, deviceMeasurements, dataType, context);
    } else {
      return getRemoteReaderByTimestamp(path, deviceMeasurements, dataType, partitionGroup,
          context);
    }
  }

  /**
   * Create a IReaderByTimestamp that read data of "path" by timestamp in the given group that does
   * not contain the local node. Send a request to one node in that group to build a reader and use
   * that reader's id to build a remote reader.
   *
   * @param path
   * @param deviceMeasurements
   * @param dataType
   * @param partitionGroup
   * @param context
   * @return
   * @throws StorageEngineException
   */
  private IReaderByTimestamp getRemoteReaderByTimestamp(
      Path path, Set<String> deviceMeasurements, TSDataType dataType,
      PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException {
    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    request.setPath(path.getFullPath());
    request.setHeader(partitionGroup.getHeader());
    request.setQueryId(context.getQueryId());
    request.setRequester(thisNode);
    request.setDataTypeOrdinal(dataType.ordinal());
    request.setDeviceMeasurements(deviceMeasurements);

    DataSourceInfo dataSourceInfo = new DataSourceInfo(partitionGroup, dataType, request,
        (RemoteQueryContext) context, this, partitionGroup);

    AsyncDataClient client = dataSourceInfo.nextDataClient(true, Long.MIN_VALUE);
    if (client != null) {
      return new RemoteSeriesReaderByTimestamp(dataSourceInfo);
    } else if (dataSourceInfo.isNoData()) {
      return new EmptyReader();
    }

    throw new StorageEngineException(
        new RequestTimeOutException("Query by timestamp: " + path + " in " + partitionGroup));
  }

  /**
   * Create a ManagedSeriesReader that can read the data of "path" with filters in the whole
   * cluster. The data groups that should be queried will be determined by the timeFilter, then for
   * each group a series reader will be created, and finally all such readers will be merged into
   * one.
   *
   * @param path
   * @param dataType
   * @param timeFilter  nullable, when null, all data groups will be queried
   * @param valueFilter nullable
   * @param context
   * @return
   * @throws StorageEngineException
   */
  public ManagedSeriesReader getSeriesReader(Path path,
      Set<String> deviceMeasurements, TSDataType dataType,
      Filter timeFilter,
      Filter valueFilter, QueryContext context)
      throws StorageEngineException {
    // make sure the partition table is new
    try {
      syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
    // find the groups that should be queried using the timeFilter
    List<PartitionGroup> partitionGroups = routeFilter(timeFilter, path);
    logger.debug("{}: Sending data query of {} to {} groups", name, path,
        partitionGroups.size());
    ManagedMergeReader mergeReader = new ManagedMergeReader(dataType);
    try {
      // build a reader for each group and merge them
      for (PartitionGroup partitionGroup : partitionGroups) {
        IPointReader seriesReader = getSeriesReader(partitionGroup, path,
            deviceMeasurements, timeFilter, valueFilter, context, dataType);
        if (seriesReader.hasNextTimeValuePair()) {
          // only add readers that have data, and they should basically not overlap with each
          // other (from different time partitions) so the priority does not matter
          mergeReader.addReader(seriesReader, 0);
        }
      }
    } catch (IOException | QueryProcessException e) {
      throw new StorageEngineException(e);
    }
    return mergeReader;
  }

  /**
   * Perform "aggregations" over "path" in some data groups and merge the results. The groups to be
   * queried is determined by "timeFilter".
   *
   * @param path
   * @param aggregations
   * @param dataType
   * @param timeFilter   nullable, when null, all groups will be queried
   * @param context
   * @return
   * @throws StorageEngineException
   */
  public List<AggregateResult> getAggregateResult(Path path,
      Set<String> deviceMeasurements, List<String> aggregations,
      TSDataType dataType, Filter timeFilter,
      QueryContext context) throws StorageEngineException {
    // make sure the partition table is new
    try {
      syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
    List<PartitionGroup> partitionGroups = routeFilter(timeFilter, path);
    logger.debug("{}: Sending aggregation query of {} to {} groups", name, path,
        partitionGroups.size());
    List<AggregateResult> results = null;
    // get the aggregation result of each group and merge them
    for (PartitionGroup partitionGroup : partitionGroups) {
      List<AggregateResult> groupResult = getAggregateResult(path, deviceMeasurements,
          aggregations, dataType,
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
   * Perform "aggregations" over "path" in "partitionGroup". If the local node is the member of the
   * group, do it locally, otherwise pull the results from a remote node.
   *
   * @param path
   * @param aggregations
   * @param dataType
   * @param timeFilter     nullable
   * @param partitionGroup
   * @param context
   * @return
   * @throws StorageEngineException
   */
  private List<AggregateResult> getAggregateResult(Path path,
      Set<String> deviceMeasurements, List<String> aggregations,
      TSDataType dataType, Filter timeFilter, PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException {
    if (!partitionGroup.contains(thisNode)) {
      return getRemoteAggregateResult(path, deviceMeasurements, aggregations, dataType, timeFilter
          , partitionGroup, context);
    } else {
      // perform the aggregations locally
      DataGroupMember dataMember = getLocalDataMember(partitionGroup.getHeader());
      try {
        logger
            .debug("{}: querying aggregation {} of {} in {} locally", name, aggregations, path,
                partitionGroup.getHeader());
        List<AggregateResult> aggrResult = dataMember
            .getAggrResult(aggregations, deviceMeasurements, dataType, path.getFullPath(),
                timeFilter, context);
        logger
            .debug("{}: queried aggregation {} of {} in {} locally are {}", name, aggregations,
                path, partitionGroup.getHeader(), aggrResult);
        return aggrResult;
      } catch (IOException | QueryProcessException e) {
        throw new StorageEngineException(e);
      }
    }
  }

  /**
   * Perform "aggregations" over "path" in a remote data group "partitionGroup". Query one node in
   * the group to get the results.
   *
   * @param path
   * @param aggregations
   * @param dataType
   * @param timeFilter     nullable
   * @param partitionGroup
   * @param context
   * @return
   * @throws StorageEngineException
   */
  private List<AggregateResult> getRemoteAggregateResult(Path
      path, Set<String> deviceMeasurements, List<String> aggregations,
      TSDataType dataType, Filter timeFilter, PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException {

    GetAggrResultRequest request = new GetAggrResultRequest();
    request.setPath(path.getFullPath());
    request.setAggregations(aggregations);
    request.setDataTypeOrdinal(dataType.ordinal());
    request.setQueryId(context.getQueryId());
    request.setRequestor(thisNode);
    request.setHeader(partitionGroup.getHeader());
    request.setDeviceMeasurements(deviceMeasurements);
    if (timeFilter != null) {
      request.setTimeFilterBytes(SerializeUtils.serializeFilter(timeFilter));
    }

    for (Node node : partitionGroup) {
      logger.debug("{}: querying aggregation {} of {} from {} of {}", name, aggregations, path,
          node, partitionGroup.getHeader());

      try {
        List<ByteBuffer> resultBuffers = getRemoteAggregateResult(node, request);
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
      } catch (TException | IOException e) {
        logger.error("{}: Cannot query aggregation {} from {}", name, path, node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("{}: query {} interrupted from {}", name, path, node, e);
      }
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query aggregate: " + path + " in " + partitionGroup));
  }

  private List<ByteBuffer> getRemoteAggregateResult(Node node, GetAggrResultRequest request)
      throws IOException, TException, InterruptedException {
    List<ByteBuffer> resultBuffers;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = getAsyncDataClient(node);
      // each buffer is an AggregationResult
      resultBuffers = SyncClientAdaptor.getAggrResult(client, request);
    } else {
      SyncDataClient syncDataClient = getSyncDataClient(node);
      resultBuffers = syncDataClient.getAggrResult(request);
      putBackSyncClient(syncDataClient);
    }
    return resultBuffers;
  }

  /**
   * Get the data groups that should be queried when querying "path" with "filter". First, the time
   * interval qualified by the filter will be extracted. If any side of the interval is open, query
   * all groups. Otherwise compute all involved groups w.r.t. the time partitioning.
   *
   * @param filter
   * @param path
   * @return
   * @throws StorageEngineException
   */
  private List<PartitionGroup> routeFilter(Filter filter, Path path) throws
      StorageEngineException {
    Intervals intervals = PartitionUtils.extractTimeInterval(filter);
    return routeIntervals(intervals, path);
  }

  private List<PartitionGroup> routeIntervals(Intervals intervals, Path path)
      throws StorageEngineException {
    List<PartitionGroup> partitionGroups = new ArrayList<>();
    long firstLB = intervals.getLowerBound(0);
    long lastUB = intervals.getUpperBound(intervals.getIntervalSize() - 1);

    if (firstLB == Long.MIN_VALUE || lastUB == Long.MAX_VALUE) {
      // as there is no TimeLowerBound or TimeUpperBound, the query should be broadcast to every
      // group
      partitionGroups.addAll(partitionTable.getGlobalGroups());
    } else {
      // compute the related data groups of all intervals
      // TODO-Cluster#690: change to a broadcast when the computation is too expensive
      try {
        String storageGroupName = IoTDB.metaManager
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


  /**
   * Query one node in "partitionGroup" for data of "path" with "timeFilter" and "valueFilter". If
   * "partitionGroup" contains the local node, a local reader will be returned. Otherwise a remote
   * reader will be returned.
   *
   * @param partitionGroup
   * @param path
   * @param deviceMeasurements
   * @param timeFilter         nullable
   * @param valueFilter        nullable
   * @param context
   * @param dataType
   * @return
   * @throws IOException
   * @throws StorageEngineException
   */
  private IPointReader getSeriesReader(PartitionGroup partitionGroup, Path path,
      Set<String> deviceMeasurements, Filter timeFilter, Filter valueFilter,
      QueryContext context, TSDataType dataType)
      throws IOException,
      StorageEngineException, QueryProcessException {
    if (partitionGroup.contains(thisNode)) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember = getLocalDataMember(partitionGroup.getHeader(),
          null, String.format("Query: %s, time filter: %s, queryId: %d", path, timeFilter,
              context.getQueryId()));
      IPointReader seriesPointReader = dataGroupMember
          .getSeriesPointReader(path, deviceMeasurements, dataType, timeFilter, valueFilter,
              context);
      if (logger.isDebugEnabled()) {
        logger.debug("{}: creating a local reader for {}#{} of {}, empty: {}", name,
            path.getFullPath(),
            context.getQueryId(), partitionGroup.getHeader(),
            !seriesPointReader.hasNextTimeValuePair());
      }
      return seriesPointReader;
    } else {
      return getRemoteSeriesPointReader(timeFilter, valueFilter, dataType, path,
          deviceMeasurements, partitionGroup, context);
    }
  }

  /**
   * Query a remote node in "partitionGroup" to get the reader of "path" with "timeFilter" and
   * "valueFilter". Firstly, a request will be sent to that node to construct a reader there, then
   * the id of the reader will be returned so that we can fetch data from that node using the reader
   * id.
   *
   * @param timeFilter         nullable
   * @param valueFilter        nullable
   * @param dataType
   * @param path
   * @param deviceMeasurements
   * @param partitionGroup
   * @param context
   * @return
   * @throws StorageEngineException
   */
  private IPointReader getRemoteSeriesPointReader(Filter timeFilter,
      Filter valueFilter, TSDataType dataType, Path path,
      Set<String> deviceMeasurements, PartitionGroup partitionGroup,
      QueryContext context)
      throws StorageEngineException {
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
    request.setDeviceMeasurements(deviceMeasurements);

    // reorder the nodes such that the nodes that suit the query best (have lowest latenct or
    // highest throughput) will be put to the front
    List<Node> orderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);

    DataSourceInfo dataSourceInfo = new DataSourceInfo(partitionGroup, dataType, request,
        (RemoteQueryContext) context, this, orderedNodes);

    AsyncDataClient client = dataSourceInfo.nextDataClient(false, Long.MIN_VALUE);
    if (client != null) {
      return new RemoteSimpleSeriesReader(dataSourceInfo);
    } else if (dataSourceInfo.isNoData()) {
      // there is no satisfying data on the remote node
      return new EmptyReader();
    }

    throw new StorageEngineException(
        new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  private AsyncClientPool getDataAsyncClientPool() {
    return dataAsyncClientPool;
  }

  private SyncClientPool getDataSyncClientPool() {
    return dataSyncClientPool;
  }


  /**
   * Get all paths after removing wildcards in the path
   *
   * @param originPath a path potentially with wildcard
   * @return all paths after removing wildcards in the path
   */
  public List<String> getMatchedPaths(String originPath) throws MetadataException {
    // make sure this node knows all storage groups
    try {
      syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e);
    }
    // get all storage groups this path may belong to
    // the key is the storage group name and the value is the path to be queried with storage group
    // added, e.g:
    // "root.*" will be translated into:
    // "root.group1" -> "root.group1.*", "root.group2" -> "root.group2.*" ...
    Map<String, String> sgPathMap = IoTDB.metaManager.determineStorageGroup(originPath);
    logger.debug("The storage groups of path {} are {}", originPath, sgPathMap.keySet());
    List<String> ret = getMatchedPaths(sgPathMap);
    logger.debug("The paths of path {} are {}", originPath, ret);
    return ret;
  }

  /**
   * Get all paths after removing wildcards in the path
   *
   * @param originalPaths, a list of paths, potentially with wildcard
   * @return a pair of path lists, the first are the existing full paths, the second are invalid
   * original paths
   */
  public Pair<List<String>, List<String>> getMatchedPaths(List<String> originalPaths) {
    ConcurrentSkipListSet<String> fullPaths = new ConcurrentSkipListSet<>();
    ConcurrentSkipListSet<String> nonExistPaths = new ConcurrentSkipListSet<>();
    ExecutorService getAllPathsService = Executors
        .newFixedThreadPool(partitionTable.getGlobalGroups().size());
    for (String pathStr : originalPaths) {
      getAllPathsService.submit(() -> {
        try {
          List<String> fullPathStrs = getMatchedPaths(pathStr);
          if (fullPathStrs.isEmpty()) {
            nonExistPaths.add(pathStr);
            logger.error("Path {} is not found.", pathStr);
          } else {
            fullPaths.addAll(fullPathStrs);
          }
        } catch (MetadataException e) {
          logger.error("Failed to get full paths of the prefix path: {} because", pathStr, e);
        }
      });
    }
    getAllPathsService.shutdown();
    try {
      getAllPathsService.awaitTermination(RaftServer.getQueryTimeoutInSec(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for get all paths services to stop", e);
    }
    return new Pair<>(new ArrayList<>(fullPaths), new ArrayList<>(nonExistPaths));
  }


  /**
   * Get all devices after removing wildcards in the path
   *
   * @param originPath a path potentially with wildcard
   * @return all paths after removing wildcards in the path
   */
  public Set<String> getMatchedDevices(String originPath) throws MetadataException {
    // make sure this node knows all storage groups
    try {
      syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new MetadataException(e);
    }
    // get all storage groups this path may belong to
    // the key is the storage group name and the value is the path to be queried with storage group
    // added, e.g:
    // "root.*" will be translated into:
    // "root.group1" -> "root.group1.*", "root.group2" -> "root.group2.*" ...
    Map<String, String> sgPathMap = IoTDB.metaManager.determineStorageGroup(originPath);
    logger.debug("The storage groups of path {} are {}", originPath, sgPathMap.keySet());
    Set<String> ret = getMatchedDevices(sgPathMap);
    logger.debug("The devices of path {} are {}", originPath, ret);

    return ret;
  }

  /**
   * Split the paths by the data group they belong to and query them from the groups separately.
   *
   * @param sgPathMap the key is the storage group name and the value is the path to be queried with
   *                  storage group added
   * @return a collection of all queried paths
   * @throws MetadataException
   */
  private List<String> getMatchedPaths(Map<String, String> sgPathMap)
      throws MetadataException {
    List<String> result = new ArrayList<>();
    // split the paths by the data group they belong to
    Map<PartitionGroup, List<String>> groupPathMap = new HashMap<>();
    for (Entry<String, String> sgPathEntry : sgPathMap.entrySet()) {
      String storageGroupName = sgPathEntry.getKey();
      String pathUnderSG = sgPathEntry.getValue();
      // find the data group that should hold the timeseries schemas of the storage group
      PartitionGroup partitionGroup = partitionTable.route(storageGroupName, 0);
      if (partitionGroup.contains(thisNode)) {
        // this node is a member of the group, perform a local query after synchronizing with the
        // leader
        getLocalDataMember(partitionGroup.getHeader()).syncLeader();
        List<String> allTimeseriesName = IoTDB.metaManager.getAllTimeseriesName(pathUnderSG);
        logger.debug("{}: get matched paths of {} locally, result {}", name, partitionGroup,
            allTimeseriesName);
        result.addAll(allTimeseriesName);
      } else {
        // batch the queries of the same group to reduce communication
        groupPathMap.computeIfAbsent(partitionGroup, p -> new ArrayList<>()).add(pathUnderSG);
      }
    }

    // query each data group separately
    for (Entry<PartitionGroup, List<String>> partitionGroupPathEntry : groupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupPathEntry.getKey();
      List<String> pathsToQuery = partitionGroupPathEntry.getValue();
      result.addAll(getMatchedPaths(partitionGroup, pathsToQuery));
    }

    return result;
  }

  private List<String> getMatchedPaths(PartitionGroup partitionGroup, List<String> pathsToQuery)
      throws MetadataException {
    // choose the node with lowest latency or highest throughput
    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      try {
        List<String> paths = getMatchedPaths(node, partitionGroup.getHeader(), pathsToQuery);
        if (logger.isDebugEnabled()) {
          logger.debug("{}: get matched paths of {} and other {} paths from {} in {}, result {}",
              name, pathsToQuery.get(0), pathsToQuery.size() - 1, node, partitionGroup.getHeader(),
              paths);
        }
        if (paths != null) {
          // query next group
          return paths;
        }
      } catch (IOException | TException e) {
        throw new MetadataException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetadataException(e);
      }
    }
    logger.warn("Cannot get paths of {} from {}", pathsToQuery, partitionGroup);
    return Collections.emptyList();
  }

  private List<String> getMatchedPaths(Node node, Node header, List<String> pathsToQuery)
      throws IOException, TException, InterruptedException {
    List<String> paths;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = getAsyncDataClient(node);
      paths = SyncClientAdaptor.getAllPaths(client, header,
          pathsToQuery);
    } else {
      SyncDataClient syncDataClient = getSyncDataClient(node);
      paths = syncDataClient.getAllPaths(header, pathsToQuery);
      putBackSyncClient(syncDataClient);
    }
    return paths;
  }

  /**
   * Split the paths by the data group they belong to and query them from the groups separately.
   *
   * @param sgPathMap the key is the storage group name and the value is the path to be queried with
   *                  storage group added
   * @return a collection of all queried devices
   * @throws MetadataException
   */
  private Set<String> getMatchedDevices(Map<String, String> sgPathMap)
      throws MetadataException {
    Set<String> result = new HashSet<>();
    // split the paths by the data group they belong to
    Map<PartitionGroup, List<String>> groupPathMap = new HashMap<>();
    for (Entry<String, String> sgPathEntry : sgPathMap.entrySet()) {
      String storageGroupName = sgPathEntry.getKey();
      String pathUnderSG = sgPathEntry.getValue();
      // find the data group that should hold the timeseries schemas of the storage group
      PartitionGroup partitionGroup = partitionTable.route(storageGroupName, 0);
      if (partitionGroup.contains(thisNode)) {
        // this node is a member of the group, perform a local query after synchronizing with the
        // leader
        getLocalDataMember(partitionGroup.getHeader()).syncLeader();
        Set<String> allDevices = IoTDB.metaManager.getDevices(pathUnderSG);
        logger.debug("{}: get matched paths of {} locally, result {}", name, partitionGroup,
            allDevices);
        result.addAll(allDevices);
      } else {
        // batch the queries of the same group to reduce communication
        groupPathMap.computeIfAbsent(partitionGroup, p -> new ArrayList<>()).add(pathUnderSG);
      }
    }

    // query each data group separately
    for (Entry<PartitionGroup, List<String>> partitionGroupPathEntry : groupPathMap.entrySet()) {
      PartitionGroup partitionGroup = partitionGroupPathEntry.getKey();
      List<String> pathsToQuery = partitionGroupPathEntry.getValue();

      result.addAll(getMatchedDevices(partitionGroup, pathsToQuery));
    }

    return result;
  }

  private Set<String> getMatchedDevices(PartitionGroup partitionGroup, List<String> pathsToQuery)
      throws MetadataException {
    // choose the node with lowest latency or highest throughput
    List<Node> coordinatedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : coordinatedNodes) {
      try {
        Set<String> paths = getMatchedDevices(node, partitionGroup.getHeader(), pathsToQuery);
        logger.debug("{}: get matched paths of {} from {}, result {}", name, partitionGroup,
            node, paths);
        if (paths != null) {
          // query next group
          return paths;
        }
      } catch (IOException | TException e) {
        throw new MetadataException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new MetadataException(e);
      }
    }
    logger.warn("Cannot get paths of {} from {}", pathsToQuery, partitionGroup);
    return Collections.emptySet();
  }

  private Set<String> getMatchedDevices(Node node, Node header, List<String> pathsToQuery)
      throws IOException, TException, InterruptedException {
    Set<String> paths;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = getAsyncDataClient(node);
      paths = SyncClientAdaptor.getAllDevices(client, header,
          pathsToQuery);
    } else {
      SyncDataClient syncDataClient = getSyncDataClient(node);
      paths = syncDataClient.getAllDevices(header, pathsToQuery);
      putBackSyncClient(syncDataClient);
    }
    return paths;
  }

  public List<String> getAllStorageGroupNames() {
    // make sure this node knows all storage groups
    syncLeader();
    return IoTDB.metaManager.getAllStorageGroupNames();
  }

  public List<StorageGroupMNode> getAllStorageGroupNodes() {
    // make sure this node knows all storage groups
    syncLeader();
    return IoTDB.metaManager.getAllStorageGroupNodes();
  }

  @SuppressWarnings("java:S2274")
  public Map<Node, Boolean> getAllNodeStatus() {
    if (getPartitionTable() == null) {
      // the cluster is being built.
      return null;
    }
    Map<Node, Boolean> nodeStatus = new HashMap<>();
    for (Node node : allNodes) {
      nodeStatus.put(node, thisNode.equals(node));
    }

    try {
      if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
        getNodeStatusAsync(nodeStatus);
      } else {
        getNodeStatusSync(nodeStatus);
      }
    } catch (TException e) {
      logger.warn("Cannot get the status of all nodes", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.warn("Cannot get the status of all nodes", e);
    }
    return nodeStatus;
  }

  @SuppressWarnings({"java:S2445", "java:S2274"})
  private void getNodeStatusAsync(Map<Node, Boolean> nodeStatus)
      throws TException, InterruptedException {
    NodeStatusHandler nodeStatusHandler = new NodeStatusHandler(nodeStatus);
    synchronized (nodeStatus) {
      for (Node node : allNodes) {
        TSMetaService.AsyncClient client = (AsyncClient) getAsyncClient(node);
        if (!node.equals(thisNode) && client != null) {
          client.checkAlive(nodeStatusHandler);
        }
      }
      nodeStatus.wait(ClusterConstant.CHECK_ALIVE_TIME_OUT_MS);
    }
  }

  private void getNodeStatusSync(Map<Node, Boolean> nodeStatus) throws TException {
    NodeStatusHandler nodeStatusHandler = new NodeStatusHandler(nodeStatus);
    for (Node node : allNodes) {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(node);
      if (!node.equals(thisNode) && client != null) {
        nodeStatusHandler.onComplete(client.checkAlive());
      }
    }
  }

  @TestOnly
  public void setPartitionTable(PartitionTable partitionTable) {
    this.partitionTable = partitionTable;
    router = new ClusterPlanRouter(partitionTable);
    DataClusterServer dClusterServer = getDataClusterServer();
    if (dClusterServer != null) {
      dClusterServer.setPartitionTable(partitionTable);
    }
  }


  /**
   * Process the request of removing a node from the cluster. Reject the request if partition table
   * is unavailable or the node is not the MetaLeader and it does not know who the leader is.
   * Otherwise (being the MetaLeader), the request will be processed locally and broadcast to every
   * node.
   *
   * @param node the node to be removed.
   */
  public long removeNode(Node node)
      throws PartitionTableUnavailableException, LogExecutionException {
    if (partitionTable == null) {
      logger.info("Cannot add node now because the partition table is not set");
      throw new PartitionTableUnavailableException(thisNode);
    }

    waitLeader();
    // try to process the request locally, if it cannot be processed locally, forward it
    return processRemoveNodeLocally(node);
  }


  /**
   * Process a node removal request locally and broadcast it to the whole cluster. The removal will
   * be rejected if number of nodes will fall below half of the replication number after this
   * operation.
   *
   * @param node the node to be removed.
   * @return Long.MIN_VALUE if further forwarding is required, or the execution result
   */
  private long processRemoveNodeLocally(Node node)
      throws LogExecutionException {
    if (character != NodeCharacter.LEADER) {
      return Response.RESPONSE_NULL;
    }

    // if we cannot have enough replica after the removal, reject it
    if (allNodes.size() <= ClusterDescriptor.getInstance().getConfig().getReplicationNum()) {
      return Response.RESPONSE_CLUSTER_TOO_SMALL;
    }

    // find the node to be removed in the node list
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
      return Response.RESPONSE_REJECT;
    }

    // node removal must be serialized to reduce potential concurrency problem
    synchronized (logManager) {
      RemoveNodeLog removeNodeLog = new RemoveNodeLog();
      removeNodeLog.setCurrLogTerm(getTerm().get());
      removeNodeLog.setCurrLogIndex(logManager.getLastLogIndex() + 1);

      removeNodeLog.setRemovedNode(target);

      logManager.append(removeNodeLog);

      int retryTime = 1;
      while (true) {
        logger.info("Send the node removal request of {} to other nodes, retry time: {}", target,
            retryTime);
        AppendLogResult result = sendLogToAllGroups(removeNodeLog);

        switch (result) {
          case OK:
            logger.info("Removal request of {} is accepted", target);
            logManager.commitTo(removeNodeLog.getCurrLogIndex(), false);
            return Response.RESPONSE_AGREE;
          case TIME_OUT:
            logger.info("Removal request of {} timed out", target);
            break;
          // retry
          case LEADERSHIP_STALE:
          default:
            return Response.RESPONSE_NULL;
        }
      }
    }
  }

  /**
   * Remove a node from the node list, partition table and update DataGroupMembers. If the removed
   * node is the local node, also stop heartbeat and catch-up service of metadata, but the heartbeat
   * and catch-up service of data are kept alive for other nodes to pull data. If the removed node
   * is a leader, send an exile to the removed node so that it can know it is removed.
   *
   * @param oldNode the node to be removed
   */
  public void applyRemoveNode(Node oldNode) {
    synchronized (allNodes) {
      if (allNodes.contains(oldNode)) {
        logger.debug("Removing a node {} from {}", oldNode, allNodes);
        allNodes.remove(oldNode);
        idNodeMap.remove(oldNode.nodeIdentifier);

        // update the partition table
        NodeRemovalResult result = partitionTable.removeNode(oldNode);

        // update DataGroupMembers, as the node is removed, the members of some groups are
        // changed and there will also be one less group
        getDataClusterServer().removeNode(oldNode, result);
        // the leader is removed, start the next election ASAP
        if (oldNode.equals(leader)) {
          setCharacter(NodeCharacter.ELECTOR);
          lastHeartbeatReceivedTime = Long.MIN_VALUE;
        }

        if (oldNode.equals(thisNode)) {
          // use super.stop() so that the data server will not be closed because other nodes may
          // want to pull data from this node
          super.stop();
          if (clientServer != null) {
            clientServer.stop();
          }
        } else if (thisNode.equals(leader)) {
          // as the old node is removed, it cannot know this by heartbeat or log, so it should be
          // directly kicked out of the cluster
          exileNode(oldNode);
        }

        // save the updated partition table
        savePartitionTable();
      }
    }
  }

  private void exileNode(Node node) {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncMetaClient asyncMetaClient = (AsyncMetaClient) getAsyncClient(node);
      try {
        asyncMetaClient.exile(new GenericHandler<>(node, null));
      } catch (TException e) {
        logger.warn("Cannot inform {} its removal", node, e);
      }
    } else {
      SyncMetaClient client = (SyncMetaClient) getSyncClient(node);
      try {
        client.exile();
      } catch (TException e) {
        logger.warn("Cannot inform {} its removal", node, e);
      }
      putBackSyncClient(client);
    }
  }

  /**
   * Generate a report containing the character, leader, term, last log and read-only-status. This
   * will help to see if the node is in a consistent and right state during debugging.
   *
   * @return
   */
  private MetaMemberReport genMemberReport() {
    return new MetaMemberReport(character, leader, term.get(),
        logManager.getLastLogTerm(), logManager.getLastLogIndex(), logManager.getCommitLogIndex()
        , logManager.getCommitLogTerm(), readOnly,
        lastHeartbeatReceivedTime);
  }

  /**
   * Generate a report containing the status of both MetaGroupMember and DataGroupMembers of this
   * node. This will help to see if the node is in a consistent and right state during debugging.
   *
   * @return
   */
  private NodeReport genNodeReport() {
    NodeReport report = new NodeReport(thisNode);
    report.setMetaMemberReport(genMemberReport());
    report.setDataMemberReportList(dataClusterServer.genMemberReports());
    return report;
  }

  @Override
  public void setAllNodes(List<Node> allNodes) {
    super.setAllNodes(allNodes);
    initPeerMap();
    idNodeMap = new HashMap<>();
    for (Node node : allNodes) {
      idNodeMap.put(node.getNodeIdentifier(), node);
    }
  }

  /**
   * Get a local DataGroupMember that is in the group of "header" and should process "request".
   *
   * @param header        the header of the group which the local node is in
   * @param resultHandler can be set to null if the request is an internal request
   * @param request       the toString() of this parameter should explain what the request is and it
   *                      is only used in logs for tracing
   * @return
   */
  public DataGroupMember getLocalDataMember(Node header,
      AsyncMethodCallback resultHandler, Object request) {
    return dataClusterServer.getDataMember(header, resultHandler, request);
  }

  /**
   * Get a local DataGroupMember that is in the group of "header" for an internal request.
   *
   * @param header the header of the group which the local node is in
   * @return
   */
  public DataGroupMember getLocalDataMember(Node header) {
    return dataClusterServer.getDataMember(header, null, "Internal call");
  }

  /**
   * Get a thrift client that will connect to "node" using the data port.
   *
   * @param node the node to be connected
   * @return
   * @throws IOException
   */
  public AsyncDataClient getAsyncDataClient(Node node) throws IOException {
    return (AsyncDataClient) getDataAsyncClientPool().getClient(node);
  }

  /**
   * Get a thrift client that will connect to "node" using the data port.
   *
   * @param node the node to be connected
   * @return
   * @throws IOException
   */
  public SyncDataClient getSyncDataClient(Node node) throws IOException {
    return (SyncDataClient) getDataSyncClientPool().getClient(node);
  }

  /**
   * Get GroupByExecutors the will executor the aggregations of "aggregationTypes" over "path".
   * First, the groups to be queried will be determined by the timeFilter. Then for group, a local
   * or remote GroupByExecutor will be created and finally all such executors will be returned.
   *
   * @param path
   * @param dataType
   * @param context
   * @param timeFilter       nullable
   * @param aggregationTypes
   * @return
   * @throws StorageEngineException
   */
  public List<GroupByExecutor> getGroupByExecutors(Path path,
      Set<String> deviceMeasurements, TSDataType dataType,
      QueryContext context, Filter timeFilter, List<Integer> aggregationTypes)
      throws StorageEngineException, QueryProcessException {
    // make sure the partition table is new
    try {
      syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new QueryProcessException(e.getMessage());
    }
    // find out the groups that should be queried
    List<PartitionGroup> partitionGroups = routeFilter(timeFilter, path);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending group by query of {} to {} groups", name, path,
          partitionGroups.size());
    }
    // create an executor for each group
    List<GroupByExecutor> executors = new ArrayList<>();
    for (PartitionGroup partitionGroup : partitionGroups) {
      GroupByExecutor groupByExecutor = getGroupByExecutor(path, deviceMeasurements, partitionGroup,
          timeFilter, context, dataType, aggregationTypes);
      executors.add(groupByExecutor);
    }
    return executors;
  }

  /**
   * Get a GroupByExecutor that will run "aggregationTypes" over "path" within "partitionGroup". If
   * the local node is a member of the group, a local executor will be created. Otherwise a remote
   * executor will be created.
   *
   * @param path
   * @param deviceMeasurements
   * @param partitionGroup
   * @param timeFilter         nullable
   * @param context
   * @param dataType
   * @param aggregationTypes
   * @return
   * @throws StorageEngineException
   */
  private GroupByExecutor getGroupByExecutor(Path path,
      Set<String> deviceMeasurements, PartitionGroup partitionGroup,
      Filter timeFilter, QueryContext context, TSDataType dataType,
      List<Integer> aggregationTypes) throws StorageEngineException, QueryProcessException {
    if (partitionGroup.contains(thisNode)) {
      // the target storage group contains this node, perform a local query
      DataGroupMember dataGroupMember = getLocalDataMember(partitionGroup.getHeader());
      logger.debug("{}: creating a local group by executor for {}#{}", name,
          path.getFullPath(), context.getQueryId());
      return dataGroupMember
          .getGroupByExecutor(path, deviceMeasurements, dataType, timeFilter, aggregationTypes,
              context);
    } else {
      return getRemoteGroupByExecutor(timeFilter, aggregationTypes, dataType, path,
          deviceMeasurements, partitionGroup, context);
    }
  }

  /**
   * Get a GroupByExecutor that will run "aggregationTypes" over "path" within a remote group
   * "partitionGroup". Send a request to one node in the group to create an executor there and use
   * the return executor id to fetch result later.
   *
   * @param timeFilter         nullable
   * @param aggregationTypes
   * @param dataType
   * @param path
   * @param deviceMeasurements
   * @param partitionGroup
   * @param context
   * @return
   * @throws StorageEngineException
   */
  private GroupByExecutor getRemoteGroupByExecutor(Filter timeFilter,
      List<Integer> aggregationTypes, TSDataType dataType, Path path,
      Set<String> deviceMeasurements, PartitionGroup partitionGroup,
      QueryContext context) throws StorageEngineException {
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
    request.setDeviceMeasurements(deviceMeasurements);

    // select a node with lowest latency or highest throughput with high priority
    List<Node> orderedNodes = QueryCoordinator.getINSTANCE().reorderNodes(partitionGroup);
    for (Node node : orderedNodes) {
      // query a remote node
      logger.debug("{}: querying group by {} from {}", name, path, node);

      try {
        Long executorId = getRemoteGroupByExecutorId(node, request);

        if (executorId == null) {
          continue;
        }

        if (executorId != -1) {
          // record the queried node to release resources later
          ((RemoteQueryContext) context).registerRemoteNode(node, partitionGroup.getHeader());
          logger.debug("{}: get an executorId {} for {}@{} from {}", name, executorId,
              aggregationTypes, path, node);
          // create a remote executor with the return id
          RemoteGroupByExecutor remoteGroupByExecutor = new RemoteGroupByExecutor(executorId,
              this, node, partitionGroup.getHeader());
          for (Integer aggregationType : aggregationTypes) {
            remoteGroupByExecutor.addAggregateResult(AggregateResultFactory.getAggrResultByType(
                AggregationType.values()[aggregationType], dataType));
          }
          return remoteGroupByExecutor;
        } else {
          // an id of -1 means there is no satisfying data on the remote node, create an empty
          // reader tp reduce further communication
          logger.debug("{}: no data for {} from {}", name, path, node);
          return new EmptyReader();
        }
      } catch (TException | IOException e) {
        logger.error("{}: Cannot query {} from {}", name, path, node, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("{}: Cannot query {} from {}", name, path, node, e);
      }
    }
    throw new StorageEngineException(
        new RequestTimeOutException("Query " + path + " in " + partitionGroup));
  }

  private Long getRemoteGroupByExecutorId(Node node, GroupByRequest request)
      throws IOException, TException, InterruptedException {
    Long executorId;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      AsyncDataClient client = getAsyncDataClient(node);
      executorId = SyncClientAdaptor.getGroupByExecutor(client, request);
    } else {
      SyncDataClient syncDataClient = getSyncDataClient(node);
      executorId = syncDataClient.getGroupByExecutor(request);
      putBackSyncClient(syncDataClient);
    }
    return executorId;
  }


  public TimeValuePair performPreviousFill(Path path, TSDataType dataType, long queryTime,
      long beforeRange, Set<String> deviceMeasurements, QueryContext context)
      throws StorageEngineException {
    // make sure the partition table is new
    try {
      syncLeaderWithConsistencyCheck();
    } catch (CheckConsistencyException e) {
      throw new StorageEngineException(e);
    }
    // find the groups that should be queried using the time range
    Intervals intervals = new Intervals();
    long lowerBound = beforeRange == -1 ? Long.MIN_VALUE : queryTime - beforeRange;
    intervals.addInterval(lowerBound, queryTime);
    List<PartitionGroup> partitionGroups = routeIntervals(intervals, path);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Sending data query of {} to {} groups", name, path,
          partitionGroups.size());
    }
    CountDownLatch latch = new CountDownLatch(partitionGroups.size());
    PreviousFillHandler handler = new PreviousFillHandler(latch);

    ExecutorService fillService = Executors.newFixedThreadPool(partitionGroups.size());
    PreviousFillArguments arguments = new PreviousFillArguments(path, dataType, queryTime,
        beforeRange, deviceMeasurements);

    for (PartitionGroup partitionGroup : partitionGroups) {
      fillService.submit(() -> performPreviousFill(arguments, context,
          partitionGroup, handler));
    }
    fillService.shutdown();
    try {
      fillService.awaitTermination(RaftServer.getQueryTimeoutInSec(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Unexpected interruption when waiting for fill pool to stop", e);
    }
    return handler.getResult();
  }

  public void performPreviousFill(PreviousFillArguments arguments, QueryContext context,
      PartitionGroup group,
      PreviousFillHandler fillHandler) {
    if (group.contains(thisNode)) {
      localPreviousFill(arguments, context, group, fillHandler);
    } else {
      remotePreviousFill(arguments, context, group, fillHandler);
    }
  }

  public void localPreviousFill(PreviousFillArguments arguments, QueryContext context,
      PartitionGroup group,
      PreviousFillHandler fillHandler) {
    DataGroupMember localDataMember = getLocalDataMember(group.getHeader());
    try {
      fillHandler
          .onComplete(
              localDataMember.localPreviousFill(arguments.getPath(), arguments.getDataType(),
                  arguments.getQueryTime(), arguments.getBeforeRange(),
                  arguments.getDeviceMeasurements(), context));
    } catch (QueryProcessException | StorageEngineException | IOException e) {
      fillHandler.onError(e);
    }
  }

  public void remotePreviousFill(PreviousFillArguments arguments, QueryContext context,
      PartitionGroup group,
      PreviousFillHandler fillHandler) {
    PreviousFillRequest request = new PreviousFillRequest(arguments.getPath().getFullPath(),
        arguments.getQueryTime(),
        arguments.getBeforeRange(), context.getQueryId(), thisNode, group.getHeader(),
        arguments.getDataType().ordinal(),
        arguments.getDeviceMeasurements());

    for (Node node : group) {
      ByteBuffer byteBuffer;
      if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
        byteBuffer = remoteAsyncPreviousFill(node, request, arguments);
      } else {
        byteBuffer = remoteSyncPreviousFill(node, request, arguments);
      }

      if (byteBuffer != null) {
        fillHandler.onComplete(byteBuffer);
        return;
      }
    }

    fillHandler.onError(new QueryTimeOutException(String.format("PreviousFill %s@%d range: %d",
        arguments.getPath().getFullPath(), arguments.getQueryTime(), arguments.getBeforeRange())));
  }

  private ByteBuffer remoteAsyncPreviousFill(Node node, PreviousFillRequest request,
      PreviousFillArguments arguments) {
    ByteBuffer byteBuffer = null;
    AsyncDataClient asyncDataClient;
    try {
      asyncDataClient = getAsyncDataClient(node);
    } catch (IOException e) {
      logger.warn("{}: Cannot connect to {} during previous fill", name, node);
      return null;
    }

    try {
      byteBuffer = SyncClientAdaptor.previousFill(asyncDataClient, request);
    } catch (Exception e) {
      logger
          .error("{}: Cannot perform previous fill of {} to {}", name, arguments.getPath(), node,
              e);
    }
    return byteBuffer;
  }

  private ByteBuffer remoteSyncPreviousFill(Node node, PreviousFillRequest request,
      PreviousFillArguments arguments) {
    ByteBuffer byteBuffer = null;
    SyncDataClient syncDataClient;
    try {
      syncDataClient = getSyncDataClient(node);
    } catch (IOException e) {
      logger.warn("{}: Cannot connect to {} during previous fill", name, node);
      return null;
    }

    try {
      byteBuffer = syncDataClient.previousFill(request);
    } catch (Exception e) {
      logger
          .error("{}: Cannot perform previous fill of {} to {}", name, arguments.getPath(), node,
              e);
    } finally {
      putBackSyncClient(syncDataClient);
    }
    return byteBuffer;
  }

  @Override
  public void closeLogManager() {
    super.closeLogManager();
    if (dataClusterServer != null) {
      dataClusterServer.closeLogManagers();
    }
  }

  protected PlanExecutor getLocalExecutor() throws QueryProcessException {
    if (localExecutor == null) {
      localExecutor = new PlanExecutor();
    }
    return localExecutor;
  }

  public StartUpStatus getStartUpStatus() {
    return startUpStatus;
  }
}
