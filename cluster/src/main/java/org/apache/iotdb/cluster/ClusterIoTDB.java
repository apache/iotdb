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
package org.apache.iotdb.cluster;

import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.ClientManager;
import org.apache.iotdb.cluster.client.IClientManager;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncClientAdaptor;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterConfig;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.coordinator.Coordinator;
import org.apache.iotdb.cluster.exception.ConfigInconsistentException;
import org.apache.iotdb.cluster.exception.StartUpCheckFailureException;
import org.apache.iotdb.cluster.metadata.CMManager;
import org.apache.iotdb.cluster.metadata.MetaPuller;
import org.apache.iotdb.cluster.partition.slot.SlotPartitionTable;
import org.apache.iotdb.cluster.partition.slot.SlotStrategy;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.ClusterRPCService;
import org.apache.iotdb.cluster.server.ClusterTSServiceImpl;
import org.apache.iotdb.cluster.server.HardLinkCleaner;
import org.apache.iotdb.cluster.server.Response;
import org.apache.iotdb.cluster.server.clusterinfo.ClusterInfoServer;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.server.monitor.NodeReport;
import org.apache.iotdb.cluster.server.raft.DataRaftHeartBeatService;
import org.apache.iotdb.cluster.server.raft.DataRaftService;
import org.apache.iotdb.cluster.server.raft.MetaRaftHeartBeatService;
import org.apache.iotdb.cluster.server.raft.MetaRaftService;
import org.apache.iotdb.cluster.server.service.DataGroupServiceImpls;
import org.apache.iotdb.cluster.server.service.MetaAsyncService;
import org.apache.iotdb.cluster.server.service.MetaSyncService;
import org.apache.iotdb.cluster.utils.ClusterUtils;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitor;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConfigCheck;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.RegisterManager;
import org.apache.iotdb.db.service.thrift.ThriftServiceThread;
import org.apache.iotdb.db.utils.TestOnly;

import org.apache.thrift.TException;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.cluster.config.ClusterConstant.THREAD_POLL_WAIT_TERMINATION_TIME_S;
import static org.apache.iotdb.cluster.utils.ClusterUtils.UNKNOWN_CLIENT_IP;

// we do not inherent IoTDB instance, as it may break the singleton mode of IoTDB.
public class ClusterIoTDB implements ClusterIoTDBMBean {

  private static final Logger logger = LoggerFactory.getLogger(ClusterIoTDB.class);
  private final String mbeanName =
      String.format(
          "%s:%s=%s", "org.apache.iotdb.cluster.service", IoTDBConstant.JMX_TYPE, "ClusterIoTDB");

  // TODO fix me: better to throw exception if the client can not be get. Then we can remove this
  // field.
  public static boolean printClientConnectionErrorStack = false;

  // establish the cluster as a seed
  private static final String MODE_START = "-s";
  // join an established cluster
  private static final String MODE_ADD = "-a";
  // send a request to remove a node, more arguments: ip-of-removed-node
  // metaport-of-removed-node
  private static final String MODE_REMOVE = "-r";

  private MetaGroupMember metaGroupEngine;

  // TODO we can split dataGroupServiceImpls into two parts: the rpc impl and the engine.
  private DataGroupServiceImpls dataGroupEngine;

  private Node thisNode;
  private Coordinator coordinator;

  private IoTDB iotdb = IoTDB.getInstance();

  // Cluster IoTDB uses a individual registerManager with its parent.
  private RegisterManager registerManager = new RegisterManager();

  /**
   * a single thread pool, every "REPORT_INTERVAL_SEC" seconds, "reportThread" will print the status
   * of all raft members in this node
   */
  private ScheduledExecutorService reportThread;

  private boolean allowReport = true;

  /** hardLinkCleaner will periodically clean expired hardlinks created during snapshots */
  private ScheduledExecutorService hardLinkCleanerThread;

  // currently, dataClientProvider is only used for those instances who do not belong to any
  // DataGroup..
  // TODO: however, why not let all dataGroupMembers getting clients from dataClientProvider
  private IClientManager clientManager;

  private ClusterIoTDB() {
    // we do not init anything here, so that we can re-initialize the instance in IT.
  }

  public void initLocalEngines() {
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    thisNode = new Node();
    // set internal rpc ip and ports
    thisNode.setInternalIp(config.getInternalIp());
    thisNode.setMetaPort(config.getInternalMetaPort());
    thisNode.setDataPort(config.getInternalDataPort());
    // set client rpc ip and ports
    thisNode.setClientPort(config.getClusterRpcPort());
    thisNode.setClientIp(IoTDBDescriptor.getInstance().getConfig().getRpcAddress());
    coordinator = new Coordinator();
    // local engine
    TProtocolFactory protocolFactory =
        ThriftServiceThread.getProtocolFactory(
            IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable());
    metaGroupEngine = new MetaGroupMember(protocolFactory, thisNode, coordinator);
    IoTDB.setClusterMode();
    IoTDB.setMetaManager(CMManager.getInstance());
    ((CMManager) IoTDB.metaManager).setMetaGroupMember(metaGroupEngine);
    ((CMManager) IoTDB.metaManager).setCoordinator(coordinator);
    MetaPuller.getInstance().init(metaGroupEngine);

    dataGroupEngine = new DataGroupServiceImpls(protocolFactory, metaGroupEngine);
    clientManager =
        new ClientManager(
            ClusterDescriptor.getInstance().getConfig().isUseAsyncServer(),
            ClientManager.Type.ClusterClient);
    initTasks();
    try {
      // we need to check config after initLocalEngines.
      startServerCheck();
    } catch (StartupException e) {
      logger.error("Failed to check cluster config.", e);
      stop();
    }
    JMXService.registerMBean(metaGroupEngine, metaGroupEngine.getMBeanName());
  }

  private void initTasks() {
    reportThread = IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("NodeReportThread");
    reportThread.scheduleAtFixedRate(
        this::generateNodeReport,
        ClusterConstant.REPORT_INTERVAL_SEC,
        ClusterConstant.REPORT_INTERVAL_SEC,
        TimeUnit.SECONDS);
    hardLinkCleanerThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("HardLinkCleaner");
    hardLinkCleanerThread.scheduleAtFixedRate(
        new HardLinkCleaner(),
        ClusterConstant.CLEAN_HARDLINK_INTERVAL_SEC,
        ClusterConstant.CLEAN_HARDLINK_INTERVAL_SEC,
        TimeUnit.SECONDS);
  }

  /**
   * Generate a report containing the status of both MetaGroupMember and DataGroupMembers of this
   * node. This will help to see if the node is in a consistent and right state during debugging.
   */
  private void generateNodeReport() {
    if (logger.isDebugEnabled() && allowReport) {
      try {
        NodeReport report = new NodeReport(thisNode);
        report.setMetaMemberReport(metaGroupEngine.genMemberReport());
        report.setDataMemberReportList(dataGroupEngine.genMemberReports());
        logger.debug(report.toString());
      } catch (Exception e) {
        logger.error("exception occurred when generating node report", e);
      }
    }
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      logger.error(
          "Usage: <-s|-a|-r> "
              + "[-D{} <configure folder>] \n"
              + "-s: start the node as a seed\n"
              + "-a: start the node as a new node\n"
              + "-r: remove the node out of the cluster\n",
          IoTDBConstant.IOTDB_CONF);
      return;
    }

    ClusterIoTDB cluster = ClusterIoTDBHolder.INSTANCE;
    // check config of iotdb,and set some configs in cluster mode
    if (!cluster.serverCheckAndInit()) {
      return;
    }
    String mode = args[0];
    logger.info("Running mode {}", mode);

    // initialize the current node and its services
    cluster.initLocalEngines();

    // we start IoTDB kernel first. then we start the cluster module.
    if (MODE_START.equals(mode)) {
      cluster.activeStartNodeMode();
    } else if (MODE_ADD.equals(mode)) {
      cluster.activeAddNodeMode();
    } else if (MODE_REMOVE.equals(mode)) {
      try {
        cluster.doRemoveNode(args);
      } catch (IOException e) {
        logger.error("Fail to remove node in cluster", e);
      }
    } else {
      logger.error("Unrecognized mode {}", mode);
    }
  }

  private boolean serverCheckAndInit() {
    try {
      IoTDBConfigCheck.getInstance().checkConfig();
    } catch (IOException e) {
      logger.error("meet error when doing start checking", e);
    }
    // init server's configuration first, because the cluster configuration may read settings from
    // the server's configuration.
    IoTDBDescriptor.getInstance().getConfig().setSyncEnable(false);
    // auto create schema is took over by cluster module, so we disable it in the server module.
    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
    // check cluster config
    String checkResult = clusterConfigCheck();
    if (checkResult != null) {
      logger.error(checkResult);
      return false;
    }
    return true;
  }

  private String clusterConfigCheck() {
    try {
      ClusterDescriptor.getInstance().replaceHostnameWithIp();
    } catch (Exception e) {
      return String.format("replace hostname with ip failed, %s", e.getMessage());
    }
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    // check the initial replicateNum and refuse to start when the replicateNum <= 0
    if (config.getReplicationNum() <= 0) {
      return String.format(
          "ReplicateNum should be greater than 0 instead of %d.", config.getReplicationNum());
    }
    // check the initial cluster size and refuse to start when the size < quorum
    int quorum = config.getReplicationNum() / 2 + 1;
    if (config.getSeedNodeUrls().size() < quorum) {
      return String.format(
          "Seed number less than quorum, seed number: %s, quorum: " + "%s.",
          config.getSeedNodeUrls().size(), quorum);
    }
    // TODO duplicate code,consider to solve it later
    Set<Node> seedNodes = new HashSet<>();
    for (String url : config.getSeedNodeUrls()) {
      Node node = ClusterUtils.parseNode(url);
      if (seedNodes.contains(node)) {
        return String.format(
            "SeedNodes must not repeat each other. SeedNodes: %s", config.getSeedNodeUrls());
      }
      seedNodes.add(node);
    }
    return null;
  }

  public void activeStartNodeMode() {
    try {
      // start iotdb server first
      IoTDB.getInstance().active();
      // some work about cluster
      preInitCluster();
      // try to build cluster
      metaGroupEngine.buildCluster();
      // register service after cluster build
      postInitCluster();
      // init ServiceImpl to handle request of client
      startClientRPC();
    } catch (StartupException
        | StartUpCheckFailureException
        | ConfigInconsistentException
        | QueryProcessException e) {
      logger.error("Fail to start  server", e);
      stop();
    }
  }

  private void preInitCluster() throws StartupException {
    stopRaftInfoReport();
    preStartCustomize();
    JMXService.registerMBean(this, mbeanName);
    // register MetaGroupMember. MetaGroupMember has the same position with "StorageEngine" in the
    // cluster moduel.
    // TODO fixme it is better to remove coordinator out of metaGroupEngine

    registerManager.register(metaGroupEngine);
    registerManager.register(dataGroupEngine);

    // rpc service initialize
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      MetaAsyncService metaAsyncService = new MetaAsyncService(metaGroupEngine);
      MetaRaftHeartBeatService.getInstance().initAsyncedServiceImpl(metaAsyncService);
      MetaRaftService.getInstance().initAsyncedServiceImpl(metaAsyncService);
      DataRaftService.getInstance().initAsyncedServiceImpl(dataGroupEngine);
      DataRaftHeartBeatService.getInstance().initAsyncedServiceImpl(dataGroupEngine);
    } else {
      MetaSyncService syncService = new MetaSyncService(metaGroupEngine);
      MetaRaftHeartBeatService.getInstance().initSyncedServiceImpl(syncService);
      MetaRaftService.getInstance().initSyncedServiceImpl(syncService);
      DataRaftService.getInstance().initSyncedServiceImpl(dataGroupEngine);
      DataRaftHeartBeatService.getInstance().initSyncedServiceImpl(dataGroupEngine);
    }
    // start RPC service
    logger.info("start Meta Heartbeat RPC service... ");
    registerManager.register(MetaRaftHeartBeatService.getInstance());
    // TODO: better to start the Meta RPC service untill the heartbeatservice has elected the
    // leader.
    // and quorum of followers have caught up.
    logger.info("start Meta RPC service... ");
    registerManager.register(MetaRaftService.getInstance());
  }

  private void postInitCluster() throws StartupException, QueryProcessException {
    logger.info("start Data Heartbeat RPC service... ");
    registerManager.register(DataRaftHeartBeatService.getInstance());
    logger.info("start Data RPC service... ");
    registerManager.register(DataRaftService.getInstance());
    // RPC based DBA API
    registerManager.register(ClusterInfoServer.getInstance());
    // JMX based DBA API
    registerManager.register(ClusterMonitor.INSTANCE);
  }

  private void startClientRPC() throws QueryProcessException, StartupException {
    // we must wait until the metaGroup established.
    // So that the ClusterRPCService can work.
    ClusterTSServiceImpl clusterRPCServiceImpl = new ClusterTSServiceImpl();
    clusterRPCServiceImpl.setCoordinator(coordinator);
    clusterRPCServiceImpl.setExecutor(metaGroupEngine);
    ClusterRPCService.getInstance().initSyncedServiceImpl(clusterRPCServiceImpl);
    registerManager.register(ClusterRPCService.getInstance());
  }

  public void activeAddNodeMode() {
    try {
      long startTime = System.currentTimeMillis();

      preInitCluster();
      metaGroupEngine.joinCluster();
      postInitCluster();
      dataGroupEngine.pullSnapshots();
      startClientRPC();
      logger.info(
          "Adding this node {} to cluster costs {} ms",
          thisNode,
          (System.currentTimeMillis() - startTime));
    } catch (StartupException
        | QueryProcessException
        | StartUpCheckFailureException
        | ConfigInconsistentException e) {
      stop();
      logger.error("Fail to join cluster", e);
    }
  }

  private void startServerCheck() throws StartupException {
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    // assert not duplicated nodes
    Set<Node> seedNodes = new HashSet<>();
    for (String url : config.getSeedNodeUrls()) {
      Node node = ClusterUtils.parseNode(url);
      if (seedNodes.contains(node)) {
        String message =
            String.format(
                "SeedNodes must not repeat each other. SeedNodes: %s", config.getSeedNodeUrls());
        throw new StartupException(metaGroupEngine.getName(), message);
      }
      seedNodes.add(node);
    }

    // assert this node is in all nodes when restart
    if (!metaGroupEngine.getAllNodes().isEmpty()) {
      if (!metaGroupEngine.getAllNodes().contains(metaGroupEngine.getThisNode())) {
        String message =
            String.format(
                "All nodes in partitionTables must contains local node in start-server mode. "
                    + "LocalNode: %s, AllNodes: %s",
                metaGroupEngine.getThisNode(), metaGroupEngine.getAllNodes());
        throw new StartupException(metaGroupEngine.getName(), message);
      } else {
        return;
      }
    }

    // assert this node is in seed nodes list
    if (!seedNodes.contains(thisNode)) {
      String message =
          String.format(
              "SeedNodes must contains local node in start-server mode. LocalNode: %s ,SeedNodes: %s",
              thisNode.toString(), config.getSeedNodeUrls());
      throw new StartupException(metaGroupEngine.getName(), message);
    }
  }

  private void doRemoveNode(String[] args) throws IOException {
    if (args.length != 3) {
      logger.error("Usage: -r <ip> <metaPort>");
      return;
    }
    String ip = args[1];
    int metaPort = Integer.parseInt(args[2]);
    ClusterConfig config = ClusterDescriptor.getInstance().getConfig();
    TProtocolFactory factory =
        config.isRpcThriftCompressionEnabled() ? new TCompactProtocol.Factory() : new Factory();
    Node nodeToRemove = new Node();
    nodeToRemove.setInternalIp(ip).setMetaPort(metaPort).setClientIp(UNKNOWN_CLIENT_IP);
    // try sending the request to each seed node
    for (String url : config.getSeedNodeUrls()) {
      Node node = ClusterUtils.parseNode(url);
      if (node == null) {
        continue;
      }
      AsyncMetaClient client = new AsyncMetaClient(factory, new TAsyncClientManager(), node, null);
      Long response = null;
      long startTime = System.currentTimeMillis();
      try {
        logger.info("Start removing node {} with the help of node {}", nodeToRemove, node);
        response = SyncClientAdaptor.removeNode(client, nodeToRemove);
      } catch (TException e) {
        logger.warn("Cannot send remove node request through {}, try next node", node);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Cannot send remove node request through {}, try next node", node);
      }
      if (response != null) {
        handleNodeRemovalResp(response, nodeToRemove, startTime);
        return;
      }
    }
  }

  private void handleNodeRemovalResp(Long response, Node nodeToRemove, long startTime) {
    if (response == Response.RESPONSE_AGREE) {
      logger.info(
          "Node {} is successfully removed, cost {}ms",
          nodeToRemove,
          (System.currentTimeMillis() - startTime));
    } else if (response == Response.RESPONSE_CLUSTER_TOO_SMALL) {
      logger.error("Cluster size is too small, cannot remove any node");
    } else if (response == Response.RESPONSE_REJECT) {
      logger.error("Node {} is not found in the cluster, please check", nodeToRemove);
    } else if (response == Response.RESPONSE_DATA_MIGRATION_NOT_FINISH) {
      logger.warn(
          "The data migration of the previous membership change operation is not finished. Please try again later");
    } else {
      logger.error("Unexpected response {}", response);
    }
  }

  /** Developers may perform pre-start customizations here for debugging or experiments. */
  @SuppressWarnings("java:S125") // leaving examples
  private void preStartCustomize() {
    // customize data distribution
    // The given example tries to divide storage groups like "root.sg_1", "root.sg_2"... into k
    // nodes evenly, and use default strategy for other groups
    SlotPartitionTable.setSlotStrategy(
        new SlotStrategy() {
          SlotStrategy defaultStrategy = new SlotStrategy.DefaultStrategy();
          int k = 3;

          @Override
          public int calculateSlotByTime(String storageGroupName, long timestamp, int maxSlotNum) {
            int sgSerialNum = extractSerialNumInSGName(storageGroupName) % k;
            if (sgSerialNum >= 0) {
              return maxSlotNum / k * sgSerialNum;
            } else {
              return defaultStrategy.calculateSlotByTime(storageGroupName, timestamp, maxSlotNum);
            }
          }

          @Override
          public int calculateSlotByPartitionNum(
              String storageGroupName, long partitionId, int maxSlotNum) {
            int sgSerialNum = extractSerialNumInSGName(storageGroupName) % k;
            if (sgSerialNum >= 0) {
              return maxSlotNum / k * sgSerialNum;
            } else {
              return defaultStrategy.calculateSlotByPartitionNum(
                  storageGroupName, partitionId, maxSlotNum);
            }
          }

          private int extractSerialNumInSGName(String storageGroupName) {
            String[] s = storageGroupName.split("_");
            if (s.length != 2) {
              return -1;
            }
            try {
              return Integer.parseInt(s[1]);
            } catch (NumberFormatException e) {
              return -1;
            }
          }
        });
  }

  //  @TestOnly
  //  public void setMetaClusterServer(MetaGroupMember RaftTSMetaServiceImpl) {
  //    metaServer = RaftTSMetaServiceImpl;
  //  }

  public void stop() {
    deactivate();
  }

  private void deactivate() {
    logger.info("Deactivating Cluster IoTDB...");
    stopThreadPools();
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("ClusterIoTDB is deactivated.");
    // stop the iotdb kernel
    iotdb.stop();
  }

  private void stopThreadPools() {
    stopThreadPool(reportThread, "reportThread");
    stopThreadPool(hardLinkCleanerThread, "hardLinkCleanerThread");
  }

  private void stopThreadPool(ExecutorService pool, String name) {
    if (pool != null) {
      pool.shutdownNow();
      try {
        pool.awaitTermination(THREAD_POLL_WAIT_TERMINATION_TIME_S, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.error("Unexpected interruption when waiting for {} to end", name, e);
      }
    }
  }

  @TestOnly
  public void setClientManager(IClientManager clientManager) {
    this.clientManager = clientManager;
  }

  public MetaGroupMember getMetaGroupEngine() {
    return metaGroupEngine;
  }

  public Node getThisNode() {
    return thisNode;
  }

  public Coordinator getCoordinator() {
    return coordinator;
  }

  public IoTDB getIotdb() {
    return iotdb;
  }

  public RegisterManager getRegisterManager() {
    return registerManager;
  }

  public DataGroupServiceImpls getDataGroupEngine() {
    return dataGroupEngine;
  }

  public void setMetaGroupEngine(MetaGroupMember metaGroupEngine) {
    this.metaGroupEngine = metaGroupEngine;
  }

  public static ClusterIoTDB getInstance() {
    return ClusterIoTDBHolder.INSTANCE;
  }

  @Override
  public boolean startRaftInfoReport() {
    logger.info("Raft status report is enabled.");
    allowReport = true;
    if (logger.isDebugEnabled()) {
      return true;
    }
    return false;
  }

  @Override
  public void stopRaftInfoReport() {
    logger.info("Raft status report is disabled.");
    allowReport = false;
  }

  @Override
  public void enablePrintClientConnectionErrorStack() {
    printClientConnectionErrorStack = true;
  }

  @Override
  public void disablePrintClientConnectionErrorStack() {
    printClientConnectionErrorStack = false;
  }

  public SyncDataClient getSyncDataClient(Node node, int readOperationTimeoutMS) {
    SyncDataClient dataClient =
        (SyncDataClient) clientManager.borrowSyncClient(node, ClientCategory.DATA);
    if (dataClient != null) {
      dataClient.setTimeout(readOperationTimeoutMS);
    }
    return dataClient;
  }

  public AsyncDataClient getAsyncDataClient(Node node, int readOperationTimeoutMS) {
    try {
      AsyncDataClient dataClient =
          (AsyncDataClient) clientManager.borrowAsyncClient(node, ClientCategory.DATA);
      if (dataClient != null) {
        dataClient.setTimeout(readOperationTimeoutMS);
      }
      return dataClient;
    } catch (IOException e) {
      logger.warn("error when get async client", e);
    }
    return null;
  }

  private static class ClusterIoTDBHolder {

    private static final ClusterIoTDB INSTANCE = new ClusterIoTDB();

    private ClusterIoTDBHolder() {}
  }
}
