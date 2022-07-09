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
package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeActiveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.IoTDBStartCheck;
import org.apache.iotdb.db.conf.IoTDBStopCheck;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.cache.CacheHitRatioMonitor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.cq.ContinuousQueryService;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationService;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.protocol.mpprest.MPPRestService;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.service.basic.StandaloneServiceProvider;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.db.service.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.sync.receiver.ReceiverService;
import org.apache.iotdb.db.sync.sender.service.SenderService;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataNode implements DataNodeMBean {
  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final String mbeanName =
      String.format(
          "%s:%s=%s", "org.apache.iotdb.datanode.service", IoTDBConstant.JMX_TYPE, "DataNode");

  /**
   * when joining a cluster this node will retry at most "DEFAULT_JOIN_RETRY" times before returning
   * a failure to the client
   */
  private static final int DEFAULT_JOIN_RETRY = 10;

  private final TEndPoint thisNode = new TEndPoint();

  private DataNode() {
    // we do not init anything here, so that we can re-initialize the instance in IT.
  }

  private static final RegisterManager registerManager = new RegisterManager();
  public static ServiceProvider serviceProvider;

  public static DataNode getInstance() {
    return DataNodeHolder.INSTANCE;
  }

  public static void main(String[] args) {
    new DataNodeServerCommandLine().doMain(args);
  }

  protected void serverCheckAndInit() throws ConfigurationException, IOException {
    IoTDBStartCheck.getInstance().checkConfig();
    // TODO: check configuration for data node

    // if client ip is the default address, set it same with internal ip
    if (config.getRpcAddress().equals("0.0.0.0")) {
      config.setRpcAddress(config.getInternalAddress());
    }

    thisNode.setIp(IoTDBDescriptor.getInstance().getConfig().getInternalAddress());
    thisNode.setPort(IoTDBDescriptor.getInstance().getConfig().getInternalPort());
  }

  protected void doAddNode(String[] args) {
    try {
      // setup InternalService
      setUpInternalService();
      // register current DataNode to ConfigNode
      registerInConfigNode();
      // setup DataNode
      active();
      // send message to config node stating that data node is ready
      activateCurrentDataNode();
      // setup rpc service
      setUpRPCService();
      logger.info("Congratulation, IoTDB DataNode is set up successfully. Now, enjoy yourself!");
    } catch (StartupException e) {
      logger.error("Fail to start server", e);
      stop();
    }
  }

  /**
   * remove datanodes from cluster
   *
   * @param args IPs for removed datanodes, split with ','
   */
  protected void doRemoveNode(String[] args) {
    try {
      removePrepare(args);
      removeNodesFromCluster(args);
      removeTail();
    } catch (Exception e) {
      logger.error("remove Data Nodes error", e);
    }
  }

  private void removePrepare(String[] args) throws BadNodeUrlException {
    ConfigNodeInfo.getInstance()
        .updateConfigNodeList(IoTDBDescriptor.getInstance().getConfig().getTargetConfigNodeList());
    try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
      TDataNodeInfoResp resp = configNodeClient.getDataNodeInfo(-1);
      // 1. online Data Node size - removed Data Node size < replication，NOT ALLOW remove
      //   But replication size is set in Config Node's configuration, so check it in remote Config
      // Node

      // 2. removed Data Node IP not contained in below map, CAN NOT remove.
      Map<Integer, TDataNodeInfo> nodeIdToNodeInfo = resp.getDataNodeInfoMap();
      List<String> removedDataNodeIps = Arrays.asList(args[1].split(","));
      List<String> onlineDataNodeIps =
          nodeIdToNodeInfo.values().stream()
              .map(TDataNodeInfo::getLocation)
              .map(TDataNodeLocation::getInternalEndPoint)
              .map(TEndPoint::getIp)
              .collect(Collectors.toList());
      IoTDBStopCheck.getInstance().checkDuplicateIp(removedDataNodeIps);
      IoTDBStopCheck.getInstance().checkIpInCluster(removedDataNodeIps, onlineDataNodeIps);
    } catch (TException e) {
      logger.error("remove Data Nodes check failed", e);
    }
  }

  private void removeNodesFromCluster(String[] args) {
    logger.info("start to remove DataNode from cluster");
    List<TDataNodeLocation> dataNodeLocations = buildDataNodeLocations(args[1]);
    if (dataNodeLocations.isEmpty()) {
      logger.error("data nodes location is empty");
      // throw Exception OR return?
      return;
    }
    logger.info(
        "there has data nodes location will be removed. size is: {}, detail: {}",
        dataNodeLocations.size(),
        dataNodeLocations);
    TDataNodeRemoveReq removeReq = new TDataNodeRemoveReq(dataNodeLocations);
    try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
      TDataNodeRemoveResp removeResp = configNodeClient.removeDataNode(removeReq);
      logger.info("Remove result {} ", removeResp.toString());
    } catch (TException e) {
      logger.error("send remove Data Node request failed!", e);
    }
  }

  /**
   * fetch all datanode info from ConfigNode, then compare with input 'ips'
   *
   * @param ips data node ip, split with ','
   * @return TDataNodeLocation list
   */
  private List<TDataNodeLocation> buildDataNodeLocations(String ips) {
    List<TDataNodeLocation> dataNodeLocations = new ArrayList<>();
    if (ips == null || ips.trim().isEmpty()) {
      return dataNodeLocations;
    }

    List<String> dataNodeIps = Arrays.asList(ips.split(","));
    try (ConfigNodeClient client = new ConfigNodeClient()) {
      dataNodeLocations =
          client.getDataNodeInfo(-1).getDataNodeInfoMap().values().stream()
              .map(TDataNodeInfo::getLocation)
              .filter(location -> dataNodeIps.contains(location.getInternalEndPoint().getIp()))
              .collect(Collectors.toList());
    } catch (TException e) {
      logger.error("get data node locations failed", e);
    }

    if (dataNodeIps.size() != dataNodeLocations.size()) {
      logger.error(
          "build DataNode locations error, "
              + "because number of input ip NOT EQUALS the number of fetched DataNodeLocations, will return empty locations");
      return dataNodeLocations;
    }

    return dataNodeLocations;
  }

  private void removeTail() {}

  /** initialize the current node and its services */
  public boolean initLocalEngines() {
    IoTDB.setClusterMode();
    return true;
  }

  /** prepare iotdb and start InternalService */
  private void setUpInternalService() throws StartupException {
    // check iotdb server first
    StartupChecks checks = new StartupChecks().withDefaultTest();
    checks.verify();

    // Register services
    JMXService.registerMBean(getInstance(), mbeanName);
    // set the mpp mode to true
    IoTDBDescriptor.getInstance().getConfig().setMppMode(true);
    IoTDBDescriptor.getInstance().getConfig().setClusterMode(true);

    // start InternalService first so that it can respond to configNode's heartbeat before joining
    // cluster
    registerManager.register(DataNodeInternalRPCService.getInstance());
  }

  /** register DataNode with ConfigNode */
  private void registerInConfigNode() throws StartupException {
    int retry = DEFAULT_JOIN_RETRY;

    ConfigNodeInfo.getInstance()
        .updateConfigNodeList(IoTDBDescriptor.getInstance().getConfig().getTargetConfigNodeList());
    while (retry > 0) {
      logger.info("start registering to the cluster.");
      try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
        TDataNodeRegisterReq req = new TDataNodeRegisterReq();
        req.setDataNodeInfo(generateDataNodeInfo());
        TDataNodeRegisterResp dataNodeRegisterResp = configNodeClient.registerDataNode(req);

        // store config node lists from resp
        List<TEndPoint> configNodeList = new ArrayList<>();
        for (TConfigNodeLocation configNodeLocation : dataNodeRegisterResp.getConfigNodeList()) {
          configNodeList.add(configNodeLocation.getInternalEndPoint());
        }
        ConfigNodeInfo.getInstance().updateConfigNodeList(configNodeList);

        if (dataNodeRegisterResp.getStatus().getCode()
                == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || dataNodeRegisterResp.getStatus().getCode()
                == TSStatusCode.DATANODE_ALREADY_REGISTERED.getStatusCode()) {
          logger.info(dataNodeRegisterResp.getStatus().getMessage());
          int dataNodeID = dataNodeRegisterResp.getDataNodeId();
          if (dataNodeID != config.getDataNodeId()) {
            IoTDBStartCheck.getInstance().serializeDataNodeId(dataNodeID);
            config.setDataNodeId(dataNodeID);
          }
          IoTDBDescriptor.getInstance().loadGlobalConfig(dataNodeRegisterResp.globalConfig);

          if (!IoTDBStartCheck.getInstance()
              .checkConsensusProtocolExists(TConsensusGroupType.DataRegion)) {
            config.setDataRegionConsensusProtocolClass(
                dataNodeRegisterResp.globalConfig.getDataRegionConsensusProtocolClass());
            IoTDBStartCheck.getInstance()
                .serializeConsensusProtocol(
                    dataNodeRegisterResp.globalConfig.getDataRegionConsensusProtocolClass(),
                    TConsensusGroupType.DataRegion);
          }

          if (!IoTDBStartCheck.getInstance()
              .checkConsensusProtocolExists(TConsensusGroupType.SchemaRegion)) {
            config.setSchemaRegionConsensusProtocolClass(
                dataNodeRegisterResp.globalConfig.getSchemaRegionConsensusProtocolClass());
            IoTDBStartCheck.getInstance()
                .serializeConsensusProtocol(
                    dataNodeRegisterResp.globalConfig.getSchemaRegionConsensusProtocolClass(),
                    TConsensusGroupType.SchemaRegion);
          }
          logger.info("Register to the cluster successfully");
          return;
        }
      } catch (IOException e) {
        logger.warn("Cannot register to the cluster, because: {}", e.getMessage());
      } catch (TException e) {
        // read config nodes from system.properties
        logger.warn("Cannot register to the cluster, because: {}", e.getMessage());
        ConfigNodeInfo.getInstance().loadConfigNodeList();
      }

      try {
        // wait 5s to start the next try
        Thread.sleep(IoTDBDescriptor.getInstance().getConfig().getJoinClusterTimeOutMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to register to the cluster", e);
        break;
      }

      // start the next try
      retry--;
    }
    // all tries failed
    logger.error("Cannot register to the cluster after {} retries", DEFAULT_JOIN_RETRY);
    throw new StartupException("Cannot register to the cluster.");
  }

  /** register services and set up DataNode */
  private void active() throws StartupException {
    try {
      setUp();
    } catch (StartupException | QueryProcessException e) {
      logger.error("meet error while starting up.", e);
      throw new StartupException("Error in activating IoTDB DataNode.");
    }
    logger.info("IoTDB DataNode has started.");

    try {
      // TODO: Start consensus layer in some where else
      SchemaRegionConsensusImpl.setupAndGetInstance().start();
      DataRegionConsensusImpl.setupAndGetInstance().start();
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private void setUp() throws StartupException, QueryProcessException {
    logger.info("Setting up IoTDB DataNode...");

    Runtime.getRuntime().addShutdownHook(new IoTDBShutdownHook());
    setUncaughtExceptionHandler();
    initServiceProvider();

    // init metric service
    registerManager.register(MetricsService.getInstance());

    logger.info("recover the schema...");
    initSchemaEngine();
    registerManager.register(new JMXService());
    registerManager.register(FlushManager.getInstance());
    registerManager.register(CacheHitRatioMonitor.getInstance());
    registerManager.register(CompactionTaskManager.getInstance());
    JMXService.registerMBean(getInstance(), mbeanName);

    // close wal when using ratis consensus
    if (config.isClusterMode()
        && config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RatisConsensus)) {
      config.setWalMode(WALMode.DISABLE);
    }
    registerManager.register(WALManager.getInstance());

    // in mpp mode we need to start some other services
    registerManager.register(StorageEngineV2.getInstance());
    registerManager.register(MPPDataExchangeService.getInstance());
    registerManager.register(DriverScheduler.getInstance());

    registerUdfServices();

    registerManager.register(ReceiverService.getInstance());

    logger.info(
        "IoTDB DataNode is setting up, some storage groups may not be ready now, please wait several seconds...");

    while (!StorageEngineV2.getInstance().isAllSgReady()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("IoTDB DataNode failed to set up.", e);
        Thread.currentThread().interrupt();
        return;
      }
    }

    registerManager.register(SenderService.getInstance());
    registerManager.register(UpgradeSevice.getINSTANCE());
    // in mpp mode we temporarily don't start settle service because it uses StorageEngine directly
    // in itself, but currently we need to use StorageEngineV2 instead of StorageEngine in mpp mode.
    // registerManager.register(SettleService.getINSTANCE());
    registerManager.register(TriggerRegistrationService.getInstance());
    registerManager.register(ContinuousQueryService.getInstance());

    // start reporter
    MetricsService.getInstance().startAllReporter();

    // start region migrate service
    registerManager.register(RegionMigrateService.getInstance());
  }

  /** send a message to ConfigNode after DataNode is available */
  private void activateCurrentDataNode() throws StartupException {
    int retry = DEFAULT_JOIN_RETRY;

    while (retry > 0) {
      logger.info("start joining the cluster.");
      try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
        TDataNodeActiveReq req = new TDataNodeActiveReq();
        req.setDataNodeInfo(generateDataNodeInfo());
        TSStatus status = configNodeClient.activeDataNode(req);
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
            || status.getCode() == TSStatusCode.DATANODE_ALREADY_ACTIVATED.getStatusCode()) {
          logger.info("Joined the cluster successfully");
          return;
        }
      } catch (TException e) {
        logger.warn("Cannot join the cluster, because: {}", e.getMessage());
      }

      try {
        // wait 5s to start the next try
        Thread.sleep(IoTDBDescriptor.getInstance().getConfig().getJoinClusterTimeOutMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to join the cluster", e);
        break;
      }
      // start the next try
      retry--;
    }
    // all tries failed
    logger.error("Cannot join the cluster after {} retries", DEFAULT_JOIN_RETRY);
    throw new StartupException("Cannot join the cluster.");
  }

  /** set up RPC and protocols after DataNode is available */
  private void setUpRPCService() throws StartupException {
    // init rpc service
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setRpcImplClassName(ClientRPCServiceImpl.class.getName());
    if (IoTDBDescriptor.getInstance().getConfig().isEnableRpcService()) {
      registerManager.register(RPCService.getInstance());
    }
    // init service protocols
    initProtocols();
  }

  /**
   * generate dataNodeInfo
   *
   * @return TDataNodeInfo
   */
  private TDataNodeInfo generateDataNodeInfo() {
    // Set DataNodeLocation
    TDataNodeLocation location = new TDataNodeLocation();
    location.setDataNodeId(config.getDataNodeId());
    location.setClientRpcEndPoint(new TEndPoint(config.getRpcAddress(), config.getRpcPort()));
    location.setInternalEndPoint(
        new TEndPoint(config.getInternalAddress(), config.getInternalPort()));
    location.setMPPDataExchangeEndPoint(
        new TEndPoint(config.getInternalAddress(), config.getMppDataExchangePort()));
    location.setDataRegionConsensusEndPoint(
        new TEndPoint(config.getInternalAddress(), config.getDataRegionConsensusPort()));
    location.setSchemaRegionConsensusEndPoint(
        new TEndPoint(config.getInternalAddress(), config.getSchemaRegionConsensusPort()));
    // Set DataNodeInfo
    TDataNodeInfo info = new TDataNodeInfo();
    info.setLocation(location);
    info.setCpuCoreNum(Runtime.getRuntime().availableProcessors());
    info.setMaxMemory(Runtime.getRuntime().totalMemory());
    return info;
  }

  private void registerUdfServices() throws StartupException {
    registerManager.register(TemporaryQueryDataFileService.getInstance());
    registerManager.register(
        UDFExecutableManager.setupAndGetInstance(
            IoTDBDescriptor.getInstance().getConfig().getTemporaryLibDir(),
            IoTDBDescriptor.getInstance().getConfig().getUdfDir()));
    registerManager.register(
        UDFClassLoaderManager.setupAndGetInstance(
            IoTDBDescriptor.getInstance().getConfig().getUdfDir()));
    registerManager.register(
        UDFRegistrationService.setupAndGetInstance(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir()
                + File.separator
                + "udf"
                + File.separator));
  }

  private void initSchemaEngine() {
    long time = System.currentTimeMillis();
    SchemaEngine.getInstance().init();
    long end = System.currentTimeMillis() - time;
    logger.info("spend {}ms to recover schema.", end);
    logger.info(
        "After initializing, sequence tsFile threshold is {}, unsequence tsFile threshold is {}",
        IoTDBDescriptor.getInstance().getConfig().getSeqTsFileSize(),
        IoTDBDescriptor.getInstance().getConfig().getUnSeqTsFileSize());
  }

  public void stop() {
    deactivate();

    // QSW
    try {
      MetricsService.getInstance().stop();
      SchemaRegionConsensusImpl.getInstance().stop();
      DataRegionConsensusImpl.getInstance().stop();
    } catch (Exception e) {
      logger.error("stop data node error", e);
    }
  }

  private void initServiceProvider() throws QueryProcessException {
    serviceProvider = new StandaloneServiceProvider();
  }

  private void initProtocols() throws StartupException {
    if (IoTDBDescriptor.getInstance().getConfig().isEnableInfluxDBRpcService()) {
      registerManager.register(InfluxDBRPCService.getInstance());
      IoTDB.initInfluxDBMManager();
    }
    if (IoTDBDescriptor.getInstance().getConfig().isEnableMQTTService()) {
      registerManager.register(MQTTService.getInstance());
    }
    if (IoTDBRestServiceDescriptor.getInstance().getConfig().isEnableRestService()) {
      registerManager.register(MPPRestService.getInstance());
    }
  }

  private void deactivate() {
    logger.info("Deactivating IoTDB DataNode...");
    // stopThreadPools();
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB DataNode is deactivated.");
  }

  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  private static class DataNodeHolder {

    private static final DataNode INSTANCE = new DataNode();

    private DataNodeHolder() {}
  }
}
