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
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeActiveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.IoTDBStartCheck;
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
import java.util.List;

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

  protected void doRemoveNode(String[] args) {
    // TODO: remove data node
  }

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
    registerManager.register(ClientRPCService.getInstance());
  }

  /** register DataNode with ConfigNode */
  private void registerInConfigNode() throws StartupException {
    int retry = DEFAULT_JOIN_RETRY;

    ConfigNodeInfo.getInstance()
        .updateConfigNodeList(IoTDBDescriptor.getInstance().getConfig().getTargetConfigNodeList());
    while (retry > 0) {
      logger.info("start registering to the cluster.");
      try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
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

        TDataNodeRegisterReq req = new TDataNodeRegisterReq();
        req.setDataNodeInfo(info);
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
  }

  /** send a message to ConfigNode after DataNode is available */
  private void activateCurrentDataNode() throws StartupException {
    int retry = DEFAULT_JOIN_RETRY;

    while (retry > 0) {
      logger.info("start joining the cluster.");
      try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
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
        TDataNodeActiveReq req = new TDataNodeActiveReq();
        req.setLocation(location);
        req.setDataNodeId(config.getDataNodeId());
        TSStatus status = configNodeClient.activeDataNode(req);
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
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
