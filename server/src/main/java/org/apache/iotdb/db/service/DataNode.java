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
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.commons.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetTriggerJarReq;
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
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.protocol.mpprest.MPPRestService;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.db.service.basic.StandaloneServiceProvider;
import org.apache.iotdb.db.service.metrics.MetricService;
import org.apache.iotdb.db.service.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.service.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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

  /** store the list when registering in config node for preparing trigger related resources */
  private List<TriggerInformation> triggerInformationList;

  private static final int JAR_NUM_OF_ONE_RPC = 10;

  public static DataNode getInstance() {
    return DataNodeHolder.INSTANCE;
  }

  public static void main(String[] args) {
    new DataNodeServerCommandLine().doMain(args);
  }

  protected void serverCheckAndInit() throws ConfigurationException, IOException {
    IoTDBStartCheck.getInstance().checkConfig();
    // TODO: check configuration for data node

    for (TEndPoint endPoint : config.getTargetConfigNodeList()) {
      if (endPoint.getIp().equals("0.0.0.0")) {
        throw new ConfigurationException(
            "The ip address of any target_config_nodes couldn't be 0.0.0.0");
      }
    }

    // if client ip is the default address, set it same with internal ip
    if (config.getRpcAddress().equals("0.0.0.0")) {
      config.setRpcAddress(config.getInternalAddress());
    }
    thisNode.setIp(config.getInternalAddress());
    thisNode.setPort(config.getInternalPort());
  }

  protected void doAddNode() {
    try {
      // prepare cluster IoTDB-DataNode
      prepareDataNode();
      // register current DataNode to ConfigNode
      registerInConfigNode();
      // get resources for trigger,udf...
      prepareResources();
      // active DataNode
      active();
      // setup rpc service
      setUpRPCService();
      logger.info("IoTDB configuration: " + config.getConfigMessage());
      logger.info("Congratulation, IoTDB DataNode is set up successfully. Now, enjoy yourself!");
    } catch (StartupException e) {
      logger.error("Fail to start server", e);
      stop();
    }
  }

  /** initialize the current node and its services */
  public boolean initLocalEngines() {
    config.setClusterMode(true);
    return true;
  }

  /** Prepare cluster IoTDB-DataNode */
  private void prepareDataNode() throws StartupException {
    // check iotdb server first
    StartupChecks checks = new StartupChecks().withDefaultTest();
    checks.verify();

    // Register services
    JMXService.registerMBean(getInstance(), mbeanName);
    // set the mpp mode to true
    config.setMppMode(true);
    config.setClusterMode(true);
  }

  /** register DataNode with ConfigNode */
  private void registerInConfigNode() throws StartupException {
    int retry = DEFAULT_JOIN_RETRY;

    ConfigNodeInfo.getInstance().updateConfigNodeList(config.getTargetConfigNodeList());
    while (retry > 0) {
      logger.info("Start registering to the cluster.");
      try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
        TDataNodeRegisterReq req = new TDataNodeRegisterReq();
        req.setDataNodeConfiguration(generateDataNodeConfiguration());
        TDataNodeRegisterResp dataNodeRegisterResp = configNodeClient.registerDataNode(req);

        // store config node lists from resp
        List<TEndPoint> configNodeList = new ArrayList<>();
        for (TConfigNodeLocation configNodeLocation : dataNodeRegisterResp.getConfigNodeList()) {
          configNodeList.add(configNodeLocation.getInternalEndPoint());
        }
        ConfigNodeInfo.getInstance().updateConfigNodeList(configNodeList);
        ClusterTemplateManager.getInstance()
            .updateTemplateSetInfo(dataNodeRegisterResp.getTemplateInfo());

        // store triggerInformationList
        getTriggerInformationList(dataNodeRegisterResp.getAllTriggerInformation());

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
          IoTDBDescriptor.getInstance().loadRatisConfig(dataNodeRegisterResp.ratisConfig);
          IoTDBDescriptor.getInstance().initClusterSchemaMemoryAllocate();

          CommonDescriptor.getInstance().loadGlobalConfig(dataNodeRegisterResp.globalConfig);

          if (!IoTDBStartCheck.getInstance()
              .checkConsensusProtocolExists(TConsensusGroupType.DataRegion)) {
            config.setDataRegionConsensusProtocolClass(
                dataNodeRegisterResp.globalConfig.getDataRegionConsensusProtocolClass());
          }

          if (!IoTDBStartCheck.getInstance()
              .checkConsensusProtocolExists(TConsensusGroupType.SchemaRegion)) {
            config.setSchemaRegionConsensusProtocolClass(
                dataNodeRegisterResp.globalConfig.getSchemaRegionConsensusProtocolClass());
          }

          // In current implementation, only MultiLeader need separated memory from Consensus
          if (!config
              .getDataRegionConsensusProtocolClass()
              .equals(ConsensusFactory.MultiLeaderConsensus)) {
            IoTDBDescriptor.getInstance().reclaimConsensusMemory();
          }

          IoTDBStartCheck.getInstance().serializeGlobalConfig(dataNodeRegisterResp.globalConfig);

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
        Thread.sleep(config.getJoinClusterTimeOutMs());
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

  private void prepareResources() throws StartupException {
    prepareTriggerResources();
  }

  /** register services and set up DataNode */
  private void active() throws StartupException {
    try {
      setUp();
    } catch (StartupException | QueryProcessException e) {
      logger.error("Meet error while starting up.", e);
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

    logger.info("Recover the schema...");
    initSchemaEngine();
    registerManager.register(new JMXService());
    registerManager.register(FlushManager.getInstance());
    registerManager.register(CacheHitRatioMonitor.getInstance());
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

    // must init after SchemaEngine and StorageEngine prepared well
    DataNodeRegionManager.getInstance().init();

    registerManager.register(SyncService.getInstance());
    registerManager.register(UpgradeSevice.getINSTANCE());
    // in mpp mode we temporarily don't start settle service because it uses StorageEngine directly
    // in itself, but currently we need to use StorageEngineV2 instead of StorageEngine in mpp mode.
    // registerManager.register(SettleService.getINSTANCE());
    registerManager.register(TriggerRegistrationService.getInstance());
    registerManager.register(ContinuousQueryService.getInstance());

    // start region migrate service
    registerManager.register(RegionMigrateService.getInstance());

    registerManager.register(MetricService.getInstance());
    registerManager.register(CompactionTaskManager.getInstance());
  }

  /** set up RPC and protocols after DataNode is available */
  private void setUpRPCService() throws StartupException {
    // Start InternalRPCService to indicate that the current DataNode can accept cluster scheduling
    registerManager.register(DataNodeInternalRPCService.getInstance());

    // Notice: During the period between starting the internal RPC service
    // and starting the client RPC service , some requests may fail because
    // DataNode is not marked as RUNNING by ConfigNode-leader yet.

    // Start client RPCService to indicate that the current DataNode provide external services
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setRpcImplClassName(ClientRPCServiceImpl.class.getName());
    if (config.isEnableRpcService()) {
      registerManager.register(RPCService.getInstance());
    }
    // init service protocols
    initProtocols();
  }

  /**
   * generate dataNodeConfiguration
   *
   * @return TDataNodeConfiguration
   */
  private TDataNodeConfiguration generateDataNodeConfiguration() {
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

    // Set NodeResource
    TNodeResource resource = new TNodeResource();
    resource.setCpuCoreNum(Runtime.getRuntime().availableProcessors());
    resource.setMaxMemory(Runtime.getRuntime().totalMemory());

    return new TDataNodeConfiguration(location, resource);
  }

  private void registerUdfServices() throws StartupException {
    registerManager.register(TemporaryQueryDataFileService.getInstance());
    registerManager.register(
        UDFExecutableManager.setupAndGetInstance(
            config.getUdfTemporaryLibDir(), config.getUdfDir()));
    registerManager.register(UDFClassLoaderManager.setupAndGetInstance(config.getUdfDir()));
    registerManager.register(
        UDFRegistrationService.setupAndGetInstance(
            config.getSystemDir() + File.separator + "udf" + File.separator));
  }

  private void initTriggerRelatedInstance() throws StartupException {
    try {
      TriggerExecutableManager.setupAndGetInstance(
          config.getTriggerTemporaryLibDir(), config.getTriggerDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private void prepareTriggerResources() throws StartupException {
    initTriggerRelatedInstance();
    if (triggerInformationList == null || triggerInformationList.isEmpty()) {
      return;
    }

    // get jars from config node
    List<TriggerInformation> triggerNeedJarList = getTriggerNeedJarList();
    int index = 0;
    while (index < triggerNeedJarList.size()) {
      List<TriggerInformation> curList = new ArrayList<>();
      int offset = 0;
      while (offset < JAR_NUM_OF_ONE_RPC && index + offset < triggerNeedJarList.size()) {
        curList.add(triggerNeedJarList.get(index + offset));
        offset++;
      }
      index += offset + 1;
      getJarOfTriggers(curList);
    }

    // create instances of stateless trigger and do registration
    try {
      for (TriggerInformation triggerInformation : triggerInformationList) {
        TriggerManagementService.getInstance().doRegister(triggerInformation);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  private void getJarOfTriggers(List<TriggerInformation> triggerInformationList)
      throws StartupException {
    try (ConfigNodeClient configNodeClient = new ConfigNodeClient()) {
      List<String> triggerNameList =
          triggerInformationList.stream()
              .map(TriggerInformation::getTriggerName)
              .collect(Collectors.toList());
      List<ByteBuffer> jarList =
          configNodeClient.getTriggerJar(new TGetTriggerJarReq(triggerNameList)).getJarList();
      // TODO getTriggerJar maybe error
      for (int i = 0, n = triggerInformationList.size(); i < n; i++) {
        TriggerExecutableManager.getInstance()
            .writeToLibDir(jarList.get(i), triggerInformationList.get(i).getJarName());
      }
    } catch (IOException | TException e) {
      throw new StartupException(e);
    }
  }

  private List<TriggerInformation> getTriggerNeedJarList() {
    List<TriggerInformation> res = new ArrayList<>();
    for (TriggerInformation triggerInformation : triggerInformationList) {
      if (triggerInformation.isStateful()) {
        // jar of stateful trigger is not needed
        continue;
      }
      // jar does not exist, add current triggerName to list
      if (!TriggerExecutableManager.getInstance()
          .hasFileUnderLibRoot(triggerInformation.getJarName())) {
        res.add(triggerInformation);
      } else {
        try {
          // local jar has conflicts with jar on config node, add current triggerName to list
          if (!TriggerManagementService.getInstance().isLocalJarCorrect(triggerInformation)) {
            res.add(triggerInformation);
          }
        } catch (TriggerManagementException e) {
          res.add(triggerInformation);
        }
      }
    }
    return res;
  }

  private void getTriggerInformationList(List<ByteBuffer> allTriggerInformation) {
    if (allTriggerInformation != null && !allTriggerInformation.isEmpty()) {
      List<TriggerInformation> list = new ArrayList<>();
      for (ByteBuffer triggerInformationByteBuffer : allTriggerInformation) {
        list.add(TriggerInformation.deserialize(triggerInformationByteBuffer));
      }
      this.triggerInformationList = list;
    }
  }

  private void initSchemaEngine() {
    long time = System.currentTimeMillis();
    SchemaEngine.getInstance().init();
    long end = System.currentTimeMillis() - time;
    logger.info("Spent {}ms to recover schema.", end);
    logger.info(
        "After initializing, sequence tsFile threshold is {}, unsequence tsFile threshold is {}",
        config.getSeqTsFileSize(),
        config.getUnSeqTsFileSize());
  }

  public void stop() {
    deactivate();

    try {
      MetricService.getInstance().stop();
      SchemaRegionConsensusImpl.getInstance().stop();
      DataRegionConsensusImpl.getInstance().stop();
    } catch (Exception e) {
      logger.error("Stop data node error", e);
    }
  }

  private void initServiceProvider() throws QueryProcessException {
    serviceProvider = new StandaloneServiceProvider();
  }

  private void initProtocols() throws StartupException {
    if (config.isEnableInfluxDBRpcService()) {
      registerManager.register(InfluxDBRPCService.getInstance());
      IoTDB.initInfluxDBMManager();
    }
    if (config.isEnableMQTTService()) {
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
