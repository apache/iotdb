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
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.IoTDBStartCheck;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.cache.CacheHitRatioMonitor;
import org.apache.iotdb.db.engine.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.engine.flush.FlushManager;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.db.metadata.template.ClusterTemplateManager;
import org.apache.iotdb.db.mpp.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.mpp.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.protocol.rest.RestService;
import org.apache.iotdb.db.service.metrics.DataNodeMetricsHelper;
import org.apache.iotdb.db.service.metrics.IoTDBInternalLocalReporter;
import org.apache.iotdb.db.service.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.service.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.sync.SyncService;
import org.apache.iotdb.db.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.trigger.service.TriggerInformationUpdater;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;
import org.apache.iotdb.db.wal.WALManager;
import org.apache.iotdb.db.wal.utils.WALMode;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.DEFAULT_CLUSTER_NAME;

public class DataNode implements DataNodeMBean {

  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final String mbeanName =
      String.format(
          "%s:%s=%s", "org.apache.iotdb.datanode.service", IoTDBConstant.JMX_TYPE, "DataNode");

  private static final File SYSTEM_PROPERTIES =
      SystemFileFactory.INSTANCE.getFile(
          config.getSchemaDir() + File.separator + IoTDBStartCheck.PROPERTIES_FILE_NAME);

  /**
   * when joining a cluster or getting configuration this node will retry at most "DEFAULT_RETRY"
   * times before returning a failure to the client
   */
  private static final int DEFAULT_RETRY = 10;

  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = config.getJoinClusterRetryIntervalMs();

  private final TEndPoint thisNode = new TEndPoint();

  /** Hold the information of trigger, udf...... */
  private final ResourcesInformationHolder resourcesInformationHolder =
      new ResourcesInformationHolder();

  /** Responsible for keeping trigger information up to date */
  private final TriggerInformationUpdater triggerInformationUpdater =
      new TriggerInformationUpdater();

  private DataNode() {
    // we do not init anything here, so that we can re-initialize the instance in IT.
  }

  private static final RegisterManager registerManager = new RegisterManager();

  public static DataNode getInstance() {
    return DataNodeHolder.INSTANCE;
  }

  public static void main(String[] args) {
    logger.info("IoTDB-DataNode environment variables: " + IoTDBConfig.getEnvironmentVariables());
    new DataNodeServerCommandLine().doMain(args);
  }

  protected void doAddNode() {
    boolean isFirstStart = false;
    try {
      // Check if this DataNode is start for the first time and do other pre-checks
      isFirstStart = prepareDataNode();

      // Set target ConfigNodeList from iotdb-datanode.properties file
      ConfigNodeInfo.getInstance().updateConfigNodeList(config.getTargetConfigNodeList());

      // Pull and check system configurations from ConfigNode-leader
      pullAndCheckSystemConfigurations();

      if (isFirstStart) {
        // Register this DataNode to the cluster when first start
        sendRegisterRequestToConfigNode();
      } else {
        // Send restart request of this DataNode
        sendRestartRequestToConfigNode();
      }

      // Active DataNode
      active();

      // Setup rpc service
      setUpRPCService();

      // Setup metric service
      setUpMetricService();

      // Serialize mutable system properties
      IoTDBStartCheck.getInstance().serializeMutableSystemPropertiesIfNecessary();

      logger.info("IoTDB configuration: " + config.getConfigMessage());
      logger.info("Congratulation, IoTDB DataNode is set up successfully. Now, enjoy yourself!");

    } catch (StartupException | ConfigurationException | IOException e) {
      logger.error("Fail to start server", e);
      if (isFirstStart) {
        // Delete the system.properties file when first start failed.
        // Therefore, the next time this DataNode is start will still be seen as the first time.
        SYSTEM_PROPERTIES.deleteOnExit();
      }
      stop();
    }
  }

  /** Prepare cluster IoTDB-DataNode */
  private boolean prepareDataNode() throws StartupException, ConfigurationException, IOException {
    // Set cluster mode
    config.setClusterMode(true);

    // Notice: Consider this DataNode as first start if the system.properties file doesn't exist
    boolean isFirstStart = IoTDBStartCheck.getInstance().checkIsFirstStart();

    // Check target ConfigNodes
    for (TEndPoint endPoint : config.getTargetConfigNodeList()) {
      if (endPoint.getIp().equals("0.0.0.0")) {
        throw new StartupException(
            "The ip address of any target_config_node_list couldn't be 0.0.0.0");
      }
    }

    // Set this node
    thisNode.setIp(config.getInternalAddress());
    thisNode.setPort(config.getInternalPort());

    // Startup checks
    StartupChecks checks = new StartupChecks(IoTDBConstant.DN_ROLE).withDefaultTest();
    checks.verify();

    return isFirstStart;
  }

  /**
   * Pull and check the following system configurations:
   *
   * <p>1. GlobalConfig
   *
   * <p>2. RatisConfig
   *
   * <p>3. CQConfig
   *
   * @throws StartupException When failed connect to ConfigNode-leader
   */
  private void pullAndCheckSystemConfigurations() throws StartupException {
    logger.info("Pulling system configurations from the ConfigNode-leader...");

    /* Pull system configurations */
    int retry = DEFAULT_RETRY;
    TSystemConfigurationResp configurationResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        configurationResp = configNodeClient.getSystemConfiguration();
        break;
      } catch (TException | ClientManagerException e) {
        // Read ConfigNodes from system.properties and retry
        logger.warn(
            "Cannot pull system configurations from ConfigNode-leader, because: {}",
            e.getMessage());
        ConfigNodeInfo.getInstance().loadConfigNodeList();
        retry--;
      }

      try {
        // wait to start the next try
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to register to the cluster", e);
        break;
      }
    }
    if (configurationResp == null) {
      // All tries failed
      logger.error(
          "Cannot pull system configurations from ConfigNode-leader after {} retries",
          DEFAULT_RETRY);
      throw new StartupException("Cannot pull system configurations from ConfigNode-leader");
    }

    /* Load system configurations */
    IoTDBDescriptor.getInstance().loadGlobalConfig(configurationResp.globalConfig);
    IoTDBDescriptor.getInstance().loadRatisConfig(configurationResp.ratisConfig);
    IoTDBDescriptor.getInstance().loadCQConfig(configurationResp.cqConfig);
    CommonDescriptor.getInstance().loadGlobalConfig(configurationResp.globalConfig);

    /* Set cluster consensus protocol class */
    if (!IoTDBStartCheck.getInstance()
        .checkConsensusProtocolExists(TConsensusGroupType.DataRegion)) {
      config.setDataRegionConsensusProtocolClass(
          configurationResp.globalConfig.getDataRegionConsensusProtocolClass());
    }

    if (!IoTDBStartCheck.getInstance()
        .checkConsensusProtocolExists(TConsensusGroupType.SchemaRegion)) {
      config.setSchemaRegionConsensusProtocolClass(
          configurationResp.globalConfig.getSchemaRegionConsensusProtocolClass());
    }

    /* Check system configurations */
    try {
      IoTDBStartCheck.getInstance().checkSystemConfig();
      IoTDBStartCheck.getInstance().checkDirectory();
      IoTDBStartCheck.getInstance().serializeGlobalConfig(configurationResp.globalConfig);
      if (!config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
        // In current implementation, only IoTConsensus need separated memory from Consensus
        IoTDBDescriptor.getInstance().reclaimConsensusMemory();
      }
    } catch (Exception e) {
      throw new StartupException(e.getMessage());
    }

    logger.info("Successfully pull system configurations from ConfigNode-leader.");
  }

  /**
   * Store runtime configurations, which includes:
   *
   * <p>1. All ConfigNodes in cluster
   *
   * <p>2. All template information
   *
   * <p>3. All UDF information
   *
   * <p>4. All trigger information
   *
   * <p>5. All TTL information
   */
  private void storeRuntimeConfigurations(
      List<TConfigNodeLocation> configNodeLocations, TRuntimeConfiguration runtimeConfiguration) {
    /* Store ConfigNodeList */
    List<TEndPoint> configNodeList = new ArrayList<>();
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      configNodeList.add(configNodeLocation.getInternalEndPoint());
    }
    ConfigNodeInfo.getInstance().updateConfigNodeList(configNodeList);

    /* Store templateSetInfo */
    ClusterTemplateManager.getInstance()
        .updateTemplateSetInfo(runtimeConfiguration.getTemplateInfo());

    /* Store udfInformationList */
    getUDFInformationList(runtimeConfiguration.getAllUDFInformation());

    /* Store triggerInformationList */
    getTriggerInformationList(runtimeConfiguration.getAllTriggerInformation());

    /* Store ttl information */
    StorageEngine.getInstance().updateTTLInfo(runtimeConfiguration.getAllTTLInformation());
  }

  /** Register this DataNode into cluster */
  private void sendRegisterRequestToConfigNode() throws StartupException, IOException {
    logger.info("Sending register request to ConfigNode-leader...");

    /* Send register request */
    int retry = DEFAULT_RETRY;
    TDataNodeRegisterReq req = new TDataNodeRegisterReq();
    req.setDataNodeConfiguration(generateDataNodeConfiguration());
    req.setClusterName(config.getClusterName());
    TDataNodeRegisterResp dataNodeRegisterResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeRegisterResp = configNodeClient.registerDataNode(req);
        break;
      } catch (TException | ClientManagerException e) {
        // Read ConfigNodes from system.properties and retry
        logger.warn("Cannot register to the cluster, because: {}", e.getMessage());
        ConfigNodeInfo.getInstance().loadConfigNodeList();
        retry--;
      }

      try {
        // wait to start the next try
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to register to the cluster", e);
        break;
      }
    }
    if (dataNodeRegisterResp == null) {
      // All tries failed
      logger.error(
          "Cannot register into cluster after {} retries. "
              + "Please check dn_target_config_node_list in iotdb-datanode.properties.",
          DEFAULT_RETRY);
      throw new StartupException("Cannot register into the cluster.");
    }

    if (dataNodeRegisterResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {

      /* Store runtime configurations when register success */
      int dataNodeID = dataNodeRegisterResp.getDataNodeId();
      config.setDataNodeId(dataNodeID);
      IoTDBStartCheck.getInstance()
          .serializeClusterNameAndDataNodeId(config.getClusterName(), dataNodeID);

      storeRuntimeConfigurations(
          dataNodeRegisterResp.getConfigNodeList(), dataNodeRegisterResp.getRuntimeConfiguration());

      logger.info("Successfully register to the cluster: {}", config.getClusterName());
    } else {
      /* Throw exception when register failed */
      logger.error(dataNodeRegisterResp.getStatus().getMessage());
      throw new StartupException("Cannot register to the cluster.");
    }
  }

  private void sendRestartRequestToConfigNode() throws StartupException {
    logger.info("Sending restart request to ConfigNode-leader...");

    /* Send restart request */
    int retry = DEFAULT_RETRY;
    TDataNodeRestartReq req = new TDataNodeRestartReq();
    req.setClusterName(
        config.getClusterName() == null ? DEFAULT_CLUSTER_NAME : config.getClusterName());
    req.setDataNodeConfiguration(generateDataNodeConfiguration());
    TDataNodeRestartResp dataNodeRestartResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
          ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeRestartResp = configNodeClient.restartDataNode(req);
        break;
      } catch (TException | ClientManagerException e) {
        // Read ConfigNodes from system.properties and retry
        logger.warn(
            "Cannot send restart request to the ConfigNode-leader, because: {}", e.getMessage());
        ConfigNodeInfo.getInstance().loadConfigNodeList();
        retry--;
      }

      try {
        // wait to start the next try
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Unexpected interruption when waiting to register to the cluster", e);
        break;
      }
    }
    if (dataNodeRestartResp == null) {
      // All tries failed
      logger.error(
          "Cannot send restart DataNode request to ConfigNode-leader after {} retries. "
              + "Please check dn_target_config_node_list in iotdb-datanode.properties.",
          DEFAULT_RETRY);
      throw new StartupException("Cannot send restart DataNode request to ConfigNode-leader.");
    }

    if (dataNodeRestartResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      /* Store runtime configurations when restart request is accepted */
      storeRuntimeConfigurations(
          dataNodeRestartResp.getConfigNodeList(), dataNodeRestartResp.getRuntimeConfiguration());
      logger.info("Restart request to cluster: {} is accepted.", config.getClusterName());
    } else {
      /* Throw exception when restart is rejected */
      throw new StartupException(dataNodeRestartResp.getStatus().getMessage());
    }
  }

  private void prepareResources() throws StartupException {
    prepareUDFResources();
    prepareTriggerResources();
  }

  /** register services and set up DataNode */
  private void active() throws StartupException {
    try {
      processPid();
      setUp();
    } catch (StartupException | QueryProcessException e) {
      logger.error("Meet error while starting up.", e);
      throw new StartupException("Error in activating IoTDB DataNode.");
    }
    logger.info("IoTDB DataNode has started.");

    try {
      SchemaRegionConsensusImpl.setupAndGetInstance().start();
      DataRegionConsensusImpl.setupAndGetInstance().start();
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  void processPid() {
    String pidFile = System.getProperty(IoTDBConstant.IOTDB_PIDFILE);
    if (pidFile != null) {
      new File(pidFile).deleteOnExit();
    }
  }

  private void setUp() throws StartupException, QueryProcessException {
    logger.info("Setting up IoTDB DataNode...");
    registerManager.register(new JMXService());
    JMXService.registerMBean(getInstance(), mbeanName);

    // get resources for trigger,udf...
    prepareResources();

    Runtime.getRuntime().addShutdownHook(new IoTDBShutdownHook());
    setUncaughtExceptionHandler();

    logger.info("Recover the schema...");
    initSchemaEngine();
    registerManager.register(FlushManager.getInstance());
    registerManager.register(CacheHitRatioMonitor.getInstance());

    // close wal when using ratis consensus
    if (config.isClusterMode()
        && config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      config.setWalMode(WALMode.DISABLE);
    }
    registerManager.register(WALManager.getInstance());

    // in mpp mode we need to start some other services
    registerManager.register(StorageEngine.getInstance());
    registerManager.register(MPPDataExchangeService.getInstance());
    registerManager.register(DriverScheduler.getInstance());

    registerUdfServices();

    logger.info(
        "IoTDB DataNode is setting up, some databases may not be ready now, please wait several seconds...");

    while (!StorageEngine.getInstance().isAllSgReady()) {
      try {
        TimeUnit.MILLISECONDS.sleep(1000);
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

    // start region migrate service
    registerManager.register(RegionMigrateService.getInstance());

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

  private void setUpMetricService() throws StartupException {
    registerManager.register(MetricService.getInstance());

    // init metric service
    if (MetricConfigDescriptor.getInstance()
        .getMetricConfig()
        .getInternalReportType()
        .equals(InternalReporterType.IOTDB)) {
      MetricService.getInstance().updateInternalReporter(new IoTDBInternalLocalReporter());
    }
    MetricService.getInstance().startInternalReporter();
    // bind predefined metrics
    DataNodeMetricsHelper.bind();
  }

  public static TDataNodeLocation generateDataNodeLocation() {
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
    return location;
  }

  /**
   * generate dataNodeConfiguration
   *
   * @return TDataNodeConfiguration
   */
  private TDataNodeConfiguration generateDataNodeConfiguration() {
    // Set DataNodeLocation
    TDataNodeLocation location = generateDataNodeLocation();

    // Set NodeResource
    TNodeResource resource = new TNodeResource();
    resource.setCpuCoreNum(Runtime.getRuntime().availableProcessors());
    resource.setMaxMemory(Runtime.getRuntime().totalMemory());

    return new TDataNodeConfiguration(location, resource);
  }

  private void registerUdfServices() throws StartupException {
    registerManager.register(TemporaryQueryDataFileService.getInstance());
    registerManager.register(UDFClassLoaderManager.setupAndGetInstance(config.getUdfDir()));
  }

  private void initUDFRelatedInstance() throws StartupException {
    try {
      UDFExecutableManager.setupAndGetInstance(config.getUdfTemporaryLibDir(), config.getUdfDir());
      UDFClassLoaderManager.setupAndGetInstance(config.getUdfDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private void prepareUDFResources() throws StartupException {
    initUDFRelatedInstance();
    if (resourcesInformationHolder.getUDFInformationList() == null
        || resourcesInformationHolder.getUDFInformationList().isEmpty()) {
      return;
    }

    // get jars from config node
    List<UDFInformation> udfNeedJarList = getJarListForUDF();
    int index = 0;
    while (index < udfNeedJarList.size()) {
      List<UDFInformation> curList = new ArrayList<>();
      int offset = 0;
      while (offset < ResourcesInformationHolder.getJarNumOfOneRpc()
          && index + offset < udfNeedJarList.size()) {
        curList.add(udfNeedJarList.get(index + offset));
        offset++;
      }
      index += (offset + 1);
      getJarOfUDFs(curList);
    }

    // create instances of triggers and do registration
    try {
      for (UDFInformation udfInformation : resourcesInformationHolder.getUDFInformationList()) {
        UDFManagementService.getInstance().doRegister(udfInformation);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }

    logger.debug("successfully registered all the UDFs");
    for (UDFInformation udfInformation :
        UDFManagementService.getInstance().getAllUDFInformation()) {
      logger.debug("get udf: {}", udfInformation.getFunctionName());
    }
  }

  private void getJarOfUDFs(List<UDFInformation> udfInformationList) throws StartupException {
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      List<String> jarNameList =
          udfInformationList.stream().map(UDFInformation::getJarName).collect(Collectors.toList());
      TGetJarInListResp resp = configNodeClient.getUDFJar(new TGetJarInListReq(jarNameList));
      if (resp.getStatus().getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
        throw new StartupException("Failed to get UDF jar from config node.");
      }
      List<ByteBuffer> jarList = resp.getJarList();
      for (int i = 0; i < udfInformationList.size(); i++) {
        UDFExecutableManager.getInstance()
            .saveToInstallDir(jarList.get(i), udfInformationList.get(i).getJarName());
      }
    } catch (IOException | TException | ClientManagerException e) {
      throw new StartupException(e);
    }
  }

  /** Generate a list for UDFs that do not have jar on this node. */
  private List<UDFInformation> getJarListForUDF() {
    List<UDFInformation> res = new ArrayList<>();
    for (UDFInformation udfInformation : resourcesInformationHolder.getUDFInformationList()) {
      if (udfInformation.isUsingURI()) {
        // jar does not exist, add current triggerInformation to list
        if (!UDFExecutableManager.getInstance()
            .hasFileUnderInstallDir(udfInformation.getJarName())) {
          res.add(udfInformation);
        } else {
          try {
            // local jar has conflicts with jar on config node, add current triggerInformation to
            // list
            if (UDFManagementService.getInstance().isLocalJarConflicted(udfInformation)) {
              res.add(udfInformation);
            }
          } catch (UDFManagementException e) {
            res.add(udfInformation);
          }
        }
      }
    }
    return res;
  }

  private void getUDFInformationList(List<ByteBuffer> allUDFInformation) {
    if (allUDFInformation != null && !allUDFInformation.isEmpty()) {
      List<UDFInformation> list = new ArrayList<>();
      for (ByteBuffer UDFInformationByteBuffer : allUDFInformation) {
        list.add(UDFInformation.deserialize(UDFInformationByteBuffer));
      }
      resourcesInformationHolder.setUDFInformationList(list);
    }
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
    if (resourcesInformationHolder.getTriggerInformationList() == null
        || resourcesInformationHolder.getTriggerInformationList().isEmpty()) {
      return;
    }

    // get jars from config node
    List<TriggerInformation> triggerNeedJarList = getJarListForTrigger();
    int index = 0;
    while (index < triggerNeedJarList.size()) {
      List<TriggerInformation> curList = new ArrayList<>();
      int offset = 0;
      while (offset < ResourcesInformationHolder.getJarNumOfOneRpc()
          && index + offset < triggerNeedJarList.size()) {
        curList.add(triggerNeedJarList.get(index + offset));
        offset++;
      }
      index += (offset + 1);
      getJarOfTriggers(curList);
    }

    // create instances of triggers and do registration
    try {
      for (TriggerInformation triggerInformation :
          resourcesInformationHolder.getTriggerInformationList()) {
        TriggerManagementService.getInstance().doRegister(triggerInformation, true);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }
    logger.debug("successfully registered all the triggers");
    for (TriggerInformation triggerInformation :
        TriggerManagementService.getInstance().getAllTriggerInformationInTriggerTable()) {
      logger.debug("get trigger: {}", triggerInformation.getTriggerName());
    }
    for (TriggerExecutor triggerExecutor :
        TriggerManagementService.getInstance().getAllTriggerExecutors()) {
      logger.debug(
          "get trigger executor: {}", triggerExecutor.getTriggerInformation().getTriggerName());
    }

    // start TriggerInformationUpdater
    triggerInformationUpdater.startTriggerInformationUpdater();
  }

  private void getJarOfTriggers(List<TriggerInformation> triggerInformationList)
      throws StartupException {
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      List<String> jarNameList =
          triggerInformationList.stream()
              .map(TriggerInformation::getJarName)
              .collect(Collectors.toList());
      TGetJarInListResp resp = configNodeClient.getTriggerJar(new TGetJarInListReq(jarNameList));
      if (resp.getStatus().getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
        throw new StartupException("Failed to get trigger jar from config node.");
      }
      List<ByteBuffer> jarList = resp.getJarList();
      for (int i = 0; i < triggerInformationList.size(); i++) {
        TriggerExecutableManager.getInstance()
            .saveToInstallDir(jarList.get(i), triggerInformationList.get(i).getJarName());
      }
    } catch (IOException | TException | ClientManagerException e) {
      throw new StartupException(e);
    }
  }

  /** Generate a list for triggers that do not have jar on this node. */
  private List<TriggerInformation> getJarListForTrigger() {
    List<TriggerInformation> res = new ArrayList<>();
    for (TriggerInformation triggerInformation :
        resourcesInformationHolder.getTriggerInformationList()) {
      if (triggerInformation.isUsingURI()) {
        // jar does not exist, add current triggerInformation to list
        if (!TriggerExecutableManager.getInstance()
            .hasFileUnderInstallDir(triggerInformation.getJarName())) {
          res.add(triggerInformation);
        } else {
          try {
            // local jar has conflicts with jar on config node, add current triggerInformation to
            // list
            if (TriggerManagementService.getInstance().isLocalJarConflicted(triggerInformation)) {
              res.add(triggerInformation);
            }
          } catch (TriggerManagementException e) {
            res.add(triggerInformation);
          }
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
      resourcesInformationHolder.setTriggerInformationList(list);
    }
  }

  private void initSchemaEngine() {
    long time = System.currentTimeMillis();
    SchemaEngine.getInstance().init();
    long end = System.currentTimeMillis() - time;
    logger.info("Spent {}ms to recover schema.", end);
  }

  public void stop() {
    deactivate();

    try {
      MetricService.getInstance().stop();
      if (SchemaRegionConsensusImpl.getInstance() != null) {
        SchemaRegionConsensusImpl.getInstance().stop();
      }
      if (DataRegionConsensusImpl.getInstance() != null) {
        DataRegionConsensusImpl.getInstance().stop();
      }
    } catch (Exception e) {
      logger.error("Stop data node error", e);
    }
  }

  private void initProtocols() throws StartupException {
    if (config.isEnableInfluxDBRpcService()) {
      registerManager.register(InfluxDBRPCService.getInstance());
    }
    if (config.isEnableMQTTService()) {
      registerManager.register(MQTTService.getInstance());
    }
    if (IoTDBRestServiceDescriptor.getInstance().getConfig().isEnableRestService()) {
      registerManager.register(RestService.getInstance());
    }
  }

  private void deactivate() {
    logger.info("Deactivating IoTDB DataNode...");
    stopTriggerRelatedServices();
    // stopThreadPools();
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB DataNode is deactivated.");
  }

  private void stopTriggerRelatedServices() {
    triggerInformationUpdater.stopTriggerInformationUpdater();
  }

  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  private static class DataNodeHolder {

    private static final DataNode INSTANCE = new DataNode();

    private DataNodeHolder() {}
  }
}
