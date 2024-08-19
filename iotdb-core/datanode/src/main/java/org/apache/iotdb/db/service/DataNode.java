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
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemPropertiesHandler;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRemoveResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.db.conf.DataNodeStartupCheck;
import org.apache.iotdb.db.conf.DataNodeSystemPropertiesHandler;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.IoTDBStartCheck;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.qp.sql.IoTDBSqlParser;
import org.apache.iotdb.db.qp.sql.SqlLexer;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.queryengine.plan.parser.ASTVisitor;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LogicalPlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.DistributionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.distribution.SourceRewriter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.service.metrics.DataNodeMetricsHelper;
import org.apache.iotdb.db.service.metrics.IoTDBInternalLocalReporter;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.buffer.CacheHitRatioMonitor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionScheduleTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.trigger.service.TriggerInformationUpdater;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;
import org.apache.iotdb.metrics.config.MetricConfig;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.thrift.TException;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.DEFAULT_CLUSTER_NAME;
import static org.apache.iotdb.db.conf.IoTDBStartCheck.PROPERTIES_FILE_NAME;

public class DataNode extends ServerCommandLine implements DataNodeMBean {

  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          IoTDBConstant.IOTDB_SERVICE_JMX_NAME,
          IoTDBConstant.JMX_TYPE,
          ServiceType.DATA_NODE.getJmxName());

  /**
   * When joining a cluster or getting configuration this node will retry at most "DEFAULT_RETRY"
   * times before returning a failure to the client.
   */
  private static final int DEFAULT_RETRY = 200;

  private final IoTDBDescriptor descriptor;
  private final CommonDescriptor commonDescriptor;
  private final ConfigNodeInfo configNodeInfo;
  private final IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager;
  private final IoTDBStartCheck startCheck;
  private final IConsensus schemaRegionConsensus;
  private final IConsensus dataRegionConsensus;
  private final SchemaEngine schemaEngine;
  private final StorageEngine storageEngine;
  private final MetricService metricService;
  private final SystemPropertiesHandler dataNodeSystemPropertiesHandler;
  private final TierManager tierManager;
  private final FlushManager flushManager;
  private final WALManager walManager;
  private final CompactionScheduleTaskManager compactionScheduleTaskManager;
  private final ClusterTemplateManager clusterTemplateManager;
  private final DataNodeRegionManager dataNodeRegionManager;
  private final DataNodeTableCache dataNodeTableCache;
  private final CacheHitRatioMonitor cacheHitRatioMonitor;
  private final MPPDataExchangeService mppDataExchangeService;
  private final DriverScheduler driverScheduler;
  private final RegionMigrateService regionMigrateService;
  private final CompactionTaskManager compactionTaskManager;
  private final DataNodeInternalRPCService dataNodeInternalRPCService;
  private final RPCService rpcService;
  private final MetricConfigDescriptor metricConfigDescriptor;
  private final TemporaryQueryDataFileService temporaryQueryDataFileService;
  private final UDFManagementService udfManagementService;
  private final UDFExecutableManager udfExecutableManager;
  private final TriggerManagementService triggerManagementService;
  private final TriggerExecutableManager triggerExecutableManager;
  private final DataNodeTTLCache dataNodeTTLCache;
  private final MQTTService mqttService;
  private final RestService restService;
  private final IoTDBRestServiceDescriptor restServiceDescriptor;
  private final PipeConfig pipeConfig;

  private final long defaultRetryIntervalInMs;

  private final TEndPoint thisNode = new TEndPoint();

  /** Hold the information of trigger, udf...... */
  private final ResourcesInformationHolder resourcesInformationHolder =
      new ResourcesInformationHolder();

  /** Responsible for keeping trigger information up to date. */
  private final TriggerInformationUpdater triggerInformationUpdater =
      new TriggerInformationUpdater();

  private static final String REGISTER_INTERRUPTION =
      "Unexpected interruption when waiting to register to the cluster";

  private boolean schemaRegionConsensusStarted = false;
  private boolean dataRegionConsensusStarted = false;

  /** Default constructor using the singletons for initializing the relationship. */
  public DataNode() {
    this(
        // Initialized in the oder of which they were initialized before.
        CommonDescriptor.getInstance(),
        IoTDBDescriptor.getInstance(),
        MetricConfigDescriptor.getInstance(),
        DataNodeSystemPropertiesHandler.getInstance(),
        ConfigNodeInfo.getInstance(),
        IoTDBStartCheck.getInstance(),
        WALManager.getInstance(),
        ClusterTemplateManager.getInstance(),
        DataNodeTTLCache.getInstance(),
        DataNodeTableCache.getInstance(),
        TierManager.getInstance(),
        UDFExecutableManager.getInstance(),
        TriggerExecutableManager.getInstance(),
        PipeConfig.getInstance(),
        SchemaEngine.getInstance(),
        MetricService.getInstance(),
        FlushManager.getInstance(),
        CacheHitRatioMonitor.getInstance(),
        CompactionScheduleTaskManager.getInstance(),
        StorageEngine.getInstance(),
        MPPDataExchangeService.getInstance(),
        DriverScheduler.getInstance(),
        TemporaryQueryDataFileService.getInstance(),
        DataNodeRegionManager.getInstance(),
        RegionMigrateService.getInstance(),
        CompactionTaskManager.getInstance(),
        DataNodeInternalRPCService.getInstance(),
        RPCService.getInstance(),
        IoTDBRestServiceDescriptor.getInstance(),
        TriggerManagementService.getInstance(),
        // The following I don't know the oder they come in.
        ConfigNodeClientManager.getInstance(),
        SchemaRegionConsensusImpl.getInstance(),
        DataRegionConsensusImpl.getInstance(),
        UDFManagementService.getInstance(),
        MQTTService.getInstance(),
        RestService.getInstance());
  }

  /**
   * Additional constructor allowing injection of custom instances (mainly for testing)
   *
   * @param configNodeInfo config node info
   * @param configNodeClientManager config node client manager
   */
  public DataNode(
      CommonDescriptor commonDescriptor,
      IoTDBDescriptor descriptor,
      MetricConfigDescriptor metricConfigDescriptor,
      SystemPropertiesHandler dataNodeSystemPropertiesHandler,
      ConfigNodeInfo configNodeInfo,
      IoTDBStartCheck startCheck,
      WALManager walManager,
      ClusterTemplateManager clusterTemplateManager,
      DataNodeTTLCache dataNodeTTLCache,
      DataNodeTableCache dataNodeTableCache,
      TierManager tierManager,
      UDFExecutableManager udfExecutableManager,
      TriggerExecutableManager triggerExecutableManager,
      PipeConfig pipeConfig,
      SchemaEngine schemaEngine,
      MetricService metricService,
      FlushManager flushManager,
      CacheHitRatioMonitor cacheHitRatioMonitor,
      CompactionScheduleTaskManager compactionScheduleTaskManager,
      StorageEngine storageEngine,
      MPPDataExchangeService mppDataExchangeService,
      DriverScheduler driverScheduler,
      TemporaryQueryDataFileService temporaryQueryDataFileService,
      DataNodeRegionManager dataNodeRegionManager,
      RegionMigrateService regionMigrateService,
      CompactionTaskManager compactionTaskManager,
      DataNodeInternalRPCService dataNodeInternalRPCService,
      RPCService rpcService,
      IoTDBRestServiceDescriptor restServiceDescriptor,
      TriggerManagementService triggerManagementService,
      // The following I don't know the oder they come in.
      IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager,
      IConsensus schemaRegionConsensus,
      IConsensus dataRegionConsensus,
      UDFManagementService udfManagementService,
      MQTTService mqttService,
      RestService restService) {
    super("DataNode");
    this.descriptor = descriptor;
    this.commonDescriptor = commonDescriptor;
    this.configNodeInfo = configNodeInfo;
    this.configNodeClientManager = configNodeClientManager;
    this.startCheck = startCheck;
    this.schemaRegionConsensus = schemaRegionConsensus;
    this.dataRegionConsensus = dataRegionConsensus;
    this.schemaEngine = schemaEngine;
    this.storageEngine = storageEngine;
    this.metricService = metricService;
    this.dataNodeSystemPropertiesHandler = dataNodeSystemPropertiesHandler;
    this.tierManager = tierManager;
    this.flushManager = flushManager;
    this.walManager = walManager;
    this.compactionScheduleTaskManager = compactionScheduleTaskManager;
    this.clusterTemplateManager = clusterTemplateManager;
    this.dataNodeRegionManager = dataNodeRegionManager;
    this.dataNodeTableCache = dataNodeTableCache;
    this.cacheHitRatioMonitor = cacheHitRatioMonitor;
    this.mppDataExchangeService = mppDataExchangeService;
    this.driverScheduler = driverScheduler;
    this.regionMigrateService = regionMigrateService;
    this.compactionTaskManager = compactionTaskManager;
    this.dataNodeInternalRPCService = dataNodeInternalRPCService;
    this.rpcService = rpcService;
    this.metricConfigDescriptor = metricConfigDescriptor;
    this.temporaryQueryDataFileService = temporaryQueryDataFileService;
    this.udfManagementService = udfManagementService;
    this.udfExecutableManager = udfExecutableManager;
    this.triggerManagementService = triggerManagementService;
    this.triggerExecutableManager = triggerExecutableManager;
    this.dataNodeTTLCache = dataNodeTTLCache;
    this.mqttService = mqttService;
    this.restService = restService;
    this.restServiceDescriptor = restServiceDescriptor;
    this.pipeConfig = pipeConfig;

    this.defaultRetryIntervalInMs = descriptor.getConfig().getJoinClusterRetryIntervalMs();
    // Save this instance in the singleton.
    setInstance(this);
  }

  // TODO: This needs removal of statics ...
  public void reinitializeStatics() {
    registerManager = new RegisterManager();
    dataNodeSystemPropertiesHandler.resetFilePath(
        descriptor.getConfig().getSystemDir() + File.separator + PROPERTIES_FILE_NAME);
  }

  private static RegisterManager registerManager = new RegisterManager();

  public static void main(String[] args) throws Exception {
    logger.info("IoTDB-DataNode environment variables: {}", IoTDBConfig.getEnvironmentVariables());
    logger.info("IoTDB-DataNode default charset is: {}", Charset.defaultCharset().displayName());
    DataNode dataNode = new DataNode();
    int returnCode = dataNode.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
  }

  @Override
  protected void start() {
    boolean isFirstStart;
    try {
      // Check if this DataNode is start for the first time and do other pre-checks
      isFirstStart = prepareDataNode();

      if (isFirstStart) {
        logger.info("DataNode is starting for the first time...");
        configNodeInfo.updateConfigNodeList(
            Collections.singletonList(descriptor.getConfig().getSeedConfigNode()));
      } else {
        logger.info("DataNode is restarting...");
        // Load registered ConfigNodes from system.properties
        configNodeInfo.loadConfigNodeList();
      }

      // Pull and check system configurations from ConfigNode-leader
      pullAndCheckSystemConfigurations();

      if (isFirstStart) {
        sendRegisterRequestToConfigNode(true);
        startCheck.generateOrOverwriteSystemPropertiesFile();
        configNodeInfo.storeConfigNodeList();
        // Register this DataNode to the cluster when first start
        sendRegisterRequestToConfigNode(false);
      } else {
        // Send restart request of this DataNode
        sendRestartRequestToConfigNode();
      }
      // TierManager need DataNodeId to do some operations so the reset method need to be invoked
      // after DataNode adding
      tierManager.resetFolders();
      // Active DataNode
      active();

      // Setup metric service
      setUpMetricService();

      // Setup rpc service
      setUpRPCService();

      // Serialize mutable system properties
      startCheck.serializeMutableSystemPropertiesIfNecessary();

      logger.info("IoTDB configuration: {}", descriptor.getConfig().getConfigMessage());
      logger.info("Congratulations, IoTDB DataNode is set up successfully. Now, enjoy yourself!");

    } catch (StartupException | IOException e) {
      logger.error("Fail to start server", e);
      stop();
      System.exit(-1);
    }
  }

  @Override
  protected void remove(Long nodeId) throws IoTDBException {
    // If the nodeId was null, this is a shorthand for removing the current dataNode.
    // In this case we need to find our nodeId.
    if (nodeId == null) {
      nodeId = (long) descriptor.getConfig().getDataNodeId();
    }

    logger.info("Starting to remove DataNode with node-id {} from cluster", nodeId);

    // Load ConfigNodeList from system.properties file
    configNodeInfo.loadConfigNodeList();

    int removeNodeId = nodeId.intValue();
    try (ConfigNodeClient configNodeClient =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      // Find a datanode location with the given node id.
      Optional<TDataNodeLocation> dataNodeLocationOpt =
          configNodeClient
              .getDataNodeConfiguration(-1)
              .getDataNodeConfigurationMap()
              .values()
              .stream()
              .map(TDataNodeConfiguration::getLocation)
              .filter(location -> location.getDataNodeId() == removeNodeId)
              .findFirst();
      if (!dataNodeLocationOpt.isPresent()) {
        throw new IoTDBException("Invalid node-id", -1);
      }
      TDataNodeLocation dataNodeLocation = dataNodeLocationOpt.get();

      //
      logger.info("Start to remove datanode, removed datanode endpoint: {}", dataNodeLocation);
      TDataNodeRemoveReq removeReq =
          new TDataNodeRemoveReq(Collections.singletonList(dataNodeLocation));
      TDataNodeRemoveResp removeResp = configNodeClient.removeDataNode(removeReq);
      logger.info("Remove result {} ", removeResp);
      if (removeResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new IoTDBException(
            removeResp.getStatus().toString(), removeResp.getStatus().getCode());
      }
      logger.info(
          "Submit remove-datanode request successfully, but the process may fail. "
              + "more details are shown in the logs of confignode-leader and removed-datanode, "
              + "and after the process of removing datanode ends successfully, "
              + "you are supposed to delete directory and data of the removed-datanode manually");
    } catch (TException | ClientManagerException e) {
      throw new IoTDBException("Failed removing datanode", e, -1);
    }
  }

  /** Prepare cluster IoTDB-DataNode */
  private boolean prepareDataNode() throws StartupException, IOException {
    long startTime = System.currentTimeMillis();
    IoTDBConfig config = descriptor.getConfig();

    // Init system properties handler
    IoTDBStartCheck.checkOldSystemConfig();

    // Set this node
    thisNode.setIp(config.getInternalAddress());
    thisNode.setPort(config.getInternalPort());

    // Startup checks
    DataNodeStartupCheck checks = new DataNodeStartupCheck(IoTDBConstant.DN_ROLE, config);
    checks.startUpCheck();
    long endTime = System.currentTimeMillis();
    logger.info("The DataNode is prepared successfully, which takes {} ms", (endTime - startTime));
    return dataNodeSystemPropertiesHandler.isFirstStart();
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
    long startTime = System.currentTimeMillis();
    /* Pull system configurations */
    int retry = DEFAULT_RETRY;
    TSystemConfigurationResp configurationResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
          configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        configurationResp = configNodeClient.getSystemConfiguration();
        break;
      } catch (TException | ClientManagerException e) {
        logger.warn("Cannot pull system configurations from ConfigNode-leader", e);
        retry--;
      }

      try {
        // wait to start the next try
        Thread.sleep(defaultRetryIntervalInMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }
    if (configurationResp == null
        || !configurationResp.isSetStatus()
        || configurationResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // All tries failed
      logger.error(
          "Cannot pull system configurations from ConfigNode-leader after {} retries.",
          DEFAULT_RETRY);
      throw new StartupException(
          "Cannot pull system configurations from ConfigNode-leader. "
              + "Please check whether the dn_seed_config_node in iotdb-system.properties is correct or alive.");
    }

    /* Load system configurations */
    descriptor.loadGlobalConfig(configurationResp.globalConfig);
    descriptor.loadRatisConfig(configurationResp.ratisConfig);
    descriptor.loadCQConfig(configurationResp.cqConfig);
    commonDescriptor.loadGlobalConfig(configurationResp.globalConfig);

    IoTDBConfig config = descriptor.getConfig();
    /* Set cluster consensus protocol class */
    if (!startCheck.checkConsensusProtocolExists(TConsensusGroupType.DataRegion)) {
      config.setDataRegionConsensusProtocolClass(
          configurationResp.globalConfig.getDataRegionConsensusProtocolClass());
    }

    if (!startCheck.checkConsensusProtocolExists(TConsensusGroupType.SchemaRegion)) {
      config.setSchemaRegionConsensusProtocolClass(
          configurationResp.globalConfig.getSchemaRegionConsensusProtocolClass());
    }

    /* Check system configurations */
    try {
      startCheck.checkSystemConfig();
      startCheck.checkDirectory();
      if (!config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
        // In current implementation, only IoTConsensus need separated memory from Consensus
        descriptor.reclaimConsensusMemory();
      }
    } catch (Exception e) {
      throw new StartupException(e.getMessage());
    }
    long endTime = System.currentTimeMillis();
    logger.info(
        "Successfully pull system configurations from ConfigNode-leader, which takes {} ms",
        (endTime - startTime));
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
   * <p>5. All Pipe information
   *
   * <p>6. All TTL information
   */
  private void storeRuntimeConfigurations(
      List<TConfigNodeLocation> configNodeLocations, TRuntimeConfiguration runtimeConfiguration)
      throws StartupException {
    /* Store ConfigNodeList */
    List<TEndPoint> configNodeList = new ArrayList<>();
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      configNodeList.add(configNodeLocation.getInternalEndPoint());
    }
    configNodeInfo.updateConfigNodeList(configNodeList);

    /* Store templateSetInfo */
    clusterTemplateManager.updateTemplateSetInfo(runtimeConfiguration.getTemplateInfo());

    /* Store udfInformationList */
    getUDFInformationList(runtimeConfiguration.getAllUDFInformation());

    /* Store triggerInformationList */
    getTriggerInformationList(runtimeConfiguration.getAllTriggerInformation());

    /* Store pipeInformationList */
    getPipeInformationList(runtimeConfiguration.getAllPipeInformation());

    /* Store ttl information */
    initTTLInformation(runtimeConfiguration.getAllTTLInformation());

    /* Store cluster ID */
    descriptor.getConfig().setClusterId(runtimeConfiguration.getClusterId());

    /* Store table info*/
    dataNodeTableCache.init(runtimeConfiguration.getTableInfo());
  }

  /**
   * Register this DataNode into cluster.
   *
   * @param isPreCheck do pre-check before formal registration
   * @throws StartupException if register failed.
   * @throws IOException if serialize cluster name and datanode id failed.
   */
  private void sendRegisterRequestToConfigNode(boolean isPreCheck)
      throws StartupException, IOException {
    logger.info("Sending register request to ConfigNode-leader...");
    long startTime = System.currentTimeMillis();
    IoTDBConfig config = descriptor.getConfig();
    /* Send register request */
    int retry = DEFAULT_RETRY;
    TDataNodeRegisterReq req = new TDataNodeRegisterReq();
    req.setPreCheck(isPreCheck);
    req.setDataNodeConfiguration(generateDataNodeConfiguration());
    req.setClusterName(config.getClusterName());
    req.setVersionInfo(new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));
    TDataNodeRegisterResp dataNodeRegisterResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
          configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeRegisterResp = configNodeClient.registerDataNode(req);
        break;
      } catch (TException | ClientManagerException e) {
        logger.warn("Cannot register to the cluster, because: {}", e.getMessage());
        retry--;
      }

      try {
        // Wait to start the next try
        Thread.sleep(defaultRetryIntervalInMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }
    if (dataNodeRegisterResp == null) {
      // All tries failed
      logger.error("Cannot register into cluster after {} retries.", DEFAULT_RETRY);
      throw new StartupException(
          "Cannot register into the cluster. "
              + "Please check whether the dn_seed_config_node in iotdb-system.properties is correct or alive.");
    }

    if (dataNodeRegisterResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      if (isPreCheck) {
        logger.info("Successfully pass the precheck, will do the formal registration soon.");
        return;
      }
      /* Store runtime configurations when register success */
      int dataNodeID = dataNodeRegisterResp.getDataNodeId();
      config.setDataNodeId(dataNodeID);
      startCheck.serializeDataNodeId(dataNodeID);

      storeRuntimeConfigurations(
          dataNodeRegisterResp.getConfigNodeList(), dataNodeRegisterResp.getRuntimeConfiguration());
      long endTime = System.currentTimeMillis();

      logger.info(
          "Successfully register to the cluster: {} , which takes {} ms.",
          config.getClusterName(),
          (endTime - startTime));
    } else {
      /* Throw exception when register failed */
      logger.error(dataNodeRegisterResp.getStatus().getMessage());
      throw new StartupException("Cannot register to the cluster.");
    }
  }

  private void removeInvalidRegions(List<ConsensusGroupId> dataNodeConsensusGroupIds) {
    List<ConsensusGroupId> invalidDataRegionConsensusGroupIds =
        dataRegionConsensus.getAllConsensusGroupIdsWithoutStarting().stream()
            .filter(consensusGroupId -> !dataNodeConsensusGroupIds.contains(consensusGroupId))
            .collect(Collectors.toList());

    List<ConsensusGroupId> invalidSchemaRegionConsensusGroupIds =
        schemaRegionConsensus.getAllConsensusGroupIdsWithoutStarting().stream()
            .filter(consensusGroupId -> !dataNodeConsensusGroupIds.contains(consensusGroupId))
            .collect(Collectors.toList());
    removeInvalidDataRegions(invalidDataRegionConsensusGroupIds);
    removeInvalidSchemaRegions(invalidSchemaRegionConsensusGroupIds);
  }

  private void removeInvalidDataRegions(List<ConsensusGroupId> invalidConsensusGroupId) {
    logger.info("Remove invalid dataRegion directories... {}", invalidConsensusGroupId);
    for (ConsensusGroupId consensusGroupId : invalidConsensusGroupId) {
      File oldDir =
          new File(dataRegionConsensus.getRegionDirFromConsensusGroupId(consensusGroupId));
      removeRegionsDir(oldDir);
    }
  }

  private void removeInvalidSchemaRegions(List<ConsensusGroupId> invalidConsensusGroupId) {
    logger.info("Remove invalid schemaRegion directories... {}", invalidConsensusGroupId);
    for (ConsensusGroupId consensusGroupId : invalidConsensusGroupId) {
      File oldDir =
          new File(schemaRegionConsensus.getRegionDirFromConsensusGroupId(consensusGroupId));
      removeRegionsDir(oldDir);
    }
  }

  private void removeRegionsDir(File regionDir) {
    if (regionDir.exists()) {
      try {
        FileUtils.recursivelyDeleteFolder(regionDir.getPath());
        logger.info("delete {} succeed.", regionDir.getAbsolutePath());
      } catch (IOException e) {
        logger.error("delete {} failed.", regionDir.getAbsolutePath());
      }
    } else {
      logger.info("delete {} failed, because it does not exist.", regionDir.getAbsolutePath());
    }
  }

  private void sendRestartRequestToConfigNode() throws StartupException {
    logger.info("Sending restart request to ConfigNode-leader...");
    long startTime = System.currentTimeMillis();
    IoTDBConfig config = descriptor.getConfig();
    /* Send restart request */
    int retry = DEFAULT_RETRY;
    TDataNodeRestartReq req = new TDataNodeRestartReq();
    req.setClusterName(
        config.getClusterName() == null ? DEFAULT_CLUSTER_NAME : config.getClusterName());
    req.setDataNodeConfiguration(generateDataNodeConfiguration());
    req.setVersionInfo(new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));
    TDataNodeRestartResp dataNodeRestartResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
          configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeRestartResp = configNodeClient.restartDataNode(req);
        break;
      } catch (TException | ClientManagerException e) {
        logger.warn(
            "Cannot send restart request to the ConfigNode-leader, because: {}", e.getMessage());
        retry--;
      }

      try {
        // Wait to start the next try
        Thread.sleep(defaultRetryIntervalInMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }
    if (dataNodeRestartResp == null) {
      // All tries failed
      logger.error(
          "Cannot send restart DataNode request to ConfigNode-leader after {} retries.",
          DEFAULT_RETRY);
      throw new StartupException(
          "Cannot send restart DataNode request to ConfigNode-leader. "
              + "Please check whether the dn_seed_config_node in iotdb-system.properties is correct or alive.");
    }

    if (dataNodeRestartResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      /* Store runtime configurations when restart request is accepted */
      storeRuntimeConfigurations(
          dataNodeRestartResp.getConfigNodeList(), dataNodeRestartResp.getRuntimeConfiguration());
      long endTime = System.currentTimeMillis();
      logger.info(
          "Restart request to cluster: {} is accepted, which takes {} ms.",
          config.getClusterName(),
          (endTime - startTime));

      List<TConsensusGroupId> consensusGroupIds = dataNodeRestartResp.getConsensusGroupIds();
      List<ConsensusGroupId> dataNodeConsensusGroupIds =
          consensusGroupIds.stream()
              .map(ConsensusGroupId.Factory::createFromTConsensusGroupId)
              .collect(Collectors.toList());

      removeInvalidRegions(dataNodeConsensusGroupIds);
    } else {
      /* Throw exception when restart is rejected */
      throw new StartupException(dataNodeRestartResp.getStatus().getMessage());
    }
  }

  private void prepareResources() throws StartupException {
    prepareUDFResources();
    prepareTriggerResources();
    preparePipeResources();
  }

  /**
   * Register services and set up DataNode.
   *
   * @throws StartupException if start up failed.
   */
  private void active() throws StartupException {
    try {
      processPid();
      setUp();
    } catch (StartupException e) {
      logger.error("Meet error while starting up.", e);
      throw new StartupException("Error in activating IoTDB DataNode.");
    }
    logger.info("IoTDB DataNode has started.");

    try {
      long startTime = System.currentTimeMillis();
      schemaRegionConsensus.start();
      long schemaRegionEndTime = System.currentTimeMillis();
      logger.info(
          "SchemaRegion consensus start successfully, which takes {} ms.",
          (schemaRegionEndTime - startTime));
      schemaRegionConsensusStarted = true;
      dataRegionConsensus.start();
      long dataRegionEndTime = System.currentTimeMillis();
      logger.info(
          "DataRegion consensus start successfully, which takes {} ms.",
          (dataRegionEndTime - schemaRegionEndTime));
      dataRegionConsensusStarted = true;
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

  private void setUp() throws StartupException {
    logger.info("Setting up IoTDB DataNode...");
    registerManager.register(new JMXService());
    JMXService.registerMBean(this, mbeanName);

    // Get resources for trigger,udf,pipe...
    prepareResources();

    Runtime.getRuntime().addShutdownHook(new IoTDBShutdownHook(generateDataNodeLocation()));
    setUncaughtExceptionHandler();

    logger.info("Recover the schema...");
    initSchemaEngine();
    classLoader();
    registerManager.register(flushManager);
    registerManager.register(cacheHitRatioMonitor);

    // Close wal when using ratis consensus
    IoTDBConfig config = descriptor.getConfig();
    if (config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      config.setWalMode(WALMode.DISABLE);
    }
    registerManager.register(walManager);

    // Must init before StorageEngine
    registerManager.register(compactionScheduleTaskManager);

    // In mpp mode we need to start some other services
    registerManager.register(storageEngine);
    registerManager.register(mppDataExchangeService);
    registerManager.register(driverScheduler);

    registerUdfServices();

    logger.info(
        "IoTDB DataNode is setting up, some databases may not be ready now, please wait several seconds...");
    long startTime = System.currentTimeMillis();
    while (!storageEngine.isAllSgReady()) {
      try {
        TimeUnit.MILLISECONDS.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("IoTDB DataNode failed to set up.", e);
        Thread.currentThread().interrupt();
        return;
      }
    }
    long endTime = System.currentTimeMillis();
    logger.info("Wait for all databases ready, which takes {} ms.", (endTime - startTime));
    // Must init after SchemaEngine and StorageEngine prepared well
    dataNodeRegionManager.init();

    // Start region migrate service
    registerManager.register(regionMigrateService);

    registerManager.register(compactionTaskManager);

    // Register subscription agent before pipe agent
    registerManager.register(SubscriptionAgent.runtime());
    registerManager.register(PipeDataNodeAgent.runtime());
  }

  /** Set up RPC and protocols after DataNode is available */
  private void setUpRPCService() throws StartupException {
    // Start InternalRPCService to indicate that the current DataNode can accept cluster scheduling
    registerManager.register(dataNodeInternalRPCService);

    IoTDBConfig config = descriptor.getConfig();

    // Notice: During the period between starting the internal RPC service
    // and starting the client RPC service , some requests may fail because
    // DataNode is not marked as RUNNING by ConfigNode-leader yet.

    // Start client RPCService to indicate that the current DataNode provide external services
    descriptor.getConfig().setRpcImplClassName(ClientRPCServiceImpl.class.getName());
    if (config.isEnableRpcService()) {
      registerManager.register(rpcService);
    }
    // init service protocols
    initProtocols();
  }

  private void setUpMetricService() throws StartupException {
    MetricConfig metricConfig = metricConfigDescriptor.getMetricConfig();
    metricConfig.setNodeId(descriptor.getConfig().getDataNodeId());
    registerManager.register(metricService);

    // init metric service
    if (metricConfig.getInternalReportType().equals(InternalReporterType.IOTDB)) {
      metricService.updateInternalReporter(new IoTDBInternalLocalReporter());
    }
    metricService.startInternalReporter();
    // bind predefined metrics
    DataNodeMetricsHelper.bind();
  }

  public TDataNodeLocation generateDataNodeLocation() {
    IoTDBConfig config = descriptor.getConfig();
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
   * Generate dataNodeConfiguration. Warning: Don't private this method !!!
   *
   * @return TDataNodeConfiguration
   */
  public TDataNodeConfiguration generateDataNodeConfiguration() {
    // Set DataNodeLocation
    TDataNodeLocation location = generateDataNodeLocation();

    // Set NodeResource
    TNodeResource resource = new TNodeResource();
    resource.setCpuCoreNum(Runtime.getRuntime().availableProcessors());
    resource.setMaxMemory(Runtime.getRuntime().totalMemory());

    return new TDataNodeConfiguration(location, resource);
  }

  private void registerUdfServices() throws StartupException {
    registerManager.register(temporaryQueryDataFileService);
    registerManager.register(
        UDFClassLoaderManager.setupAndGetInstance(descriptor.getConfig().getUdfDir()));
  }

  private void initUDFRelatedInstance() throws StartupException {
    try {
      IoTDBConfig config = descriptor.getConfig();
      UDFExecutableManager.setupAndGetInstance(config.getUdfTemporaryLibDir(), config.getUdfDir());
      UDFClassLoaderManager.setupAndGetInstance(config.getUdfDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private void prepareUDFResources() throws StartupException {
    long startTime = System.currentTimeMillis();
    initUDFRelatedInstance();
    if (resourcesInformationHolder.getUDFInformationList() == null
        || resourcesInformationHolder.getUDFInformationList().isEmpty()) {
      return;
    }

    // Get jars from config node
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

    // Create instances of udf and do registration
    try {
      for (UDFInformation udfInformation : resourcesInformationHolder.getUDFInformationList()) {
        udfManagementService.doRegister(udfInformation);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }
    long endTime = System.currentTimeMillis();
    logger.debug("successfully registered all the UDFs, which takes {} ms.", (endTime - startTime));
    if (logger.isDebugEnabled()) {
      for (UDFInformation udfInformation : udfManagementService.getAllUDFInformation()) {
        logger.debug("get udf: {}", udfInformation.getFunctionName());
      }
    }
  }

  private void getJarOfUDFs(List<UDFInformation> udfInformationList) throws StartupException {
    try (ConfigNodeClient configNodeClient =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      List<String> jarNameList =
          udfInformationList.stream().map(UDFInformation::getJarName).collect(Collectors.toList());
      TGetJarInListResp resp = configNodeClient.getUDFJar(new TGetJarInListReq(jarNameList));
      if (resp.getStatus().getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
        throw new StartupException("Failed to get UDF jar from config node.");
      }
      List<ByteBuffer> jarList = resp.getJarList();
      for (int i = 0; i < udfInformationList.size(); i++) {
        udfExecutableManager.saveToInstallDir(
            jarList.get(i), udfInformationList.get(i).getJarName());
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
        // Jar does not exist, add current udfInformation to list
        if (!udfExecutableManager.hasFileUnderInstallDir(udfInformation.getJarName())) {
          res.add(udfInformation);
        } else {
          try {
            // Local jar has conflicts with jar on config node, add current triggerInformation to
            // list
            if (udfManagementService.isLocalJarConflicted(udfInformation)) {
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
      IoTDBConfig config = descriptor.getConfig();
      TriggerExecutableManager.setupAndGetInstance(
          config.getTriggerTemporaryLibDir(), config.getTriggerDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private void prepareTriggerResources() throws StartupException {
    long startTime = System.currentTimeMillis();
    initTriggerRelatedInstance();
    if (resourcesInformationHolder.getTriggerInformationList() == null
        || resourcesInformationHolder.getTriggerInformationList().isEmpty()) {
      return;
    }

    // Get jars from config node
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

    // Create instances of triggers and do registration
    try {
      for (TriggerInformation triggerInformation :
          resourcesInformationHolder.getTriggerInformationList()) {
        triggerManagementService.doRegister(triggerInformation, true);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }

    if (logger.isDebugEnabled()) {
      for (TriggerInformation triggerInformation :
          triggerManagementService.getAllTriggerInformationInTriggerTable()) {
        logger.debug("get trigger: {}", triggerInformation.getTriggerName());
      }
      for (TriggerExecutor triggerExecutor : triggerManagementService.getAllTriggerExecutors()) {
        logger.debug(
            "get trigger executor: {}", triggerExecutor.getTriggerInformation().getTriggerName());
      }
    }
    // Start TriggerInformationUpdater
    triggerInformationUpdater.startTriggerInformationUpdater();
    long endTime = System.currentTimeMillis();
    logger.info(
        "successfully registered all the triggers, which takes {} ms.", (endTime - startTime));
  }

  private void getJarOfTriggers(List<TriggerInformation> triggerInformationList)
      throws StartupException {
    try (ConfigNodeClient configNodeClient =
        configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
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
        triggerExecutableManager.saveToInstallDir(
            jarList.get(i), triggerInformationList.get(i).getJarName());
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
        if (!triggerExecutableManager.hasFileUnderInstallDir(triggerInformation.getJarName())) {
          res.add(triggerInformation);
        } else {
          try {
            // local jar has conflicts with jar on config node, add current triggerInformation to
            // list
            if (triggerManagementService.isLocalJarConflicted(triggerInformation)) {
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

  private void preparePipeResources() throws StartupException {
    long startTime = System.currentTimeMillis();
    PipeDataNodeAgent.runtime().preparePipeResources(resourcesInformationHolder);
    long endTime = System.currentTimeMillis();
    logger.info("Prepare pipe resources successfully, which takes {} ms.", (endTime - startTime));
  }

  private void getPipeInformationList(List<ByteBuffer> allPipeInformation) {
    final List<PipePluginMeta> list = new ArrayList<>();
    if (allPipeInformation != null) {
      for (ByteBuffer pipeInformationByteBuffer : allPipeInformation) {
        list.add(PipePluginMeta.deserialize(pipeInformationByteBuffer));
      }
    }
    resourcesInformationHolder.setPipePluginMetaList(list);
  }

  private void initTTLInformation(byte[] allTTLInformation) throws StartupException {
    if (allTTLInformation == null) {
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(allTTLInformation);
    int mapSize = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < mapSize; i++) {
      try {
        dataNodeTTLCache.setTTL(
            PathUtils.splitPathToDetachedNodes(
                Objects.requireNonNull(ReadWriteIOUtils.readString(buffer))),
            ReadWriteIOUtils.readLong(buffer));
      } catch (IllegalPathException e) {
        throw new StartupException(e);
      }
    }
  }

  private void initSchemaEngine() {
    long startTime = System.currentTimeMillis();
    schemaEngine.init();
    long endTime = System.currentTimeMillis();
    logger.info("Recover schema successfully, which takes {} ms.", (endTime - startTime));
  }

  private void classLoader() {
    try {
      // StatementGenerator
      Class.forName(StatementGenerator.class.getName());
      Class.forName(ASTVisitor.class.getName());
      Class.forName(SqlLexer.class.getName());
      Class.forName(CommonTokenStream.class.getName());
      Class.forName(IoTDBSqlParser.class.getName());
      // SourceRewriter
      Class.forName(SourceRewriter.class.getName());
      Class.forName(DistributionPlanContext.class.getName());
      // LogicalPlaner
      Class.forName(LogicalPlanVisitor.class.getName());
      Class.forName(LogicalQueryPlan.class.getName());
      // TsFileProcessor
      Class.forName(TsFileProcessor.class.getName());
    } catch (ClassNotFoundException e) {
      logger.error("load class error: ", e);
    }
  }

  public void stop() {
    deactivate();
    schemaEngine.clear();
    try {
      metricService.stop();
      if (schemaRegionConsensusStarted) {
        schemaRegionConsensus.stop();
      }
      if (dataRegionConsensusStarted) {
        dataRegionConsensus.stop();
      }
    } catch (Exception e) {
      logger.error("Stop data node error", e);
    }
  }

  private void initProtocols() throws StartupException {
    if (descriptor.getConfig().isEnableMQTTService()) {
      registerManager.register(mqttService);
    }
    if (restServiceDescriptor.getConfig().isEnableRestService()) {
      registerManager.register(restService);
    }
    if (pipeConfig.getPipeAirGapReceiverEnabled()) {
      registerManager.register(PipeDataNodeAgent.receiver().airGap());
    }
  }

  private void deactivate() {
    logger.info("Deactivating IoTDB DataNode...");
    stopTriggerRelatedServices();
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

    private static DataNode instance;

    private DataNodeHolder() {
      // Empty constructor
    }
  }

  public static DataNode getInstance() {
    // Make sure the singleton is initialized (Mainly in Unit-Tests)
    if (DataNodeHolder.instance == null) {
      new DataNode();
    }
    return DataNodeHolder.instance;
  }

  public static void setInstance(DataNode dataNode) {
    if (DataNodeHolder.instance != null) {
      throw new RuntimeException("DataNode has already been initialized");
    }
    DataNodeHolder.instance = dataNode;
  }
}
