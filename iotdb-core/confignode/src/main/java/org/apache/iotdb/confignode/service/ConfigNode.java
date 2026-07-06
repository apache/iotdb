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

package org.apache.iotdb.confignode.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.ServerCommandLine;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.concurrent.ThreadModule;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.ThreadPoolMetrics;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.memory.MemoryConfig;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.JvmGcMonitorMetrics;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.cpu.CpuUsageMetrics;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.ConfigNodeStartupCheck;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.pipe.agent.PipeConfigNodeAgent;
import org.apache.iotdb.confignode.manager.pipe.metric.PipeConfigNodeMetrics;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCService;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;
import org.apache.iotdb.db.service.metrics.ProcessMetrics;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.metricsets.UpTimeMetrics;
import org.apache.iotdb.metrics.metricsets.disk.DiskMetrics;
import org.apache.iotdb.metrics.metricsets.jvm.JvmMetrics;
import org.apache.iotdb.metrics.metricsets.logback.LogbackMetrics;
import org.apache.iotdb.metrics.metricsets.net.NetMetrics;
import org.apache.iotdb.metrics.metricsets.system.SystemMetrics;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.util.ExitUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ConfigNode extends ServerCommandLine implements ConfigNodeMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private static final int STARTUP_RETRY_NUM = 20;
  private static final long STARTUP_RETRY_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(3);
  private static final int SCHEDULE_WAITING_RETRY_NUM =
      (int) (COMMON_CONFIG.getCnConnectionTimeoutInMS() / STARTUP_RETRY_INTERVAL_IN_MS);
  private static final int SEED_CONFIG_NODE_ID = 0;

  private static final int INIT_NON_SEED_CONFIG_NODE_ID = -1;

  private static final String CONFIGURATION = "IoTDB configuration: {}";

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          IoTDBConstant.IOTDB_SERVICE_JMX_NAME,
          IoTDBConstant.JMX_TYPE,
          ServiceType.CONFIG_NODE.getJmxName());
  protected final RegisterManager registerManager = new RegisterManager();

  protected ConfigManager configManager;

  private int exitStatusCode = 0;

  public ConfigNode() {
    super("ConfigNode");
    // We do not init anything here, so that we can re-initialize the instance in IT.
    ConfigNodeHolder.instance = this;
  }

  public static void main(String[] args) throws Exception {
    LOGGER.info(
        ConfigNodeMessages.ENVIRONMENT_VARIABLES,
        ConfigNodeConstant.GLOBAL_NAME,
        ConfigNodeConfig.getEnvironmentVariables());
    LOGGER.info(
        ConfigNodeMessages.DEFAULT_CHARSET_IS,
        ConfigNodeConstant.GLOBAL_NAME,
        Charset.defaultCharset().displayName());
    // let IoTDB handle the exception instead of ratis
    ExitUtils.disableSystemExit();
    ConfigNode configNode = new ConfigNode();
    int returnCode = configNode.run(args);
    if (returnCode != 0) {
      System.exit(returnCode);
    }
  }

  @Override
  protected void start() throws IoTDBException {
    try {
      // Do ConfigNode startup checks
      LOGGER.info(ConfigNodeMessages.STARTING_IOTDB, IoTDBConstant.VERSION_WITH_BUILD);
      ConfigNodeStartupCheck checks = new ConfigNodeStartupCheck(IoTDBConstant.CN_ROLE);
      checks.startUpCheck();
      MemoryConfig.getInstance();
    } catch (StartupException | ConfigurationException | IOException e) {
      LOGGER.error(ConfigNodeMessages.MEET_ERROR_WHEN_DOING_START_CHECKING, e);
      throw new IoTDBException(ConfigNodeMessages.ERROR_STARTING, -1);
    }
    active();
    LOGGER.info(ConfigNodeMessages.IOTDB_STARTED);
  }

  @Override
  protected void remove(Set<Integer> nodeIds) throws IoTDBException {
    throw new IoTDBException(
        ConfigNodeMessages.THE_REMOVE_CONFIGNODE_SCRIPT_HAS_BEEN_DEPRECATED_PLEASE_CONNECT_TO, -1);
  }

  public void active() {
    LOGGER.info(ConfigNodeMessages.ACTIVATING, ConfigNodeConstant.GLOBAL_NAME);

    try {
      processPid();
      // Add shutdown hook
      addShutDownHook();
      // Set up internal services
      setUpInternalServices();
      // Init ConfigManager
      initConfigManager();

      /* Restart */
      if (SystemPropertiesUtils.isRestarted()) {
        LOGGER.info(ConfigNodeMessages.IS_IN_RESTARTING_PROCESS, ConfigNodeConstant.GLOBAL_NAME);

        int configNodeId = CONF.getConfigNodeId();
        configManager.initConsensusManager();
        upgrade();
        waitForLeaderElected();
        setUpMetricService();
        // Notice: We always set up Seed-ConfigNode's RPC service lastly to ensure
        // that the external service is not provided until ConfigNode is fully available
        setUpRPCService();
        LOGGER.info(CONFIGURATION, CONF.getConfigMessage());
        LOGGER.info(
            ConfigNodeMessages.HAS_SUCCESSFULLY_RESTARTED_AND_JOINED_THE_CLUSTER,
            ConfigNodeConstant.GLOBAL_NAME,
            CONF.getClusterName());

        // Update item during restart
        // This will always be executed until the consensus write succeeds
        while (true) {
          TSStatus status =
              configManager
                  .getNodeManager()
                  .updateConfigNodeIfNecessary(
                      configNodeId,
                      new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));
          if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            break;
          } else {
            startUpSleep("restart ConfigNode failed! ");
          }
        }
        loadSecretKey();
        loadHardwareCode();
        return;
      } else {
        saveSecretKey();
        saveHardwareCode();
      }

      /* Initial startup of Seed-ConfigNode */
      if (ConfigNodeDescriptor.getInstance().isSeedConfigNode()) {
        LOGGER.info(
            ConfigNodeMessages.THE_CURRENT_IS_NOW_STARTING_AS_THE_SEED_CONFIGNODE,
            ConfigNodeConstant.GLOBAL_NAME);

        /* Always set ConfigNodeId before initConsensusManager */
        CONF.setConfigNodeId(SEED_CONFIG_NODE_ID);
        configManager.initConsensusManager();
        // Generate the builtin admin users after initConsensusManager
        initBuiltinUsers();

        // Persistence system parameters after the consensusGroup is built,
        // or the consensusGroup will not be initialized successfully otherwise.
        SystemPropertiesUtils.storeSystemParameters();

        // Wait for ConfigNode-leader elected before applying itself
        waitForLeaderElected();

        // Seed-ConfigNode should apply itself when first start
        configManager
            .getNodeManager()
            .applyConfigNode(
                CONF.generateLocalConfigNodeLocationWithSpecifiedNodeId(SEED_CONFIG_NODE_ID),
                new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));
        setUpMetricService();
        // Notice: We always set up Seed-ConfigNode's RPC service lastly to ensure
        // that the external service is not provided until Seed-ConfigNode is fully initialized
        setUpRPCService();
        // The initial startup of Seed-ConfigNode finished
        LOGGER.info(CONFIGURATION, CONF.getConfigMessage());
        LOGGER.info(
            ConfigNodeMessages.HAS_SUCCESSFULLY_STARTED_AND_JOINED_THE_CLUSTER,
            ConfigNodeConstant.GLOBAL_NAME,
            CONF.getClusterName());
        return;
      }

      /* Initial startup of Non-Seed-ConfigNode */
      // We set up Non-Seed ConfigNode's RPC service before sending the register request
      // in order to facilitate the scheduling of capacity expansion process in ConfigNode-leader
      setUpRPCService();
      sendRegisterConfigNodeRequest();
      // The initial startup of Non-Seed-ConfigNode is not yet finished,
      // we should wait for leader's scheduling
      LOGGER.info(CONFIGURATION, CONF.getConfigMessage());
      LOGGER.info(
          ConfigNodeMessages.HAS_REGISTERED_SUCCESSFULLY_WAITING_FOR_THE_LEADER_S_SCHEDULING_TO,
          ConfigNodeConstant.GLOBAL_NAME,
          CONF.getConfigNodeId(),
          CONF.getClusterName());
      setUpMetricService();

      boolean isJoinedCluster = false;
      for (int retry = 0; retry < SCHEDULE_WAITING_RETRY_NUM; retry++) {
        if (!configManager
            .getConsensusManager()
            .getConsensusImpl()
            .getAllConsensusGroupIds()
            .isEmpty()) {
          isJoinedCluster = true;
          break;
        }
        startUpSleep("Waiting leader's scheduling is interrupted.");
      }

      if (!isJoinedCluster) {
        LOGGER.error(
            ConfigNodeMessages.THE_CURRENT_CONFIGNODE_CAN_T_JOINED_THE_CLUSTER_BECAUSE_LEADER);
        stop();
      }
    } catch (Throwable e) {
      LOGGER.error(ConfigNodeMessages.MEET_ERROR_WHILE_STARTING_UP, e);
      exitStatusCode = StatusUtils.retrieveExitStatusCode(e);
      stop();
    }
  }

  protected void initBuiltinUsers() {
    // nothing to do
  }

  void processPid() {
    String pidFile = System.getProperty(IoTDBConstant.IOTDB_PIDFILE);
    if (pidFile != null) {
      new File(pidFile).deleteOnExit();
    }
  }

  private void setUpInternalServices() throws StartupException {
    // Setup JMXService
    registerManager.register(new JMXService());
    JMXService.registerMBean(this, mbeanName);

    // Init Pipe Runtime Agent
    registerManager.register(PipeConfigNodeAgent.runtime());

    LOGGER.info(ConfigNodeMessages.SUCCESSFULLY_SETUP_INTERNAL_SERVICES);
  }

  private void setUpMetricService() throws StartupException {
    MetricConfigDescriptor.getInstance().getMetricConfig().setNodeId(CONF.getConfigNodeId());
    registerManager.register(MetricService.getInstance());
    // Bind predefined metric sets
    MetricService.getInstance().addMetricSet(new UpTimeMetrics());
    MetricService.getInstance().addMetricSet(new JvmMetrics());
    MetricService.getInstance().addMetricSet(new LogbackMetrics());
    MetricService.getInstance().addMetricSet(new ProcessMetrics());
    MetricService.getInstance().addMetricSet(new DiskMetrics(IoTDBConstant.CN_ROLE));
    MetricService.getInstance().addMetricSet(new NetMetrics(IoTDBConstant.CN_ROLE));
    MetricService.getInstance().addMetricSet(JvmGcMonitorMetrics.getInstance());
    MetricService.getInstance().addMetricSet(ClientManagerMetrics.getInstance());
    MetricService.getInstance().addMetricSet(ThreadPoolMetrics.getInstance());
    initCpuMetrics();
    initSystemMetrics();
    MetricService.getInstance()
        .addMetricSet(new PipeConfigNodeMetrics(configManager.getPipeManager()));
  }

  private void initSystemMetrics() {
    ArrayList<String> diskDirs = new ArrayList<>();
    diskDirs.add(CONF.getSystemDir());
    diskDirs.add(CONF.getConsensusDir());
    SystemMetrics.getInstance().setDiskDirs(diskDirs);
    MetricService.getInstance().addMetricSet(SystemMetrics.getInstance());
  }

  private void initCpuMetrics() {
    List<String> threadModules = new ArrayList<>();
    Arrays.stream(ThreadModule.values()).forEach(x -> threadModules.add(x.toString()));
    List<String> pools = new ArrayList<>();
    Arrays.stream(ThreadName.values()).forEach(x -> pools.add(x.name()));
    MetricService.getInstance()
        .addMetricSet(
            new CpuUsageMetrics(
                threadModules,
                pools,
                x -> ThreadName.getModuleTheThreadBelongs(x).toString(),
                x -> ThreadName.getThreadPoolTheThreadBelongs(x).name()));
  }

  void initConfigManager() {
    try {
      setConfigManager();
    } catch (Exception e) {
      LOGGER.error(ConfigNodeMessages.CAN_T_START_CONFIGNODE_CONSENSUS_GROUP, e);
      stop();
    }
    LOGGER.info(ConfigNodeMessages.SUCCESSFULLY_INITIALIZE_CONFIGMANAGER);
  }

  protected void setConfigManager() throws Exception {
    this.configManager = new ConfigManager();
  }

  /**
   * Register Non-seed {@link ConfigNode} when first startup.
   *
   * @throws StartupException if register failed.
   * @throws IOException if {@link ConsensusManager} init failed.
   */
  private void sendRegisterConfigNodeRequest() throws StartupException, IOException {
    TConfigNodeRegisterReq req =
        new TConfigNodeRegisterReq(
            configManager.getClusterParameters(),
            CONF.generateLocalConfigNodeLocationWithSpecifiedNodeId(INIT_NON_SEED_CONFIG_NODE_ID));

    req.setVersionInfo(new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));

    TEndPoint seedConfigNode = CONF.getSeedConfigNode();
    if (seedConfigNode == null) {
      LOGGER.error(ConfigNodeMessages.PLEASE_SET_THE_CN_SEED_CONFIG_NODE_PARAMETER_IN_IOTDB);
      throw new StartupException(ConfigNodeMessages.THE_SEEDCONFIGNODE_SETTING_IN_CONF_IS_EMPTY);
    }

    for (int retry = 0; retry < STARTUP_RETRY_NUM; retry++) {
      TSStatus status;
      TConfigNodeRegisterResp resp = null;
      Object obj =
          SyncConfigNodeClientPool.getInstance()
              .sendSyncRequestToConfigNodeWithRetry(
                  seedConfigNode, req, CnToCnNodeRequestType.REGISTER_CONFIG_NODE);

      if (obj instanceof TConfigNodeRegisterResp) {
        resp = (TConfigNodeRegisterResp) obj;
        status = resp.getStatus();
      } else {
        status = (TSStatus) obj;
      }

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        if (resp == null) {
          LOGGER.error(ConfigNodeMessages.THE_RESULT_OF_REGISTER_CONFIGNODE_IS_EMPTY);
          throw new StartupException(ConfigNodeMessages.THE_RESULT_OF_REGISTER_CONFIGNODE_IS_EMPTY);
        }
        /* Always set ConfigNodeId before initConsensusManager */
        CONF.setConfigNodeId(resp.getConfigNodeId());
        configManager.initConsensusManager();
        return;
      } else if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        seedConfigNode = status.getRedirectNode();
        LOGGER.info(ConfigNodeMessages.CONFIGNODE_NEED_REDIRECT_TO_RETRY, seedConfigNode, retry);
      } else if (status.getCode() == TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode()) {
        LOGGER.warn(
            ConfigNodeMessages.THE_RESULT_OF_REGISTER_SELF_CONFIGNODE_IS_RETRY, status, retry);
      } else if (status.getCode() == TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()) {
        LOGGER.info(
            ConfigNodeMessages
                .MESSAGE_CONFIGNODE_LEADER_IS_WARMING_UP_BEFORE_SERVING_THE_REGISTERING_CONFIGNODE_WILL_WAIT_AND_RETRY_STATUS_ARG_RETRY_ARG_3C924873,
            status,
            retry);
      } else {
        throw new StartupException(status.getMessage());
      }
      startUpSleep("Register ConfigNode failed!");
    }

    LOGGER.error(ConfigNodeMessages.THE_CURRENT_CONFIGNODE_CAN_T_SEND_REGISTER_REQUEST_TO_THE);
    stop();
  }

  protected void saveSecretKey() {
    // Do nothing
  }

  protected void saveHardwareCode() {
    // Do nothing
  }

  protected void loadSecretKey() throws IOException {
    // Do nothing
  }

  protected void loadHardwareCode() throws IOException {
    // Do nothing
  }

  private TConfigNodeLocation generateConfigNodeLocation(int configNodeId) {
    return new TConfigNodeLocation(
        configNodeId,
        new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort()),
        new TEndPoint(CONF.getInternalAddress(), CONF.getConsensusPort()));
  }

  private void startUpSleep(String errorMessage) throws StartupException {
    try {
      TimeUnit.MILLISECONDS.sleep(STARTUP_RETRY_INTERVAL_IN_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StartupException(errorMessage);
    }
  }

  private void setUpRPCService() throws StartupException {
    // Setup RPCService
    ConfigNodeRPCService configNodeRPCService = new ConfigNodeRPCService();
    ConfigNodeRPCServiceProcessor configNodeRPCServiceProcessor =
        getConfigNodeRPCServiceProcessor();
    configNodeRPCService.initSyncedServiceImpl(configNodeRPCServiceProcessor);
    registerManager.register(configNodeRPCService);
  }

  protected ConfigNodeRPCServiceProcessor getConfigNodeRPCServiceProcessor() {
    return new ConfigNodeRPCServiceProcessor(configManager);
  }

  private TConfigNodeLocation waitForLeaderElected() {
    while (!configManager.getConsensusManager().isLeaderExist()) {
      LOGGER.info(ConfigNodeMessages.LEADER_HAS_NOT_BEEN_ELECTED_YET_WAIT_FOR_1_SECOND);
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn(ConfigNodeMessages.UNEXPECTED_INTERRUPTION_DURING_WAITING_FOR_LEADER_ELECTION);
      }
    }
    return configManager.getConsensusManager().getLeaderLocation();
  }

  /**
   * Deactivating {@link ConfigNode} internal services.
   *
   * @throws IOException if close {@link ConfigNode} failed.
   */
  public void deactivate() throws IOException {
    LOGGER.info(ConfigNodeMessages.DEACTIVATING, ConfigNodeConstant.GLOBAL_NAME);
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    if (configManager != null) {
      configManager.close();
    }
    LOGGER.info(ConfigNodeMessages.IS_DEACTIVATED, ConfigNodeConstant.GLOBAL_NAME);
  }

  public void stop() {
    try {
      deactivate();
    } catch (IOException e) {
      LOGGER.error(ConfigNodeMessages.MEET_ERROR_WHEN_DEACTIVATE_CONFIGNODE, e);
    }
    System.exit(exitStatusCode);
  }

  /**
   * During the reboot, perform some upgrade works to adapt to the optimizations in the new version.
   */
  private void upgrade() throws IllegalPathException {
    // upgrade from old database-level ttl to new device-level ttl
    if (configManager.getTTLManager().getTTLCount() == 1) {
      configManager
          .getTTLManager()
          .setTTL(configManager.getClusterSchemaManager().getTTLInfoForUpgrading());
    }
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new ConfigNodeShutdownHook());
  }

  @TestOnly
  public void setConfigManager(ConfigManager configManager) {
    this.configManager = configManager;
  }

  private static class ConfigNodeHolder {

    private static ConfigNode instance;

    private ConfigNodeHolder() {
      // Empty constructor
    }
  }

  public static ConfigNode getInstance() {
    return ConfigNodeHolder.instance;
  }

  public static void setInstance(ConfigNode configNode) {
    ConfigNodeHolder.instance = configNode;
  }
}
