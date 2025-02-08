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
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.JvmGcMonitorMetrics;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.cpu.CpuUsageMetrics;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.client.CnToCnNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.ConfigNodeRemoveCheck;
import org.apache.iotdb.confignode.conf.ConfigNodeStartupCheck;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
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

  private static final int STARTUP_RETRY_NUM = 10;
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

  public ConfigNode() {
    super("ConfigNode");
    // We do not init anything here, so that we can re-initialize the instance in IT.
    ConfigNodeHolder.instance = this;
  }

  public static void main(String[] args) throws Exception {
    LOGGER.info(
        "{} environment variables: {}",
        ConfigNodeConstant.GLOBAL_NAME,
        ConfigNodeConfig.getEnvironmentVariables());
    LOGGER.info(
        "{} default charset is: {}",
        ConfigNodeConstant.GLOBAL_NAME,
        Charset.defaultCharset().displayName());
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
      LOGGER.info("Starting IoTDB {}", IoTDBConstant.VERSION_WITH_BUILD);
      ConfigNodeStartupCheck checks = new ConfigNodeStartupCheck(IoTDBConstant.CN_ROLE);
      checks.startUpCheck();
    } catch (StartupException | ConfigurationException | IOException e) {
      LOGGER.error("Meet error when doing start checking", e);
      throw new IoTDBException("Error starting", -1);
    }
    active();
  }

  @Override
  protected void remove(Set<Integer> nodeIds) throws IoTDBException {
    // If the nodeId was null, this is a shorthand for removing the current dataNode.
    // In this case we need to find our nodeId.

    int removeConfigNodeId = -1;
    if (nodeIds == null) {
      removeConfigNodeId = CONF.getConfigNodeId();
    } else {
      if (nodeIds.size() != 1) {
        throw new IoTDBException(
            "It is not allowed to remove multiple ConfigNodes at the same time.", -1);
      }
      removeConfigNodeId = nodeIds.iterator().next();
    }

    try {
      LOGGER.info("Starting to remove ConfigNode with node-id {}", removeConfigNodeId);

      try {
        TConfigNodeLocation removeConfigNodeLocation =
            ConfigNodeRemoveCheck.getInstance().removeCheck(Long.toString(removeConfigNodeId));
        if (removeConfigNodeLocation == null) {
          LOGGER.error(
              "The ConfigNode to be removed is not in the cluster, or the input format is incorrect.");
          return;
        }

        ConfigNodeRemoveCheck.getInstance().removeConfigNode(removeConfigNodeLocation);
      } catch (BadNodeUrlException e) {
        LOGGER.warn("No ConfigNodes need to be removed.", e);
        return;
      }

      LOGGER.info(
          "ConfigNode: {} is removed. If the confignode data directory is no longer needed, you can delete it manually.",
          removeConfigNodeId);
    } catch (IOException e) {
      LOGGER.error("Meet error when doing remove ConfigNode", e);
      throw new IoTDBException("Error removing", -1);
    }
  }

  public void active() {
    LOGGER.info("Activating {}...", ConfigNodeConstant.GLOBAL_NAME);

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
        LOGGER.info("{} is in restarting process...", ConfigNodeConstant.GLOBAL_NAME);

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
            "{} has successfully restarted and joined the cluster: {}.",
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
        return;
      }

      /* Initial startup of Seed-ConfigNode */
      if (ConfigNodeDescriptor.getInstance().isSeedConfigNode()) {
        LOGGER.info(
            "The current {} is now starting as the Seed-ConfigNode.",
            ConfigNodeConstant.GLOBAL_NAME);

        /* Always set ConfigNodeId before initConsensusManager */
        CONF.setConfigNodeId(SEED_CONFIG_NODE_ID);
        configManager.initConsensusManager();

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
            "{} has successfully started and joined the cluster: {}.",
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
          "{} {} has registered successfully. Waiting for the leader's scheduling to join the cluster: {}.",
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
            "The current ConfigNode can't joined the cluster because leader's scheduling failed. The possible cause is that the ip:port configuration is incorrect.");
        stop();
      }
    } catch (StartupException | IOException | IllegalPathException e) {
      LOGGER.error("Meet error while starting up.", e);
      stop();
    }
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

    LOGGER.info("Successfully setup internal services.");
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
      LOGGER.error("Can't start ConfigNode consensus group!", e);
      stop();
    }
    LOGGER.info("Successfully initialize ConfigManager.");
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
      LOGGER.error("Please set the cn_seed_config_node parameter in iotdb-system.properties file.");
      throw new StartupException("The seedConfigNode setting in conf is empty");
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
          LOGGER.error("The result of register ConfigNode is empty!");
          throw new StartupException("The result of register ConfigNode is empty!");
        }
        /* Always set ConfigNodeId before initConsensusManager */
        CONF.setConfigNodeId(resp.getConfigNodeId());
        configManager.initConsensusManager();
        return;
      } else if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        seedConfigNode = status.getRedirectNode();
        LOGGER.info("ConfigNode need redirect to  {}, retry {} ...", seedConfigNode, retry);
      } else if (status.getCode() == TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode()) {
        LOGGER.warn("The result of register self ConfigNode is {}, retry {} ...", status, retry);
      } else {
        throw new StartupException(status.getMessage());
      }
      startUpSleep("Register ConfigNode failed!");
    }

    LOGGER.error(
        "The current ConfigNode can't send register request to the ConfigNode-leader after all retries!");
    stop();
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

  private void waitForLeaderElected() {
    while (!configManager.getConsensusManager().isLeaderExist()) {
      LOGGER.info("Leader has not been elected yet, wait for 1 second");
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.warn("Unexpected interruption during waiting for leader election.");
      }
    }
  }

  /**
   * Deactivating {@link ConfigNode} internal services.
   *
   * @throws IOException if close {@link ConfigNode} failed.
   */
  public void deactivate() throws IOException {
    LOGGER.info("Deactivating {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    if (configManager != null) {
      configManager.close();
    }
    LOGGER.info("{} is deactivated.", ConfigNodeConstant.GLOBAL_NAME);
  }

  public void stop() {
    try {
      deactivate();
    } catch (IOException e) {
      LOGGER.error("Meet error when deactivate ConfigNode", e);
    }
    System.exit(-1);
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
