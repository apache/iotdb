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
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.client.sync.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCService;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;
import org.apache.iotdb.db.service.metrics.ProcessMetrics;
import org.apache.iotdb.db.service.metrics.SystemMetrics;
import org.apache.iotdb.metrics.metricsets.jvm.JvmMetrics;
import org.apache.iotdb.metrics.metricsets.logback.LogbackMetrics;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ConfigNode implements ConfigNodeMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig COMMON_CONF = CommonDescriptor.getInstance().getConfig();

  private static final int SCHEDULE_WAITING_RETRY_NUM = 20;

  private static final int SEED_CONFIG_NODE_ID = 0;

  private static final int INIT_NON_SEED_CONFIG_NODE_ID = -1;

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          ConfigNodeConstant.CONFIGNODE_PACKAGE, ConfigNodeConstant.JMX_TYPE, "ConfigNode");
  private final RegisterManager registerManager = new RegisterManager();

  private ConfigManager configManager;

  private ConfigNode() {
    // We do not init anything here, so that we can re-initialize the instance in IT.
  }

  public static void main(String[] args) {
    new ConfigNodeCommandLine().doMain(args);
  }

  public void active(boolean isStandAlone) {
    LOGGER.info("Activating {}...", ConfigNodeConstant.GLOBAL_NAME);

    if (isStandAlone) {
      System.out.println("StandAlone!");
      CONF.setDataReplicationFactor(1);
      CONF.setSchemaReplicationFactor(1);
      CONF.setConfigNodeConsensusProtocolClass("org.apache.iotdb.consensus.ratis.RatisConsensus");
      CONF.setDataRegionConsensusProtocolClass("org.apache.iotdb.consensus.simple.SimpleConsensus");
      CONF.setSchemaRegionConsensusProtocolClass(
          "org.apache.iotdb.consensus.simple.SimpleConsensus");
      CONF.setInternalAddress("127.0.0.1");
      try {
        CONF.setTargetConfigNode(
            NodeUrlUtils.parseTEndPointUrl("127.0.0.1:" + CONF.getInternalPort()));
      } catch (Exception ignore) {
        // Typically there won't be any exception here
      }
    }
    try {
      processPid();
      // Set up internal services
      setUpInternalServices();
      // Init ConfigManager
      initConfigManager();

      /* Restart */
      if (SystemPropertiesUtils.isRestarted()) {
        LOGGER.info("{} is in restarting process...", ConfigNodeConstant.GLOBAL_NAME);
        /* Always set ConfigNodeId before initConsensusManager */
        CONF.setConfigNodeId(SystemPropertiesUtils.loadConfigNodeIdWhenRestarted());
        configManager.initConsensusManager();
        setUpRPCService();
        LOGGER.info(
            "{} has successfully started and joined the cluster.", ConfigNodeConstant.GLOBAL_NAME);
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

        // Seed-ConfigNode should apply itself when first start
        configManager
            .getNodeManager()
            .applyConfigNode(
                new TConfigNodeLocation(
                    SEED_CONFIG_NODE_ID,
                    new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort()),
                    new TEndPoint(CONF.getInternalAddress(), CONF.getConsensusPort())));
        // We always set up Seed-ConfigNode's RPC service lastly to ensure that
        // the external service is not provided until Seed-ConfigNode is fully initialized
        setUpRPCService();
        // The initial startup of Seed-ConfigNode finished

        LOGGER.info(
            "{} has successfully started and joined the cluster.", ConfigNodeConstant.GLOBAL_NAME);
        return;
      }

      /* Initial startup of Non-Seed-ConfigNode */
      // We set up Non-Seed ConfigNode's RPC service before sending the register request
      // in order to facilitate the scheduling of capacity expansion process in ConfigNode-leader
      setUpRPCService();
      registerConfigNode();
      // The initial startup of Non-Seed-ConfigNode is not yet finished,
      // we should wait for leader's scheduling
      LOGGER.info(
          "{} {} has registered successfully. Waiting for the leader's scheduling to join the cluster.",
          ConfigNodeConstant.GLOBAL_NAME,
          CONF.getConfigNodeId());

      boolean isJoinedCluster = false;
      for (int retry = 0; retry < SCHEDULE_WAITING_RETRY_NUM; retry++) {
        if (configManager.getConsensusManager().getConsensusImpl().getAllConsensusGroupIds().size()
            > 0) {
          isJoinedCluster = true;
          break;
        }

        try {
          TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
          LOGGER.warn("Waiting leader's scheduling is interrupted.");
        }
      }

      if (!isJoinedCluster) {
        LOGGER.error(
            "The current ConfigNode can't joined the cluster because leader's scheduling failed. The possible cause is that the ip:port configuration is incorrect.");
        stop();
      }
    } catch (StartupException | IOException e) {
      LOGGER.error("Meet error while starting up.", e);
      try {
        stop();
      } catch (IOException e2) {
        LOGGER.error("Meet error when stop ConfigNode!", e);
      }
    }
  }

  void processPid() {
    String pidFile = System.getProperty(ConfigNodeConstant.IOTDB_PIDFILE);
    if (pidFile != null) {
      new File(pidFile).deleteOnExit();
    }
  }

  private void initConfigManager() {
    try {
      configManager = new ConfigManager();
    } catch (IOException e) {
      LOGGER.error("Can't start ConfigNode consensus group!", e);
      try {
        stop();
      } catch (IOException e2) {
        LOGGER.error("Meet error when stop ConfigNode!", e);
      }
    }
    // Add some Metrics for configManager
    configManager.addMetrics();
    LOGGER.info("Successfully initialize ConfigManager.");
  }

  private void setUpInternalServices() throws StartupException, IOException {
    // Setup JMXService
    registerManager.register(new JMXService());
    JMXService.registerMBean(this, mbeanName);

    registerManager.register(MetricService.getInstance());
    // bind predefined metric sets
    MetricService.getInstance().addMetricSet(new JvmMetrics());
    MetricService.getInstance().addMetricSet(new LogbackMetrics());
    MetricService.getInstance().addMetricSet(new ProcessMetrics());
    MetricService.getInstance().addMetricSet(new SystemMetrics(false));

    LOGGER.info("Successfully setup internal services.");
  }

  /** Register Non-seed ConfigNode when first startup */
  private void registerConfigNode() throws StartupException, IOException {
    TConfigNodeRegisterReq req =
        new TConfigNodeRegisterReq(
            new TConfigNodeLocation(
                INIT_NON_SEED_CONFIG_NODE_ID,
                new TEndPoint(CONF.getInternalAddress(), CONF.getInternalPort()),
                new TEndPoint(CONF.getInternalAddress(), CONF.getConsensusPort())),
            CONF.getDataRegionConsensusProtocolClass(),
            CONF.getSchemaRegionConsensusProtocolClass(),
            CONF.getSeriesPartitionSlotNum(),
            CONF.getSeriesPartitionExecutorClass(),
            COMMON_CONF.getDefaultTTLInMs(),
            CONF.getTimePartitionInterval(),
            CONF.getSchemaReplicationFactor(),
            CONF.getSchemaRegionPerDataNode(),
            CONF.getDataReplicationFactor(),
            CONF.getDataRegionPerProcessor(),
            CONF.getReadConsistencyLevel(),
            COMMON_CONF.getDiskSpaceWarningThreshold(),
            CONF.getLeastDataRegionGroupNum());

    TEndPoint targetConfigNode = CONF.getTargetConfigNode();
    if (targetConfigNode == null) {
      LOGGER.error("The targetConfigNode setting in conf is empty");
      throw new StartupException("The targetConfigNode setting in conf is empty");
    }

    for (int retry = 0; retry < 3; retry++) {
      TSStatus status;
      TConfigNodeRegisterResp resp = null;
      Object obj =
          SyncConfigNodeClientPool.getInstance()
              .sendSyncRequestToConfigNodeWithRetry(
                  targetConfigNode, req, ConfigNodeRequestType.REGISTER_CONFIG_NODE);

      if (obj instanceof TConfigNodeRegisterResp) {
        resp = (TConfigNodeRegisterResp) obj;
        status = resp.getStatus();
      } else {
        status = (TSStatus) obj;
      }

      if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        /* Always set ConfigNodeId before initConsensusManager */
        CONF.setConfigNodeId(resp.getConfigNodeId());
        configManager.initConsensusManager();
        return;
      } else if (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        targetConfigNode = status.getRedirectNode();
        LOGGER.info("ConfigNode need redirect to  {}.", targetConfigNode);
      } else if (status.getCode() == TSStatusCode.CONFIGURATION_ERROR.getStatusCode()) {
        LOGGER.error(status.getMessage());
        throw new StartupException("Configuration are not consistent!");
      } else if (status.getCode() == TSStatusCode.CONSENSUS_NOT_INITIALIZED.getStatusCode()) {
        LOGGER.error(status.getMessage());
        throw new StartupException(
            "The target ConfigNode is not started successfully, "
                + "please check the cn_target_config_node_list config!");
      }

      try {
        TimeUnit.MILLISECONDS.sleep(1000);
      } catch (InterruptedException e) {
        throw new StartupException("Register ConfigNode failed!");
      }
    }

    LOGGER.error(
        "The current ConfigNode can't send register request to the Seed-ConfigNode after all retries!");
    stop();
  }

  private void setUpRPCService() throws StartupException {
    // Setup RPCService
    ConfigNodeRPCService configNodeRPCService = new ConfigNodeRPCService();
    ConfigNodeRPCServiceProcessor configNodeRPCServiceProcessor =
        new ConfigNodeRPCServiceProcessor(configManager);
    configNodeRPCService.initSyncedServiceImpl(configNodeRPCServiceProcessor);
    registerManager.register(configNodeRPCService);
  }

  public void stop() throws IOException {
    LOGGER.info("Deactivating {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    if (configManager != null) {
      configManager.close();
    }
    LOGGER.info("{} is deactivated.", ConfigNodeConstant.GLOBAL_NAME);
    System.exit(-1);
  }

  public ConfigManager getConfigManager() {
    return configManager;
  }

  @TestOnly
  public void setConfigManager(ConfigManager configManager) {
    this.configManager = configManager;
  }

  private static class ConfigNodeHolder {

    private static final ConfigNode INSTANCE = new ConfigNode();

    private ConfigNodeHolder() {
      // Empty constructor
    }
  }

  public static ConfigNode getInstance() {
    return ConfigNodeHolder.INSTANCE;
  }
}
