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
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.client.sync.confignode.SyncConfigNodeClientPool;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.SystemPropertiesUtils;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCService;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;
import org.apache.iotdb.db.service.metrics.MetricsService;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ConfigNode implements ConfigNodeMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);

  private static final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          ConfigNodeConstant.CONFIGNODE_PACKAGE, ConfigNodeConstant.JMX_TYPE, "ConfigNode");

  private static final int scheduleWaitingRetryNum = 20;
  private final RegisterManager registerManager = new RegisterManager();

  private ConfigManager configManager;

  private ConfigNode() {
    // we do not init anything here, so that we can re-initialize the instance in IT.
  }

  public static void main(String[] args) {
    new ConfigNodeCommandLine().doMain(args);
  }

  public void active() {
    LOGGER.info("Activating {}...", ConfigNodeConstant.GLOBAL_NAME);

    try {
      // Set up internal services
      setUpInternalServices();
      // Init ConfigManager
      initConfigManager();

      /* Restart */
      if (SystemPropertiesUtils.isRestarted()) {
        setUpRPCService();
        LOGGER.info(
            "{} has successfully started and joined the cluster.", ConfigNodeConstant.GLOBAL_NAME);
        return;
      }

      /* Initial startup of Seed-ConfigNode */
      if (ConfigNodeDescriptor.getInstance().isSeedConfigNode()) {
        SystemPropertiesUtils.storeSystemParameters();
        // Seed-ConfigNode should apply itself when first start
        configManager
            .getNodeManager()
            .applyConfigNode(
                new TConfigNodeLocation(
                    0,
                    new TEndPoint(conf.getInternalAddress(), conf.getInternalPort()),
                    new TEndPoint(conf.getInternalAddress(), conf.getConsensusPort())));
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
          "{} has registered successfully. Waiting for the leader's scheduling to join the cluster.",
          ConfigNodeConstant.GLOBAL_NAME);

      boolean isJoinedCluster = false;
      for (int retry = 0; retry < scheduleWaitingRetryNum; retry++) {
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

    // Setup UDFService
    registerManager.register(
        UDFExecutableManager.setupAndGetInstance(conf.getTemporaryLibDir(), conf.getUdfLibDir()));
    registerManager.register(UDFClassLoaderManager.setupAndGetInstance(conf.getUdfLibDir()));
    registerManager.register(UDFRegistrationService.setupAndGetInstance(conf.getSystemUdfDir()));

    // Setup MetricsService
    registerManager.register(MetricsService.getInstance());
    MetricsService.getInstance().startAllReporter();

    LOGGER.info("Successfully setup internal services.");
  }

  /** Register Non-seed ConfigNode when first startup */
  private void registerConfigNode() throws StartupException, IOException {
    TConfigNodeRegisterReq req =
        new TConfigNodeRegisterReq(
            new TConfigNodeLocation(
                -1,
                new TEndPoint(conf.getInternalAddress(), conf.getInternalPort()),
                new TEndPoint(conf.getInternalAddress(), conf.getConsensusPort())),
            conf.getDataRegionConsensusProtocolClass(),
            conf.getSchemaRegionConsensusProtocolClass(),
            conf.getSeriesPartitionSlotNum(),
            conf.getSeriesPartitionExecutorClass(),
            CommonDescriptor.getInstance().getConfig().getDefaultTTL(),
            conf.getTimePartitionInterval(),
            conf.getSchemaReplicationFactor(),
            conf.getSchemaRegionPerDataNode(),
            conf.getDataReplicationFactor(),
            conf.getDataRegionPerProcessor(),
            conf.getReadConsistencyLevel());

    TEndPoint targetConfigNode = conf.getTargetConfigNode();
    for (int retry = 0; retry < 3; retry++) {
      TConfigNodeRegisterResp resp =
          (TConfigNodeRegisterResp)
              SyncConfigNodeClientPool.getInstance()
                  .sendSyncRequestToConfigNodeWithRetry(
                      targetConfigNode, req, ConfigNodeRequestType.REGISTER_CONFIG_NODE);
      if (resp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        conf.setPartitionRegionId(resp.getPartitionRegionId().getId());
        return;
      } else if (resp.getStatus().getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
        targetConfigNode = resp.getStatus().getRedirectNode();
        LOGGER.info("ConfigNode need redirect to  {}.", targetConfigNode);
      } else if (resp.getStatus().getCode() == TSStatusCode.ERROR_GLOBAL_CONFIG.getStatusCode()) {
        LOGGER.error("Configuration may not be consistent, {}", req);
        throw new StartupException("Configuration are not consistent!");
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
