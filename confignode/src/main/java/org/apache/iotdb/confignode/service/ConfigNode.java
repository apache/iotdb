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
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.conf.ConfigNodeRemoveCheck;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCService;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConfigNode implements ConfigNodeMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);
  private static final int onlineConfigNodeNum = 1;

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          ConfigNodeConstant.CONFIGNODE_PACKAGE, ConfigNodeConstant.JMX_TYPE, "ConfigNode");

  private final RegisterManager registerManager = new RegisterManager();

  private static ConfigNodeRPCService configNodeRPCService;
  private static ConfigNodeRPCServiceProcessor configNodeRPCServiceProcessor;

  private ConfigManager configManager;

  private ConfigNode() {
    // we do not init anything here, so that we can re-initialize the instance in IT.
  }

  private void initConfigManager() {
    // Init ConfigManager
    try {
      this.configManager = new ConfigManager();
    } catch (IOException e) {
      LOGGER.error("Can't start ConfigNode consensus group!", e);
      try {
        stop();
      } catch (IOException e2) {
        LOGGER.error("Meet error when stop ConfigNode!", e);
      }
      System.exit(-1);
    }

    // Init RPC service
    configNodeRPCService = new ConfigNodeRPCService();
    configNodeRPCServiceProcessor = new ConfigNodeRPCServiceProcessor(configManager);
  }

  public static void main(String[] args) {
    new ConfigNodeCommandLine().doMain(args);
  }

  /** Register services */
  private void setUp() throws StartupException, IOException {
    LOGGER.info("Setting up {}...", ConfigNodeConstant.GLOBAL_NAME);
    // Init ConfigManager
    initConfigManager();

    registerManager.register(new JMXService());
    JMXService.registerMBean(this, mbeanName);

    configNodeRPCService.initSyncedServiceImpl(configNodeRPCServiceProcessor);
    registerManager.register(configNodeRPCService);
    LOGGER.info("Init rpc server success");
  }

  public void active() {
    try {
      setUp();
    } catch (StartupException | IOException e) {
      LOGGER.error("Meet error while starting up.", e);
      try {
        deactivate();
      } catch (IOException e2) {
        LOGGER.error("Meet error when stop ConfigNode!", e);
      }
      return;
    }

    LOGGER.info(
        "{} has successfully started and joined the cluster.", ConfigNodeConstant.GLOBAL_NAME);
  }

  public void deactivate() throws IOException {
    LOGGER.info("Deactivating {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    configManager.close();
    LOGGER.info("{} is deactivated.", ConfigNodeConstant.GLOBAL_NAME);
  }

  public void stop() throws IOException {
    deactivate();
  }

  public void doRemoveNode(String[] args) throws IOException {
    LOGGER.info("Starting to remove {}...", ConfigNodeConstant.GLOBAL_NAME);
    if (args.length != 3) {
      LOGGER.info("Usage: -r <ip1>:<rpcPort1>,<ip2>:<rpcPort2>");
      return;
    }

    try {
      List<TEndPoint> removeConfigNodes =
          NodeUrlUtils.parseTEndPointUrls(new ArrayList<>(Arrays.asList(args[2].split(","))));
      for (TEndPoint endPoint : removeConfigNodes) {
        if (!ConfigNodeRemoveCheck.getInstance().removeCheck(endPoint)) {
          LOGGER.error("The Current endpoint is not in the Cluster.");
          return;
        }
      }
    } catch (BadNodeUrlException e) {
      LOGGER.warn("No ConfigNodes need to be removed.", e);
    }
    LOGGER.info("{} is removed.", ConfigNodeConstant.GLOBAL_NAME);
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
