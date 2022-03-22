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

import org.apache.iotdb.commons.exception.ShutdownException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.StartupChecks;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.service.thrift.server.ConfigNodeRPCServer;
import org.apache.iotdb.confignode.service.thrift.server.ConfigNodeRPCServerProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigNode implements ConfigNodeMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          ConfigNodeConstant.CONFIGNODE_PACKAGE, ConfigNodeConstant.JMX_TYPE, "ConfigNode");

  private static final RegisterManager registerManager = new RegisterManager();

  private ConfigNode() {
    // empty constructor
  }

  public static void main(String[] args) {
    new ConfigNodeCommandLine().doMain(args);
  }

  /** Register services */
  private void setUp() throws StartupException {
    LOGGER.info("Setting up {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.register(JMXService.getInstance());
    JMXService.registerMBean(getInstance(), mbeanName);

    ConfigNodeRPCServerProcessor configNodeRPCServerProcessor = new ConfigNodeRPCServerProcessor();
    ConfigNodeRPCServer.getInstance().initSyncedServiceImpl(configNodeRPCServerProcessor);
    registerManager.register(ConfigNodeRPCServer.getInstance());
    LOGGER.info("Init rpc server success");
  }

  public void active() {
    StartupChecks checks = new StartupChecks().withDefaultTest();
    try {
      // Startup environment check
      checks.verify();
    } catch (StartupException e) {
      LOGGER.error(
          "{}: failed to start because some checks failed. ", ConfigNodeConstant.GLOBAL_NAME, e);
      return;
    }

    try {
      setUp();
    } catch (StartupException e) {
      LOGGER.error("Meet error while starting up.", e);
      deactivate();
      return;
    }

    LOGGER.info("{} has started.", ConfigNodeConstant.GLOBAL_NAME);
  }

  public void deactivate() {
    LOGGER.info("Deactivating {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info("{} is deactivated.", ConfigNodeConstant.GLOBAL_NAME);
  }

  @TestOnly
  public void shutdown() throws ShutdownException {
    LOGGER.info("Deactivating {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.shutdownAll();
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info("{} is deactivated.", ConfigNodeConstant.GLOBAL_NAME);
  }

  public void stop() {
    deactivate();
  }

  private static class ConfigNodeHolder {

    private static final ConfigNode INSTANCE = new ConfigNode();

    private ConfigNodeHolder() {
      // empty constructor
    }
  }

  public static ConfigNode getInstance() {
    return ConfigNodeHolder.INSTANCE;
  }
}
