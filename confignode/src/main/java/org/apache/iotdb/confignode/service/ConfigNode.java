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

import org.apache.iotdb.confignode.conf.ConfigNodeConfCheck;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.exception.ConfigNodeException;
import org.apache.iotdb.confignode.exception.StartupException;
import org.apache.iotdb.confignode.service.startup.StartupChecks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ConfigNode implements ConfigNodeMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          ConfigNodeConstant.CONFIGNODE_PACKAGE, ConfigNodeConstant.JMX_TYPE, "ConfigNode");

  private static final RegisterManager registerManager = new RegisterManager();

  public ConfigNode() {
    // empty constructor
  }

  public static void main(String[] args) {
    try {
      ConfigNodeConfCheck.getInstance().checkConfig();
    } catch (ConfigNodeException | IOException e) {
      LOGGER.error("Meet error when doing start checking", e);
      System.exit(1);
    }

    ConfigNode daemon = ConfigNode.getInstance();
    daemon.active();
  }

  public void active() {
    StartupChecks checks = new StartupChecks().withDefaultTest();
    try {
      checks.verify();
    } catch (StartupException e) {
      LOGGER.error(
          "{}: failed to start because some checks failed. ", ConfigNodeConstant.GLOBAL_NAME, e);
      return;
    }

    try {
      setUp();
    } catch (StartupException e) {
      LOGGER.error("meet error while starting up.", e);
      deactivate();
      LOGGER.error("{} exit", ConfigNodeConstant.GLOBAL_NAME);
      return;
    }

    LOGGER.info("{} has started.", ConfigNodeConstant.GLOBAL_NAME);
  }

  private void setUp() throws StartupException {
    LOGGER.info("Setting up {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.register(JMXService.getInstance());
    JMXService.registerMBean(getInstance(), mbeanName);
    LOGGER.info(
        "Congratulation, {} is set up successfully. Now, enjoy yourself!",
        ConfigNodeConstant.GLOBAL_NAME);
  }

  public void deactivate() {
    LOGGER.info("Deactivating {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    LOGGER.info("{} is deactivated.", ConfigNodeConstant.GLOBAL_NAME);
  }

  public void shutdown() {
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
