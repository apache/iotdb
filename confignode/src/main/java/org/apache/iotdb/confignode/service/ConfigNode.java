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

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.confignode.conf.ConfigNodeConstant;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCService;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ConfigNode implements ConfigNodeMBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNode.class);

  private final String mbeanName =
      String.format(
          "%s:%s=%s",
          ConfigNodeConstant.CONFIGNODE_PACKAGE, ConfigNodeConstant.JMX_TYPE, "ConfigNode");

  private final RegisterManager registerManager = new RegisterManager();

  private final ConfigNodeRPCService configNodeRPCService;

  private ConfigManager configManager;

  public ConfigNode() {
    this.configNodeRPCService = new ConfigNodeRPCService();

    try {
      this.configManager = new ConfigManager();
    } catch (IOException e) {
      LOGGER.error("Can't start ConfigNode consensus group!", e);
      try {
        stop();
      } catch (IOException e2) {
        LOGGER.error("Meet error when stop ConfigNode!", e);
      }
      System.exit(0);
    }
  }

  public static void main(String[] args) {
    new ConfigNodeCommandLine().doMain(args);
  }

  /** Register services */
  private void setUp() throws StartupException, IOException {
    LOGGER.info("Setting up {}...", ConfigNodeConstant.GLOBAL_NAME);
    registerManager.register(new JMXService());
    JMXService.registerMBean(this, mbeanName);

    configNodeRPCService.initSyncedServiceImpl(new ConfigNodeRPCServiceProcessor(configManager));
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

    LOGGER.info("{} has started.", ConfigNodeConstant.GLOBAL_NAME);
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
}
