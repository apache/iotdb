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
package org.apache.iotdb;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.ConfigurationException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.ConfigNodeStartupCheck;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.confignode.service.ConfigNodeShutdownHook;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.service.DataNodeServerCommandLine;
import org.apache.iotdb.db.service.IoTDBShutdownHook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ManualStartServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(ManualStartServer.class);

  private boolean configNodeStarted = false;
  private boolean dataNodeStarted = false;
  private boolean started = false;
  private File iotdbBaseDir = new File("storage/iotdb");

  public void start(File iotdbBaseDir) {
    this.iotdbBaseDir = iotdbBaseDir;
    start();
  }

  public void start() {
    configure();

    // Start up the DB
    LOGGER.info("Starting IoTDB config-node ...");
    boolean success = startConfigNodeService();
    if (success) {
      LOGGER.info("Starting IoTDB config-node: SUCCESS");
    } else {
      LOGGER.error("Starting IoTDB config-node: ERROR");
      return;
    }
    LOGGER.info("Starting IoTDB data-node ...");
    success = startDataNodeService();
    if (success) {
      LOGGER.info("Starting IoTDB data-node: SUCCESS");
    } else {
      LOGGER.error("Starting IoTDB data-node: ERROR");
      return;
    }

    started = true;
  }

  public void stop() {
    if (!started) {
      return;
    }

    started = false;
    // Shut down the DB
    LOGGER.info("Stopping IoTDB data-node ...");
    boolean success = stopDataNodeService();
    if (success) {
      LOGGER.info("Stopping IoTDB data-node: SUCCESS");
    } else {
      LOGGER.error("Stopping IoTDB data-node: ERROR");
    }
    LOGGER.info("Stopping IoTDB config-node ...");
    success = stopConfigNodeService();
    if (success) {
      LOGGER.info("Stopping IoTDB config-node: SUCCESS");
    } else {
      LOGGER.error("Stopping IoTDB config-node: ERROR");
    }
  }

  public void configure() {
    // Enable the rest-server.
    IoTDBRestServiceDescriptor.getInstance().getConfig().setEnableRestService(true);

    // Init the configurations.
    // (All of them need to be initialized or initializing some later will invalidate the config we
    // did before)
    CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
    ConfigNodeConfig configNodeConfig = ConfigNodeDescriptor.getInstance().getConf();
    IoTDBConfig dataNodeConfig = IoTDBDescriptor.getInstance().getConfig();

    // Configure the data-directory.
    LOGGER.info(
        "Using {} as base directory for TimechoDB data storage", iotdbBaseDir.getAbsolutePath());
    // General settings
    commonConfig.setUserFolder(
        new File(iotdbBaseDir, "data/datanode/system/users").getAbsolutePath());
    commonConfig.setRoleFolder(
        new File(iotdbBaseDir, "data/datanode/system/roles").getAbsolutePath());
    commonConfig.setProcedureWalFolder(
        new File(iotdbBaseDir, "data/datanode/system/procedure").getAbsolutePath());
    commonConfig.setSyncDir(new File(iotdbBaseDir, "data/datanode/sync").getAbsolutePath());
    commonConfig.setWalDirs(
        new String[] {new File(iotdbBaseDir, "data/datanode/wal").getAbsolutePath()});
    // Disable the OS-level metrics, of the sandbox audit will fail and kill the application.
    // TODO: Find a replacement for this ...
    // commonConfig.setOsMetricsEnabled(false);
    // Config-node settings
    configNodeConfig.setSystemDir(
        new File(iotdbBaseDir, "data/confignode/system").getAbsolutePath());
    configNodeConfig.setPipeReceiverFileDir(
        new File(iotdbBaseDir, "data/confignode/system/pipe/receiver").getAbsolutePath());
    configNodeConfig.setConsensusDir(
        new File(iotdbBaseDir, "data/confignode/consensus").getAbsolutePath());
    configNodeConfig.setExtLibDir(new File(iotdbBaseDir, "ext").getAbsolutePath());
    configNodeConfig.setUdfDir(new File(iotdbBaseDir, "ext/udf").getAbsolutePath());
    configNodeConfig.setTriggerDir(new File(iotdbBaseDir, "ext/trigger").getAbsolutePath());
    configNodeConfig.setPipeDir(new File(iotdbBaseDir, "ext/pipe").getAbsolutePath());
    // Data-node settings
    dataNodeConfig.setSystemDir(new File(iotdbBaseDir, "data/datanode/system").getAbsolutePath());
    dataNodeConfig.setPipeReceiverFileDirs(
        new String[] {
          new File(iotdbBaseDir, "data/datanode/system/pipe/receiver").getAbsolutePath()
        });
    dataNodeConfig.setIndexRootFolder(new File(iotdbBaseDir, "data/index").getAbsolutePath());
    dataNodeConfig.setSchemaDir(
        new File(iotdbBaseDir, "data/datanode/system/schema").getAbsolutePath());
    dataNodeConfig.setQueryDir(new File(iotdbBaseDir, "data/datanode/query").getAbsolutePath());
    dataNodeConfig.setRatisDataRegionSnapshotDir(
        new File(iotdbBaseDir, "data/datanode/data/snapshot").getAbsolutePath());
    dataNodeConfig.setConsensusDir(
        new File(iotdbBaseDir, "data/datanode/consensus").getAbsolutePath());
    dataNodeConfig.setSortTmpDir(new File(iotdbBaseDir, "data/datanode/tmp").getAbsolutePath());
    dataNodeConfig.setTierDataDirs(
        new String[][] {{new File(iotdbBaseDir, "data/datanode/data").getAbsolutePath()}});
    dataNodeConfig.setExtDir(new File(iotdbBaseDir, "ext").getAbsolutePath());
    dataNodeConfig.setUdfDir(new File(iotdbBaseDir, "ext/udf").getAbsolutePath());
    dataNodeConfig.setTriggerDir(new File(iotdbBaseDir, "ext/trigger").getAbsolutePath());
    dataNodeConfig.setPipeLibDir(new File(iotdbBaseDir, "ext/pipe").getAbsolutePath());
    dataNodeConfig.setExtPipeDir(new File(iotdbBaseDir, "ext/extPipe").getAbsolutePath());
    dataNodeConfig.setExtPipeDir(new File(iotdbBaseDir, "ext/mqtt").getAbsolutePath());
  }

  public boolean startConfigNodeService() {
    try {
      // Startup environment check
      ConfigNodeStartupCheck checks = new ConfigNodeStartupCheck(IoTDBConstant.CN_ROLE);
      // Do ConfigNode startup checks
      checks.startUpCheck();
    } catch (StartupException | ConfigurationException | IOException e) {
      LOGGER.error("Meet error when doing start checking", e);
      return false;
    }
    ConfigNode configNode = ConfigNode.getInstance();
    configNode.active();
    configNodeStarted = true;
    return true;
  }

  protected synchronized boolean stopConfigNodeService() {
    if (configNodeStarted) {
      ConfigNodeShutdownHook configNodeShutdownHook = new ConfigNodeShutdownHook();
      // ! We don't want this to execute asynchronously.
      //noinspection CallToThreadRun
      configNodeShutdownHook.run();
      configNodeStarted = false;
      return true;
    }
    return false;
  }

  public boolean startDataNodeService() {
    if (!configNodeStarted) {
      LOGGER.error("IoTDB's Config Node must be started first.");
      return false;
    }
    new DataNodeServerCommandLine().doMain(new String[] {"-s"});
    dataNodeStarted = true;
    return true;
  }

  protected synchronized boolean stopDataNodeService() {
    if (dataNodeStarted) {
      IoTDBShutdownHook dataNodeShutdownHook = new IoTDBShutdownHook();
      // ! We don't want this to execute asynchronously.
      //noinspection CallToThreadRun
      dataNodeShutdownHook.run();
      dataNodeStarted = false;
      return true;
    }
    return false;
  }

  public static void main(String[] args) {
    ManualStartServer server = new ManualStartServer();
    server.start();
  }
}
