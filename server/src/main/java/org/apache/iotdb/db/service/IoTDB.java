/**
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

import org.apache.iotdb.db.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.adapter.IoTDBConfigDynamicAdapter;
import org.apache.iotdb.db.cost.statistic.Measurement;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.monitor.StatMonitor;
import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.sync.receiver.SyncServerManager;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDB implements IoTDBMBean {

  private static final Logger logger = LoggerFactory.getLogger(IoTDB.class);
  private final String mbeanName = String.format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE,
      IoTDBConstant.JMX_TYPE, "IoTDB");
  private RegisterManager registerManager = new RegisterManager();

  public static IoTDB getInstance() {
    return IoTDBHolder.INSTANCE;
  }

  public static void main(String[] args) {
    IoTDB daemon = IoTDB.getInstance();
    daemon.active();
  }

  public void active() {
    StartupChecks checks = new StartupChecks().withDefaultTest();
    try {
      checks.verify();
    } catch (StartupException e) {
      // TODO: what are some checks
      logger.error("{}: failed to start because some checks failed. ",
          IoTDBConstant.GLOBAL_DB_NAME, e);
      return;
    }
    try {
      setUp();
    } catch (StartupException e) {
      logger.error("meet error while starting up.", e);
      deactivate();
      logger.error("{} exit", IoTDBConstant.GLOBAL_DB_NAME);
      return;
    }
    logger.info("{} has started.", IoTDBConstant.GLOBAL_DB_NAME);
  }

  private void setUp() throws StartupException {
    logger.info("Setting up IoTDB...");

    Runtime.getRuntime().addShutdownHook(new IoTDBShutdownHook());
    setUncaughtExceptionHandler();

    boolean enableWAL = IoTDBDescriptor.getInstance().getConfig().isEnableWal();
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(enableWAL);

    initMManager();
    registerManager.register(StorageEngine.getInstance());
    registerManager.register(MultiFileLogNodeManager.getInstance());
    registerManager.register(JMXService.getInstance());
    registerManager.register(JDBCService.getInstance());
    registerManager.register(Monitor.getInstance());
    registerManager.register(StatMonitor.getInstance());
    registerManager.register(Measurement.INSTANCE);
    registerManager.register(SyncServerManager.getInstance());
    registerManager.register(TVListAllocator.getInstance());

    JMXService.registerMBean(getInstance(), mbeanName);

    // When registering statMonitor, we should start recovering some statistics
    // with latest values stored
    // Warn: registMonitor() method should be called after systemDataRecovery()
    if (IoTDBDescriptor.getInstance().getConfig().isEnableStatMonitor()) {
      StatMonitor.getInstance().recovery();
    }

    logger.info("IoTDB is set up.");
  }

  private void deactivate() {
    logger.info("Deactivating IoTDB...");
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB is deactivated.");
  }

  private void initMManager(){
    MManager.getInstance().init();
    IoTDBConfigDynamicAdapter.getInstance().setInitialized(true);
    logger.debug("After initializing, ");
    logger.debug(
        "After initializing, max memTable num is {}, tsFile threshold is {}, memtableSize is {}",
        IoTDBDescriptor.getInstance().getConfig().getMaxMemtableNumber(),
        IoTDBDescriptor.getInstance().getConfig().getTsFileSizeThreshold(),
        IoTDBDescriptor.getInstance().getConfig().getMemtableSizeThreshold());

  }

  @Override
  public void stop() {
    deactivate();
  }

  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  private static class IoTDBHolder {

    private static final IoTDB INSTANCE = new IoTDB();

    private IoTDBHolder() {

    }
  }

}
