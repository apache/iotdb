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
package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.upgrade.UpgradeLog;
import org.apache.iotdb.db.engine.upgrade.UpgradeTask;
import org.apache.iotdb.db.utils.UpgradeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class UpgradeSevice implements IService {

  private static final Logger logger = LoggerFactory.getLogger(UpgradeSevice.class);

  private ExecutorService upgradeThreadPool;
  private static final AtomicInteger cntUpgradeFileNum = new AtomicInteger();

  private UpgradeSevice() {}

  public static UpgradeSevice getINSTANCE() {
    return InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final UpgradeSevice INSTANCE = new UpgradeSevice();

    private InstanceHolder() {}
  }

  @Override
  public void start() {
    int updateThreadNum = IoTDBDescriptor.getInstance().getConfig().getUpgradeThreadCount();
    if (updateThreadNum <= 0) {
      updateThreadNum = 1;
    }
    upgradeThreadPool = IoTDBThreadPoolFactory.newFixedThreadPool(updateThreadNum, "UpgradeThread");
    UpgradeLog.createUpgradeLog();
    countUpgradeFiles();
    if (cntUpgradeFileNum.get() == 0) {
      stop();
      return;
    }
    upgradeAll();
  }

  @Override
  public void stop() {
    UpgradeLog.closeLogWriter();
    UpgradeUtils.clearUpgradeRecoverMap();
    if (upgradeThreadPool != null) {
      upgradeThreadPool.shutdownNow();
      logger.info("Waiting for upgrade task pool to shut down");
      upgradeThreadPool = null;
      logger.info("Upgrade service stopped");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UPGRADE_SERVICE;
  }

  public static AtomicInteger getTotalUpgradeFileNum() {
    return cntUpgradeFileNum;
  }

  public void submitUpgradeTask(UpgradeTask upgradeTask) {
    upgradeThreadPool.submit(upgradeTask);
  }

  private static void countUpgradeFiles() {
    //    cntUpgradeFileNum.addAndGet(StorageEngine.getInstance().countUpgradeFiles());
    //    logger.info("finish counting upgrading files, total num:{}", cntUpgradeFileNum);
  }

  private static void upgradeAll() {
    //    try {
    //      StorageEngine.getInstance().upgradeAll();
    //    } catch (StorageEngineException e) {
    //      logger.error("Cannot perform a global upgrade because", e);
    //    }
  }
}
