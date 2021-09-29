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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.settle.SettleLog;
import org.apache.iotdb.db.engine.settle.SettleTask;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.tools.settle.TsFileAndModSettleTool;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class SettleService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(SettleService.class);

  private AtomicInteger threadCnt = new AtomicInteger();
  private ExecutorService settleThreadPool;
  private static AtomicInteger filesToBeSettledCount = new AtomicInteger();
  private static PartialPath storageGroupPath;

  public static SettleService getINSTANCE() {
    return InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final SettleService INSTANCE = new SettleService();

    private InstanceHolder() {}
  }

  @Override
  public void start() {
    try {
      startSettling();
    } catch (WriteProcessException | StorageEngineException e) {
      e.printStackTrace();
    }
  }

  public void startSettling() throws WriteProcessException, StorageEngineException {
    int settleThreadNum = IoTDBDescriptor.getInstance().getConfig().getSettleThreadNum();
    settleThreadPool =
        Executors.newFixedThreadPool(
            settleThreadNum, r -> new Thread(r, "SettleThread-" + threadCnt.getAndIncrement()));
    TsFileAndModSettleTool.findFilesToBeRecovered();
    countSettleFiles();
    if (!SettleLog.createSettleLog() || filesToBeSettledCount.get() == 0) {
      stop();
      return;
    }
    settleAll();
  }

  @Override
  public void stop() {
    SettleLog.closeLogWriter();
    TsFileAndModSettleTool.clearRecoverSettleFileMap();
    setStorageGroupPath(null);
    filesToBeSettledCount.set(0);
    if (settleThreadPool != null) {
      settleThreadPool.shutdownNow();
      logger.info("Waiting for settle task pool to shut down");
      settleThreadPool = null;
      logger.info("Settle service stopped");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SETTLE_SERVICE;
  }

  private void settleAll() throws WriteProcessException, StorageEngineException {
    logger.info(
        "Totally find "
            + getFilesToBeSettledCount()
            + " tsFiles to be settled, including "
            + TsFileAndModSettleTool.recoverSettleFileMap.size()
            + " tsFiles to be recovered.");
    StorageEngine.getInstance().settleAll(getStorageGroupPath());
  }

  public static AtomicInteger getFilesToBeSettledCount() {
    return filesToBeSettledCount;
  }

  private static void countSettleFiles() throws StorageEngineException {
    filesToBeSettledCount.addAndGet(
        StorageEngine.getInstance().countSettleFiles(getStorageGroupPath()));
  }

  public void submitSettleTask(SettleTask settleTask) {
    settleThreadPool.submit(settleTask);
  }

  /** This method is used to settle TsFile in the main thread. */
  public void settleTsFile(SettleTask settleTask) throws Exception {
    settleTask.settleTsFile();
  }

  public static PartialPath getStorageGroupPath() {
    return storageGroupPath;
  }

  public static void setStorageGroupPath(PartialPath storageGroupPath) {
    SettleService.storageGroupPath = storageGroupPath;
  }
}
