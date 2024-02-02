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

package org.apache.iotdb.db.storageengine.dataregion.compaction.repair;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RepairScheduleTaskManager implements IService {

  private int maxScanTaskNum = 4;
  private ExecutorService repairScheduleTaskThreadPool;
  private static Logger logger = LoggerFactory.getLogger(RepairScheduleTaskManager.class);
  private static final RepairScheduleTaskManager INSTANCE = new RepairScheduleTaskManager();

  @Override
  public synchronized void start() throws StartupException {
    if (repairScheduleTaskThreadPool == null && maxScanTaskNum > 0) {
      initThreadPool();
    }
    logger.info("Repair schedule task manager started.");
  }

  @Override
  public void stop() {
    if (repairScheduleTaskThreadPool == null) {
      return;
    }
    repairScheduleTaskThreadPool.shutdownNow();
    logger.info("Waiting for repair schedule task thread pool to shut down");
    waitForThreadPoolTerminated();
  }

  @Override
  public void waitAndStop(long milliseconds) {
    try {
      repairScheduleTaskThreadPool.shutdownNow();
      repairScheduleTaskThreadPool.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("Repair schedule task thread pool can not be closed in {} ms", milliseconds);
      Thread.currentThread().interrupt();
    }
    waitForThreadPoolTerminated();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.REPAIR_DATA_SERVICE;
  }

  public int getMaxScanTaskNum() {
    return maxScanTaskNum;
  }

  public Future<Void> submitScanTask(UnsortedDataScanTask scanTask) {
    return repairScheduleTaskThreadPool.submit(scanTask);
  }

  private void initThreadPool() {
    this.repairScheduleTaskThreadPool =
        IoTDBThreadPoolFactory.newCachedThreadPool(
            ThreadName.REPAIR_DATA.getName(), maxScanTaskNum);
  }

  private void waitForThreadPoolTerminated() {
    long startTime = System.currentTimeMillis();
    int timeMillis = 0;
    while (!repairScheduleTaskThreadPool.isTerminated()) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      timeMillis += 200;
      long time = System.currentTimeMillis() - startTime;
      if (timeMillis % 60_000 == 0) {
        logger.info("RepairScheduleTaskManager has wait for {} seconds to stop", time / 1000);
      }
    }
    repairScheduleTaskThreadPool = null;
    logger.info("RepairScheduleTaskManager stopped");
  }

  public static RepairScheduleTaskManager getInstance() {
    return INSTANCE;
  }
}
