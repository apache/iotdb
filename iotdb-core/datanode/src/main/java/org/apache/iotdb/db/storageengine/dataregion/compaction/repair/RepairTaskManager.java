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
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RepairTaskManager implements IService {

  private final int maxScanTaskNum =
      IoTDBDescriptor.getInstance().getConfig().getRepairScanTaskNum();
  private ExecutorService repairScheduleTaskThreadPool;
  private static final Logger logger = LoggerFactory.getLogger(RepairTaskManager.class);
  private static final RepairTaskManager INSTANCE = new RepairTaskManager();
  private final Set<Future<Void>> repairTasks = new HashSet<>();

  /** a repair task is running */
  private final AtomicBoolean isRepairingData = new AtomicBoolean(false);

  private volatile boolean init = false;

  public boolean markRepairTaskStart() {
    return isRepairingData.compareAndSet(false, true);
  }

  public boolean hasRunningRepairTask() {
    return isRepairingData.get() || !repairTasks.isEmpty();
  }

  public void markRepairTaskFinish() {
    isRepairingData.set(false);
  }

  public void markRepairTaskStopped() throws IOException {
    isRepairingData.set(false);
    String repairLogDirPath =
        IoTDBDescriptor.getInstance().getConfig().getSystemDir()
            + File.separator
            + RepairLogger.repairLogDir
            + File.separator
            + RepairLogger.stopped;
    File stoppedMark = new File(repairLogDirPath);
    if (!stoppedMark.exists()) {
      Files.createFile(stoppedMark.toPath());
    }
  }

  @Override
  public synchronized void start() throws StartupException {
    if (repairScheduleTaskThreadPool == null && maxScanTaskNum > 0) {
      initThreadPool();
    }
    logger.info("Repair schedule task manager started.");
    init = true;
  }

  public void waitReady() throws InterruptedException {
    while (!init) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
    }
  }

  @Override
  public synchronized void stop() {
    if (!init) {
      return;
    }
    repairScheduleTaskThreadPool.shutdownNow();
    logger.info("Waiting for repair schedule task thread pool to shut down");
    waitForThreadPoolTerminated();
  }

  @Override
  public synchronized void waitAndStop(long milliseconds) {
    try {
      repairScheduleTaskThreadPool.shutdownNow();
      repairScheduleTaskThreadPool.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("Repair schedule task thread pool can not be closed in {} ms", milliseconds);
      Thread.currentThread().interrupt();
    }
    waitForThreadPoolTerminated();
  }

  public synchronized void abortRepairTask() {
    for (Future<Void> repairTask : repairTasks) {
      repairTask.cancel(true);
    }
    for (Future<Void> repairTask : repairTasks) {
      if (repairTask.isDone()) {
        continue;
      }
      try {
        repairTask.get();
      } catch (Exception ignored) {
      }
    }
    repairTasks.clear();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.REPAIR_DATA_SERVICE;
  }

  public synchronized Future<Void> submitScanTask(RepairTimePartitionScanTask scanTask) {
    Future<Void> future = repairScheduleTaskThreadPool.submit(scanTask);
    repairTasks.add(future);
    return future;
  }

  private synchronized void initThreadPool() {
    this.repairScheduleTaskThreadPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(maxScanTaskNum, ThreadName.REPAIR_DATA.getName());
  }

  private synchronized void waitForThreadPoolTerminated() {
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
    logger.info("RepairScheduleTaskManager stopped");
  }

  public static RepairTaskManager getInstance() {
    return INSTANCE;
  }
}
