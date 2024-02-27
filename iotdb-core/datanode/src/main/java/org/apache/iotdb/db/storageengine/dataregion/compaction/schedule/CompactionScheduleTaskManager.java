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

package org.apache.iotdb.db.storageengine.dataregion.compaction.schedule;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.RepairLogger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.RepairTaskStatus;
import org.apache.iotdb.db.storageengine.dataregion.compaction.repair.RepairTimePartitionScanTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class CompactionScheduleTaskManager implements IService {

  private final int workerNum = 4;
  private ExecutorService compactionScheduleTaskThreadPool;
  private static final Logger logger = LoggerFactory.getLogger(CompactionScheduleTaskManager.class);
  private static final CompactionScheduleTaskManager INSTANCE = new CompactionScheduleTaskManager();
  private static final List<DataRegion> dataRegionList = new CopyOnWriteArrayList<>();
  private final RepairDataTaskManager REPAIR_TASK_MANAGER_INSTANCE = new RepairDataTaskManager();
  private final Set<Future<Void>> submitTaskFutures = ConcurrentHashMap.newKeySet();
  private volatile boolean init = false;

  @Override
  public void start() throws StartupException {
    if (init) {
      return;
    }
    initThreadPool();
    startScheduleTasks();
    logger.info("Compaction schedule task manager started.");
  }

  public void stopRunningTasks() {
    for (Future<Void> repairTask : submitTaskFutures) {
      repairTask.cancel(true);
    }
    for (Future<Void> repairTask : submitTaskFutures) {
      if (repairTask.isDone()) {
        continue;
      }
      try {
        repairTask.get();
      } catch (Exception ignored) {
      }
    }
    submitTaskFutures.clear();
  }

  public void startScheduleTasks() {
    for (int workerId = 0; workerId < workerNum; workerId++) {
      Future<Void> future =
          compactionScheduleTaskThreadPool.submit(
              new CompactionScheduleTaskWorker(dataRegionList, workerId, workerNum));
      submitTaskFutures.add(future);
    }
  }

  @Override
  public void stop() {
    if (!init) {
      return;
    }
    compactionScheduleTaskThreadPool.shutdownNow();
    logger.info("Waiting for compaction schedule task thread pool to shut down");
    waitForThreadPoolTerminated();
  }

  @Override
  public synchronized void waitAndStop(long milliseconds) {
    if (!init) {
      return;
    }
    try {
      compactionScheduleTaskThreadPool.shutdownNow();
      compactionScheduleTaskThreadPool.awaitTermination(milliseconds, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.warn("compaction schedule task thread pool can not be closed in {} ms", milliseconds);
      Thread.currentThread().interrupt();
    }
    waitForThreadPoolTerminated();
  }

  private synchronized void initThreadPool() {
    this.compactionScheduleTaskThreadPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            workerNum, ThreadName.COMPACTION_SCHEDULE.getName());
    init = true;
  }

  private synchronized void waitForThreadPoolTerminated() {
    long startTime = System.currentTimeMillis();
    int timeMillis = 0;
    while (!compactionScheduleTaskThreadPool.isTerminated()) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      timeMillis += 200;
      long time = System.currentTimeMillis() - startTime;
      if (timeMillis % 60_000 == 0) {
        logger.info("CompactionScheduleTaskManager has wait for {} seconds to stop", time / 1000);
      }
    }
    logger.info("CompactionScheduleTaskManager stopped");
  }

  @Override
  public ServiceType getID() {
    return ServiceType.COMPACTION_SCHEDULE_SERVICE;
  }

  public static CompactionScheduleTaskManager getInstance() {
    return INSTANCE;
  }

  public static RepairDataTaskManager getRepairTaskManagerInstance() {
    return INSTANCE.REPAIR_TASK_MANAGER_INSTANCE;
  }

  public void registerDataRegion(DataRegion dataRegion) {
    dataRegionList.add(dataRegion);
  }

  public void unregisterDataRegion(DataRegion dataRegion) {
    dataRegionList.remove(dataRegion);
  }

  public class RepairDataTaskManager {

    /** a repair task is running */
    private final AtomicReference<RepairTaskStatus> repairTaskStatus =
        new AtomicReference<>(RepairTaskStatus.STOPPED);

    public boolean markRepairTaskStart() {
      return repairTaskStatus.compareAndSet(RepairTaskStatus.STOPPED, RepairTaskStatus.RUNNING);
    }

    public boolean hasRunningRepairTask() {
      return repairTaskStatus.get() != RepairTaskStatus.STOPPED;
    }

    public RepairTaskStatus getRepairTaskStatus() {
      return repairTaskStatus.get();
    }

    public void markRepairTaskFinish() {
      if (repairTaskStatus.compareAndSet(RepairTaskStatus.RUNNING, RepairTaskStatus.STOPPED)) {
        return;
      }
      if (repairTaskStatus.compareAndSet(RepairTaskStatus.STOPPING, RepairTaskStatus.STOPPED)) {
        // rename progress file as stopped
        String repairLogDirPath =
            IoTDBDescriptor.getInstance().getConfig().getSystemDir()
                + File.separator
                + RepairLogger.repairLogDir;
        File progressFile =
            new File(repairLogDirPath + File.separator + RepairLogger.repairProgressFileName);
        File stoppedFile =
            new File(
                repairLogDirPath + File.separator + RepairLogger.repairProgressStoppedFileName);
        if (progressFile.exists()) {
          try {
            Files.move(progressFile.toPath(), stoppedFile.toPath());
          } catch (IOException e) {
            logger.error("[RepairTaskManager] Failed to rename repair data progress file");
          }
        }
      }
    }

    public void markRepairTaskStopping() throws IOException {
      repairTaskStatus.compareAndSet(RepairTaskStatus.RUNNING, RepairTaskStatus.STOPPING);
    }

    public void abortRepairTask() {
      if (repairTaskStatus.get() == RepairTaskStatus.STOPPED) {
        return;
      }
      stopRunningTasks();
    }

    public Future<Void> submitRepairScanTask(RepairTimePartitionScanTask scanTask) {
      Future<Void> future = compactionScheduleTaskThreadPool.submit(scanTask);
      submitTaskFutures.add(future);
      return future;
    }

    public void waitRepairTaskFinish() {
      for (Future<Void> result : submitTaskFutures) {
        try {
          result.get();
        } catch (CancellationException cancellationException) {
          logger.warn("[RepairScheduler] scan task is cancelled");
        } catch (Exception e) {
          logger.error("[RepairScheduler] Meet errors when scan time partition files", e);
        }
      }
      submitTaskFutures.clear();
    }
  }
}
