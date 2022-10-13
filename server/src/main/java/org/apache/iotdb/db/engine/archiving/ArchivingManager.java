/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.archiving;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.utils.DateTimeUtils;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ArchivingManager keep tracks of all Archiving Tasks, creates the threads to check/run the
 * ArchivingTasks.
 */
public class ArchivingManager {
  private static final Logger logger = LoggerFactory.getLogger(ArchivingManager.class);
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final ReentrantLock lock = new ReentrantLock();
  // region lock resources
  private ArchivingOperateWriter logWriter;
  // define ordering for archivingTasks, dictionary order on (status, startTime, storageGroup, -ttl)
  private final Set<ArchivingTask> archivingTasks =
      new TreeSet<>(
          (task1, task2) -> {
            int statusDiff = task1.getStatus().ordinal() - task2.getStatus().ordinal();
            if (statusDiff != 0) return statusDiff;
            long startTimeDiff = task1.getStartTime() - task2.getStartTime();
            if (startTimeDiff != 0) return startTimeDiff > 0 ? 1 : -1;
            int storageGroupDiff = task1.getStorageGroup().compareTo(task2.getStorageGroup());
            if (storageGroupDiff != 0) return storageGroupDiff;
            long ttlDiff = task2.getTTL() - task1.getTTL();
            return ttlDiff > 0 ? 1 : -1;
          });
  // the current largest ArchivingTask id + 1, used to create new tasks
  private long currentTaskId = 0;
  // endregion

  // single thread to iterate through archivingTasks and check start
  private ScheduledExecutorService archivingTaskCheckThread;
  // multiple threads to run the tasks
  private ExecutorService archivingTaskThreadPool;

  private boolean initialized = false;
  private static final long ARCHIVING_CHECK_INTERVAL = 60 * 1000L;

  private static final File LOG_FILE =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVING_FOLDER_NAME,
                  "log.bin")
              .toString());
  private static final File ARCHIVING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVING_FOLDER_NAME,
                  IoTDBConstant.ARCHIVING_LOG_FOLDER_NAME)
              .toString());

  // singleton
  private static class ArchivingManagerHolder {
    private ArchivingManagerHolder() {}

    private static final ArchivingManager INSTANCE = new ArchivingManager();
  }

  public static ArchivingManager getInstance() {
    return ArchivingManagerHolder.INSTANCE;
  }

  public void init() {
    try {
      lock.lock();

      if (initialized) {
        return;
      }

      // create necessary log files/dirs
      if (ARCHIVING_LOG_DIR == null) logger.error("ARCHIVING_LOG_DIR is null");
      if (!ARCHIVING_LOG_DIR.exists()) {
        if (ARCHIVING_LOG_DIR.mkdirs()) {
          logger.info("ARCHIVING_LOG_DIR {} created successfully", ARCHIVING_LOG_DIR);
        } else {
          logger.error("ARCHIVING_LOG_DIR {} create error", ARCHIVING_LOG_DIR);
        }
      }
      if (!ARCHIVING_LOG_DIR.isDirectory())
        logger.error("{} already exists but is not directory", ARCHIVING_LOG_DIR);
      if (!LOG_FILE.getParentFile().exists()) LOG_FILE.getParentFile().mkdirs();
      if (!LOG_FILE.exists()) {
        try {
          LOG_FILE.createNewFile();
        } catch (IOException e) {
          logger.error("{} log file could not be created", LOG_FILE.getName());
        }
      }

      // recover
      ArchivingRecover recover = new ArchivingRecover();
      recover.recover();
      this.archivingTasks.addAll(recover.getArchivingTasks());
      this.currentTaskId = recover.getCurrentTaskId();

      try {
        logWriter = new ArchivingOperateWriter(LOG_FILE);
      } catch (FileNotFoundException e) {
        logger.error("Cannot find/create log for archiving.");
        return;
      }

      archivingTaskCheckThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.ARCHIVING_CHECK.getName());
      archivingTaskCheckThread.scheduleAtFixedRate(
          this::checkArchivingTasks,
          ARCHIVING_CHECK_INTERVAL,
          ARCHIVING_CHECK_INTERVAL,
          TimeUnit.MILLISECONDS);

      archivingTaskThreadPool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              config.getArchivingThreadNum(), ThreadName.ARCHIVING_TASK.getName());
      logger.info("start archiving check thread successfully.");
      initialized = true;
    } finally {
      lock.unlock();
    }
  }

  /** close all resources used */
  public void close() {
    initialized = false;
    archivingTaskCheckThread.shutdown();
    archivingTaskThreadPool.shutdown();

    try {
      logWriter.close();
    } catch (Exception e) {
      logger.error("Cannot close archiving log writer, because:", e);
    }

    for (ArchivingTask task : archivingTasks) {
      task.close();
    }
    archivingTasks.clear();
    currentTaskId = 0;
  }

  public void clear() {
    close();
    LOG_FILE.delete();
  }

  /** creates a copy of archivingTasks and returns */
  public List<ArchivingTask> getArchivingTasks() {
    try {
      lock.lock();
      return new ArrayList<>(archivingTasks);
    } finally {
      lock.unlock();
    }
  }

  /**
   * add archiving task to archivingTasks
   *
   * @return true if set successful, false if exists duplicates or unsuccessful
   */
  public boolean setArchiving(PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    try {
      lock.lock();

      // check if there are duplicates
      for (ArchivingTask archivingTask : archivingTasks) {
        if (archivingTask.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())
            && archivingTask.getTargetDir().equals(targetDir)
            && archivingTask.getTTL() == ttl
            && archivingTask.getStartTime() == startTime) {
          logger.warn("archiving task already equals archiving task {}", archivingTask.getTaskId());
          return false;
        }
      }

      ArchivingTask newTask =
          new ArchivingTask(currentTaskId, storageGroup, targetDir, ttl, startTime);
      try {
        logWriter.log(ArchivingOperate.ArchivingOperateType.SET, newTask);
      } catch (IOException e) {
        logger.error("write log error");
        return false;
      }
      archivingTasks.add(newTask);
      currentTaskId++;
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** @return the status after operate archivingOperateType */
  private ArchivingTask.ArchivingTaskStatus statusFromOperateType(
      ArchivingOperate.ArchivingOperateType archivingOperateType) {
    switch (archivingOperateType) {
      case RESUME:
        return ArchivingTask.ArchivingTaskStatus.READY;
      case CANCEL:
        return ArchivingTask.ArchivingTaskStatus.CANCELED;
      case START:
        return ArchivingTask.ArchivingTaskStatus.RUNNING;
      case PAUSE:
        return ArchivingTask.ArchivingTaskStatus.PAUSED;
      case FINISHED:
        return ArchivingTask.ArchivingTaskStatus.FINISHED;
      case ERROR:
        return ArchivingTask.ArchivingTaskStatus.ERROR;
    }
    return null;
  }

  /**
   * Operate on task (pause, cancel, resume, etc)
   *
   * @param archivingOperateType the operator on task
   * @param taskId taskId of ArchivingTask to operate on
   * @return true if exists task with taskId and operate successfully
   */
  public boolean operate(ArchivingOperate.ArchivingOperateType archivingOperateType, long taskId) {
    try {
      lock.lock();

      ArchivingTask task = null;
      // find matching task
      for (ArchivingTask archivingTask : archivingTasks) {
        if (archivingTask.getTaskId() == taskId) {
          task = archivingTask;
          break;
        }
      }
      if (task == null) {
        // no matches
        return false;
      }

      return operate(archivingOperateType, task);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Operate on task (pause, cancel, resume, etc)
   *
   * @param archivingOperateType the operator on task
   * @param storageGroup StorageGroup of ArchivingTask to operate on
   * @return true if exists task with storageGroup and operate successfully
   */
  public boolean operate(
      ArchivingOperate.ArchivingOperateType archivingOperateType, PartialPath storageGroup) {
    try {
      lock.lock();

      ArchivingTask task = null;
      // find matching task
      for (ArchivingTask archivingTask : archivingTasks) {
        if (archivingTask.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())) {
          task = archivingTask;
          break;
        }
      }
      if (task == null) {
        // no matches
        return false;
      }

      return operate(archivingOperateType, task);
    } finally {
      lock.unlock();
    }
  }

  private boolean operate(
      ArchivingOperate.ArchivingOperateType archivingOperateType, ArchivingTask task) {
    // check if task has valid status
    switch (archivingOperateType) {
      case SET:
      case START:
      case FINISHED:
      case ERROR:
        return false;
      case CANCEL:
      case PAUSE:
        // can cancel/pause only when status=READY/RUNNING
        if (!(task.getStatus() == ArchivingTask.ArchivingTaskStatus.READY
            || task.getStatus() == ArchivingTask.ArchivingTaskStatus.RUNNING)) {
          logger.warn(
              "Cannot cancel or pause archiving task when it's in the {} status.",
              task.getStatus());
          return false;
        }
        break;
      case RESUME:
        // can resume only when status=PAUSED
        if (!(task.getStatus() == ArchivingTask.ArchivingTaskStatus.PAUSED)) {
          logger.warn("Cannot resume archiving task when it's in the {} status.", task.getStatus());
          return false;
        }
        break;
    }

    // operate
    switch (archivingOperateType) {
      case PAUSE:
      case CANCEL:
      case RESUME:
        // write to log
        try {
          logWriter.log(archivingOperateType, task);
        } catch (IOException e) {
          logger.error("write log error");
          return false;
        }
        task.setStatus(statusFromOperateType(archivingOperateType));
        return true;
      default:
        return false;
    }
  }

  /** check if any of the archivingTasks can start */
  public void checkArchivingTasks() {
    try {
      lock.lock();

      for (ArchivingTask task : archivingTasks) {

        if (task.getStartTime() - DateTimeUtils.currentTime() <= 0
            && task.getStatus() == ArchivingTask.ArchivingTaskStatus.READY) {

          // storage group has no data
          if (!StorageEngine.getInstance().getProcessorMap().containsKey(task.getStorageGroup())) {
            return;
          }

          // set task to running
          task.setStatus(ArchivingTask.ArchivingTaskStatus.RUNNING);

          // push check archivingTask to storageGroupManager, use Runnable to give task to thread
          // pool
          archivingTaskThreadPool.execute(
              () -> {
                try {
                  logWriter.log(ArchivingOperate.ArchivingOperateType.START, task);
                  task.startTask();
                } catch (IOException e) {
                  logger.error("write log error");
                  task.setStatus(ArchivingTask.ArchivingTaskStatus.ERROR);
                  return;
                }

                StorageEngine.getInstance()
                    .getProcessorMap()
                    .get(task.getStorageGroup())
                    .checkArchivingTask(task);

                // set state and remove
                try {
                  logWriter.log(ArchivingOperate.ArchivingOperateType.FINISHED, task);
                  task.finish();
                } catch (IOException e) {
                  logger.error("write log error");
                  task.setStatus(ArchivingTask.ArchivingTaskStatus.ERROR);
                  return;
                }
                task.setStatus(ArchivingTask.ArchivingTaskStatus.FINISHED);
              });
        }
      }
    } finally {
      lock.unlock();
    }
  }

  // test
  public void setCheckThreadTime(long checkThreadTime) {
    archivingTaskCheckThread.shutdown();

    archivingTaskCheckThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.ARCHIVING_CHECK.getName());
    archivingTaskCheckThread.scheduleAtFixedRate(
        this::checkArchivingTasks, checkThreadTime, checkThreadTime, TimeUnit.MILLISECONDS);
  }
}
