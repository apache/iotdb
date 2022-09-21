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
package org.apache.iotdb.db.engine.archive;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
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
 * ArchiveManager keep tracks of all Archive Tasks, creates the threads to check/run the
 * ArchiveTasks.
 */
public class ArchiveManager {
  private static final Logger logger = LoggerFactory.getLogger(ArchiveManager.class);
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final ReentrantLock lock = new ReentrantLock();
  // region lock resources
  private ArchiveOperateWriter logWriter;
  // define ordering for archiveTasks, dictionary order on (status, startTime, storageGroup, -ttl)
  private final Set<ArchiveTask> archiveTasks =
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
  // the current largest ArchiveTask id + 1, used to create new tasks
  private long currentTaskId = 0;
  // endregion

  // single thread to iterate through archiveTasks and check start
  private ScheduledExecutorService archiveTaskCheckThread;
  // multiple threads to run the tasks
  private ExecutorService archiveTaskThreadPool;

  private boolean initialized = false;
  private static final long ARCHIVE_CHECK_INTERVAL = 60 * 1000L;

  private static final File LOG_FILE =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVE_FOLDER_NAME,
                  "log.bin")
              .toString());
  private static final File ARCHIVING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(
                  FilePathUtils.regularizePath(config.getSystemDir()),
                  IoTDBConstant.ARCHIVE_FOLDER_NAME,
                  IoTDBConstant.ARCHIVE_LOG_FOLDER_NAME)
              .toString());

  // singleton
  private static class ArchiveManagerHolder {
    private ArchiveManagerHolder() {}

    private static final ArchiveManager INSTANCE = new ArchiveManager();
  }

  public static ArchiveManager getInstance() {
    return ArchiveManagerHolder.INSTANCE;
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
      ArchiveRecover recover = new ArchiveRecover();
      recover.recover();
      this.archiveTasks.addAll(recover.getArchiveTasks());
      this.currentTaskId = recover.getCurrentTaskId();

      try {
        logWriter = new ArchiveOperateWriter(LOG_FILE);
      } catch (FileNotFoundException e) {
        logger.error("Cannot find/create log for archiving.");
        return;
      }

      archiveTaskCheckThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.ARCHIVE_CHECK.getName());
      archiveTaskCheckThread.scheduleAtFixedRate(
          this::checkArchiveTasks,
          ARCHIVE_CHECK_INTERVAL,
          ARCHIVE_CHECK_INTERVAL,
          TimeUnit.MILLISECONDS);

      archiveTaskThreadPool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              config.getArchiveThreadNum(), ThreadName.ARCHIVE_TASK.getName());
      logger.info("start archive check thread successfully.");
      initialized = true;
    } finally {
      lock.unlock();
    }
  }

  /** close all resources used */
  public void close() {
    initialized = false;
    archiveTaskCheckThread.shutdown();
    archiveTaskThreadPool.shutdown();

    try {
      logWriter.close();
    } catch (Exception e) {
      logger.error("Cannot close archive log writer, because:", e);
    }

    for (ArchiveTask task : archiveTasks) {
      task.close();
    }
    archiveTasks.clear();
    currentTaskId = 0;
  }

  /** creates a copy of archiveTasks and returns */
  public List<ArchiveTask> getArchiveTasks() {
    try {
      lock.lock();
      return new ArrayList<>(archiveTasks);
    } finally {
      lock.unlock();
    }
  }

  /**
   * add archive task to archiveTasks
   *
   * @return true if set successful, false if exists duplicates or unsuccessful
   */
  public boolean setArchive(PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    try {
      lock.lock();

      // check if there are duplicates
      for (ArchiveTask archiveTask : archiveTasks) {
        if (archiveTask.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())
            && archiveTask.getTargetDir().equals(targetDir)
            && archiveTask.getTTL() == ttl
            && archiveTask.getStartTime() == startTime) {
          logger.info("archive task already equals archive task {}", archiveTask.getTaskId());
          return false;
        }
      }

      ArchiveTask newTask = new ArchiveTask(currentTaskId, storageGroup, targetDir, ttl, startTime);
      try {
        logWriter.log(ArchiveOperate.ArchiveOperateType.SET, newTask);
      } catch (IOException e) {
        logger.error("write log error");
        return false;
      }
      archiveTasks.add(newTask);
      currentTaskId++;
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** @return the status after operate archiveOperateType */
  private ArchiveTask.ArchiveTaskStatus statusFromOperateType(
      ArchiveOperate.ArchiveOperateType archiveOperateType) {
    switch (archiveOperateType) {
      case RESUME:
        return ArchiveTask.ArchiveTaskStatus.READY;
      case CANCEL:
        return ArchiveTask.ArchiveTaskStatus.CANCELED;
      case START:
        return ArchiveTask.ArchiveTaskStatus.RUNNING;
      case PAUSE:
        return ArchiveTask.ArchiveTaskStatus.PAUSED;
      case FINISHED:
        return ArchiveTask.ArchiveTaskStatus.FINISHED;
      case ERROR:
        return ArchiveTask.ArchiveTaskStatus.ERROR;
    }
    return null;
  }

  /**
   * Operate on task (pause, cancel, resume, etc)
   *
   * @param archiveOperateType the operator on task
   * @param taskId taskId of ArchiveTask to operate on
   * @return true if exists task with taskId and operate successfully
   */
  public boolean operate(ArchiveOperate.ArchiveOperateType archiveOperateType, long taskId) {
    try {
      lock.lock();

      ArchiveTask task = null;
      // find matching task
      for (ArchiveTask archiveTask : archiveTasks) {
        if (archiveTask.getTaskId() == taskId) {
          task = archiveTask;
          break;
        }
      }
      if (task == null) {
        // no matches
        return false;
      }

      return operate(archiveOperateType, task);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Operate on task (pause, cancel, resume, etc)
   *
   * @param archiveOperateType the operator on task
   * @param storageGroup StorageGroup of ArchiveTask to operate on
   * @return true if exists task with storageGroup and operate successfully
   */
  public boolean operate(
      ArchiveOperate.ArchiveOperateType archiveOperateType, PartialPath storageGroup) {
    try {
      lock.lock();

      ArchiveTask task = null;
      // find matching task
      for (ArchiveTask archiveTask : archiveTasks) {
        if (archiveTask.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())) {
          task = archiveTask;
          break;
        }
      }
      if (task == null) {
        // no matches
        return false;
      }

      return operate(archiveOperateType, task);
    } finally {
      lock.unlock();
    }
  }

  private boolean operate(ArchiveOperate.ArchiveOperateType archiveOperateType, ArchiveTask task) {
    // check if task has valid status
    switch (archiveOperateType) {
      case SET:
      case START:
      case FINISHED:
      case ERROR:
        return false;
      case CANCEL:
      case PAUSE:
        // can cancel/pause only when status=READY/RUNNING
        if (!(task.getStatus() == ArchiveTask.ArchiveTaskStatus.READY
            || task.getStatus() == ArchiveTask.ArchiveTaskStatus.RUNNING)) {
          return false;
        }
        break;
      case RESUME:
        // can resume only when status=PAUSED
        if (!(task.getStatus() == ArchiveTask.ArchiveTaskStatus.PAUSED)) {
          return false;
        }
        break;
    }

    // operate
    switch (archiveOperateType) {
      case PAUSE:
      case CANCEL:
      case RESUME:
        // write to log
        try {
          logWriter.log(archiveOperateType, task);
        } catch (IOException e) {
          logger.error("write log error");
          return false;
        }
        task.setStatus(statusFromOperateType(archiveOperateType));
        return true;
      default:
        return false;
    }
  }

  /** check if any of the archiveTasks can start */
  public void checkArchiveTasks() {
    try {
      lock.lock();

      logger.info("checking archingTasks");
      for (ArchiveTask task : archiveTasks) {

        if (task.getStartTime() - DatetimeUtils.currentTime() <= 0
            && task.getStatus() == ArchiveTask.ArchiveTaskStatus.READY) {

          // storage group has no data
          if (!StorageEngine.getInstance().getProcessorMap().containsKey(task.getStorageGroup())) {
            return;
          }

          // set task to running
          task.setStatus(ArchiveTask.ArchiveTaskStatus.RUNNING);

          // push check archiveTask to storageGroupManager, use Runnable to give task to thread pool
          archiveTaskThreadPool.execute(
              () -> {
                try {
                  logWriter.log(ArchiveOperate.ArchiveOperateType.START, task);
                  task.startTask();
                } catch (IOException e) {
                  logger.error("write log error");
                  task.setStatus(ArchiveTask.ArchiveTaskStatus.ERROR);
                  return;
                }

                StorageEngine.getInstance()
                    .getProcessorMap()
                    .get(task.getStorageGroup())
                    .checkArchiveTask(task);
                logger.info("check archive task successfully.");

                // set state and remove
                try {
                  logWriter.log(ArchiveOperate.ArchiveOperateType.FINISHED, task);
                  task.finish();
                } catch (IOException e) {
                  logger.error("write log error");
                  task.setStatus(ArchiveTask.ArchiveTaskStatus.ERROR);
                  return;
                }
                task.setStatus(ArchiveTask.ArchiveTaskStatus.FINISHED);
              });
        }
      }
    } finally {
      lock.unlock();
    }
  }

  // test
  public void setCheckThreadTime(long checkThreadTime) {
    archiveTaskCheckThread.shutdown();

    archiveTaskCheckThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(ThreadName.ARCHIVE_CHECK.getName());
    archiveTaskCheckThread.scheduleAtFixedRate(
        this::checkArchiveTasks, checkThreadTime, checkThreadTime, TimeUnit.MILLISECONDS);
  }
}
