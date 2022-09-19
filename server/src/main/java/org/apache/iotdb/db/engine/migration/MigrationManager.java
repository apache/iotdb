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
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
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
 * MigrationManager keep tracks of all Migration Tasks, creates the threads to check/run the
 * MigrationTask.
 */
public class MigrationManager {
  private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final ReentrantLock lock = new ReentrantLock();
  // region lock resources
  private MigrationOperateWriter logWriter;
  // define ordering for migrationTasks, dictionary order on (status, startTime, storageGroup, -ttl)
  private final Set<MigrationTask> migrationTasks =
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
  // the current largest MigrationTask id + 1, used to create new tasks
  private long currentTaskId = 0;
  // endregion

  // single thread to iterate through migrationTasks and check start
  private ScheduledExecutorService migrationCheckThread;
  // multiple threads to run the tasks
  private ExecutorService migrationTaskThreadPool;

  private boolean initialized = false;
  private static final long MIGRATE_CHECK_INTERVAL = 60 * 1000L;

  private static final File LOG_FILE =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "log.bin")
              .toString());
  private static final File MIGRATING_LOG_DIR =
      SystemFileFactory.INSTANCE.getFile(
          Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "migrating")
              .toString());

  // singleton
  private static class MigrationManagerHolder {
    private MigrationManagerHolder() {}

    private static final MigrationManager INSTANCE = new MigrationManager();
  }

  public static MigrationManager getInstance() {
    return MigrationManagerHolder.INSTANCE;
  }

  public void init() {
    try {
      lock.lock();

      if (initialized) {
        return;
      }

      // create necessary log files/dirs
      if (MIGRATING_LOG_DIR == null) logger.error("MIGRATING_LOG_DIR is null");
      if (!MIGRATING_LOG_DIR.exists()) {
        if (MIGRATING_LOG_DIR.mkdirs()) {
          logger.info("MIGRATING_LOG_DIR {} created successfully", MIGRATING_LOG_DIR);
        } else {
          logger.error("MIGRATING_LOG_DIR {} create error", MIGRATING_LOG_DIR);
        }
      }
      if (!MIGRATING_LOG_DIR.isDirectory())
        logger.error("{} already exists but is not directory", MIGRATING_LOG_DIR);
      if (!LOG_FILE.getParentFile().exists()) LOG_FILE.getParentFile().mkdirs();
      if (!LOG_FILE.exists()) {
        try {
          LOG_FILE.createNewFile();
        } catch (IOException e) {
          logger.error("{} log file could not be created", LOG_FILE.getName());
        }
      }

      // recover
      MigrationRecover recover = new MigrationRecover();
      recover.recover();
      this.migrationTasks.addAll(recover.getMigrationTasks());
      this.currentTaskId = recover.getCurrentTaskId();

      try {
        logWriter = new MigrationOperateWriter(LOG_FILE);
      } catch (FileNotFoundException e) {
        logger.error("Cannot find/create log for migration.");
        return;
      }

      migrationCheckThread =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.MIGRATION_CHECK.getName());
      migrationCheckThread.scheduleAtFixedRate(
          this::checkMigration,
          MIGRATE_CHECK_INTERVAL,
          MIGRATE_CHECK_INTERVAL,
          TimeUnit.MILLISECONDS);

      migrationTaskThreadPool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              config.getMigrationThreadNum(), ThreadName.MIGRATION_TASK.getName());
      logger.info("start migration check thread successfully.");
      initialized = true;
    } finally {
      lock.unlock();
    }
  }

  /** close all resources used */
  public void close() {
    initialized = false;
    migrationCheckThread.shutdown();
    migrationTaskThreadPool.shutdown();

    try {
      logWriter.close();
    } catch (Exception e) {
      logger.error("Cannot close migration log writer, because:", e);
    }

    for (MigrationTask task : migrationTasks) {
      task.close();
    }
    migrationTasks.clear();
    currentTaskId = 0;
  }

  /** creates a copy of migrationTasks and returns */
  public List<MigrationTask> getMigrateTasks() {
    try {
      lock.lock();
      return new ArrayList<>(migrationTasks);
    } finally {
      lock.unlock();
    }
  }

  /** add migration task to migrationTasks */
  public void setMigrate(PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    try {
      lock.lock();

      // check if there are duplicates
      for (MigrationTask migrationTask : migrationTasks) {
        if (migrationTask.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())
            && migrationTask.getTargetDir().equals(targetDir)
            && migrationTask.getTTL() == ttl
            && migrationTask.getStartTime() == startTime) {
          logger.info("set migration equals migration task {}", migrationTask.getTaskId());
          return;
        }
      }

      MigrationTask newTask =
          new MigrationTask(currentTaskId, storageGroup, targetDir, ttl, startTime);
      try {
        logWriter.log(MigrationOperate.MigrationOperateType.SET, newTask);
      } catch (IOException e) {
        logger.error("write log error");
        return;
      }
      migrationTasks.add(newTask);
      currentTaskId++;
    } finally {
      lock.unlock();
    }
  }

  /** @return the status after migrationOperateType */
  private MigrationTask.MigrationTaskStatus statusFromOperateType(
      MigrationOperate.MigrationOperateType migrationOperateType) {
    switch (migrationOperateType) {
      case RESUME:
        return MigrationTask.MigrationTaskStatus.READY;
      case CANCEL:
        return MigrationTask.MigrationTaskStatus.CANCELED;
      case START:
        return MigrationTask.MigrationTaskStatus.RUNNING;
      case PAUSE:
        return MigrationTask.MigrationTaskStatus.PAUSED;
      case FINISHED:
        return MigrationTask.MigrationTaskStatus.FINISHED;
      case ERROR:
        return MigrationTask.MigrationTaskStatus.ERROR;
    }
    return null;
  }

  /**
   * Operate on task (pause, cancel, resume, etc)
   *
   * @param migrationOperateType the operator on task
   * @param taskId taskId of MigrationTask to operate on
   * @return true if exists task with taskId and operate successfully
   */
  public boolean operate(MigrationOperate.MigrationOperateType migrationOperateType, long taskId) {
    try {
      lock.lock();

      MigrationTask task = null;
      // find matching task
      for (MigrationTask migrationTask : migrationTasks) {
        if (migrationTask.getTaskId() == taskId) {
          task = migrationTask;
          break;
        }
      }
      if (task == null) {
        // no matches
        return false;
      }

      return operate(migrationOperateType, task);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Operate on task (pause, cancel, resume, etc)
   *
   * @param migrationOperateType the operator on task
   * @param storageGroup StorageGroup of MigrationTask to operate on
   * @return true if exists task with storageGroup and operate successfully
   */
  public boolean operate(
      MigrationOperate.MigrationOperateType migrationOperateType, PartialPath storageGroup) {
    try {
      lock.lock();

      MigrationTask task = null;
      // find matching task
      for (MigrationTask migrationTask : migrationTasks) {
        if (migrationTask.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())) {
          task = migrationTask;
          break;
        }
      }
      if (task == null) {
        // no matches
        return false;
      }

      return operate(migrationOperateType, task);
    } finally {
      lock.unlock();
    }
  }

  private boolean operate(
      MigrationOperate.MigrationOperateType migrationOperateType, MigrationTask task) {
    // check if task has valid status
    switch (migrationOperateType) {
      case SET:
      case START:
      case FINISHED:
      case ERROR:
        return false;
      case CANCEL:
      case PAUSE:
        // can cancel/pause only when status=READY/RUNNING
        if (!(task.getStatus() == MigrationTask.MigrationTaskStatus.READY
            || task.getStatus() == MigrationTask.MigrationTaskStatus.RUNNING)) {
          return false;
        }
        break;
      case RESUME:
        // can resume only when status=PAUSED
        if (!(task.getStatus() == MigrationTask.MigrationTaskStatus.PAUSED)) {
          return false;
        }
        break;
    }

    // operate
    switch (migrationOperateType) {
      case PAUSE:
      case CANCEL:
      case RESUME:
        // write to log
        try {
          logWriter.log(migrationOperateType, task);
        } catch (IOException e) {
          logger.error("write log error");
          return false;
        }
        task.setStatus(statusFromOperateType(migrationOperateType));
        return true;
      default:
        return false;
    }
  }

  /** check if any of the migrationTasks can start */
  public void checkMigration() {
    try {
      lock.lock();

      logger.info("check migration");
      for (MigrationTask task : migrationTasks) {

        if (task.getStartTime() - DatetimeUtils.currentTime() <= 0
            && task.getStatus() == MigrationTask.MigrationTaskStatus.READY) {

          // storage group has no data
          if (!StorageEngine.getInstance().getProcessorMap().containsKey(task.getStorageGroup())) {
            return;
          }

          // set task to running
          task.setStatus(MigrationTask.MigrationTaskStatus.RUNNING);

          // push check migration to storageGroupManager, use Runnable to give task to thread pool
          migrationTaskThreadPool.execute(
              () -> {
                try {
                  logWriter.log(MigrationOperate.MigrationOperateType.START, task);
                  task.startTask();
                } catch (IOException e) {
                  logger.error("write log error");
                  task.setStatus(MigrationTask.MigrationTaskStatus.ERROR);
                  return;
                }

                StorageEngine.getInstance()
                    .getProcessorMap()
                    .get(task.getStorageGroup())
                    .checkMigration(task);
                logger.info("check migration task successfully.");

                // set state and remove
                try {
                  logWriter.log(MigrationOperate.MigrationOperateType.FINISHED, task);
                  task.finish();
                } catch (IOException e) {
                  logger.error("write log error");
                  task.setStatus(MigrationTask.MigrationTaskStatus.ERROR);
                  return;
                }
                task.setStatus(MigrationTask.MigrationTaskStatus.FINISHED);
              });
        }
      }
    } finally {
      lock.unlock();
    }
  }

  // test
  public void setCheckThreadTime(long checkThreadTime) {
    migrationCheckThread.shutdown();

    migrationCheckThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.MIGRATION_CHECK.getName());
    migrationCheckThread.scheduleAtFixedRate(
        this::checkMigration, checkThreadTime, checkThreadTime, TimeUnit.MILLISECONDS);
  }
}
