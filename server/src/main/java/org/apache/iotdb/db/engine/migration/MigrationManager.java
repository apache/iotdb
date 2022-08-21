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
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.migration.MigrationLogWriter.MigrationLog;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.utils.DatetimeUtils;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MigrationManager keep tracks of all Migration Tasks, creates the threads to check/run the
 * MigrationTask.
 */
public class MigrationManager {
  private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);
  protected static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private MigrationLogWriter logWriter;

  // taskId -> MigrationTask
  private ConcurrentHashMap<Long, MigrationTask> migrationTasks = new ConcurrentHashMap<>();
  // the current largest MigrationTask id + 1, used to create new tasks
  private long currentTaskId = 0;
  // single thread to iterate through migrationTasks and check start
  private ScheduledExecutorService migrationCheckThread;
  // multiple threads to run the tasks
  private ExecutorService migrationTaskThreadPool;

  private boolean initialized = false;
  private static final long MIGRATE_CHECK_INTERVAL = 60 * 1000L;
  private static final String LOG_FILE_NAME =
      Paths.get(FilePathUtils.regularizePath(config.getSystemDir()), "migration", "log.bin")
          .toString();

  protected MigrationManager() {
    init();
  }

  // singleton
  private static class MigrationManagerHolder {
    private MigrationManagerHolder() {}

    private static final MigrationManager INSTANCE = new MigrationManager();
  }

  public static MigrationManager getInstance() {
    return MigrationManagerHolder.INSTANCE;
  }

  public synchronized void init() {
    if (initialized) {
      return;
    }

    try {
      logWriter = new MigrationLogWriter(LOG_FILE_NAME);
    } catch (FileNotFoundException e) {
      logger.error("Cannot find/create log for migration.");
    }

    // read from logReader
    try {
      MigrationLogReader logReader = new MigrationLogReader(LOG_FILE_NAME);
      Set<Long> errorSet = new HashSet<>();

      while (logReader.hasNext()) {
        MigrationLog log = logReader.next();

        switch (log.type) {
          case SET:
            setMigrationFromLog(
                log.taskId,
                log.storageGroup,
                FSFactoryProducer.getFSFactory().getFile(log.targetDirPath),
                log.ttl,
                log.startTime);
            break;
          case UNSET:
            unsetMigrationFromLog(log.taskId);
            break;
          case START:
            // if task started but didn't finish, then error occurred
            errorSet.add(log.taskId);
            break;
          case PAUSE:
            errorSet.remove(log.taskId);
            pauseMigrationFromLog(log.taskId);
            break;
          case UNPAUSE:
            unpauseMigrationFromLog(log.taskId);
            break;
          case FINISHED:
            // finished task => remove from list and remove from potential error task
            errorSet.remove(log.taskId);
            migrationTasks.remove(log.taskId);
            finishFromLog(log.taskId);
            break;
          case ERROR:
            // already put error in log
            errorSet.remove(log.taskId);
            errorFromLog(log.taskId);
            break;
          default:
            logger.error("read migration log: unknown type");
        }
      }

      // for each task in errorSet, the task started but didn't finish (an error)
      for (long errIndex : errorSet) {
        if (migrationTasks.containsKey(errIndex)) {
          // write to log and set task in ERROR in memory
          logWriter.error(migrationTasks.get(errIndex));
          errorFromLog(errIndex);
        } else {
          logger.error("unknown error index");
        }
      }

    } catch (IOException e) {
      logger.error("Cannot read log for migration.");
    }

    // finished migrating the tsfiles that were in process
    try {
      MigratingFileLogManager.getInstance().recover();
    } catch (IOException e) {
      logger.error("migratingfile could not recover");
    }

    migrationCheckThread =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Migration-Check");
    migrationCheckThread.scheduleAtFixedRate(
        this::checkMigration,
        MIGRATE_CHECK_INTERVAL,
        MIGRATE_CHECK_INTERVAL,
        TimeUnit.MILLISECONDS);

    migrationTaskThreadPool =
        IoTDBThreadPoolFactory.newFixedThreadPool(config.getMigrationThread(), "Migration-Task");
    logger.info("start migration check thread successfully.");
    initialized = true;
  }

  /** close the log writer */
  public void clear() {
    try {
      logWriter.close();
    } catch (Exception e) {
      logger.error("Cannot close migration log writer, because:", e);
    }
  }

  /** shutdown all threads used */
  public void shutdown() {
    migrationCheckThread.shutdown();
    migrationTaskThreadPool.shutdown();
  }

  /** creates a copy of migrationTask and returns */
  public ConcurrentHashMap<Long, MigrationTask> getMigrateTasks() {
    return new ConcurrentHashMap<>(migrationTasks);
  }

  /** add migration task to migrationTasks */
  public void setMigrate(PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    MigrationTask newTask =
        new MigrationTask(currentTaskId, storageGroup, targetDir, ttl, startTime);
    try {
      logWriter.setMigration(newTask);
    } catch (IOException e) {
      logger.error("write log error");
      return;
    }
    migrationTasks.put(currentTaskId, newTask);
    currentTaskId++;
  }

  /** add migration task to migrationTasks from log, does not write to log */
  public void setMigrationFromLog(
      long taskId, PartialPath storageGroup, File targetDir, long ttl, long startTime) {
    if (currentTaskId > taskId) {
      logger.error("set migration error, current index larger than log index");
    }

    MigrationTask newTask = new MigrationTask(taskId, storageGroup, targetDir, ttl, startTime);
    migrationTasks.put(taskId, newTask);
    currentTaskId = taskId + 1;
  }

  /**
   * unset migration task from migrationTasks list using taskId
   *
   * @param unsetTaskId taskId of task to remove
   * @return true if task with taskId exists, false otherwise
   */
  public boolean unsetMigration(long unsetTaskId) {
    if (migrationTasks.containsKey(unsetTaskId)) {
      MigrationTask task = migrationTasks.get(unsetTaskId);
      if (task.getTaskId() == unsetTaskId
          && (task.getStatus() == MigrationTask.MigrationTaskStatus.READY
              || task.getStatus() == MigrationTask.MigrationTaskStatus.RUNNING)) {
        // write to log
        try {
          logWriter.unsetMigration(task);
        } catch (IOException e) {
          logger.error("write log error");
        }
        // change status to unset
        task.setStatus(MigrationTask.MigrationTaskStatus.UNSET);
        return true;
      }
    }
    return false;
  }

  /**
   * Unset migration task from migrationTasks list using storage group. If multiple tasks with such
   * storage group exists, remove the one with the lowest taskId.
   *
   * @param storageGroup sg for task to remove
   * @return true if exists task with storageGroup
   */
  public boolean unsetMigration(PartialPath storageGroup) {
    for (MigrationTask task : migrationTasks.values()) {
      if (task.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())
          && (task.getStatus() == MigrationTask.MigrationTaskStatus.READY
              || task.getStatus() == MigrationTask.MigrationTaskStatus.RUNNING)) {
        // write to log
        try {
          logWriter.unsetMigration(task);
        } catch (IOException e) {
          logger.error("write log error");
        }
        // change status to UNSET
        task.setStatus(MigrationTask.MigrationTaskStatus.UNSET);
        return true;
      }
    }
    return false;
  }

  /** same as unsetMigrate using taskId except does not write to log */
  public boolean unsetMigrationFromLog(long taskId) {
    if (migrationTasks.containsKey(taskId)) {
      migrationTasks.remove(taskId);
      return true;
    } else {
      return false;
    }
  }

  /**
   * pause migration task from migrationTasks list using taskId
   *
   * @param pauseTaskId taskId of task to pause
   * @return true if task with taskId exists and pauseable
   */
  public boolean pauseMigration(long pauseTaskId) {
    if (migrationTasks.containsKey(pauseTaskId)) {
      MigrationTask task = migrationTasks.get(pauseTaskId);
      if (task.getStatus() == MigrationTask.MigrationTaskStatus.READY
          || task.getStatus() == MigrationTask.MigrationTaskStatus.RUNNING) {
        // write to log
        try {
          logWriter.pauseMigration(task);
        } catch (IOException e) {
          logger.error("write log error");
        }
        // change status to PAUSED
        task.setStatus(MigrationTask.MigrationTaskStatus.PAUSED);
        return true;
      }
    }
    return false;
  }

  /**
   * Pause migration task from migrationTasks list using storage group. If multiple tasks with such
   * storage group exists (and pauseable), pause the one with the lowest taskId.
   *
   * @param storageGroup sg for task to pause
   * @return true if exists task with storageGroup and pauseable
   */
  public boolean pauseMigration(PartialPath storageGroup) {
    for (MigrationTask task : migrationTasks.values()) {
      if (task.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())
          && (task.getStatus() == MigrationTask.MigrationTaskStatus.READY
              || task.getStatus() == MigrationTask.MigrationTaskStatus.RUNNING)) {
        // write to log
        try {
          logWriter.pauseMigration(task);
        } catch (IOException e) {
          logger.error("write log error");
        }
        // change status to PAUSED
        task.setStatus(MigrationTask.MigrationTaskStatus.PAUSED);
        return true;
      }
    }
    return false;
  }

  /** same as pauseMigrate using taskId except does not write to log */
  public boolean pauseMigrationFromLog(long pauseTaskId) {
    if (migrationTasks.containsKey(pauseTaskId)) {
      migrationTasks.get(pauseTaskId).setStatus(MigrationTask.MigrationTaskStatus.PAUSED);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Unpause migration task from migrationTasks list using taskId
   *
   * @param unpauseTaskId taskId of task to unpause
   * @return true if task with index exists and paused
   */
  public boolean unpauseMigration(long unpauseTaskId) {
    if (migrationTasks.containsKey(unpauseTaskId)) {
      MigrationTask task = migrationTasks.get(unpauseTaskId);
      if (task.getStatus() == MigrationTask.MigrationTaskStatus.PAUSED) {
        // write to log
        try {
          logWriter.unpauseMigration(task);
        } catch (IOException e) {
          logger.error("write log error");
        }
        // change status to READY
        task.setStatus(MigrationTask.MigrationTaskStatus.READY);
        return true;
      }
    }
    return false;
  }

  /**
   * Unpause migration task from migrationTasks list using storage group. If multiple tasks with
   * such storage group exists, remove the one with the lowest taskId and paused.
   *
   * @param storageGroup sg for task to remove
   * @return true if exists task with storageGroup
   */
  public boolean unpauseMigration(PartialPath storageGroup) {
    for (MigrationTask task : migrationTasks.values()) {
      if (task.getStorageGroup().getFullPath().equals(storageGroup.getFullPath())
          && (task.getStatus() == MigrationTask.MigrationTaskStatus.PAUSED)) {
        // write to log
        try {
          logWriter.unpauseMigration(task);
        } catch (IOException e) {
          logger.error("write log error");
        }
        // change status to READY
        task.setStatus(MigrationTask.MigrationTaskStatus.READY);
        return true;
      }
    }
    return false;
  }

  /** same as unpauseMigration using taskId except does not write to log */
  public boolean unpauseMigrationFromLog(long unpauseTaskId) {
    if (migrationTasks.containsKey(unpauseTaskId)) {
      migrationTasks.get(unpauseTaskId).setStatus(MigrationTask.MigrationTaskStatus.READY);
      return true;
    } else {
      return false;
    }
  }

  /** set to finish status */
  public boolean finishFromLog(long taskId) {
    if (migrationTasks.containsKey(taskId)) {
      migrationTasks.get(taskId).setStatus(MigrationTask.MigrationTaskStatus.FINISHED);
      return true;
    } else {
      return false;
    }
  }

  /** set to error status */
  public boolean errorFromLog(long index) {
    if (migrationTasks.containsKey(index)) {
      migrationTasks.get(index).setStatus(MigrationTask.MigrationTaskStatus.ERROR);
      return true;
    } else {
      return false;
    }
  }

  /** check if any of the migrationTasks can start */
  public synchronized void checkMigration() {
    logger.info("check migration");
    for (MigrationTask task : migrationTasks.values()) {

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
                logWriter.startMigration(task);
              } catch (IOException e) {
                logger.error("write log error");
              }

              StorageEngine.getInstance()
                  .getProcessorMap()
                  .get(task.getStorageGroup())
                  .checkMigration(task);
              logger.info("check migration task successfully.");

              // set state and remove
              try {
                logWriter.finishMigration(task);
              } catch (IOException e) {
                logger.error("write log error");
              }
              task.setStatus(MigrationTask.MigrationTaskStatus.FINISHED);
            });
      }
    }
  }
}
