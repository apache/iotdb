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

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.backup.executor.*;
import org.apache.iotdb.db.engine.backup.task.AbstractBackupFileTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.utils.BackupUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BackupService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(BackupService.class);
  private static int BACKUP_TMP_FILE_CHECK_INTERVAL_IN_MINUTE = 5;
  private ExecutorService backupThreadPool;

  /** Records the files that can't be hard-linked and should be copied. */
  private List<AbstractBackupFileTask> backupFileTaskList = new ArrayList<>();

  /** Record the files in incremental backup. */
  private Map<String, File> backupTsFileMap = new HashMap<>();

  private Map<String, TsFileResource> databaseTsFileResourceMap = new HashMap<>();

  private ScheduledExecutorService backupTmpFileCheckPool;

  private AtomicInteger backupByCopyCount = new AtomicInteger();
  private AtomicBoolean isBackupRunning = new AtomicBoolean();
  private AbstractFullBackupExecutor fullBackupExecutor;
  private AbstractIncrementalBackupExecutor incrementalBackupExecutor;

  public static BackupService getINSTANCE() {
    return BackupService.InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final BackupService INSTANCE = new BackupService();

    private InstanceHolder() {}
  }

  @Override
  public void start() throws StartupException {
    fullBackupExecutor =
        new SimpleFullBackupExecutor(
            this::onSubmitBackupTaskCallBack, this::onBackupFileTaskFinishCallBack);
    incrementalBackupExecutor =
        new SimpleIncrementalBackupExecutor(
            this::onSubmitBackupTaskCallBack, this::onBackupFileTaskFinishCallBack);
    if (backupThreadPool == null) {
      int backupThreadNum = IoTDBDescriptor.getInstance().getConfig().getBackupThreadNum();
      backupThreadPool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              backupThreadNum, ThreadName.BACKUP_SERVICE.getName());
      backupTmpFileCheckPool =
          IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
              ThreadName.BACKUP_TEMPORARY_FILE_CHECK.getName());
      backupTmpFileCheckPool.scheduleWithFixedDelay(
          this::cleanUpBackupTmpDir, 0, BACKUP_TMP_FILE_CHECK_INTERVAL_IN_MINUTE, TimeUnit.MINUTES);
    }
    if (BackupUtils.checkConfDir()) {
      logger.info("Found the config directory: " + BackupUtils.getConfDir());
    } else {
      logger.error("Couldn't find the config directory, will skip it during backup.");
    }
  }

  @Override
  public void stop() {
    if (backupThreadPool != null) {
      backupThreadPool.shutdownNow();
    }
    if (!BackupUtils.deleteBackupTmpDir()) {
      logger.warn("Failed to delete backup temporary directories when stopping BackupService.");
    }
  }

  @FunctionalInterface
  public interface OnSubmitBackupTaskCallBack {
    List<Future<Boolean>> call(List<AbstractBackupFileTask> backupTaskList);
  }

  @FunctionalInterface
  public interface OnBackupFileTaskFinishCallBack {
    void call();
  }

  private List<Future<Boolean>> onSubmitBackupTaskCallBack(
      List<AbstractBackupFileTask> backupFileTaskList) {
    List<Future<Boolean>> taskFutureList = new ArrayList<>();
    backupByCopyCount.addAndGet(backupFileTaskList.size());
    for (AbstractBackupFileTask backupFileTask : backupFileTaskList) {
      taskFutureList.add(backupThreadPool.submit(backupFileTask));
    }
    return taskFutureList;
  }

  private void onBackupFileTaskFinishCallBack() {
    if (backupByCopyCount.addAndGet(-1) == 0) {
      logger.info("Backup completed.");
      cleanUpBackupTmpDir();
      isBackupRunning.set(false);
    }
  }

  /**
   * Back up TsFiles, system files and config files.
   *
   * @param outputPath
   * @param isSync
   */
  public void performFullBackup(String outputPath, boolean isSync) {
    if (isBackupRunning.get()) {
      logger.error("Another backup task is already running, please try later.");
      return;
    }
    List<TsFileResource> resources = new ArrayList<>();
    StorageEngine.getInstance().syncCloseAllProcessor();
    StorageEngine.getInstance().applyReadLockAndCollectFilesForBackup(resources);
    fullBackupExecutor.executeBackup(resources, outputPath, isSync);
  }

  /**
   * Incremental backup on TsFiles, system files and config files.
   *
   * @param outputPath
   * @param isSync
   */
  public void performIncrementalBackup(String outputPath, boolean isSync) {
    if (isBackupRunning.get()) {
      logger.error("Another backup task is already running, please try later.");
      return;
    }
    List<TsFileResource> resources = new ArrayList<>();
    StorageEngine.getInstance().syncCloseAllProcessor();
    StorageEngine.getInstance().applyReadLockAndCollectFilesForBackup(resources);
    incrementalBackupExecutor.executeBackup(resources, outputPath, isSync);
  }

  public AtomicInteger getBackupByCopyCount() {
    return backupByCopyCount;
  }

  public AtomicBoolean getIsBackupRunning() {
    return isBackupRunning;
  }

  public void cleanUpBackupTmpDir() {
    if (isBackupRunning.get()) {
      logger.info("Backup is running, will not remove temporary files.");
      return;
    }
    logger.info("Removing back up temporary files now.");
    if (BackupUtils.deleteBackupTmpDir()) {
      logger.info("Back up temporary files are all clear.");
    } else {
      logger.warn("Failed to delete some backup temporary files. Will try later.");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.BACKUP_SERVICE;
  }
}
