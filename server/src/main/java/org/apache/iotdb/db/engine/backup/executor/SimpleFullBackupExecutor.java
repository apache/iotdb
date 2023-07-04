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

package org.apache.iotdb.db.engine.backup.executor;

import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.backup.task.BackupByMoveTask;
import org.apache.iotdb.db.engine.backup.task.DummyTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.service.BackupService;
import org.apache.iotdb.db.utils.BackupUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleFullBackupExecutor extends AbstractFullBackupExecutor {
  private static final Logger logger = LoggerFactory.getLogger(SimpleFullBackupExecutor.class);

  public SimpleFullBackupExecutor(
      BackupService.OnSubmitBackupTaskCallBack onSubmitBackupTaskCallBack,
      BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack) {
    super(onSubmitBackupTaskCallBack, onBackupFileTaskFinishCallBack);
  }

  @Override
  public void executeBackup(String outputPath, boolean isSync) {
    if (!checkBackupPathValid(outputPath)) {
      logger.error("Full backup path invalid. Backup aborted.");
      return;
    }
    if (!BackupUtils.deleteBackupTmpDir()) {
      logger.error("Failed to delete backup temporary directories before backup. Backup aborted.");
      return;
    }
    List<TsFileResource> resources = new ArrayList<>();
    StorageEngine.getInstance().syncCloseAllProcessor();
    StorageEngine.getInstance().applyReadLockAndCollectFilesForBackup(resources);
    for (TsFileResource resource : resources) {
      try {
        String tsfileTargetPath = BackupUtils.getTsFileTargetPath(resource.getTsFile(), outputPath);
        if (BackupUtils.createTargetDirAndTryCreateLink(
            new File(tsfileTargetPath), resource.getTsFile())) {
          BackupUtils.createTargetDirAndTryCreateLink(
              new File(tsfileTargetPath + TsFileResource.RESOURCE_SUFFIX),
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
          if (resource.getModFile().exists()) {
            BackupUtils.createTargetDirAndTryCreateLink(
                new File(tsfileTargetPath + ModificationFile.FILE_SUFFIX),
                new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
          }
        } else {
          String tsfileTmpPath = BackupUtils.getTsFileTmpLinkPath(resource.getTsFile());
          BackupUtils.createTargetDirAndTryCreateLink(
              new File(tsfileTmpPath), resource.getTsFile());
          backupFileTaskList.add(
              new BackupByMoveTask(
                  tsfileTmpPath, tsfileTargetPath, onBackupFileTaskFinishCallBack));
          BackupUtils.createTargetDirAndTryCreateLink(
              new File(tsfileTmpPath + TsFileResource.RESOURCE_SUFFIX),
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
          backupFileTaskList.add(
              new BackupByMoveTask(
                  tsfileTmpPath + TsFileResource.RESOURCE_SUFFIX,
                  tsfileTargetPath + TsFileResource.RESOURCE_SUFFIX,
                  onBackupFileTaskFinishCallBack));
          if (resource.getModFile().exists()) {
            BackupUtils.createTargetDirAndTryCreateLink(
                new File(tsfileTmpPath + ModificationFile.FILE_SUFFIX),
                new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
            backupFileTaskList.add(
                new BackupByMoveTask(
                    tsfileTmpPath + ModificationFile.FILE_SUFFIX,
                    tsfileTargetPath + ModificationFile.FILE_SUFFIX,
                    onBackupFileTaskFinishCallBack));
          }
        }
      } catch (IOException e) {
        logger.error("Failed to create directory during backup: " + e.getMessage());
      } finally {
        resource.readUnlock();
      }
    }

    int systemFileCount = -1;
    try {
      systemFileCount = backupSystemFiles(outputPath);
    } catch (IOException e) {
      // TODO
    }
    int configFileCount = -1;
    try {
      configFileCount = backupConfigFiles(outputPath);
    } catch (IOException e) {
      // TODO
    }

    logger.info(
        String.format(
            "Backup starting, found %d TsFiles and their related files, %d system files and %d config files.",
            resources.size(), systemFileCount, configFileCount));
    logger.info(
        String.format(
            "%d files can't be hard-linked and should be copied.", backupFileTaskList.size()));

    if (backupFileTaskList.size() == 0) {
      backupFileTaskList.add(new DummyTask("", "", onBackupFileTaskFinishCallBack));
    }
    List<Future<Boolean>> taskFutureList = onSubmitBackupTaskCallBack.call(backupFileTaskList);
    if (isSync) {
      boolean isAllSuccess = true;
      try {
        for (Future<Boolean> future : taskFutureList) {
          isAllSuccess = isAllSuccess && future.get();
        }
        // TODO: how to return this status
      } catch (ExecutionException e) {
        // TODO: what's this
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
