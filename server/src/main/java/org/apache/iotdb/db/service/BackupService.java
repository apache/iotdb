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
import org.apache.iotdb.db.engine.backup.BackupByCopyTask;
import org.apache.iotdb.db.engine.backup.BackupSystemFileTask;
import org.apache.iotdb.db.engine.backup.BackupTsFileTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.tools.backup.BackupTool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class BackupService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(BackupService.class);
  private ExecutorService backupThreadPool;

  /** Records the files that can't be hard-linked and should be copied. */
  private final List<BackupByCopyTask> backupByCopyTaskList = new ArrayList<>();

  private AtomicInteger backupByCopyCount = new AtomicInteger();

  public static BackupService getINSTANCE() {
    return BackupService.InstanceHolder.INSTANCE;
  }

  public static class InstanceHolder {
    private static final BackupService INSTANCE = new BackupService();

    private InstanceHolder() {}
  }

  @Override
  public void start() throws StartupException {
    if (backupThreadPool == null) {
      int backupThreadNum = IoTDBDescriptor.getInstance().getConfig().getBackupThreadNum();
      backupThreadPool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              backupThreadNum, ThreadName.BACKUP_SERVICE.getName());
    }
  }

  @Override
  public void stop() {}

  public AtomicInteger getBackupByCopyCount() {
    return backupByCopyCount;
  }

  private void submitBackupTsFileTask(BackupTsFileTask task) {
    task.backupTsFile();
  }

  private void submitBackupSystemFileTask(BackupSystemFileTask task) {
    task.backupSystemFile();
  }

  private void submitBackupByCopyTask(BackupByCopyTask task) {
    task.backupByCopy();
  }

  public void checkBackupPathValid(String outputPath) throws WriteProcessException {
    File tempFile = new File(outputPath);

    try {
      if (!tempFile.createNewFile()) {
        if (tempFile.isFile()) {
          throw new WriteProcessException("Backup output path can not be an existing file.");
        }
        String[] files = tempFile.list();
        if (files != null && files.length != 0) {
          throw new WriteProcessException("Can not backup to a non-empty folder.");
        }
      } else {
        tempFile.delete();
      }
    } catch (IOException e) {
      throw new WriteProcessException("Failed to create the backup directory.");
    }
  }

  /**
   * Backup given TsFiles, and will backup system files.
   *
   * @param resources
   * @param outputPath
   * @throws WriteProcessException
   */
  public void backupFiles(List<TsFileResource> resources, String outputPath)
      throws WriteProcessException {
    backupByCopyTaskList.clear();

    for (TsFileResource resource : resources) {
      String tsfileTargetPath = BackupTool.getTsFileTargetPath(resource, outputPath);
      try {
        if (BackupTool.createTargetDirAndTryCreateLink(
            new File(tsfileTargetPath), resource.getTsFile())) {
          BackupTool.createTargetDirAndTryCreateLink(
              new File(tsfileTargetPath + TsFileResource.RESOURCE_SUFFIX),
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
          if (resource.getModFile().exists()) {
            BackupTool.createTargetDirAndTryCreateLink(
                new File(tsfileTargetPath + ModificationFile.FILE_SUFFIX),
                new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
          }
        } else {
          String tsfileTmpPath = BackupTool.getTsFileTmpLinkPath(resource);
          BackupTool.createTargetDirAndTryCreateLink(new File(tsfileTmpPath), resource.getTsFile());
          backupByCopyTaskList.add(new BackupByCopyTask(tsfileTmpPath, tsfileTargetPath));
          BackupTool.createTargetDirAndTryCreateLink(
              new File(tsfileTmpPath + TsFileResource.RESOURCE_SUFFIX),
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
          backupByCopyTaskList.add(
              new BackupByCopyTask(
                  tsfileTmpPath + TsFileResource.RESOURCE_SUFFIX,
                  tsfileTargetPath + TsFileResource.RESOURCE_SUFFIX));
          if (resource.getModFile().exists()) {
            BackupTool.createTargetDirAndTryCreateLink(
                new File(tsfileTmpPath + ModificationFile.FILE_SUFFIX),
                new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
            backupByCopyTaskList.add(
                new BackupByCopyTask(
                    tsfileTmpPath + ModificationFile.FILE_SUFFIX,
                    tsfileTargetPath + ModificationFile.FILE_SUFFIX));
          }
        }
      } catch (IOException e) {
        throw new WriteProcessException(
            "Failed to create directory during backup: " + e.getMessage());
      } finally {
        resource.readUnlock();
      }
    }

    String systemDirPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    List<File> systemFiles = BackupTool.getAllFilesInOneDir(systemDirPath);
    for (File file : systemFiles) {
      String systemFileTargetPath = BackupTool.getSystemFileTargetPath(file, outputPath);
      try {
        if (!BackupTool.createTargetDirAndTryCreateLink(new File(systemFileTargetPath), file)) {
          String systemFileTmpPath = BackupTool.getSystemFileTmpLinkPath(file);
          BackupTool.createTargetDirAndTryCreateLink(new File(systemFileTmpPath), file);
          backupByCopyTaskList.add(new BackupByCopyTask(systemFileTmpPath, systemFileTargetPath));
        }
      } catch (IOException e) {
        throw new WriteProcessException(
            "Failed to create directory during backup: " + e.getMessage());
      }
    }

    logger.info(
        String.format(
            "Backup starting, found %d TsFiles and their related files, %d system files.",
            resources.size(), systemFiles.size()));
    logger.debug(
        String.format(
            "%d files can't be hard-linked and should be copied.", backupByCopyTaskList.size()));

    if (backupByCopyTaskList.size() == 0) {
      logger.info("Backup completed.");
      return;
    }
    backupByCopyCount.set(backupByCopyTaskList.size());
    for (BackupByCopyTask backupByCopyTask : backupByCopyTaskList) {
      submitBackupByCopyTask(backupByCopyTask);
    }
  }

  public void cleanUpAfterBackup() {
    logger.info("Backup completed, cleaning up temporary files now.");
    if (BackupTool.deleteBackupTmpDir()) {
      logger.info("Backup temporary files successfully deleted.");
    } else {
      logger.warn("Failed to delete some backup temporary files.");
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.BACKUP_SERVICE;
  }
}
