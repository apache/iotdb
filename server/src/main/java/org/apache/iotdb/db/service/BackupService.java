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
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.utils.BackupUtils;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

public class BackupService implements IService {
  private static final Logger logger = LoggerFactory.getLogger(BackupService.class);
  private ExecutorService backupThreadPool;

  /** Records the files that can't be hard-linked and should be copied. */
  private List<BackupByCopyTask> backupByCopyTaskList = new ArrayList<>();

  /** Record the files in incremental backup. */
  private HashMap<String, File> backupTsFileMap = new HashMap<>();

  private HashMap<String, File> databaseTsFileMap = new HashMap<>();

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
    if (!BackupUtils.deleteBackupTmpDir()) {
      logger.warn("Failed to delete backup temporary directories when starting BackupService.");
    }
    if (backupThreadPool == null) {
      int backupThreadNum = IoTDBDescriptor.getInstance().getConfig().getBackupThreadNum();
      backupThreadPool =
          IoTDBThreadPoolFactory.newFixedThreadPool(
              backupThreadNum, ThreadName.BACKUP_SERVICE.getName());
    }
    if (BackupUtils.checkConfDir()) {
      logger.info("BackupService found the config directory: " + BackupUtils.getConfDir());
    } else {
      logger.error("BackupService couldn't find the config directory, will skip it during backup.");
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

  public AtomicInteger getBackupByCopyCount() {
    return backupByCopyCount;
  }

  private void submitBackupByCopyTask(BackupByCopyTask task) {
    task.backupByCopy();
  }

  public void checkFullBackupPathValid(String outputPath) throws WriteProcessException {
    File tempFile = new File(outputPath);
    try {
      if (!tempFile.createNewFile()) {
        if (tempFile.isFile()) {
          throw new WriteProcessException("Full backup output path can not be an existing file.");
        }
        String[] files = tempFile.list();
        if (files != null && files.length != 0) {
          throw new WriteProcessException("Can not perform full backup to a non-empty folder.");
        }
      } else {
        tempFile.delete();
      }
    } catch (IOException e) {
      throw new WriteProcessException("Failed to create backup output directory.");
    }
  }

  public void checkIncrementalBackupPathValid(String outputPath) throws WriteProcessException {
    File tempFile = new File(outputPath);
    String[] files = tempFile.list();
    if (!tempFile.exists() || tempFile.isFile() || files == null) {
      throw new WriteProcessException(
          "Output path for incremental backup should be a non-empty folder.");
    }
  }

  /**
   * Backup given TsFiles, and will backup system files and config files.
   *
   * @param resources
   * @param outputPath
   */
  public void performFullBackup(List<TsFileResource> resources, String outputPath) {
    if (!BackupUtils.deleteBackupTmpDir()) {
      logger.error("Failed to delete backup temporary directories before backup. Backup aborted.");
      return;
    }
    backupByCopyTaskList.clear();
    for (TsFileResource resource : resources) {
      try {
        String tsfileTargetPath = BackupUtils.getTsFileTargetPath(resource, outputPath);
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
          String tsfileTmpPath = BackupUtils.getTsFileTmpLinkPath(resource);
          BackupUtils.createTargetDirAndTryCreateLink(
              new File(tsfileTmpPath), resource.getTsFile());
          backupByCopyTaskList.add(new BackupByCopyTask(tsfileTmpPath, tsfileTargetPath));
          BackupUtils.createTargetDirAndTryCreateLink(
              new File(tsfileTmpPath + TsFileResource.RESOURCE_SUFFIX),
              new File(resource.getTsFilePath() + TsFileResource.RESOURCE_SUFFIX));
          backupByCopyTaskList.add(
              new BackupByCopyTask(
                  tsfileTmpPath + TsFileResource.RESOURCE_SUFFIX,
                  tsfileTargetPath + TsFileResource.RESOURCE_SUFFIX));
          if (resource.getModFile().exists()) {
            BackupUtils.createTargetDirAndTryCreateLink(
                new File(tsfileTmpPath + ModificationFile.FILE_SUFFIX),
                new File(resource.getTsFilePath() + ModificationFile.FILE_SUFFIX));
            backupByCopyTaskList.add(
                new BackupByCopyTask(
                    tsfileTmpPath + ModificationFile.FILE_SUFFIX,
                    tsfileTargetPath + ModificationFile.FILE_SUFFIX));
          }
        }
      } catch (IOException e) {
        logger.error("Failed to create directory during backup: " + e.getMessage());
      } finally {
        resource.readUnlock();
      }
    }

    String systemDirPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    List<File> systemFiles = BackupUtils.getAllFilesInOneDir(systemDirPath);
    for (File file : systemFiles) {
      String systemFileTargetPath = BackupUtils.getSystemFileTargetPath(file, outputPath);
      try {
        if (!BackupUtils.createTargetDirAndTryCreateLink(new File(systemFileTargetPath), file)) {
          String systemFileTmpPath = BackupUtils.getSystemFileTmpLinkPath(file);
          BackupUtils.createTargetDirAndTryCreateLink(new File(systemFileTmpPath), file);
          backupByCopyTaskList.add(new BackupByCopyTask(systemFileTmpPath, systemFileTargetPath));
        }
      } catch (IOException e) {
        logger.error("Failed to create directory during backup: " + e.getMessage());
      }
    }

    String configDirPath = BackupUtils.getConfDir();
    List<File> configFiles = new ArrayList<>();
    if (configDirPath != null) {
      configFiles = BackupUtils.getAllFilesInOneDir(configDirPath);
      for (File file : configFiles) {
        String configFileTargetPath = BackupUtils.getConfigFileTargetPath(file, outputPath);
        backupByCopyTaskList.add(
            new BackupByCopyTask(file.getAbsolutePath(), configFileTargetPath));
      }
    } else {
      logger.warn("Can't find config directory during backup, skipping.");
    }

    logger.info(
        String.format(
            "Backup starting, found %d TsFiles and their related files, %d system files and %d config files.",
            resources.size(), systemFiles.size(), configFiles.size()));
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

  public void performIncrementalBackup(List<TsFileResource> resources, String outputPath) {
    if (!BackupUtils.deleteBackupTmpDir()) {
      logger.error("Failed to delete backup temporary directories before backup. Backup aborted.");
      return;
    }
    backupByCopyTaskList.clear();
    prepareBackupTsFileMap(outputPath);
    prepareDatabaseTsFileMap(resources);
    for (String tsFileName : backupTsFileMap.keySet()) {
      if (!databaseTsFileMap.containsKey(tsFileName)) {
        File tsFile = backupTsFileMap.get(tsFileName);
        tsFile.delete();
        new File(tsFile.getAbsolutePath() + TsFileResource.RESOURCE_SUFFIX).delete();
        new File(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX).delete();
      } else {
        databaseTsFileMap.remove(tsFileName);
      }
      backupTsFileMap.remove(tsFileName);
    }
    for (File tsFile : databaseTsFileMap.values()) {
      // TODO: copy files
    }
  }

  private void prepareBackupTsFileMap(String outputPath) {
    backupTsFileMap.clear();
    List<File> tsFileList =
        BackupUtils.getAllFilesWithSuffixInOneDir(outputPath, TsFileConstant.TSFILE_SUFFIX);
    for (File tsFile : tsFileList) {
      backupTsFileMap.put(tsFile.getName(), tsFile);
    }
  }

  private void prepareDatabaseTsFileMap(List<TsFileResource> resources) {
    databaseTsFileMap.clear();
    for (TsFileResource resource : resources) {
      File tsFile = resource.getTsFile();
      databaseTsFileMap.put(tsFile.getName(), tsFile);
    }
  }

  public void cleanUpAfterBackup() {
    logger.info("Backup completed, cleaning up temporary files now.");
    if (BackupUtils.deleteBackupTmpDir()) {
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
