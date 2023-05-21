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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.backup.task.BackupByCopyTask;
import org.apache.iotdb.db.service.BackupService;
import org.apache.iotdb.db.utils.BackupUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractFullBackupExecutor extends AbstractBackupExecutor {

  protected AbstractFullBackupExecutor(
      BackupService.OnSubmitBackupTaskCallBack onSubmitBackupTaskCallBack,
      BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack) {
    super(onSubmitBackupTaskCallBack, onBackupFileTaskFinishCallBack);
  }

  @Override
  public boolean checkBackupPathValid(String outputPath) {
    File tempFile = new File(outputPath);
    try {
      if (!tempFile.createNewFile()) {
        if (tempFile.isFile()) {
          // Full backup output path can not be an existing file.
          return false;
        }
        String[] files = tempFile.list();
        if (files != null && files.length != 0) {
          // throw new WriteProcessException("Can not perform full backup to a non-empty folder.");
          return false;
        }
      } else {
        tempFile.delete();
      }
    } catch (IOException e) {
      // throw new WriteProcessException("Failed to create backup output directory.");
      return false;
    }
    return true;
  }

  @Override
  protected int backupSystemFiles(String outputPath) throws IOException {
    String systemDirPath = IoTDBDescriptor.getInstance().getConfig().getSystemDir();
    List<File> systemFiles = BackupUtils.getAllFilesInOneDir(systemDirPath);
    for (File file : systemFiles) {
      String systemFileTargetPath = BackupUtils.getSystemFileTargetPath(file, outputPath);
      backupFileTaskList.add(
          new BackupByCopyTask(
              file.getPath(), systemFileTargetPath, onBackupFileTaskFinishCallBack));
    }
    return systemFiles.size();
  }

  @Override
  protected int backupConfigFiles(String outputPath) throws IOException {
    String configDirPath = BackupUtils.getConfDir();
    List<File> configFiles = new ArrayList<>();
    if (configDirPath != null) {
      configFiles = BackupUtils.getAllFilesInOneDir(configDirPath);
      for (File file : configFiles) {
        String configFileTargetPath = BackupUtils.getConfigFileTargetPath(file, outputPath);
        backupFileTaskList.add(
            new BackupByCopyTask(
                file.getAbsolutePath(), configFileTargetPath, onBackupFileTaskFinishCallBack));
      }
    } else {
      // logger.warn("Can't find config directory during backup, skipping.");
    }
    return configFiles.size();
  }
}
