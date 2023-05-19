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

package org.apache.iotdb.db.engine.backup.task;

import org.apache.iotdb.db.service.BackupService;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class BackupByMoveTask extends AbstractBackupFileTask {
  private static final Logger logger = LoggerFactory.getLogger(BackupByMoveTask.class);

  public BackupByMoveTask(
      String sourcePath,
      String targetPath,
      BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack) {
    super(sourcePath, targetPath, onBackupFileTaskFinishCallBack);
  }

  @Override
  public boolean backupFile() {
    boolean isSuccess = true;
    try {
      logger.info(String.format("Moving temporary file: from %s to %s", sourcePath, targetPath));
      FileUtils.moveFile(new File(sourcePath), new File(targetPath));
    } catch (IOException e) {
      isSuccess = false;
      logger.error(
          String.format(
              "Failed to move temporary file during backup: from %s to %s",
              sourcePath, targetPath));
    }
    onBackupFileTaskFinishCallBack.call();
    return isSuccess;
  }
}
