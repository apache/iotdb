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

import org.apache.iotdb.db.engine.backup.task.AbstractBackupFileTask;
import org.apache.iotdb.db.service.BackupService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractBackupExecutor {
  /** Records the files that can't be hard-linked and should be copied. */
  protected List<AbstractBackupFileTask> backupFileTaskList = new ArrayList<>();

  protected BackupService.OnSubmitBackupTaskCallBack onSubmitBackupTaskCallBack;
  protected BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack;

  protected AbstractBackupExecutor(
      BackupService.OnSubmitBackupTaskCallBack onSubmitBackupTaskCallBack,
      BackupService.OnBackupFileTaskFinishCallBack onBackupFileTaskFinishCallBack) {
    this.onSubmitBackupTaskCallBack = onSubmitBackupTaskCallBack;
    this.onBackupFileTaskFinishCallBack = onBackupFileTaskFinishCallBack;
  }

  public abstract boolean checkBackupPathValid(String outputPath);

  public abstract void executeBackup(String outputPath, boolean isSync);

  protected abstract int backupSystemFiles(String outputPath) throws IOException;

  protected abstract int backupConfigFiles(String outputPath) throws IOException;
}
