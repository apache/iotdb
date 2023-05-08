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

package org.apache.iotdb.db.engine.backup;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.service.BackupService;
import org.apache.iotdb.db.utils.BackupUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class BackupByCopyTask extends WrappedRunnable {
  private static final Logger logger = LoggerFactory.getLogger(BackupByCopyTask.class);
  String sourcePath;
  String targetPath;

  public BackupByCopyTask(String sourcePath, String targetPath) {
    this.sourcePath = sourcePath;
    this.targetPath = targetPath;
  }

  @Override
  public void runMayThrow() throws Exception {
    backupByCopy();
  }

  public void backupByCopy() {
    try {
      BackupUtils.copyFile(Paths.get(sourcePath), Paths.get(targetPath));
    } catch (IOException e) {
      logger.error(
          String.format("Copy failed during backup: from %s to %s", sourcePath, targetPath));
    }
    if (BackupService.getINSTANCE().getBackupByCopyCount().addAndGet(-1) == 0) {
      logger.info("Backup completed.");
      BackupService.getINSTANCE().cleanUpBackupTmpDir();
      BackupService.getINSTANCE().getIsBackupRunning().set(false);
    }
  }
}
