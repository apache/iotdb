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

package com.timecho.iotdb.dataregion.migration;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import com.timecho.iotdb.i18n.TimechoServerMessages;
import com.timecho.iotdb.os.utils.RemoteStorageBlock;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class RemoteMigrationTask extends MigrationTask {
  private static final Logger logger = LoggerFactory.getLogger(RemoteMigrationTask.class);

  protected RemoteMigrationTask(MigrationCause cause, TsFileResource tsFile, String targetDir)
      throws IOException {
    super(cause, tsFile, targetDir);
    toLocal = false;
  }

  @Override
  public void migrate() throws Exception {
    long migratedFileSize = 0;

    // dest tsfile may exist if the last same migration task hasn't completed when the system
    // shutdown.
    filesShouldDelete.addAll(Arrays.asList(destTsFile, destResourceFile, destModsFile));
    cleanup();

    // migrate TsFile
    tsFileResource.readLock();
    try {
      migratedFileSize += srcFile.length();
      MigrationTaskManager.getInstance()
          .acquireMigrateSpeedLimiter(destTierLevel - 1, srcFile.length());
      migrateFile(srcFile, destTsFile);
    } catch (Exception e) {
      cleanup();
      throw e;
    } finally {
      tsFileResource.readUnlock();
    }

    // set Remote info
    tsFileResource.writeLock();
    try {
      tsFileResource.setRemoteStorageBlock(
          new RemoteStorageBlock(FSUtils.getFSType(destTsFile), destTsFile.getAbsolutePath()));
      tsFileResource.serialize();
    } catch (Exception e) {
      logger.error(
          TimechoServerMessages.FAIL_TO_SERIALIZE_REMOTE_STORAGE_INFO_INTO_FILE, srcFile, e);
      cleanup();
      return;
    } finally {
      tsFileResource.writeUnlock();
    }

    // migrate resource file
    tsFileResource.readLock();
    try {
      migratedFileSize += srcResourceFile.length();
      MigrationTaskManager.getInstance()
          .acquireMigrateSpeedLimiter(destTierLevel - 1, srcResourceFile.length());
      migrateFile(srcResourceFile, destResourceFile);
    } catch (Exception e) {
      if (!tsFileResource.isDeleted()) {
        logger.error(
            TimechoServerMessages.FAIL_TO_MIGRATE_RESOURCE_FROM_LOCAL_TO_REMOTE,
            srcFile,
            destTsFile,
            e);
      }
      cleanup();
      throw e;
    } finally {
      tsFileResource.readUnlock();
    }

    long waitLockStartTime = System.nanoTime();
    // clear src files
    tsFileResource.writeLock();
    try {
      MIGRATION_METRICS.recordMigrationWaitLockTime(
          destTierLevel, toLocal, System.nanoTime() - waitLockStartTime);
      filesShouldDelete.clear();
      filesShouldDelete.add(srcFile);
      cleanup();
      tsFileResource.increaseTierLevel();
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL_ON_REMOTE);
    } catch (Exception e) {
      if (!tsFileResource.isDeleted()) {
        logger.error(TimechoServerMessages.FAIL_TO_DELETE_LOCAL_TSFILE, srcFile, e);
      }
    } finally {
      tsFileResource.writeUnlock();
    }

    MIGRATION_METRICS.recordMigrationFileSize(destTierLevel, toLocal, migratedFileSize);
  }
}
