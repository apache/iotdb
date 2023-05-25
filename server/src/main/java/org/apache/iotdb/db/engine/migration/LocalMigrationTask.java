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
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class LocalMigrationTask extends MigrationTask {
  private static final Logger logger = LoggerFactory.getLogger(LocalMigrationTask.class);

  protected LocalMigrationTask(MigrationCause cause, TsFileResource tsFile, String targetDir)
      throws IOException {
    super(cause, tsFile, targetDir);
  }

  @Override
  public void migrate() {
    // dest tsfile may exist if the last same migration task hasn't completed when the system
    // shutdown.
    filesShouldDelete.addAll(Arrays.asList(destTsFile, destResourceFile, destModsFile));
    cleanup();

    // copy TsFile and resource file
    tsFileResource.readLock();
    try {
      destTsFile.getParentFile().mkdirs();
      migrateFile(srcFile, destTsFile);
      migrateFile(srcResourceFile, destResourceFile);
    } catch (Exception e) {
      if (!tsFileResource.isDeleted()) {
        logger.error("Fail to copy TsFile from local {} to local {}", srcFile, srcResourceFile, e);
      }
      cleanup();
      return;
    } finally {
      tsFileResource.readUnlock();
    }

    // close mods file and replace TsFile path
    tsFileResource.writeLock();
    try {
      tsFileResource.resetModFile();
      // migrate MOD file only when it exists
      if (srcModsFile.exists()) {
        migrateFile(srcModsFile, destModsFile);
      }
      tsFileResource.setFile(destTsFile);
      tsFileResource.increaseTierLevel();
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    } catch (Exception e) {
      if (!tsFileResource.isDeleted()) {
        logger.error(
            "Fail to copy mods file from local {} to local {}", srcModsFile, destModsFile, e);
      }
      cleanup();
      return;
    } finally {
      tsFileResource.writeUnlock();
    }

    // clear src files
    filesShouldDelete.clear();
    filesShouldDelete.addAll(Arrays.asList(srcFile, srcResourceFile, srcModsFile));
    cleanup();
  }
}
