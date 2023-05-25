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

public class RemoteMigrationTask extends MigrationTask {
  private static final Logger logger = LoggerFactory.getLogger(RemoteMigrationTask.class);

  protected RemoteMigrationTask(MigrationCause cause, TsFileResource tsFile, String targetDir)
      throws IOException {
    super(cause, tsFile, targetDir);
  }

  @Override
  public void migrate() {
    // dest tsfile may exist if the last same migration task hasn't completed when the system
    // shutdown.
    filesShouldDelete.addAll(Arrays.asList(destTsFile, destResourceFile));
    cleanup();

    // copy TsFile and resource file
    tsFileResource.readLock();
    try {
      migrateFile(srcFile, destTsFile);
      migrateFile(srcResourceFile, destResourceFile);
    } catch (Exception e) {
      if (!tsFileResource.isDeleted()) {
        logger.error("Fail to copy TsFile from local {} to remote {}", srcFile, srcResourceFile);
      }
      cleanup();
      return;
    } finally {
      tsFileResource.readUnlock();
    }

    // clear src files
    tsFileResource.writeLock();
    try {
      filesShouldDelete.clear();
      filesShouldDelete.add(srcFile);
      cleanup();
      tsFileResource.increaseTierLevel();
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL_ON_REMOTE);
    } catch (Exception e) {
      if (!tsFileResource.isDeleted()) {
        logger.error("Fail to delete local TsFile {}", srcFile);
      }
    } finally {
      tsFileResource.writeUnlock();
    }
  }
}
