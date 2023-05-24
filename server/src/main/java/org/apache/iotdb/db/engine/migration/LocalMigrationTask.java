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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class LocalMigrationTask extends MigrationTask {
  private static final Logger logger = LoggerFactory.getLogger(LocalMigrationTask.class);

  protected LocalMigrationTask(MigrationCause cause, TsFileResource tsFile, String targetDir)
      throws IOException {
    super(cause, tsFile, targetDir);
  }

  @Override
  public void migrate() {
    // copy TsFile and resource file
    tsFileResource.readLock();
    try {
      fsFactory.copyFile(srcFile, destTsFile);
      fsFactory.copyFile(srcResourceFile, destResourceFile);
    } catch (IOException e) {
      logger.error("Fail to copy TsFile {}", srcFile);
      destTsFile.delete();
      destResourceFile.delete();
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
        fsFactory.copyFile(srcModsFile, destModsFile);
      }
      tsFileResource.setFile(destTsFile);
    } catch (IOException e) {
      logger.error("Fail to copy mods file {}", srcModsFile);
      destTsFile.delete();
      destResourceFile.delete();
      destModsFile.delete();
      return;
    } finally {
      tsFileResource.writeUnlock();
    }
    // clear src files
    srcFile.delete();
    srcResourceFile.delete();
    srcModsFile.delete();
  }
}
