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

public class RemoteMigrationTask extends MigrationTask {
  private static final Logger logger = LoggerFactory.getLogger(RemoteMigrationTask.class);

  RemoteMigrationTask(MigrationCause cause, TsFileResource tsFile, String targetDir) {
    super(cause, tsFile, targetDir);
  }

  @Override
  public void migrate() {
    // copy TsFile and resource file
    tsFile.readLock();
    try {
      fsFactory.copyFile(srcTsFile, destTsFile);
      fsFactory.copyFile(srcResourceFile, destResourceFile);
    } catch (IOException e) {
      logger.error("Fail to copy TsFile {}", srcTsFile);
      destTsFile.delete();
      destResourceFile.delete();
      return;
    } finally {
      tsFile.readUnlock();
    }
    // clear src files
    tsFile.writeLock();
    try {
      srcTsFile.delete();
    } finally {
      tsFile.writeUnlock();
    }
  }
}
