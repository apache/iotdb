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

import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.FSUtils;

import java.io.File;

public abstract class MigrationTask implements Runnable {
  protected static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();

  protected final MigrationCause cause;
  protected final TsFileResource tsFile;
  protected final String targetDir;

  protected final File srcTsFile;
  protected final File destTsFile;
  protected final File srcResourceFile;
  protected final File destResourceFile;
  protected final File srcModsFile;
  protected final File destModsFile;

  MigrationTask(MigrationCause cause, TsFileResource tsFile, String targetDir) {
    this.cause = cause;
    this.tsFile = tsFile;
    this.targetDir = targetDir;
    this.srcTsFile = tsFile.getTsFile();
    this.destTsFile = fsFactory.getFile(targetDir, tsFile.getTsFile().getName());
    this.srcResourceFile =
        fsFactory.getFile(
            srcTsFile.getParentFile(), srcTsFile.getName() + TsFileResource.RESOURCE_SUFFIX);
    this.destResourceFile =
        fsFactory.getFile(targetDir, tsFile.getTsFile().getName() + TsFileResource.RESOURCE_SUFFIX);
    this.srcModsFile =
        fsFactory.getFile(
            srcTsFile.getParentFile(), srcTsFile.getName() + ModificationFile.FILE_SUFFIX);
    this.destModsFile =
        fsFactory.getFile(targetDir, tsFile.getTsFile().getName() + ModificationFile.FILE_SUFFIX);
  }

  public static MigrationTask newTask(
      MigrationCause cause, TsFileResource sourceTsFile, String targetDir) {
    if (FSUtils.isLocal(targetDir)) {
      return new LocalMigrationTask(cause, sourceTsFile, targetDir);
    } else {
      return new RemoteMigrationTask(cause, sourceTsFile, targetDir);
    }
  }

  @Override
  public void run() {
    if (canMigrate()) {
      tsFile.setIsMigrating(true);
      if (!canMigrate()) {
        tsFile.setIsMigrating(false);
        return;
      }
    } else {
      return;
    }

    migrate();

    tsFile.setIsMigrating(false);
  }

  protected boolean canMigrate() {
    return tsFile.getStatus() == TsFileResourceStatus.NORMAL;
  }

  public abstract void migrate();
}
