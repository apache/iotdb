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

import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import com.timecho.iotdb.i18n.TimechoServerMessages;
import com.timecho.iotdb.metrics.MigrationMetrics;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class MigrationTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);
  protected static final FSFactory fsFactory = FSFactoryProducer.getFSFactory();
  protected static final MigrationMetrics MIGRATION_METRICS = MigrationMetrics.getInstance();

  protected boolean toLocal;
  protected final MigrationCause cause;
  protected final TsFileResource tsFileResource;
  protected final int destTierLevel;

  protected final String targetDir;
  protected final File srcFile;
  protected final File destTsFile;
  protected final File srcResourceFile;
  protected final File destResourceFile;
  protected final File srcModsFile;
  protected final File destModsFile;

  protected final List<File> filesShouldDelete = new ArrayList<>();

  protected MigrationTask(MigrationCause cause, TsFileResource tsFileResource, String targetDir)
      throws IOException {
    this.cause = cause;
    this.tsFileResource = tsFileResource;
    this.destTierLevel = tsFileResource.getTierLevel() + 1;
    this.targetDir = targetDir;
    this.srcFile = tsFileResource.getTsFile();
    this.destTsFile = fsFactory.getFile(targetDir, getDestTsFilePath(srcFile));
    this.srcResourceFile =
        fsFactory.getFile(
            srcFile.getParentFile(), srcFile.getName() + TsFileResource.RESOURCE_SUFFIX);
    this.destResourceFile =
        fsFactory.getFile(targetDir, getDestTsFilePath(srcFile) + TsFileResource.RESOURCE_SUFFIX);
    this.srcModsFile =
        fsFactory.getFile(
            srcFile.getParentFile(), srcFile.getName() + ModificationFile.FILE_SUFFIX);
    this.destModsFile =
        fsFactory.getFile(targetDir, getDestTsFilePath(srcFile) + ModificationFile.FILE_SUFFIX);
  }

  private String getDestTsFilePath(File src) throws IOException {
    return FSUtils.getLocalTsFileShortPath(src, FSUtils.PATH_FROM_DATABASE_LEVEL);
  }

  public static MigrationTask newTask(
      MigrationCause cause, TsFileResource sourceTsFile, String targetDir) throws IOException {
    if (FSUtils.isLocal(targetDir)) {
      return new LocalMigrationTask(cause, sourceTsFile, targetDir);
    } else {
      return new RemoteMigrationTask(cause, sourceTsFile, targetDir);
    }
  }

  @Override
  public void run() {
    long taskStartTime = System.nanoTime();

    try {
      MIGRATION_METRICS.recordMigrationCause(cause);
      migrate();
    } catch (Throwable t) {
      logger.warn(TimechoServerMessages.MIGRATE_TASK_ERROR, t);
      return;
    } finally {
      // try to set the final status to NORMAL to avoid migrate failure
      // TODO: this setting may occur side effects
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);
    }

    long taskTimeCost = System.nanoTime() - taskStartTime;
    MIGRATION_METRICS.recordMigrationTotalTime(destTierLevel, toLocal, taskTimeCost);
    logger.info(
        "Successfully migrate local TsFile {} to the {} {}, this migration operation is caused by {} and costs {}ns.",
        srcFile,
        toLocal ? "local" : "remote",
        destTsFile,
        cause,
        taskTimeCost);
  }

  public abstract void migrate() throws Exception;

  protected void migrateFile(File src, File dest) throws IOException {
    long copyStartTime = System.nanoTime();
    fsFactory.copyFile(src, dest);
    MIGRATION_METRICS.recordMigrationFileCopyTime(
        destTierLevel, toLocal, System.nanoTime() - copyStartTime);
    filesShouldDelete.add(dest);
  }

  protected void cleanup() {
    filesShouldDelete.forEach(this::deleteIfExist);
    filesShouldDelete.clear();
  }

  protected void deleteIfExist(File file) {
    if (file.exists()) {
      file.delete();
    }
  }
}
