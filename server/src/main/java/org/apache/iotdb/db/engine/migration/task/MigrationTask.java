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

package org.apache.iotdb.db.engine.migration.task;

import org.apache.iotdb.db.engine.migration.utils.MigrationLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.FSPath;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.utils.FSUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MigrationTask implements IMigrationTask {

  private static final Logger logger = LoggerFactory.getLogger(MigrationTask.class);

  private List<TsFileResource> srcTsFileResources;
  private File targetDir;
  private boolean sequence;
  private long timPartitionId;
  private MigrationCallBack callBack;
  private String storageGroupName;
  private String storageGroupSysDir;

  public MigrationTask(
      List<TsFileResource> srcTsFileResources,
      File targetDir,
      boolean sequence,
      long timPartitionId,
      MigrationCallBack callBack,
      String storageGroupName,
      String storageGroupSysDir) {
    this.srcTsFileResources = srcTsFileResources;
    this.targetDir = targetDir;
    this.sequence = sequence;
    this.timPartitionId = timPartitionId;
    this.callBack = callBack;
    this.storageGroupName = storageGroupName;
    this.storageGroupSysDir = storageGroupSysDir;
  }

  void migrate() {
    logger.info(
        "[Migration] Start migrating files of {} in time partition {}.",
        storageGroupName,
        timPartitionId);
    if (srcTsFileResources == null || srcTsFileResources.isEmpty() || targetDir == null) {
      logger.info("[Migration] No files to migrate, so will abort task.");
      return;
    }
    long startTimeMillis = System.currentTimeMillis();
    List<File> srcFiles =
        srcTsFileResources.stream().map(TsFileResource::getTsFile).collect(Collectors.toList());
    MigrationLogger migrationLogger = null;
    try {
      migrationLogger = new MigrationLogger(storageGroupSysDir, timPartitionId);
      // log migration basic info
      migrationLogger.logSourceFiles(srcFiles, sequence);
      migrationLogger.logTargetDir(targetDir);
      migrationLogger.startMigration();
      if (!targetDir.exists()) {
        targetDir.mkdirs();
      }
      FSType targetFsType = FSUtils.getFSType(targetDir);
      // log each source file's migration status
      for (TsFileResource srcTsFileResource : srcTsFileResources) {
        File srcFile = srcTsFileResource.getTsFile();
        migrationLogger.startMigrateTsFile(srcFile);
        File targetFile = FSPath.parse(targetDir).getChildFile(srcFile.getName());
        // use move op if filesystems are same, use copy op if filesystems are diff
        if (FSUtils.getFSType(srcFile).equals(targetFsType)) {
          // call the callback method which contains moving file operation
          if (callBack != null) {
            callBack.call(
                srcFile,
                targetFile,
                sequence,
                (src, target) -> {
                  FSFactory fsFactory = FSFactoryProducer.getFSFactory(FSUtils.getFSType(src));
                  // firstly move .tsfile.resource, then move .tsfile
                  fsFactory.moveFile(
                      FSPath.parse(src).postConcat(TsFileResource.RESOURCE_SUFFIX).getFile(),
                      FSPath.parse(target).postConcat(TsFileResource.RESOURCE_SUFFIX).getFile());
                  fsFactory.moveFile(src, target);
                  logger.info(
                      "[Migration] move {} to {}.",
                      src.getAbsolutePath(),
                      target.getAbsolutePath());
                });
          }
          migrationLogger.endMoveTsFile();
        } else {
          // firstly copy .tsfile.resource, then copy .tsfile
          FSFactory fsFactory = FSFactoryProducer.getFSFactory(FSUtils.getFSType(srcFile));
          File srcResource =
              FSPath.parse(srcFile).postConcat(TsFileResource.RESOURCE_SUFFIX).getFile();
          File targetResource =
              FSPath.parse(targetFile).postConcat(TsFileResource.RESOURCE_SUFFIX).getFile();
          fsFactory.copyFile(srcResource, targetResource);
          fsFactory.copyFile(srcFile, targetFile);
          logger.info(
              "[Migration] copy {} to {}.",
              srcFile.getAbsolutePath(),
              targetFile.getAbsolutePath());
          migrationLogger.endCopyTsFile();
          // call the callback method
          if (callBack != null) {
            callBack.call(srcFile, targetFile, sequence, (src, target) -> {});
          }
          // remove old file
          srcResource.delete();
          srcFile.delete();
          logger.info("[Migration] remove old file {}.", srcFile.getAbsolutePath());
        }
        srcTsFileResource.setMigrating(false);
        migrationLogger.endMigrateTsFile();
      }
      migrationLogger.endMigration();
      migrationLogger.close();
      File logFile = migrationLogger.getLogFile();
      logFile.delete();
    } catch (IOException e) {
      logger.error("[Migration] Error occurred in logging migration info.", e);
      if (migrationLogger != null) {
        try {
          migrationLogger.close();
        } catch (IOException ioException) {
          logger.error(
              "[Migration] Fail to closing migration log {}.",
              migrationLogger.getLogFile().getAbsolutePath());
        }
      }
    } finally {
      logger.info(
          "[Migration] migration end, consumption: {} ms",
          System.currentTimeMillis() - startTimeMillis);
      for (TsFileResource tsFileResource : srcTsFileResources) {
        tsFileResource.setMigrating(false);
      }
    }
  }

  @Override
  public void run() {
    migrate();
  }
}
