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

import com.timecho.iotdb.i18n.TimechoServerMessages;
import com.timecho.iotdb.metrics.MigrationMetrics;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;

/** Independent OBJECT {@code .bin} migration task (not bound to a TsFile migration). */
public class ObjectMigrationTask implements Runnable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ObjectMigrationTask.class);
  private static final MigrationMetrics MIGRATION_METRICS = MigrationMetrics.getInstance();

  private final MigrationCause cause;
  private final File srcFile;
  private final String relativePath;
  private final int srcTier;
  private final int destTier;
  private final Set<String> inFlightKeys;

  public ObjectMigrationTask(
      MigrationCause cause,
      File srcFile,
      String relativePath,
      int srcTier,
      int destTier,
      Set<String> inFlightKeys) {
    this.cause = cause;
    this.srcFile = srcFile;
    this.relativePath = relativePath;
    this.srcTier = srcTier;
    this.destTier = destTier;
    this.inFlightKeys = inFlightKeys;
  }

  @Override
  public void run() {
    long taskStartTime = System.nanoTime();
    String key = srcFile.getAbsolutePath();
    try {
      if (!srcFile.exists()) {
        return;
      }
      if (ObjectFileMigrationUtils.hasTempSibling(srcFile)) {
        LOGGER.info(TimechoServerMessages.SKIP_OBJECT_FILE_BECAUSE_TEMP_SIBLING_EXISTS, srcFile);
        return;
      }
      MIGRATION_METRICS.recordMigrationCause(cause);
      File destFile = ObjectFileMigrationUtils.migrateObjectFile(srcFile, relativePath, destTier);
      boolean toLocal = FSUtils.isLocal(destFile.getAbsolutePath());
      MIGRATION_METRICS.recordMigrationFileSize(destTier, toLocal, destFile.length());
      long taskTimeCost = System.nanoTime() - taskStartTime;
      MIGRATION_METRICS.recordMigrationTotalTime(destTier, toLocal, taskTimeCost);
      LOGGER.info(
          TimechoServerMessages.SUCCESSFULLY_MIGRATE_OBJECT_FILE,
          srcFile,
          destFile,
          cause,
          taskTimeCost);
    } catch (Throwable t) {
      LOGGER.warn(TimechoServerMessages.FAIL_TO_MIGRATE_OBJECT_FILE, srcFile, relativePath, t);
    } finally {
      inFlightKeys.remove(key);
    }
  }

  public int getSrcTier() {
    return srcTier;
  }
}
