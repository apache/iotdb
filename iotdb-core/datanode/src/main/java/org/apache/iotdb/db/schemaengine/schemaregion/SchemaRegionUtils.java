/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.schemaengine.schemaregion;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.i18n.DataNodeSchemaMessages;

import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;
import java.util.function.LongConsumer;

public class SchemaRegionUtils {

  private SchemaRegionUtils() {
    // not allowed construction
  }

  public static void deleteSchemaRegionFolder(String schemaRegionDirPath, Logger logger)
      throws MetadataException {
    deleteSchemaRegionFolder(schemaRegionDirPath, logger, null);
  }

  public static void deleteSchemaRegionFolder(
      String schemaRegionDirPath, Logger logger, LongConsumer deleteRateLimiter)
      throws MetadataException {
    File schemaRegionDir = SystemFileFactory.INSTANCE.getFile(schemaRegionDirPath);
    File[] sgFiles = schemaRegionDir.listFiles();
    if (sgFiles == null) {
      throw new MetadataException(
          String.format(
              DataNodeSchemaMessages.CANNOT_GET_FILES_IN_SCHEMA_REGION_DIR, schemaRegionDirPath));
    }
    for (File file : sgFiles) {
      try {
        if (deleteRateLimiter != null) {
          deleteRateLimiter.accept(FileUtils.estimateFileOrDirectoryRemoveCost(file));
        }
        Files.delete(file.toPath());
        logger.info(DataNodeSchemaMessages.DELETE_SCHEMA_REGION_FILE, file.getAbsolutePath());
      } catch (IOException e) {
        logger.warn(
            DataNodeSchemaMessages.DELETE_SCHEMA_REGION_FILE_FAILED, file.getAbsolutePath());
        throw new MetadataException(
            String.format(
                DataNodeSchemaMessages.FAILED_TO_DELETE_SCHEMA_REGION_FILE,
                file.getAbsolutePath()));
      }
    }

    try {
      if (deleteRateLimiter != null) {
        deleteRateLimiter.accept(FileUtils.estimateFileOrDirectoryRemoveCost(schemaRegionDir));
      }
      Files.delete(schemaRegionDir.toPath());
      logger.info(
          DataNodeSchemaMessages.DELETE_SCHEMA_REGION_FOLDER, schemaRegionDir.getAbsolutePath());
    } catch (IOException e) {
      logger.warn(
          DataNodeSchemaMessages.DELETE_SCHEMA_REGION_FOLDER_FAILED,
          schemaRegionDir.getAbsolutePath());
      throw new MetadataException(
          String.format(
              DataNodeSchemaMessages.FAILED_TO_DELETE_SCHEMA_REGION_FOLDER,
              schemaRegionDir.getAbsolutePath()));
    }
    final File storageGroupDir = schemaRegionDir.getParentFile();
    if (Objects.requireNonNull(storageGroupDir.listFiles()).length == 0) {
      try {
        Files.delete(storageGroupDir.toPath());
        logger.info(
            DataNodeSchemaMessages.DELETE_DATABASE_SCHEMA_FOLDER,
            storageGroupDir.getAbsolutePath());
      } catch (IOException e) {
        logger.warn(
            DataNodeSchemaMessages.DELETE_DATABASE_SCHEMA_FOLDER_FAILED,
            storageGroupDir.getAbsolutePath());
      }
    }
  }
}
