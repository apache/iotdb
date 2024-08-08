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

package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;

import org.slf4j.Logger;

import java.io.File;
import java.util.Objects;

public class SchemaRegionUtils {

  public static void deleteSchemaRegionFolder(String schemaRegionDirPath, Logger logger)
      throws MetadataException {
    File schemaRegionDir = SystemFileFactory.INSTANCE.getFile(schemaRegionDirPath);
    File[] sgFiles = schemaRegionDir.listFiles();
    if (sgFiles == null) {
      throw new MetadataException(
          String.format("Can't get files in schema region dir %s", schemaRegionDirPath));
    }
    for (File file : sgFiles) {
      if (file.delete()) {
        logger.info("delete schema region file {}", file.getAbsolutePath());
      } else {
        logger.info("delete schema region file {} failed.", file.getAbsolutePath());
        throw new MetadataException(
            String.format("Failed to delete schema region file %s", file.getAbsolutePath()));
      }
    }

    if (schemaRegionDir.delete()) {
      logger.info("delete schema region folder {}", schemaRegionDir.getAbsolutePath());
    } else {
      logger.info("delete schema region folder {} failed.", schemaRegionDir.getAbsolutePath());
      throw new MetadataException(
          String.format(
              "Failed to delete schema region folder %s", schemaRegionDir.getAbsolutePath()));
    }
    final File storageGroupDir = schemaRegionDir.getParentFile();
    if (Objects.requireNonNull(storageGroupDir.listFiles()).length == 0) {
      storageGroupDir.delete();
    }
  }
}
